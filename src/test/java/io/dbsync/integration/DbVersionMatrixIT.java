package io.dbsync.integration;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.engine.DebeziumEngineManager;
import io.dbsync.progress.SyncProgressRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Version-matrix parameterized tests.
 * Each row = one source image → one target image combination.
 * Containers are started and stopped per test invocation to ensure isolation.
 * Only snapshot (no CDC) to keep execution time reasonable.
 */
@DisplayName("DB version matrix (snapshot only)")
class DbVersionMatrixIT extends AbstractSyncIT {

    /**
     * Columns: sourceType, sourceImage, targetType, targetImage
     *
     * Note: MySQL 8.4 removed SHOW MASTER STATUS which Debezium 2.7.x still calls in
     * MySqlSnapshotChangeEventSource. MySQL 8.4 source support requires Debezium 2.8+.
     * The matrix covers MySQL 8.0, MariaDB 10.11/11.4, and PostgreSQL 14/15/16.
     */
    @ParameterizedTest(name = "{0}:{1} → {2}:{3}")
    @CsvSource({
            // MySQL 8.0 source → PG target (multiple PG versions)
            "mysql, mysql:8.0,  postgresql, postgres:14-alpine",
            "mysql, mysql:8.0,  postgresql, postgres:16-alpine",
            // PG source → PG target (three PG versions as source)
            "postgresql, postgres:14-alpine, postgresql, postgres:16-alpine",
            "postgresql, postgres:15-alpine, postgresql, postgres:16-alpine",
            "postgresql, postgres:16-alpine, postgresql, postgres:16-alpine",
            // PG source → MySQL target
            "postgresql, postgres:16-alpine, mysql, mysql:8.0",
            "postgresql, postgres:14-alpine, mysql, mysql:8.0",
            // MySQL source → MySQL target
            "mysql, mysql:8.0,  mysql, mysql:8.0",
            // MariaDB source → PG target
            "mariadb, mariadb:10.11, postgresql, postgres:16-alpine",
            "mariadb, mariadb:11.4,  postgresql, postgres:16-alpine",
            // MariaDB source → MySQL target
            "mariadb, mariadb:10.11, mysql, mysql:8.0",
            // MySQL source → MariaDB target
            "mysql,   mysql:8.0,    mariadb, mariadb:10.11",
            // PG source → MariaDB target
            "postgresql, postgres:16-alpine, mariadb, mariadb:10.11",
    })
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void snapshotOnly(String srcType, String srcImage, String tgtType, String tgtImage)
            throws Exception {
        srcImage = srcImage.trim();
        tgtImage = tgtImage.trim();

        try (JdbcDatabaseContainer<?> source = startSource(srcType, srcImage);
             JdbcDatabaseContainer<?> target = startTarget(tgtType, tgtImage)) {

            // ── Setup source ──────────────────────────────────────────────
            if ("mysql".equals(srcType)) {
                grantMysqlReplicationPrivileges((MySQLContainer<?>) source);
                try (Connection c = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    createMysqlStaffTable(c);
                    seedStaff(c);
                }
            } else if ("mariadb".equals(srcType)) {
                grantMariadbReplicationPrivileges((MariaDBContainer<?>) source);
                String mariaUrl = source.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
                try (Connection c = DriverManager.getConnection(
                        mariaUrl, source.getUsername(), source.getPassword())) {
                    createMysqlStaffTable(c);
                    seedStaff(c);
                }
            } else {
                try (Connection c = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    createPgStaffTable(c);
                    seedStaff(c);
                }
            }

            // ── Config & schema sync ──────────────────────────────────────
            DatabaseConfig srcCfg = buildDbConfig(srcType, source);
            DatabaseConfig tgtCfg = buildDbConfig(tgtType, target);

            Path workDir  = Files.createTempDirectory("dbsync-matrix-");
            SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

            buildApplier().sync(cfg);

            // ── Snapshot ──────────────────────────────────────────────────
            SyncProgressRegistry registry = new SyncProgressRegistry();
            DebeziumEngineManager engine   = new DebeziumEngineManager();
            try {
                engine.start(cfg, registry);
                waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(
                        "mariadb".equals(tgtType)
                            ? target.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:")
                            : target.getJdbcUrl(),
                        target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3,
                            "3 rows snapshotted from " + srcImage + " to " + tgtImage);
                }
            } finally {
                engine.stop();
            }
        }
    }

    // ── Kingbase target rows ──────────────────────────────────────────────

    private static final String KB_IMAGE = "kingbase_v009r001c010b0004_single_x86:v1";

    /**
     * Parameterized snapshot test with KingbaseES as the write target.
     * GenericContainer is not a JdbcDatabaseContainer, so we handle it separately.
     * Columns: sourceType, sourceImage, kbDatabase
     *
     * <p>Skipped automatically if the Kingbase image is not available locally.
     */
    @ParameterizedTest(name = "{0}:{1} → KingbaseES (db={2})")
    @CsvSource({
            "mysql, mysql:8.0, test",
    })
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void snapshotToKingbase(String srcType, String srcImage, String kbDb)
            throws Exception {
        assumeTrue(isDockerImageAvailable(KB_IMAGE),
                "Skipping Kingbase target test: image not available locally: " + KB_IMAGE);

        srcImage = srcImage.trim();
        kbDb     = kbDb.trim();

        try (JdbcDatabaseContainer<?> source = startSource(srcType, srcImage);
             GenericContainer<?> kb = kingbaseTarget(KB_IMAGE)) {

            kb.start();

            // ── Setup source ──────────────────────────────────────────────
            if ("mysql".equals(srcType)) {
                grantMysqlReplicationPrivileges((MySQLContainer<?>) source);
                try (Connection c = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    createMysqlStaffTable(c);
                    seedStaff(c);
                }
            } else if ("mariadb".equals(srcType)) {
                grantMariadbReplicationPrivileges((MariaDBContainer<?>) source);
                String mariaUrl = source.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
                try (Connection c = DriverManager.getConnection(
                        mariaUrl, source.getUsername(), source.getPassword())) {
                    createMysqlStaffTable(c);
                    seedStaff(c);
                }
            } else {
                try (Connection c = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    createPgStaffTable(c);
                    seedStaff(c);
                }
            }

            DatabaseConfig srcCfg = buildDbConfig(srcType, source);
            DatabaseConfig tgtCfg = dbConfigKingbase(kb, kbDb);
            Path workDir  = Files.createTempDirectory("dbsync-matrix-kb-");
            SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

            buildApplier().sync(cfg);

            SyncProgressRegistry registry = new SyncProgressRegistry();
            DebeziumEngineManager engine   = new DebeziumEngineManager();
            try {
                engine.start(cfg, registry);
                waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(
                        tgtCfg.jdbcUrl(), tgtCfg.getUsername(), tgtCfg.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3,
                            "3 rows snapshotted from " + srcImage + " to KingbaseES");
                }
            } finally {
                engine.stop();
                kb.stop();
            }
        }
    }

    // ── Kingbase source rows ──────────────────────────────────────────────

    /**
     * Parameterized snapshot test with KingbaseES as the CDC source.
     * Uses Debezium PostgreSQL connector + decoderbufs plugin.
     * Columns: targetType, targetImage
     *
     * <p>Skipped automatically if the Kingbase image is not available locally.
     */
    @ParameterizedTest(name = "KingbaseES → {0}:{1}")
    @CsvSource({
            "mysql,      mysql:8.0",
            "postgresql, postgres:16-alpine",
    })
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void snapshotFromKingbase(String tgtType, String tgtImage)
            throws Exception {
        assumeTrue(isDockerImageAvailable(KB_IMAGE),
                "Skipping Kingbase source test: image not available locally: " + KB_IMAGE);

        tgtImage = tgtImage.trim();

        GenericContainer<?> kb = kingbaseSource(KB_IMAGE);
        kb.start();
        enableKingbaseLogicalReplication(kb);

        try (JdbcDatabaseContainer<?> target = startTarget(tgtType, tgtImage)) {

            // Seed source data in Kingbase
            DatabaseConfig kbCfg = dbConfigKingbase(kb, "test");
            try (Connection c = DriverManager.getConnection(
                    kbCfg.jdbcUrl(), kbCfg.getUsername(), kbCfg.getPassword())) {
                createPgStaffTable(c);
                seedStaff(c);
            }

            DatabaseConfig tgtCfg = buildDbConfig(tgtType, target);
            Path workDir  = Files.createTempDirectory("dbsync-matrix-kbsrc-");
            SyncConfig cfg = syncConfig(kbCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

            buildApplier().sync(cfg);

            SyncProgressRegistry registry = new SyncProgressRegistry();
            DebeziumEngineManager engine   = new DebeziumEngineManager();
            try {
                engine.start(cfg, registry);
                waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

                String connUrl = "mariadb".equals(tgtType)
                        ? target.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:")
                        : target.getJdbcUrl();
                try (Connection tgt = DriverManager.getConnection(
                        connUrl, target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3,
                            "3 rows snapshotted from KingbaseES to " + tgtImage);
                }
            } finally {
                engine.stop();
            }
        } finally {
            kb.stop();
        }
    }

    // ── Container factory helpers ─────────────────────────────────────────

    @SuppressWarnings("resource")  // try-with-resources in caller
    private JdbcDatabaseContainer<?> startSource(String type, String image) {
        JdbcDatabaseContainer<?> c = switch (type) {
            case "mysql"      -> mysqlSource(image);
            case "mariadb"    -> mariadbSource(image);
            default           -> pgSource(image);
        };
        c.start();
        return c;
    }

    @SuppressWarnings("resource")
    private JdbcDatabaseContainer<?> startTarget(String type, String image) {
        JdbcDatabaseContainer<?> c = switch (type) {
            case "mysql"   -> mysqlTarget(image);
            case "mariadb" -> mariadbTarget(image);
            default        -> pgTarget(image);
        };
        c.start();
        return c;
    }

    private DatabaseConfig buildDbConfig(String type, JdbcDatabaseContainer<?> c) {
        return switch (type) {
            case "mysql"      -> dbConfig((MySQLContainer<?>) c);
            case "mariadb"    -> dbConfig((MariaDBContainer<?>) c);
            default           -> dbConfig((PostgreSQLContainer<?>) c);
        };
    }
}
