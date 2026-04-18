package io.dbsync.integration;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.engine.DebeziumEngineManager;
import io.dbsync.progress.SyncProgressRegistry;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end integration test for MySQL → KingbaseES sync.
 *
 * <p>Uses the local Docker image {@code kingbase_v009r001c010b0004_single_x86:v1}
 * with {@code DB_MODE=pg} (PostgreSQL-compatibility mode).  The Kingbase8 JDBC
 * driver ({@code kingbase8-9.0.0.jar}) must be present in {@code libs/}.
 *
 * <p>If the Kingbase image is not available locally, all tests in this class
 * are automatically skipped (no Docker pull attempted).
 */
@DisplayName("MySQL → KingbaseES sync")
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MysqlToKingbaseSyncIT extends AbstractSyncIT {

    private static final String KB_IMAGE = "kingbase_v009r001c010b0004_single_x86:v1";
    private static final String KB_DB    = "test";

    @Container
    static final MySQLContainer<?> source = mysqlSource("mysql:8.0");

    // Managed manually so we can skip before Docker tries to pull the image
    private GenericContainer<?> target;

    // ── Setup ─────────────────────────────────────────────────────────────

    @BeforeAll
    void setup() throws Exception {
        assumeTrue(isDockerImageAvailable(KB_IMAGE),
                "Skipping MySQL→KingbaseES tests: image not available locally: " + KB_IMAGE);

        target = kingbaseTarget(KB_IMAGE);
        target.start();

        grantMysqlReplicationPrivileges(source);
        try (Connection c = DriverManager.getConnection(
                source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
            createMysqlStaffTable(c);
            seedStaff(c);
        }
    }

    @AfterAll
    void teardown() {
        if (target != null) {
            target.stop();
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    @DisplayName("Schema sync + snapshot + CDC insert/update/delete → KingbaseES")
    void fullSyncCycle() throws Exception {
        DatabaseConfig srcCfg = dbConfig(source);
        DatabaseConfig tgtCfg = dbConfigKingbase(target, KB_DB);
        Path workDir = Files.createTempDirectory("dbsync-mysql-kingbase-");
        SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

        buildApplier().sync(cfg);

        SyncProgressRegistry registry = new SyncProgressRegistry();
        DebeziumEngineManager engine = new DebeziumEngineManager();
        try {
            engine.start(cfg, registry);

            // ── Snapshot ──────────────────────────────────────────────────
            waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

            try (Connection tgt = kbConn()) {
                assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows from MySQL → KingbaseES");
            }

            // ── CDC INSERT ────────────────────────────────────────────────
            try (Connection src = DriverManager.getConnection(
                    source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                src.createStatement().execute(
                        "INSERT INTO staff (name, dept, score) VALUES ('Dave', 'Sales', 70)");
            }
            waitFor(() -> registry.get(STAFF_TABLE).getCdcInserts().get() >= 1, CDC_TIMEOUT_SEC);

            // ── CDC UPDATE ────────────────────────────────────────────────
            try (Connection src = DriverManager.getConnection(
                    source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                src.createStatement().execute(
                        "UPDATE staff SET score = 99 WHERE name = 'Alice'");
            }
            waitFor(() -> registry.get(STAFF_TABLE).getCdcUpdates().get() >= 1, CDC_TIMEOUT_SEC);

            // ── CDC DELETE ────────────────────────────────────────────────
            try (Connection src = DriverManager.getConnection(
                    source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                src.createStatement().execute("DELETE FROM staff WHERE name = 'Dave'");
            }
            waitFor(() -> registry.get(STAFF_TABLE).getCdcDeletes().get() >= 1, CDC_TIMEOUT_SEC);

            try (Connection tgt = kbConn()) {
                assertRowCount(tgt, STAFF_TABLE, 3, "After CDC: 3 rows remain in KingbaseES");
                long aliceScore = queryLong(tgt, "SELECT score FROM staff WHERE name='Alice'");
                Assertions.assertEquals(99, aliceScore, "Alice's score updated via CDC to KingbaseES");
            }
        } finally {
            engine.stop();
        }
    }

    // ── JDBC helper ───────────────────────────────────────────────────────

    private Connection kbConn() throws Exception {
        DatabaseConfig cfg = dbConfigKingbase(target, KB_DB);
        return DriverManager.getConnection(cfg.jdbcUrl(), cfg.getUsername(), cfg.getPassword());
    }
}
