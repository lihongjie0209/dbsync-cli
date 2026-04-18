package io.dbsync.integration;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.engine.DebeziumEngineManager;
import io.dbsync.progress.SyncProgressRegistry;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end integration test: MySQL 8.0 → PostgreSQL 16.
 * Covers schema sync, initial snapshot, and CDC insert/update/delete.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("MySQL 8.0 → PostgreSQL 16")
class MysqlToPgSyncIT extends AbstractSyncIT {

    @Container
    static final MySQLContainer<?> source = mysqlSource("mysql:8.0");

    @Container
    static final PostgreSQLContainer<?> target = pgTarget("postgres:16-alpine");

    @BeforeAll
    void setup() throws Exception {
        grantMysqlReplicationPrivileges(source);
        try (Connection c = DriverManager.getConnection(
                source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
            createMysqlStaffTable(c);
            seedStaff(c);
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @DisplayName("Schema sync + snapshot + CDC insert/update/delete")
    void fullSyncCycle() throws Exception {
        Path workDir = Files.createTempDirectory("dbsync-mysql2pg-");
        DatabaseConfig srcCfg = dbConfig(source);
        DatabaseConfig tgtCfg = dbConfig(target);
        SyncConfig config    = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

        // ── Step 1: schema sync ───────────────────────────────────────────
        buildApplier().sync(config);

        // ── Step 2: snapshot ──────────────────────────────────────────────
        SyncProgressRegistry registry = new SyncProgressRegistry();
        DebeziumEngineManager engine   = new DebeziumEngineManager();
        try {
            engine.start(config, registry);
            waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

            try (Connection tgt = DriverManager.getConnection(
                    target.getJdbcUrl(), target.getUsername(), target.getPassword())) {

                assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows");

                // ── Step 3: CDC insert ────────────────────────────────────
                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "INSERT INTO staff (name, dept, score) VALUES ('Dave','HR',70)");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcInserts().get() >= 1, CDC_TIMEOUT_SEC);
                assertRowCount(tgt, STAFF_TABLE, 4, "After CDC insert: 4 rows");

                // ── Step 4: CDC update ────────────────────────────────────
                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "UPDATE staff SET score = 95 WHERE name = 'Alice'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcUpdates().get() >= 1, CDC_TIMEOUT_SEC);
                assertEquals(95L, queryLong(tgt,
                        "SELECT score FROM staff WHERE name = 'Alice'"),
                        "Alice's score updated to 95");

                // ── Step 5: CDC delete ────────────────────────────────────
                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute("DELETE FROM staff WHERE name = 'Bob'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcDeletes().get() >= 1, CDC_TIMEOUT_SEC);
                assertRowCount(tgt, STAFF_TABLE, 3, "After CDC delete: 3 rows");
            }
        } finally {
            engine.stop();
        }
    }
}
