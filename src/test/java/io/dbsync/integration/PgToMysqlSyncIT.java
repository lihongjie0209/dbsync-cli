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
 * End-to-end integration test: PostgreSQL 16 → MySQL 8.0.
 * Exercises the PG→MySQL reverse direction including type translation.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("PostgreSQL 16 → MySQL 8.0")
class PgToMysqlSyncIT extends AbstractSyncIT {

    @Container
    static final PostgreSQLContainer<?> source = pgSource("postgres:16-alpine");

    @Container
    static final MySQLContainer<?> target = mysqlTarget("mysql:8.0");

    @BeforeAll
    void setup() throws Exception {
        try (Connection c = DriverManager.getConnection(
                source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
            createPgStaffTable(c);
            seedStaff(c);
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @DisplayName("Schema sync + snapshot + CDC insert/update/delete")
    void fullSyncCycle() throws Exception {
        Path workDir = Files.createTempDirectory("dbsync-pg2mysql-");
        DatabaseConfig srcCfg = dbConfig(source);
        DatabaseConfig tgtCfg = dbConfig(target);
        SyncConfig config     = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

        buildApplier().sync(config);

        SyncProgressRegistry registry = new SyncProgressRegistry();
        DebeziumEngineManager engine   = new DebeziumEngineManager();
        try {
            engine.start(config, registry);
            waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

            try (Connection tgt = DriverManager.getConnection(
                    target.getJdbcUrl(), target.getUsername(), target.getPassword())) {

                assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows");

                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "INSERT INTO staff (name, dept, score) VALUES ('Dave','HR',70)");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcInserts().get() >= 1, CDC_TIMEOUT_SEC);
                assertRowCount(tgt, STAFF_TABLE, 4, "After CDC insert: 4 rows");

                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "UPDATE staff SET score = 95 WHERE name = 'Alice'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcUpdates().get() >= 1, CDC_TIMEOUT_SEC);
                assertEquals(95L, queryLong(tgt,
                        "SELECT score FROM staff WHERE name = 'Alice'"),
                        "Alice's score updated to 95");

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
