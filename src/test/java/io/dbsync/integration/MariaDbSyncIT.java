package io.dbsync.integration;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.engine.DebeziumEngineManager;
import io.dbsync.progress.SyncProgressRegistry;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MariaDBContainer;
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

/**
 * End-to-end integration tests for MariaDB as source or target.
 *
 * <p>Covers all four MariaDB combinations:
 * <ul>
 *   <li>MariaDB 10.11 → PostgreSQL 16</li>
 *   <li>MariaDB 10.11 → MySQL 8.0</li>
 *   <li>MySQL 8.0 → MariaDB 10.11</li>
 *   <li>PostgreSQL 16 → MariaDB 10.11</li>
 * </ul>
 */
@DisplayName("MariaDB sync tests")
class MariaDbSyncIT {

    // ── MariaDB → PostgreSQL ──────────────────────────────────────────────

    @Testcontainers
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @DisplayName("MariaDB 10.11 → PostgreSQL 16")
    class MariaDbToPg extends AbstractSyncIT {

        @Container
        static final MariaDBContainer<?> source = mariadbSource("mariadb:10.11");

        @Container
        static final PostgreSQLContainer<?> target = pgTarget("postgres:16-alpine");

        @BeforeAll
        void setup() throws Exception {
            grantMariadbReplicationPrivileges(source);
            String mariaUrl = source.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
            try (Connection c = DriverManager.getConnection(
                    mariaUrl, source.getUsername(), source.getPassword())) {
                createMysqlStaffTable(c);
                seedStaff(c);
            }
        }

        @Test
        @Timeout(value = 4, unit = TimeUnit.MINUTES)
        @DisplayName("Schema sync + snapshot + CDC insert/update/delete")
        void fullSyncCycle() throws Exception {
            DatabaseConfig srcCfg = dbConfig(source);
            DatabaseConfig tgtCfg = dbConfig(target);
            Path workDir = Files.createTempDirectory("dbsync-mariadb-pg-");
            SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

            buildApplier().sync(cfg);

            SyncProgressRegistry registry = new SyncProgressRegistry();
            DebeziumEngineManager engine = new DebeziumEngineManager();
            try {
                engine.start(cfg, registry);
                waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(
                        target.getJdbcUrl(), target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows from MariaDB → PG");
                }

                // CDC INSERT
                String mariaUrl = source.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
                try (Connection src = DriverManager.getConnection(mariaUrl,
                        source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "INSERT INTO staff (name, dept, score) VALUES ('Dave', 'Sales', 70)");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcInserts().get() >= 1, CDC_TIMEOUT_SEC);

                // CDC UPDATE
                try (Connection src = DriverManager.getConnection(mariaUrl,
                        source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "UPDATE staff SET score = 99 WHERE name = 'Alice'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcUpdates().get() >= 1, CDC_TIMEOUT_SEC);

                // CDC DELETE
                try (Connection src = DriverManager.getConnection(mariaUrl,
                        source.getUsername(), source.getPassword())) {
                    src.createStatement().execute("DELETE FROM staff WHERE name = 'Dave'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcDeletes().get() >= 1, CDC_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(
                        target.getJdbcUrl(), target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "After CDC: 3 rows remain");
                    long aliceScore = queryLong(tgt,
                            "SELECT score FROM staff WHERE name='Alice'");
                    Assertions.assertEquals(99, aliceScore, "Alice's score updated via CDC");
                }
            } finally {
                engine.stop();
            }
        }
    }

    // ── MariaDB → MySQL ───────────────────────────────────────────────────

    @Testcontainers
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @DisplayName("MariaDB 10.11 → MySQL 8.0")
    class MariaDbToMysql extends AbstractSyncIT {

        @Container
        static final MariaDBContainer<?> source = mariadbSource("mariadb:10.11");

        @Container
        static final MySQLContainer<?> target = mysqlTarget("mysql:8.0");

        @BeforeAll
        void setup() throws Exception {
            grantMariadbReplicationPrivileges(source);
            String mariaUrl = source.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
            try (Connection c = DriverManager.getConnection(
                    mariaUrl, source.getUsername(), source.getPassword())) {
                createMysqlStaffTable(c);
                seedStaff(c);
            }
        }

        @Test
        @Timeout(value = 4, unit = TimeUnit.MINUTES)
        @DisplayName("Schema sync + snapshot + CDC insert/delete")
        void fullSyncCycle() throws Exception {
            DatabaseConfig srcCfg = dbConfig(source);
            DatabaseConfig tgtCfg = dbConfig(target);
            Path workDir = Files.createTempDirectory("dbsync-mariadb-mysql-");
            SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

            buildApplier().sync(cfg);

            SyncProgressRegistry registry = new SyncProgressRegistry();
            DebeziumEngineManager engine = new DebeziumEngineManager();
            try {
                engine.start(cfg, registry);
                waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(
                        target.getJdbcUrl(), target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows from MariaDB → MySQL");
                }

                // CDC INSERT
                String mariaUrl = source.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
                try (Connection src = DriverManager.getConnection(mariaUrl,
                        source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "INSERT INTO staff (name, dept, score) VALUES ('Eve', 'Finance', 80)");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcInserts().get() >= 1, CDC_TIMEOUT_SEC);

                // CDC DELETE
                try (Connection src = DriverManager.getConnection(mariaUrl,
                        source.getUsername(), source.getPassword())) {
                    src.createStatement().execute("DELETE FROM staff WHERE name = 'Eve'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcDeletes().get() >= 1, CDC_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(
                        target.getJdbcUrl(), target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "After CDC: 3 rows remain");
                }
            } finally {
                engine.stop();
            }
        }
    }

    // ── MySQL → MariaDB ───────────────────────────────────────────────────

    @Testcontainers
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @DisplayName("MySQL 8.0 → MariaDB 10.11")
    class MysqlToMariaDb extends AbstractSyncIT {

        @Container
        static final MySQLContainer<?> source = mysqlSource("mysql:8.0");

        @Container
        static final MariaDBContainer<?> target = mariadbTarget("mariadb:10.11");

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
        @Timeout(value = 4, unit = TimeUnit.MINUTES)
        @DisplayName("Schema sync + snapshot + CDC insert/delete")
        void fullSyncCycle() throws Exception {
            DatabaseConfig srcCfg = dbConfig(source);
            DatabaseConfig tgtCfg = dbConfig(target);
            Path workDir = Files.createTempDirectory("dbsync-mysql-mariadb-");
            SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

            buildApplier().sync(cfg);

            SyncProgressRegistry registry = new SyncProgressRegistry();
            DebeziumEngineManager engine = new DebeziumEngineManager();
            String mariaUrl = target.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
            try {
                engine.start(cfg, registry);
                waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(mariaUrl,
                        target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows from MySQL → MariaDB");
                }

                // CDC INSERT
                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "INSERT INTO staff (name, dept, score) VALUES ('Frank', 'HR', 65)");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcInserts().get() >= 1, CDC_TIMEOUT_SEC);

                // CDC DELETE
                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute("DELETE FROM staff WHERE name = 'Frank'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcDeletes().get() >= 1, CDC_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(mariaUrl,
                        target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "After CDC: 3 rows remain");
                }
            } finally {
                engine.stop();
            }
        }
    }

    // ── PostgreSQL → MariaDB ──────────────────────────────────────────────

    @Testcontainers
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @DisplayName("PostgreSQL 16 → MariaDB 10.11")
    class PgToMariaDb extends AbstractSyncIT {

        @Container
        static final PostgreSQLContainer<?> source = pgSource("postgres:16-alpine");

        @Container
        static final MariaDBContainer<?> target = mariadbTarget("mariadb:10.11");

        @BeforeAll
        void setup() throws Exception {
            try (Connection c = DriverManager.getConnection(
                    source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                createPgStaffTable(c);
                seedStaff(c);
            }
        }

        @Test
        @Timeout(value = 4, unit = TimeUnit.MINUTES)
        @DisplayName("Schema sync + snapshot + CDC insert/delete")
        void fullSyncCycle() throws Exception {
            DatabaseConfig srcCfg = dbConfig(source);
            DatabaseConfig tgtCfg = dbConfig(target);
            Path workDir = Files.createTempDirectory("dbsync-pg-mariadb-");
            SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

            buildApplier().sync(cfg);

            SyncProgressRegistry registry = new SyncProgressRegistry();
            DebeziumEngineManager engine = new DebeziumEngineManager();
            String mariaUrl = target.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
            try {
                engine.start(cfg, registry);
                waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(mariaUrl,
                        target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows from PG → MariaDB");
                }

                // CDC INSERT
                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute(
                            "INSERT INTO staff (name, dept, score) VALUES ('Grace', 'QA', 88)");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcInserts().get() >= 1, CDC_TIMEOUT_SEC);

                // CDC DELETE
                try (Connection src = DriverManager.getConnection(
                        source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                    src.createStatement().execute("DELETE FROM staff WHERE name = 'Grace'");
                }
                waitFor(() -> registry.get(STAFF_TABLE).getCdcDeletes().get() >= 1, CDC_TIMEOUT_SEC);

                try (Connection tgt = DriverManager.getConnection(mariaUrl,
                        target.getUsername(), target.getPassword())) {
                    assertRowCount(tgt, STAFF_TABLE, 3, "After CDC: 3 rows remain");
                }
            } finally {
                engine.stop();
            }
        }
    }
}
