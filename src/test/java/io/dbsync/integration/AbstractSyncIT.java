package io.dbsync.integration;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.config.SyncOptions;
import io.dbsync.progress.SyncProgressRegistry;
import io.dbsync.schema.DdlTranslator;
import io.dbsync.schema.SchemaApplier;
import io.dbsync.schema.SchemaReader;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Shared helpers for all source→target end-to-end sync integration tests.
 *
 * <p>Each subclass provides a source container, a target container, and one
 * or more {@code @Test} methods that exercise the full pipeline:
 * schema sync → Debezium snapshot → CDC insert/update/delete.
 */
public abstract class AbstractSyncIT {

    protected static final int SNAPSHOT_TIMEOUT_SEC = 90;
    protected static final int CDC_TIMEOUT_SEC      = 40;

    /** Standard test table name used across all combos. */
    protected static final String STAFF_TABLE = "staff";

    // ── Docker image availability ─────────────────────────────────────────

    /**
     * Returns {@code true} if the given Docker image is available in the local daemon.
     * Use with {@code assumeTrue(isDockerImageAvailable(image), "...")} to skip tests
     * that depend on images not present on the current machine.
     */
    protected static boolean isDockerImageAvailable(String image) {
        try {
            DockerClientFactory.lazyClient().inspectImageCmd(image).exec();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ── Container factories ───────────────────────────────────────────────

    /**
     * MySQL source container with binlog enabled in ROW format.
     * The MySQL Docker image entrypoint prepends "mysqld" when the first argument starts with "--",
     * so passing only the extra flags here is safe and correct.
     * GTID mode is enabled for two reasons:
     *   1) It is best-practice for MySQL CDC (allows connector restarts without position drift).
     *   2) MySQL 8.4 removed the legacy SHOW MASTER STATUS command; with GTID enabled Debezium
     *      uses SELECT @@global.gtid_executed instead, which works on all supported versions.
     */
    protected static MySQLContainer<?> mysqlSource(String image) {
        return new MySQLContainer<>(image)
                .withCommand(
                        "--log-bin=mysql-bin",
                        "--binlog-format=ROW",
                        "--binlog-row-image=FULL",
                        "--server-id=1",
                        "--gtid-mode=ON",
                        "--enforce-gtid-consistency=ON");
    }

    /** PostgreSQL source container with logical replication enabled. */
    protected static PostgreSQLContainer<?> pgSource(String image) {
        return new PostgreSQLContainer<>(image)
                .withCommand("postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_replication_slots=10",
                        "-c", "max_wal_senders=10");
    }

    /**
     * MariaDB source container with binary logging enabled.
     * Uses the MariaDB JDBC driver and the Debezium MariaDB connector.
     */
    protected static MariaDBContainer<?> mariadbSource(String image) {
        return new MariaDBContainer<>(image)
                .withCommand(
                        "--log-bin=mariadb-bin",
                        "--binlog-format=ROW",
                        "--binlog-row-image=FULL",
                        "--server-id=1");
    }

    protected static MariaDBContainer<?> mariadbTarget(String image) {
        return new MariaDBContainer<>(image);
    }

    protected static MySQLContainer<?> mysqlTarget(String image) {
        return new MySQLContainer<>(image);
    }

    protected static PostgreSQLContainer<?> pgTarget(String image) {
        return new PostgreSQLContainer<>(image);
    }

    /**
     * KingbaseES source container with logical replication enabled.
     * Uses decoderbufs plugin (pgoutput is not available in KingbaseES).
     * The sys_hba.conf inside the image defaults to trust/scram auth — no extra config needed.
     *
     * <p>After starting this container, call {@link #enableKingbaseLogicalReplication(GenericContainer)}
     * to set {@code wal_level=logical} and restart the server before creating a replication slot.
     */
    @SuppressWarnings("resource")
    protected static GenericContainer<?> kingbaseSource(String image) {
        return new GenericContainer<>(image)
                .withEnv("DB_USER",     "system")
                .withEnv("DB_PASSWORD", "Kingbase@123")
                .withEnv("DB_MODE",     "pg")
                .withEnv("ENABLE_CI",   "no")
                .withExposedPorts(54321)
                .waitingFor(Wait.forListeningPort());
    }

    /**
     * Patches {@code wal_level=logical} into the running Kingbase container and restarts
     * the database server.  Must be called after the container is started but before
     * any replication slot is created.
     */
    protected static void enableKingbaseLogicalReplication(GenericContainer<?> c) throws Exception {
        c.execInContainer("bash", "-c",
            "sed -i 's/^#wal_level = replica/wal_level = logical/' " +
            "/home/kingbase/userdata/data/kingbase.conf");
        c.execInContainer(
            "/home/kingbase/install/kingbase/bin/sys_ctl",
            "-D", "/home/kingbase/userdata/data",
            "restart", "-w", "-t", "60");
        // Brief pause for the server to fully accept connections after restart
        Thread.sleep(3000);
    }

    /**
     * KingbaseES target container.
     * DB_MODE=pg puts Kingbase into PostgreSQL-compatibility mode.
     * ENABLE_CI=no skips internal CI setup scripts that may not be needed.
     */
    @SuppressWarnings("resource")
    protected static GenericContainer<?> kingbaseTarget(String image) {
        return new GenericContainer<>(image)
                .withEnv("DB_USER",     "system")
                .withEnv("DB_PASSWORD", "Kingbase@123")
                .withEnv("DB_MODE",     "pg")
                .withEnv("ENABLE_CI",   "no")
                .withExposedPorts(54321)
                .waitingFor(Wait.forListeningPort());
    }

    // ── Config builders ───────────────────────────────────────────────────

    protected DatabaseConfig dbConfig(MySQLContainer<?> c) {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setType("mysql");
        cfg.setHost(c.getHost());
        cfg.setPort(c.getMappedPort(3306));
        cfg.setDatabase(c.getDatabaseName());
        cfg.setUsername(c.getUsername());
        cfg.setPassword(c.getPassword());
        cfg.setUrl(c.getJdbcUrl());
        return cfg;
    }

    protected DatabaseConfig dbConfig(MariaDBContainer<?> c) {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setType("mariadb");
        cfg.setHost(c.getHost());
        cfg.setPort(c.getMappedPort(3306));
        cfg.setDatabase(c.getDatabaseName());
        cfg.setUsername(c.getUsername());
        cfg.setPassword(c.getPassword());
        // Use MariaDB JDBC URL (not MySQL connector/J)
        cfg.setUrl(c.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:"));
        return cfg;
    }

    protected DatabaseConfig dbConfig(PostgreSQLContainer<?> c) {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setType("postgresql");
        cfg.setHost(c.getHost());
        cfg.setPort(c.getMappedPort(5432));
        cfg.setDatabase(c.getDatabaseName());
        cfg.setUsername(c.getUsername());
        cfg.setPassword(c.getPassword());
        cfg.setUrl(c.getJdbcUrl());
        return cfg;
    }

    /**
     * Builds a DatabaseConfig for a Kingbase GenericContainer.
     *
     * @param database the database to connect to (default is {@code test})
     */
    protected DatabaseConfig dbConfigKingbase(GenericContainer<?> c, String database) {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setType("kingbase");
        cfg.setHost(c.getHost());
        cfg.setPort(c.getMappedPort(54321));
        cfg.setDatabase(database);
        cfg.setUsername("system");
        cfg.setPassword("Kingbase@123");
        return cfg;
    }

    protected SyncConfig syncConfig(DatabaseConfig src, DatabaseConfig tgt,
                                    List<String> tables, Path workDir) {
        SyncOptions opts = new SyncOptions();
        opts.setTables(tables);
        opts.setSchemaSync(true);
        opts.setOffsetStorePath(workDir.resolve("offsets.dat").toString());
        opts.setSchemaHistoryPath(workDir.resolve("schema-history.dat").toString());

        SyncConfig cfg = new SyncConfig();
        cfg.setSource(src);
        cfg.setTarget(tgt);
        cfg.setSync(opts);
        return cfg;
    }

    protected SchemaApplier buildApplier() {
        return new SchemaApplier(new SchemaReader(), new DdlTranslator());
    }

    // ── MySQL helpers ─────────────────────────────────────────────────────

    /**
     * Grants Debezium-required global privileges to the MySQL non-root test user.
     * MySQL containers default the root password to the same value as the user password.
     */
    protected void grantMysqlReplicationPrivileges(MySQLContainer<?> c) throws Exception {
        try (Connection conn = DriverManager.getConnection(
                c.getJdbcUrl(), "root", c.getPassword())) {
            conn.createStatement().execute(
                    "GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, " +
                    "REPLICATION CLIENT, SELECT, LOCK TABLES ON *.* TO '" +
                    c.getUsername() + "'@'%'");
            conn.createStatement().execute("FLUSH PRIVILEGES");
        }
    }

    protected void grantMariadbReplicationPrivileges(MariaDBContainer<?> c) throws Exception {
        String rootUrl = c.getJdbcUrl().replace("jdbc:mysql:", "jdbc:mariadb:");
        try (Connection conn = DriverManager.getConnection(rootUrl, "root", c.getPassword())) {
            conn.createStatement().execute(
                    "GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, " +
                    "REPLICATION CLIENT, SELECT, LOCK TABLES ON *.* TO '" +
                    c.getUsername() + "'@'%'");
            conn.createStatement().execute("FLUSH PRIVILEGES");
        }
    }

    // ── DDL helpers ───────────────────────────────────────────────────────

    protected void createMysqlStaffTable(Connection conn) throws Exception {
        conn.createStatement().execute("""
            CREATE TABLE IF NOT EXISTS staff (
                id    INT NOT NULL AUTO_INCREMENT,
                name  VARCHAR(100) NOT NULL,
                dept  VARCHAR(50),
                score INT DEFAULT 0,
                PRIMARY KEY (id)
            )""");
    }

    protected void createPgStaffTable(Connection conn) throws Exception {
        conn.createStatement().execute("""
            CREATE TABLE IF NOT EXISTS staff (
                id    SERIAL PRIMARY KEY,
                name  VARCHAR(100) NOT NULL,
                dept  VARCHAR(50),
                score INTEGER DEFAULT 0
            )""");
        // Full replica identity ensures all column values are present in DELETE before-payload
        conn.createStatement().execute("ALTER TABLE staff REPLICA IDENTITY FULL");
    }

    /** Inserts 3 seed rows that all tests rely on. */
    protected void seedStaff(Connection conn) throws Exception {
        conn.createStatement().execute("""
            INSERT INTO staff (name, dept, score) VALUES
                ('Alice', 'Engineering', 90),
                ('Bob',   'Marketing',   75),
                ('Carol', 'Engineering', 85)""");
    }

    // ── Wait helpers ──────────────────────────────────────────────────────

    protected void waitFor(BooleanSupplier condition, int timeoutSec) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Condition not met within " + timeoutSec + "s");
            }
            Thread.sleep(500);
        }
    }

    protected boolean snapshotComplete(SyncProgressRegistry registry, String table, long minRows) {
        var state = registry.get(table);
        return state != null && state.getSnapshotScanned().get() >= minRows;
    }

    // ── DB assertion helpers ──────────────────────────────────────────────

    protected long countRows(Connection conn, String table) throws Exception {
        try (ResultSet rs = conn.createStatement()
                .executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    protected long queryLong(Connection conn, String sql) throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    protected void assertRowCount(Connection conn, String table, long expected, String message)
            throws Exception {
        assertEquals(expected, countRows(conn, table), message);
    }
}
