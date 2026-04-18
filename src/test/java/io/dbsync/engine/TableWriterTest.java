package io.dbsync.engine;

import io.dbsync.config.DatabaseConfig;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TableWriterTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");

    @BeforeAll
    static void createTables() throws Exception {
        try (Connection c = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            c.createStatement().execute("""
                CREATE TABLE events (
                    id    BIGINT          NOT NULL,
                    name  VARCHAR(100),
                    score DOUBLE PRECISION,
                    PRIMARY KEY (id)
                )""");
        }
        try (Connection c = DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            c.createStatement().execute("""
                CREATE TABLE events (
                    id    BIGINT       NOT NULL,
                    name  VARCHAR(100),
                    score DOUBLE,
                    PRIMARY KEY (id)
                )""");
        }
    }

    @BeforeEach
    void truncate() throws Exception {
        try (Connection c = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            c.createStatement().execute("TRUNCATE TABLE events");
        }
        try (Connection c = DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            c.createStatement().execute("TRUNCATE TABLE events");
        }
    }

    // ── PostgreSQL tests ──────────────────────────────────────────────────

    @Test
    void pg_insertRowSuccessfully() throws Exception {
        try (Connection c = pgConn()) {
            new TableWriter(pgConfig(), "events", c)
                    .insert(json(Map.of("id", 1, "name", "click", "score", 3.14)));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM events WHERE id=1")) {
                assertTrue(rs.next());
                assertEquals("click", rs.getString("name"));
                assertEquals(3.14,    rs.getDouble("score"), 0.001);
            }
        }
    }

    @Test
    void pg_upsertInsertsWhenNoPkConflict() throws Exception {
        try (Connection c = pgConn()) {
            new TableWriter(pgConfig(), "events", c)
                    .upsert(json(Map.of("id", 2, "name", "view")), json(Map.of()));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT name FROM events WHERE id=2")) {
                assertTrue(rs.next());
                assertEquals("view", rs.getString("name"));
            }
        }
    }

    @Test
    void pg_upsertUpdatesOnConflict() throws Exception {
        try (Connection c = pgConn()) {
            TableWriter w = new TableWriter(pgConfig(), "events", c);
            w.insert(json(Map.of("id", 3, "name", "original")));
            w.upsert(json(Map.of("id", 3, "name", "updated")),
                     json(Map.of("id", 3, "name", "original")));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT name FROM events WHERE id=3")) {
                assertTrue(rs.next());
                assertEquals("updated", rs.getString("name")); // ON CONFLICT DO UPDATE
            }
        }
    }

    @Test
    void pg_deleteRemovesRow() throws Exception {
        try (Connection c = pgConn()) {
            c.createStatement().execute("INSERT INTO events (id, name) VALUES (4, 'del')");
            new TableWriter(pgConfig(), "events", c).delete(json(Map.of("id", 4)));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM events WHERE id=4")) {
                assertFalse(rs.next(), "Row should have been deleted");
            }
        }
    }

    @Test
    void pg_insertEmptyNodeIsNoop() throws Exception {
        try (Connection c = pgConn()) {
            new TableWriter(pgConfig(), "events", c)
                    .insert(new com.fasterxml.jackson.databind.ObjectMapper().readTree("{}"));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT COUNT(*) FROM events")) {
                rs.next();
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void pg_closeShutsDownConnection() throws Exception {
        Connection c = pgConn();
        new TableWriter(pgConfig(), "events", c).close();
        assertTrue(c.isClosed(), "Connection should be closed after writer.close()");
    }

    // ── MySQL tests ───────────────────────────────────────────────────────

    @Test
    void mysql_insertRowSuccessfully() throws Exception {
        try (Connection c = mysqlConn()) {
            new TableWriter(mysqlConfig(), "events", c)
                    .insert(json(Map.of("id", 10, "name", "signup", "score", 9.9)));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT name FROM events WHERE id=10")) {
                assertTrue(rs.next());
                assertEquals("signup", rs.getString("name"));
            }
        }
    }

    @Test
    void mysql_upsertUpdatesOnDuplicateKey() throws Exception {
        try (Connection c = mysqlConn()) {
            c.createStatement().execute("INSERT INTO events (id, name) VALUES (11, 'before')");
            new TableWriter(mysqlConfig(), "events", c)
                    .upsert(json(Map.of("id", 11, "name", "after")),
                            json(Map.of("id", 11, "name", "before")));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT name FROM events WHERE id=11")) {
                assertTrue(rs.next());
                assertEquals("after", rs.getString("name")); // ON DUPLICATE KEY UPDATE
            }
        }
    }

    @Test
    void mysql_deleteRemovesRow() throws Exception {
        try (Connection c = mysqlConn()) {
            c.createStatement().execute("INSERT INTO events (id, name) VALUES (12, 'toDel')");
            new TableWriter(mysqlConfig(), "events", c).delete(json(Map.of("id", 12)));

            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM events WHERE id=12")) {
                assertFalse(rs.next(), "Row should have been deleted");
            }
        }
    }

    // ── coerce() unit tests (no DB needed) ───────────────────────────────

    @Test
    void coerce_booleanColumn_convertsIntToBoolean() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        assertEquals(Boolean.TRUE,  w.coerce(1, Types.BOOLEAN));
        assertEquals(Boolean.FALSE, w.coerce(0, Types.BOOLEAN));
        assertEquals(Boolean.TRUE,  w.coerce(1L, Types.BOOLEAN));
    }

    @Test
    void coerce_timestampColumn_convertsLongToTimestamp() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        long epochMs = 1_700_000_000_000L;
        Object result = w.coerce(epochMs, Types.TIMESTAMP);
        assertInstanceOf(java.sql.Timestamp.class, result);
        assertEquals(epochMs, ((java.sql.Timestamp) result).getTime());
    }

    @Test
    void coerce_dateColumn_convertsEpochDaysToDate() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        // epoch day 0 = 1970-01-01
        Object result = w.coerce(0, Types.DATE);
        assertInstanceOf(java.sql.Date.class, result);
        assertEquals("1970-01-01", result.toString());
    }

    @Test
    void coerce_nullValueReturnsNull() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        assertNull(w.coerce(null, Types.BOOLEAN));
    }

    @Test
    void coerce_nullTypeReturnsValueUnchanged() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        assertEquals("hello", w.coerce("hello", null));
    }

    @Test
    void coerce_unknownTypePassesThrough() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        assertEquals("raw", w.coerce("raw", Types.VARCHAR));
    }

    @Test
    void coerce_timestampWithTimezoneColumn_convertsIsoStringToTimestamp() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        String iso = "2026-04-18T04:34:21Z";
        Object result = w.coerce(iso, Types.TIMESTAMP_WITH_TIMEZONE);
        assertInstanceOf(java.sql.Timestamp.class, result);
    }

    @Test
    void coerce_timestampWithTimezoneColumn_convertsLongToTimestamp() {
        TableWriter w = new TableWriter(pgConfig(), "t");
        long epochMs = 1_700_000_000_000L;
        Object result = w.coerce(epochMs, Types.TIMESTAMP_WITH_TIMEZONE);
        assertInstanceOf(java.sql.Timestamp.class, result);
        assertEquals(epochMs, ((java.sql.Timestamp) result).getTime());
    }



    @Test
    void pg_insertBoolean_andTimestamp_viaTypeCoercion() throws Exception {
        // Create a table with BOOLEAN and TIMESTAMP columns
        try (Connection c = pgConn()) {
            c.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS typed_test (
                    id         BIGINT  PRIMARY KEY,
                    active     BOOLEAN,
                    created_at TIMESTAMP
                )""");
            c.createStatement().execute("TRUNCATE TABLE typed_test");

            long epochMs = 1_700_000_000_000L;
            // Simulates Debezium: active=1 (TINYINT), created_at=long millis
            new TableWriter(pgConfig(), "typed_test", c)
                    .insert(json(Map.of("id", 99, "active", 1, "created_at", epochMs)));

            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT active, created_at FROM typed_test WHERE id=99")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("active"), "TINYINT(1) 1 should be coerced to true");
                assertNotNull(rs.getTimestamp("created_at"), "epoch millis should be a valid timestamp");
            }
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────

    static Connection pgConn() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    static Connection mysqlConn() throws Exception {
        return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    /** Minimal config with just type set — used for coerce() unit tests (no DB). */
    private static DatabaseConfig pgConfig() {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("postgresql");
        c.setUsername(postgres.getUsername());
        c.setPassword(postgres.getPassword());
        c.setUrl(postgres.getJdbcUrl());
        return c;
    }

    static DatabaseConfig mysqlConfig() {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("mysql");
        c.setDatabase(mysql.getDatabaseName());
        c.setUsername(mysql.getUsername());
        c.setPassword(mysql.getPassword());
        c.setUrl(mysql.getJdbcUrl());
        return c;
    }

    static com.fasterxml.jackson.databind.JsonNode json(Map<String, Object> fields) {
        return new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(fields);
    }
}
