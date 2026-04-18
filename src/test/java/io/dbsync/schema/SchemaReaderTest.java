package io.dbsync.schema;

import io.dbsync.config.DatabaseConfig;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class SchemaReaderTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");

    private SchemaReader reader;

    @BeforeAll
    static void createTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id      BIGINT       NOT NULL,
                    name    VARCHAR(100) NOT NULL,
                    email   VARCHAR(200),
                    PRIMARY KEY (id)
                )""");
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id  BIGINT       NOT NULL,
                    user_id   BIGINT       NOT NULL,
                    total     DECIMAL(10,2),
                    PRIMARY KEY (order_id)
                )""");
        }
        try (Connection conn = DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id    BIGINT       NOT NULL,
                    name  VARCHAR(100) NOT NULL,
                    email VARCHAR(200),
                    PRIMARY KEY (id)
                )""");
        }
    }

    @BeforeEach
    void setUp() {
        reader = new SchemaReader();
    }

    // ── PostgreSQL tests (schema="public" branch) ─────────────────────────

    @Test
    void pg_readsAllTablesWhenFilterEmpty() throws Exception {
        List<TableDef> tables = reader.readTables(pgConfig(), List.of());
        List<String> names = tables.stream().map(TableDef::getName).toList();
        assertTrue(names.contains("users"),  "Expected users, got: " + names);
        assertTrue(names.contains("orders"), "Expected orders, got: " + names);
    }

    @Test
    void pg_filterByTableName() throws Exception {
        List<TableDef> tables = reader.readTables(pgConfig(), List.of("users"));
        assertEquals(1, tables.size());
        assertEquals("users", tables.get(0).getName());
    }

    @Test
    void pg_readsColumnsForTable() throws Exception {
        List<TableDef> tables = reader.readTables(pgConfig(), List.of("users"));
        List<String> colNames = tables.get(0).getColumns().stream().map(ColumnDef::getName).toList();
        assertTrue(colNames.contains("id"),    "Missing id");
        assertTrue(colNames.contains("name"),  "Missing name");
        assertTrue(colNames.contains("email"), "Missing email");
    }

    @Test
    void pg_detectsPrimaryKey() throws Exception {
        List<TableDef> tables = reader.readTables(pgConfig(), List.of("users"));
        List<String> pks = tables.get(0).getPrimaryKeys();
        assertTrue(pks.contains("id"), "Expected PK id, got: " + pks);
    }

    @Test
    void pg_readsDecimalColumnPrecision() throws Exception {
        List<TableDef> tables = reader.readTables(pgConfig(), List.of("orders"));
        ColumnDef total = tables.get(0).getColumns().stream()
                .filter(c -> c.getName().equals("total")).findFirst()
                .orElseThrow(() -> new AssertionError("total column not found"));
        assertEquals(10, total.getSize());
        assertEquals(2,  total.getDecimalDigits());
    }

    @Test
    void pg_notNullColumnIsMarkedNotNullable() throws Exception {
        List<TableDef> tables = reader.readTables(pgConfig(), List.of("users"));
        ColumnDef nameCol = tables.get(0).getColumns().stream()
                .filter(c -> c.getName().equals("name")).findFirst().orElseThrow();
        assertFalse(nameCol.isNullable(), "name should be NOT NULL");
    }

    @Test
    void pg_nullableColumnIsMarkedNullable() throws Exception {
        List<TableDef> tables = reader.readTables(pgConfig(), List.of("users"));
        ColumnDef emailCol = tables.get(0).getColumns().stream()
                .filter(c -> c.getName().equals("email")).findFirst().orElseThrow();
        assertTrue(emailCol.isNullable(), "email should be nullable");
    }

    // ── MySQL tests (catalog=dbName branch) ──────────────────────────────

    @Test
    void mysql_readsTablesViaDbCatalog() throws Exception {
        List<TableDef> tables = reader.readTables(mysqlConfig(), List.of("users"));
        assertEquals(1, tables.size());
        assertEquals("users", tables.get(0).getName());
    }

    @Test
    void mysql_detectsPrimaryKey() throws Exception {
        List<TableDef> tables = reader.readTables(mysqlConfig(), List.of("users"));
        List<String> pks = tables.get(0).getPrimaryKeys();
        assertTrue(pks.contains("id"), "Expected PK id, got: " + pks);
    }

    @Test
    void mysql_readsAllTablesWhenFilterEmpty() throws Exception {
        List<TableDef> tables = reader.readTables(mysqlConfig(), List.of());
        assertFalse(tables.isEmpty(), "MySQL should return at least one table");
    }

    // ── helpers ──────────────────────────────────────────────────────────

    static DatabaseConfig pgConfig() {
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
}
