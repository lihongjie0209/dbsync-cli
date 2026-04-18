package io.dbsync.schema;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.config.SyncOptions;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class SchemaApplierTest {

    /** MySQL as heterogeneous source — exercises MySQL catalog branch in SchemaReader. */
    @Container
    static final MySQLContainer<?> mysqlSrc = new MySQLContainer<>("mysql:8.0");

    /** PostgreSQL as target — exercises PG quoting + ON CONFLICT DDL generation. */
    @Container
    static final PostgreSQLContainer<?> pgTgt = new PostgreSQLContainer<>("postgres:16-alpine");

    @BeforeEach
    void setUp() throws Exception {
        // Ensure a fresh source table before each test
        try (Connection c = DriverManager.getConnection(
                mysqlSrc.getJdbcUrl(), mysqlSrc.getUsername(), mysqlSrc.getPassword())) {
            c.createStatement().execute("DROP TABLE IF EXISTS products");
            c.createStatement().execute("""
                CREATE TABLE products (
                    id    BIGINT       NOT NULL AUTO_INCREMENT,
                    name  VARCHAR(255) NOT NULL,
                    price DECIMAL(10,2),
                    PRIMARY KEY (id)
                )""");
        }
        // Clean target before each test
        try (Connection c = DriverManager.getConnection(
                pgTgt.getJdbcUrl(), pgTgt.getUsername(), pgTgt.getPassword())) {
            c.createStatement().execute("DROP TABLE IF EXISTS products");
        }
    }

    @Test
    void createsTableOnTargetWhenItDoesNotExist() throws Exception {
        buildApplier().sync(buildConfig(List.of("products"), true));

        try (Connection c = DriverManager.getConnection(
                pgTgt.getJdbcUrl(), pgTgt.getUsername(), pgTgt.getPassword())) {
            assertTrue(tableExists(c, "products"), "products should have been created on target");
        }
    }

    @Test
    void syncWithSchemaSyncFalseSkipsDdl() throws Exception {
        buildApplier().sync(buildConfig(List.of("products"), false));

        try (Connection c = DriverManager.getConnection(
                pgTgt.getJdbcUrl(), pgTgt.getUsername(), pgTgt.getPassword())) {
            assertFalse(tableExists(c, "products"), "Should NOT create table when schemaSync=false");
        }
    }

    @Test
    void addsNewColumnWhenTargetTableAlreadyExists() throws Exception {
        // Pre-create target without 'price'
        try (Connection c = DriverManager.getConnection(
                pgTgt.getJdbcUrl(), pgTgt.getUsername(), pgTgt.getPassword())) {
            c.createStatement().execute("""
                CREATE TABLE products (
                    id   BIGINT       NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    PRIMARY KEY (id)
                )""");
        }

        buildApplier().sync(buildConfig(List.of("products"), true));

        try (Connection c = DriverManager.getConnection(
                pgTgt.getJdbcUrl(), pgTgt.getUsername(), pgTgt.getPassword())) {
            assertTrue(columnExists(c, "products", "price"), "'price' should have been added");
        }
    }

    @Test
    void returnsListOfSyncedTables() throws Exception {
        List<TableDef> result = buildApplier().sync(buildConfig(List.of("products"), true));
        assertFalse(result.isEmpty());
        assertTrue(result.stream().anyMatch(t -> t.getName().equalsIgnoreCase("products")));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private SchemaApplier buildApplier() {
        SchemaApplier a = new SchemaApplier();
        a.schemaReader  = new SchemaReader();
        a.ddlTranslator = new DdlTranslator();
        return a;
    }

    private SyncConfig buildConfig(List<String> tables, boolean schemaSync) {
        DatabaseConfig src = new DatabaseConfig();
        src.setType("mysql");
        src.setDatabase(mysqlSrc.getDatabaseName());
        src.setUsername(mysqlSrc.getUsername());
        src.setPassword(mysqlSrc.getPassword());
        src.setUrl(mysqlSrc.getJdbcUrl());

        DatabaseConfig tgt = new DatabaseConfig();
        tgt.setType("postgresql");
        tgt.setUsername(pgTgt.getUsername());
        tgt.setPassword(pgTgt.getPassword());
        tgt.setUrl(pgTgt.getJdbcUrl());

        SyncOptions opts = new SyncOptions();
        opts.setTables(tables);
        opts.setSchemaSync(schemaSync);

        SyncConfig config = new SyncConfig();
        config.setSource(src);
        config.setTarget(tgt);
        config.setSync(opts);
        return config;
    }

    private boolean tableExists(Connection conn, String tableName) throws Exception {
        try (ResultSet rs = conn.getMetaData().getTables(null, "public", tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    private boolean columnExists(Connection conn, String tableName, String colName) throws Exception {
        try (ResultSet rs = conn.getMetaData().getColumns(null, "public", tableName, colName)) {
            return rs.next();
        }
    }
}
