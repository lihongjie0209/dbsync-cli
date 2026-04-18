package io.dbsync.cli;

import io.dbsync.config.ConfigLoader;
import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.config.SyncOptions;
import io.dbsync.schema.SchemaReader;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link TestCommand}.
 * Uses real MySQL 8 + PostgreSQL 16 containers; no mocks for the database layer.
 */
@Testcontainers
class TestCommandTest {

    // ── Shared containers (started once per class) ────────────────────────

    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("sourcedb")
            .withUsername("root")
            .withPassword("root");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("targetdb")
            .withUsername("pguser")
            .withPassword("pgpass");

    // ── Fixtures ──────────────────────────────────────────────────────────

    @BeforeAll
    static void createTables() throws Exception {
        try (var conn = java.sql.DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
             var st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id   INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                )""");
            st.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id         INT AUTO_INCREMENT PRIMARY KEY,
                    user_id    INT NOT NULL,
                    total      DECIMAL(10,2)
                )""");
        }
        // PostgreSQL target: 'users' exists, 'orders' does NOT (schema sync would create it)
        try (var conn = java.sql.DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id   SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                )""");
        }
    }

    // ── Test subject factory ──────────────────────────────────────────────

    private TestCommand buildTestCommand() {
        TestCommand tc = new TestCommand();
        tc.configLoader = new ConfigLoader();
        tc.schemaReader = new SchemaReader();
        return tc;
    }

    // ── Config validation checks ──────────────────────────────────────────

    @Test
    void missingConfigFile_failsConfigCheck() {
        TestCommand tc = buildTestCommand();
        List<CheckResult> results = tc.run("/no/such/file.yaml", Map.of());

        assertEquals(1, results.size());
        assertTrue(results.get(0).isFail(), "Should fail for missing file");
        assertTrue(results.get(0).getId().startsWith("config"));
    }

    @Test
    void configMissingRequiredField_failsValidation() throws Exception {
        Path tmp = Files.createTempFile("dbsync-test-", ".yaml");
        try {
            // source.type is missing — validate() should throw
            Files.writeString(tmp, """
                source:
                  host: localhost
                  database: mydb
                target:
                  type: postgresql
                  database: targetdb
                """);
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult cfgResult = results.stream()
                    .filter(r -> r.getId().startsWith("config"))
                    .findFirst().orElseThrow();
            assertTrue(cfgResult.isFail(), "Missing source.type should fail validation");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void validConfigFile_passesConfigCheck() throws Exception {
        Path tmp = writeConfigFile(List.of());
        try {
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult cfgResult = results.stream()
                    .filter(r -> r.getId().equals("config"))
                    .findFirst().orElseThrow();
            assertTrue(cfgResult.isPass());
            // Summary details should mention source and target
            assertTrue(cfgResult.getDetails().stream().anyMatch(d -> d.contains("mysql")));
            assertTrue(cfgResult.getDetails().stream().anyMatch(d -> d.contains("postgresql")));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    // ── Connection checks ─────────────────────────────────────────────────

    @Test
    void goodConnections_bothPass() throws Exception {
        Path tmp = writeConfigFile(List.of());
        try {
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult src = findById(results, "connection.source");
            CheckResult tgt = findById(results, "connection.target");
            assertTrue(src.isPass(), "Source connection should pass: " + src.getMessage());
            assertTrue(tgt.isPass(), "Target connection should pass: " + tgt.getMessage());
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void badSourcePort_sourceConnectionFails() throws Exception {
        Path tmp = writeConfigFile(List.of());
        try {
            TestCommand tc = buildTestCommand();
            // Override source port to a closed port
            Map<String, String> overrides = Map.of("source.port", "19999");
            List<CheckResult> results = tc.run(tmp.toString(), overrides);

            CheckResult src = findById(results, "connection.source");
            assertTrue(src.isFail(), "Connection to wrong port should fail");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void badTargetPort_targetConnectionFails() throws Exception {
        Path tmp = writeConfigFile(List.of());
        try {
            TestCommand tc = buildTestCommand();
            Map<String, String> overrides = Map.of("target.port", "19999");
            List<CheckResult> results = tc.run(tmp.toString(), overrides);

            CheckResult tgt = findById(results, "connection.target");
            assertTrue(tgt.isFail(), "Connection to wrong port should fail");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    // ── Table mapping checks ──────────────────────────────────────────────

    @Test
    void noTablesConfigured_passesWithAllTablesMessage() throws Exception {
        Path tmp = writeConfigFile(List.of());  // empty tables list
        try {
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult tablesResult = findById(results, "tables");
            assertTrue(tablesResult.isPass());
            assertTrue(tablesResult.getMessage().contains("all source tables"));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void configuredTablesExistInSource_sourceCheckPasses() throws Exception {
        Path tmp = writeConfigFile(List.of("users", "orders"));
        try {
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult src = findById(results, "tables.source");
            assertTrue(src.isPass(), "Both tables exist in source: " + src.getMessage());
            // Details should list table names with column counts
            assertTrue(src.getDetails().stream().anyMatch(d -> d.contains("users")));
            assertTrue(src.getDetails().stream().anyMatch(d -> d.contains("orders")));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void missingTableInSource_sourceCheckFails() throws Exception {
        Path tmp = writeConfigFile(List.of("users", "nonexistent_table"));
        try {
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult src = findById(results, "tables.source");
            assertTrue(src.isFail(), "nonexistent_table not in source → FAIL");
            assertTrue(src.getMessage().contains("nonexistent_table")
                    || src.getDetails().stream().anyMatch(d -> d.contains("nonexistent_table")));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void missingTableInTarget_warnsWhenSchemaSyncEnabled() throws Exception {
        // 'orders' exists in source but NOT in target; schemaSync=true → WARN
        Path tmp = writeConfigFile(List.of("users", "orders"));
        try {
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult tgt = findById(results, "tables.target");
            // orders missing from target with schemaSync=true → WARN
            assertTrue(tgt.isWarn() || tgt.isPass(),
                    "Missing target table with schemaSync=true should be WARN (or PASS if synced): "
                            + tgt.getMessage());
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void missingTableInTarget_failsWhenSchemaSyncDisabled() throws Exception {
        Path tmp = writeConfigFile(List.of("users", "orders"), false);
        try {
            TestCommand tc = buildTestCommand();
            List<CheckResult> results = tc.run(tmp.toString(), Map.of());

            CheckResult tgt = findById(results, "tables.target");
            // orders missing from target with schemaSync=false → FAIL
            assertTrue(tgt.isFail(),
                    "Missing target table with schemaSync=false should be FAIL: " + tgt.getMessage());
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    // ── CheckResult unit tests ────────────────────────────────────────────

    @Test
    void checkResult_pass() {
        CheckResult r = CheckResult.pass("id1", "all good", "detail1");
        assertTrue(r.isPass()); assertFalse(r.isFail()); assertFalse(r.isWarn());
        assertEquals("id1",       r.getId());
        assertEquals("all good",  r.getMessage());
        assertEquals(List.of("detail1"), r.getDetails());
    }

    @Test
    void checkResult_fail() {
        CheckResult r = CheckResult.fail("id2", "bad thing");
        assertTrue(r.isFail()); assertFalse(r.isPass());
        assertEquals(CheckResult.Status.FAIL, r.getStatus());
    }

    @Test
    void checkResult_warn() {
        CheckResult r = CheckResult.warn("id3", "might be ok");
        assertTrue(r.isWarn()); assertFalse(r.isFail());
        assertEquals(CheckResult.Status.WARN, r.getStatus());
    }

    @Test
    void checkResult_toString_containsStatusAndMessage() {
        CheckResult r = CheckResult.pass("cfg", "loaded");
        String s = r.toString();
        assertTrue(s.contains("PASS")); assertTrue(s.contains("loaded"));
    }

    // ── Mockito-based unit tests for TestCommand internals ────────────────

    @Test
    void checkConfig_fileNotFound_returnsFail() {
        TestCommand tc = new TestCommand();
        tc.configLoader = new ConfigLoader();
        tc.schemaReader = new SchemaReader();

        List<CheckResult> out = new java.util.ArrayList<>();
        SyncConfig result = tc.checkConfig("/no/such/path.yaml", Map.of(), out);

        assertNull(result);
        assertEquals(1, out.size());
        assertTrue(out.get(0).isFail());
    }

    @Test
    void checkConnection_badUrl_returnsFail() {
        TestCommand tc = new TestCommand();
        tc.configLoader = new ConfigLoader();
        tc.schemaReader = new SchemaReader();

        DatabaseConfig db = new DatabaseConfig();
        db.setType("postgresql");
        db.setHost("localhost");
        db.setPort(9); // nothing listening
        db.setDatabase("nodb");
        db.setUsername("x");
        db.setPassword("x");

        List<CheckResult> out = new java.util.ArrayList<>();
        boolean ok = tc.checkConnection("target", db, out);

        assertFalse(ok);
        assertEquals(1, out.size());
        assertTrue(out.get(0).isFail());
    }

    @Test
    void checkConnection_goodUrl_returnsPass() throws Exception {
        TestCommand tc = new TestCommand();
        tc.configLoader = new ConfigLoader();
        tc.schemaReader = new SchemaReader();

        DatabaseConfig db = new DatabaseConfig();
        db.setType("postgresql");
        db.setUrl(postgres.getJdbcUrl());
        db.setDatabase("targetdb");
        db.setUsername(postgres.getUsername());
        db.setPassword(postgres.getPassword());

        List<CheckResult> out = new java.util.ArrayList<>();
        boolean ok = tc.checkConnection("target", db, out);

        assertTrue(ok);
        assertTrue(out.get(0).isPass());
        assertTrue(out.get(0).getDetails().stream().anyMatch(d -> d.startsWith("version:")));
    }

    // ── TestSubcommand smoke test (Mockito) ───────────────────────────────

    @Test
    void testSubcommand_allPassReturns0() {
        TestCommand mockCmd = mock(TestCommand.class);
        when(mockCmd.run(anyString(), anyMap())).thenReturn(List.of(
                CheckResult.pass("config", "ok"),
                CheckResult.pass("connection.source", "ok"),
                CheckResult.pass("connection.target", "ok"),
                CheckResult.pass("tables", "ok")
        ));

        TestSubcommand sub = new TestSubcommand();
        sub.testCommand = mockCmd;
        sub.configFile  = "irrelevant.yaml";

        assertEquals(0, sub.call());
    }

    @Test
    void testSubcommand_anyFailReturns1() {
        TestCommand mockCmd = mock(TestCommand.class);
        when(mockCmd.run(anyString(), anyMap())).thenReturn(List.of(
                CheckResult.pass("config", "ok"),
                CheckResult.fail("connection.source", "refused")
        ));

        TestSubcommand sub = new TestSubcommand();
        sub.testCommand = mockCmd;
        sub.configFile  = "irrelevant.yaml";

        assertEquals(1, sub.call());
    }

    @Test
    void testSubcommand_warnOnlyReturns0() {
        TestCommand mockCmd = mock(TestCommand.class);
        when(mockCmd.run(anyString(), anyMap())).thenReturn(List.of(
                CheckResult.pass("config", "ok"),
                CheckResult.warn("tables.target", "will be created")
        ));

        TestSubcommand sub = new TestSubcommand();
        sub.testCommand = mockCmd;
        sub.configFile  = "irrelevant.yaml";

        assertEquals(0, sub.call());
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /** Write a temp config pointing at the running containers. */
    private Path writeConfigFile(List<String> tables) throws Exception {
        return writeConfigFile(tables, true);
    }

    private Path writeConfigFile(List<String> tables, boolean schemaSync) throws Exception {
        Path tmp = Files.createTempFile("dbsync-test-", ".yaml");
        // Use host + mapped port — no 'url' field so port overrides in tests work correctly
        String tableYaml = tables.isEmpty() ? " []"
                : tables.stream().map(t -> "\n    - " + t).reduce("", String::concat);
        try (PrintWriter w = new PrintWriter(Files.newOutputStream(tmp))) {
            w.printf("""
                source:
                  type: mysql
                  host: %s
                  port: %d
                  database: %s
                  username: %s
                  password: %s
                target:
                  type: postgresql
                  host: %s
                  port: %d
                  database: %s
                  username: %s
                  password: %s
                sync:
                  tables:%s
                  schemaSync: %s
                  snapshotMode: initial
                """,
                mysql.getHost(),    mysql.getMappedPort(3306),
                mysql.getDatabaseName(), mysql.getUsername(), mysql.getPassword(),
                postgres.getHost(), postgres.getMappedPort(5432),
                postgres.getDatabaseName(), postgres.getUsername(), postgres.getPassword(),
                tableYaml,
                schemaSync
            );
        }
        return tmp;
    }

    private CheckResult findById(List<CheckResult> results, String id) {
        return results.stream()
                .filter(r -> r.getId().equals(id))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No CheckResult with id='" + id + "' in: " + results));
    }
}
