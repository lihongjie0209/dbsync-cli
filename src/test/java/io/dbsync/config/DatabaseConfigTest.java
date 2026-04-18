package io.dbsync.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseConfigTest {

    @Test
    void mysqlJdbcUrl() {
        DatabaseConfig c = mysqlConfig("myhost", 3306, "mydb");
        assertEquals(
            "jdbc:mysql://myhost:3306/mydb?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true",
            c.jdbcUrl());
    }

    @Test
    void postgresqlJdbcUrl() {
        DatabaseConfig c = pgConfig("pghost", 5432, "pgdb");
        assertEquals("jdbc:postgresql://pghost:5432/pgdb", c.jdbcUrl());
    }

    @Test
    void urlOverrideTakesPrecedence() {
        DatabaseConfig c = mysqlConfig("ignored", 3306, "ignored");
        c.setUrl("jdbc:h2:mem:test");
        assertEquals("jdbc:h2:mem:test", c.jdbcUrl());
    }

    @Test
    void defaultPortMysql() {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("mysql");
        assertEquals(3306, c.getPort());
    }

    @Test
    void defaultPortPostgresql() {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("postgresql");
        assertEquals(5432, c.getPort());
    }

    @Test
    void explicitPortOverridesDefault() {
        DatabaseConfig c = mysqlConfig("h", 3307, "d");
        assertEquals(3307, c.getPort());
    }

    @Test
    void unsupportedTypeThrows() {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("oracle");
        c.setHost("h");
        c.setDatabase("d");
        assertThrows(IllegalArgumentException.class, c::jdbcUrl);
    }

    @Test
    void gettersAndSetters() {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("mysql");
        c.setHost("myhost");
        c.setPort(3306);
        c.setDatabase("db");
        c.setUsername("u");
        c.setPassword("p");
        c.setServerId(99);
        c.setSlotName("slot");
        c.setPluginName("pgoutput");

        assertEquals("mysql",     c.getType());
        assertEquals("myhost",    c.getHost());
        assertEquals(3306,        c.getPort());
        assertEquals("db",        c.getDatabase());
        assertEquals("u",         c.getUsername());
        assertEquals("p",         c.getPassword());
        assertEquals(99,          c.getServerId());
        assertEquals("slot",      c.getSlotName());
        assertEquals("pgoutput",  c.getPluginName());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    static DatabaseConfig mysqlConfig(String host, int port, String db) {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("mysql"); c.setHost(host); c.setPort(port);
        c.setDatabase(db);  c.setUsername("u"); c.setPassword("p");
        return c;
    }

    static DatabaseConfig pgConfig(String host, int port, String db) {
        DatabaseConfig c = new DatabaseConfig();
        c.setType("postgresql"); c.setHost(host); c.setPort(port);
        c.setDatabase(db);       c.setUsername("u"); c.setPassword("p");
        return c;
    }
}
