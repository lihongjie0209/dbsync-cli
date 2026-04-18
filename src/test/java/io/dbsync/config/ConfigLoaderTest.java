package io.dbsync.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {

    private final ConfigLoader loader = new ConfigLoader();

    @Test
    void loadsYamlFromFile(@TempDir Path dir) throws Exception {
        Path cfg = writeYaml(dir, """
                source:
                  type: mysql
                  host: srchost
                  port: 3306
                  database: srcdb
                  username: root
                  password: pass
                target:
                  type: postgresql
                  host: tgthost
                  port: 5432
                  database: tgtdb
                  username: pguser
                  password: pgpass
                sync:
                  schemaSync: false
                  snapshotMode: never
                  batchSize: 100
                """);

        SyncConfig c = loader.load(cfg.toString(), Map.of());

        assertEquals("mysql",      c.getSource().getType());
        assertEquals("srchost",    c.getSource().getHost());
        assertEquals("srcdb",      c.getSource().getDatabase());
        assertEquals("postgresql", c.getTarget().getType());
        assertEquals("tgthost",    c.getTarget().getHost());
        assertFalse(c.getSync().isSchemaSync());
        assertEquals("never",      c.getSync().getSnapshotMode());
        assertEquals(100,          c.getSync().getBatchSize());
    }

    @Test
    void appliesSourceHostOverride(@TempDir Path dir) throws Exception {
        Path cfg = writeMinimalYaml(dir);
        SyncConfig c = loader.load(cfg.toString(), Map.of("source.host", "overridden-host"));
        assertEquals("overridden-host", c.getSource().getHost());
    }

    @Test
    void appliesSourcePortOverride(@TempDir Path dir) throws Exception {
        Path cfg = writeMinimalYaml(dir);
        SyncConfig c = loader.load(cfg.toString(), Map.of("source.port", "3307"));
        assertEquals(3307, c.getSource().getPort());
    }

    @Test
    void appliesTargetHostOverride(@TempDir Path dir) throws Exception {
        Path cfg = writeMinimalYaml(dir);
        SyncConfig c = loader.load(cfg.toString(), Map.of("target.host", "new-tgt-host"));
        assertEquals("new-tgt-host", c.getTarget().getHost());
    }

    @Test
    void appliesAllOverrides(@TempDir Path dir) throws Exception {
        Path cfg = writeMinimalYaml(dir);
        Map<String, String> overrides = Map.of(
            "source.type",     "postgresql",
            "source.host",     "s-host",
            "source.database", "s-db",
            "source.username", "s-user",
            "source.password", "s-pass",
            "target.type",     "mysql",
            "target.host",     "t-host",
            "target.database", "t-db"
        );
        SyncConfig c = loader.load(cfg.toString(), overrides);
        assertEquals("postgresql", c.getSource().getType());
        assertEquals("s-host",     c.getSource().getHost());
        assertEquals("s-db",       c.getSource().getDatabase());
        assertEquals("mysql",      c.getTarget().getType());
        assertEquals("t-host",     c.getTarget().getHost());
    }

    @Test
    void loadsTestConfigYaml() throws Exception {
        String path = getClass().getClassLoader().getResource("test-config.yaml").getFile();
        SyncConfig c = loader.load(path, Map.of());
        assertEquals("mysql",          c.getSource().getType());
        assertEquals("db.example.com", c.getSource().getHost());
        assertEquals(42,               c.getSource().getServerId());
        assertEquals("postgresql",     c.getTarget().getType());
        assertEquals(2,                c.getSync().getTables().size());
        assertEquals(200,              c.getSync().getBatchSize());
    }

    @Test
    void missingFileThrowsException() {
        assertThrows(Exception.class, () -> loader.load("no-such-file.yaml", Map.of()));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private Path writeYaml(Path dir, String content) throws IOException {
        Path p = dir.resolve("config.yaml");
        Files.writeString(p, content);
        return p;
    }

    private Path writeMinimalYaml(Path dir) throws IOException {
        return writeYaml(dir, """
                source:
                  type: mysql
                  host: localhost
                  database: srcdb
                  username: root
                  password: pass
                target:
                  type: postgresql
                  host: localhost
                  database: tgtdb
                  username: pguser
                  password: pgpass
                """);
    }
}
