package io.dbsync.engine;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.config.SyncOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumEngineManagerTest {

    private final DebeziumEngineManager mgr = new DebeziumEngineManager();

    @Test
    void mysqlConnectorClass() {
        Properties p = mgr.buildConnectorProps(buildConfig("mysql"));
        assertEquals("io.debezium.connector.mysql.MySqlConnector",
                p.getProperty("connector.class"));
    }

    @Test
    void mysqlHostPortDatabase() {
        Properties p = mgr.buildConnectorProps(buildConfig("mysql"));
        assertEquals("dbhost",  p.getProperty("database.hostname"));
        assertEquals("3306",    p.getProperty("database.port"));
        assertEquals("testdb",  p.getProperty("database.include.list"));
    }

    @Test
    void mysqlSchemaHistorySet() {
        Properties p = mgr.buildConnectorProps(buildConfig("mysql"));
        assertNotNull(p.getProperty("schema.history.internal"));
        assertNotNull(p.getProperty("schema.history.internal.file.filename"));
    }

    @Test
    void mysqlServerId() {
        SyncConfig cfg = buildConfig("mysql");
        cfg.getSource().setServerId(42);
        Properties p = mgr.buildConnectorProps(cfg);
        assertEquals("42", p.getProperty("database.server.id"));
    }

    @Test
    void mysqlTableFilter() {
        SyncConfig cfg = buildConfig("mysql");
        cfg.getSync().setTables(List.of("users", "orders"));
        Properties p = mgr.buildConnectorProps(cfg);
        String tableList = p.getProperty("table.include.list");
        assertNotNull(tableList);
        assertTrue(tableList.contains("users"));
        assertTrue(tableList.contains("orders"));
    }

    @Test
    void postgresqlConnectorClass() {
        Properties p = mgr.buildConnectorProps(buildConfig("postgresql"));
        assertEquals("io.debezium.connector.postgresql.PostgresConnector",
                p.getProperty("connector.class"));
    }

    @Test
    void postgresqlHostPortDatabase() {
        Properties p = mgr.buildConnectorProps(buildConfig("postgresql"));
        assertEquals("dbhost",  p.getProperty("database.hostname"));
        assertEquals("5432",    p.getProperty("database.port"));
        assertEquals("testdb",  p.getProperty("database.dbname"));
    }

    @Test
    void postgresqlSlotAndPlugin() {
        SyncConfig cfg = buildConfig("postgresql");
        cfg.getSource().setSlotName("myslot");
        cfg.getSource().setPluginName("pgoutput");
        Properties p = mgr.buildConnectorProps(cfg);
        assertEquals("myslot",   p.getProperty("slot.name"));
        assertEquals("pgoutput", p.getProperty("plugin.name"));
    }

    @Test
    void postgresqlTableFilter() {
        SyncConfig cfg = buildConfig("postgresql");
        cfg.getSync().setTables(List.of("users"));
        Properties p = mgr.buildConnectorProps(cfg);
        String tableList = p.getProperty("table.include.list");
        assertNotNull(tableList);
        assertTrue(tableList.contains("users"));
    }

    @Test
    void offsetStorageIsConfigured() {
        Properties p = mgr.buildConnectorProps(buildConfig("mysql"));
        assertEquals("org.apache.kafka.connect.storage.FileOffsetBackingStore",
                p.getProperty("offset.storage"));
        assertNotNull(p.getProperty("offset.storage.file.filename"));
    }

    @Test
    void snapshotModeIsPropagated() {
        SyncConfig cfg = buildConfig("mysql");
        cfg.getSync().setSnapshotMode("never");
        Properties p = mgr.buildConnectorProps(cfg);
        assertEquals("never", p.getProperty("snapshot.mode"));
    }

    @Test
    void unsupportedSourceTypeThrows() {
        SyncConfig cfg = buildConfig("oracle");
        assertThrows(IllegalArgumentException.class, () -> mgr.buildConnectorProps(cfg));
    }

    @Test
    void topicPrefixIsDbsync() {
        Properties p = mgr.buildConnectorProps(buildConfig("mysql"));
        assertEquals("dbsync", p.getProperty("topic.prefix"));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private SyncConfig buildConfig(String srcType) {
        SyncConfig c = new SyncConfig();
        DatabaseConfig src = new DatabaseConfig();
        src.setType(srcType);
        src.setHost("dbhost");
        src.setPort("mysql".equals(srcType) ? 3306 : 5432);
        src.setDatabase("testdb");
        src.setUsername("u");
        src.setPassword("p");
        src.setServerId(1);
        src.setSlotName("slot");
        src.setPluginName("pgoutput");
        c.setSource(src);

        DatabaseConfig tgt = new DatabaseConfig();
        tgt.setType("postgresql");
        tgt.setDatabase("tgt");
        c.setTarget(tgt);

        SyncOptions opts = new SyncOptions();
        opts.setOffsetStorePath("./offsets.dat");
        opts.setSchemaHistoryPath("./schema-hist.dat");
        c.setSync(opts);
        return c;
    }
}
