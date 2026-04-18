package io.dbsync.cli;

import io.dbsync.config.ConfigLoader;
import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.config.SyncOptions;
import io.dbsync.engine.DebeziumEngineManager;
import io.dbsync.progress.ProgressReporter;
import io.dbsync.progress.SyncProgressRegistry;
import io.dbsync.schema.SchemaApplier;
import io.dbsync.schema.TableDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SyncCommandTest {

    private SyncCommand cmd;
    private ConfigLoader configLoader;
    private SchemaApplier schemaApplier;
    private SyncProgressRegistry progressRegistry;
    private DebeziumEngineManager engineManager;
    private ProgressReporter progressReporter;

    @BeforeEach
    void setUp() {
        cmd              = new SyncCommand();
        configLoader     = mock(ConfigLoader.class);
        schemaApplier    = mock(SchemaApplier.class);
        progressRegistry = new SyncProgressRegistry();   // real — it's a simple ConcurrentMap
        engineManager    = mock(DebeziumEngineManager.class);
        progressReporter = mock(ProgressReporter.class);

        // Direct field injection — fields are package-private, test is in the same package
        cmd.configLoader     = configLoader;
        cmd.schemaApplier    = schemaApplier;
        cmd.progressRegistry = progressRegistry;
        cmd.engineManager    = engineManager;
        cmd.progressReporter = progressReporter;
    }

    @Test
    void executeReturnsZeroOnSuccess() throws Exception {
        when(configLoader.load(anyString(), anyMap())).thenReturn(buildConfig());
        when(schemaApplier.sync(any())).thenReturn(List.of());

        int result = cmd.execute("sync-config.yaml", Map.of());

        assertEquals(0, result);
    }

    @Test
    void executeRegistersTablesReturnedBySchemaApplier() throws Exception {
        when(configLoader.load(anyString(), anyMap())).thenReturn(buildConfig());
        when(schemaApplier.sync(any())).thenReturn(
                List.of(new TableDef("users"), new TableDef("orders")));

        cmd.execute("sync-config.yaml", Map.of());

        assertNotNull(progressRegistry.get("users"),  "users should be registered");
        assertNotNull(progressRegistry.get("orders"), "orders should be registered");
    }

    @Test
    void executePassesOverridesToConfigLoader() throws Exception {
        Map<String, String> overrides = Map.of("source.host", "db-host");
        when(configLoader.load(anyString(), anyMap())).thenReturn(buildConfig());
        when(schemaApplier.sync(any())).thenReturn(List.of());

        cmd.execute("custom.yaml", overrides);

        verify(configLoader).load("custom.yaml", overrides);
    }

    @Test
    void executeStartsDebeziumEngine() throws Exception {
        when(configLoader.load(anyString(), anyMap())).thenReturn(buildConfig());
        when(schemaApplier.sync(any())).thenReturn(List.of());

        cmd.execute("sync-config.yaml", Map.of());

        verify(engineManager).start(any(), any());
    }

    @Test
    void executeStopsEngineAfterTui() throws Exception {
        when(configLoader.load(anyString(), anyMap())).thenReturn(buildConfig());
        when(schemaApplier.sync(any())).thenReturn(List.of());

        cmd.execute("sync-config.yaml", Map.of());

        verify(engineManager).stop();
    }

    @Test
    void executeLaunchesProgressReporter() throws Exception {
        when(configLoader.load(anyString(), anyMap())).thenReturn(buildConfig());
        when(schemaApplier.sync(any())).thenReturn(List.of());

        cmd.execute("sync-config.yaml", Map.of());

        verify(progressReporter).start(progressRegistry);
    }

    @Test
    void executeWithEmptyTableListRegistersNothing() throws Exception {
        when(configLoader.load(anyString(), anyMap())).thenReturn(buildConfig());
        when(schemaApplier.sync(any())).thenReturn(List.of());

        cmd.execute("sync-config.yaml", Map.of());

        assertTrue(progressRegistry.getAll().isEmpty(), "No tables should be registered");
    }

    // ── helpers ──────────────────────────────────────────────────────────

    static SyncConfig buildConfig() {
        DatabaseConfig src = new DatabaseConfig();
        src.setType("mysql");
        src.setHost("localhost");
        src.setPort(3306);
        src.setDatabase("testdb");
        src.setUsername("root");
        src.setPassword("secret");

        DatabaseConfig tgt = new DatabaseConfig();
        tgt.setType("postgresql");
        tgt.setHost("localhost");
        tgt.setPort(5432);
        tgt.setDatabase("targetdb");
        tgt.setUsername("postgres");
        tgt.setPassword("secret");

        SyncOptions opts = new SyncOptions();

        SyncConfig config = new SyncConfig();
        config.setSource(src);
        config.setTarget(tgt);
        config.setSync(opts);
        return config;
    }
}
