package io.dbsync.integration;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.engine.DebeziumEngineManager;
import io.dbsync.progress.SyncProgressRegistry;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end integration test for KingbaseES → PostgreSQL sync.
 *
 * <p>Uses KingbaseES V9 as a Debezium source (PostgreSQL connector + {@code decoderbufs}).
 *
 * <p><b>Snapshot only</b>: KingbaseES V9's bundled {@code decoderbufs} plugin produces
 * binary messages with non-standard type codes that Debezium 2.7.3 cannot decode, and
 * neither {@code test_decoding} nor {@code wal2json} is available in the image.
 * This test validates snapshot sync only; ongoing CDC is not supported.
 *
 * <p>Skipped automatically if the Kingbase image is not available locally.
 */
@DisplayName("KingbaseES → PostgreSQL sync")
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KingbaseToPgSyncIT extends AbstractSyncIT {

    private static final String KB_IMAGE = "kingbase_v009r001c010b0004_single_x86:v1";
    private static final String KB_DB    = "test";

    @Container
    static final PostgreSQLContainer<?> target = pgTarget("postgres:16-alpine");

    private GenericContainer<?> source;

    // ── Setup ─────────────────────────────────────────────────────────────

    @BeforeAll
    void setup() throws Exception {
        assumeTrue(isDockerImageAvailable(KB_IMAGE),
                "Skipping KingbaseES→PostgreSQL tests: image not available locally: " + KB_IMAGE);

        source = kingbaseSource(KB_IMAGE);
        source.start();

        enableKingbaseLogicalReplication(source);

        try (Connection c = kbConn()) {
            createPgStaffTable(c);
            seedStaff(c);
        }
    }

    @AfterAll
    void teardown() {
        if (source != null) source.stop();
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    @DisplayName("Schema sync + snapshot → PostgreSQL (CDC not supported for KingbaseES source)")
    void snapshotSync() throws Exception {
        DatabaseConfig srcCfg = dbConfigKingbase(source, KB_DB);
        DatabaseConfig tgtCfg = dbConfig(target);
        Path workDir = Files.createTempDirectory("dbsync-kingbase-pg-");
        SyncConfig cfg = syncConfig(srcCfg, tgtCfg, List.of(STAFF_TABLE), workDir);

        buildApplier().sync(cfg);

        SyncProgressRegistry registry = new SyncProgressRegistry();
        DebeziumEngineManager engine = new DebeziumEngineManager();
        try {
            engine.start(cfg, registry);

            // Wait for snapshot to complete (engine uses initial_only mode for Kingbase source)
            waitFor(() -> snapshotComplete(registry, STAFF_TABLE, 3), SNAPSHOT_TIMEOUT_SEC);

            try (Connection tgt = pgConn()) {
                assertRowCount(tgt, STAFF_TABLE, 3, "Snapshot: 3 rows from KingbaseES → PostgreSQL");
                long aliceScore = queryLong(tgt, "SELECT score FROM staff WHERE name='Alice'");
                Assertions.assertEquals(90, aliceScore, "Alice's score correctly synced from KingbaseES");
            }
        } finally {
            engine.stop();
        }
    }

    // ── JDBC helpers ──────────────────────────────────────────────────────

    private Connection kbConn() throws Exception {
        DatabaseConfig cfg = dbConfigKingbase(source, KB_DB);
        return DriverManager.getConnection(cfg.jdbcUrl(), cfg.getUsername(), cfg.getPassword());
    }

    private Connection pgConn() throws Exception {
        return DriverManager.getConnection(target.getJdbcUrl(),
                target.getUsername(), target.getPassword());
    }
}
