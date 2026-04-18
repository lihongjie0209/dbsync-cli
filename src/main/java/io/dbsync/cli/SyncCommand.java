package io.dbsync.cli;

import io.dbsync.config.ConfigLoader;
import io.dbsync.config.SyncConfig;
import io.dbsync.engine.DebeziumEngineManager;
import io.dbsync.progress.ProgressReporter;
import io.dbsync.progress.SyncProgressRegistry;
import io.dbsync.schema.SchemaApplier;
import io.dbsync.schema.TableDef;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;

/**
 * CDI service that orchestrates the full sync lifecycle:
 * schema sync → Debezium engine → progress reporter.
 */
@ApplicationScoped
public class SyncCommand {

    private static final Logger log = Logger.getLogger(SyncCommand.class);

    @Inject ConfigLoader          configLoader;
    @Inject SchemaApplier         schemaApplier;
    @Inject SyncProgressRegistry  progressRegistry;
    @Inject DebeziumEngineManager engineManager;
    @Inject ProgressReporter      progressReporter;

    public int execute(String configFile, Map<String, String> overrides) throws Exception {
        log.infof("Loading config from: %s", configFile);
        SyncConfig config = configLoader.load(configFile, overrides);

        log.infof("Source: %s @ %s:%d/%s",
                config.getSource().getType(), config.getSource().getHost(),
                config.getSource().getPort(), config.getSource().getDatabase());
        log.infof("Target: %s @ %s:%d/%s",
                config.getTarget().getType(), config.getTarget().getHost(),
                config.getTarget().getPort(), config.getTarget().getDatabase());

        // 1. Schema sync
        log.info("Step 1/2: Synchronising schema...");
        List<TableDef> tables = schemaApplier.sync(config);
        tables.forEach(t -> progressRegistry.register(t.getName()));

        // 2. Start Debezium engine (snapshot then CDC)
        log.info("Step 2/2: Starting Debezium engine...");
        engineManager.start(config, progressRegistry);

        // Block and report progress every 15 s; exits on Ctrl+C / SIGTERM
        log.info("Sync running — status logged every 15 s.  Press Ctrl+C to stop.");
        progressReporter.start(progressRegistry);

        // Graceful shutdown
        engineManager.stop();
        return 0;
    }
}
