package io.dbsync.engine;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.config.SyncOptions;
import io.dbsync.progress.SyncProgressRegistry;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ApplicationScoped
public class DebeziumEngineManager {

    private static final Logger log = Logger.getLogger(DebeziumEngineManager.class);

    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;
    private ChangeEventHandler handler;

    public void start(SyncConfig config, SyncProgressRegistry registry) {
        handler = new ChangeEventHandler(config, registry);
        Properties props = buildConnectorProps(config);

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(handler)
                .using(new DebeziumEngine.ConnectorCallback() {
                    @Override public void taskStarted() { log.info("Debezium connector task started"); }
                    @Override public void taskStopped()  { log.info("Debezium connector task stopped"); }
                })
                .using((success, message, error) -> {
                    if (!success) {
                        log.errorf(error, "Debezium engine error: %s", message);
                    } else {
                        log.infof("Debezium engine finished: %s", message);
                    }
                })
                .build();

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "debezium-engine");
            t.setDaemon(false);
            return t;
        });
        executor.execute(engine);
        log.info("Debezium engine submitted to executor");
    }

    public void stop() throws Exception {
        log.info("Stopping Debezium engine...");
        if (engine != null) engine.close();
        if (handler != null) handler.close();
        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    // ------------------------------------------------------------------

    // Package-private for unit testing
    Properties buildConnectorProps(SyncConfig config) {
        Properties p = new Properties();
        DatabaseConfig src = config.getSource();
        SyncOptions   opts = config.getSync();

        p.setProperty("name", "dbsync-connector");

        switch (src.getType()) {
            case "mysql" -> {
                p.setProperty("connector.class",        "io.debezium.connector.mysql.MySqlConnector");
                p.setProperty("database.hostname",      src.getHost());
                p.setProperty("database.port",          String.valueOf(src.getPort()));
                p.setProperty("database.user",          src.getUsername());
                p.setProperty("database.password",      src.getPassword());
                p.setProperty("database.server.id",     String.valueOf(src.getServerId()));
                p.setProperty("topic.prefix",           "dbsync");
                p.setProperty("database.include.list",  src.getDatabase());
                // MySQL 8.4 removed SHOW MASTER STATUS; enabling GTID handling lets the connector
                // use SELECT @@global.gtid_executed instead, which works across 8.0 and 8.4.
                p.setProperty("gtid.source.includes.ddl.changes", "false");
                if (!opts.getTables().isEmpty()) {
                    String list = opts.getTables().stream()
                            .map(t -> src.getDatabase() + "." + t)
                            .collect(Collectors.joining(","));
                    p.setProperty("table.include.list", list);
                }
                // Schema history (required for MySQL connector)
                p.setProperty("schema.history.internal",
                        "io.debezium.storage.file.history.FileSchemaHistory");
                p.setProperty("schema.history.internal.file.filename",
                        opts.getSchemaHistoryPath());
            }
            case "mariadb" -> {
                p.setProperty("connector.class",        "io.debezium.connector.mariadb.MariaDbConnector");
                p.setProperty("database.hostname",      src.getHost());
                p.setProperty("database.port",          String.valueOf(src.getPort()));
                p.setProperty("database.user",          src.getUsername());
                p.setProperty("database.password",      src.getPassword());
                p.setProperty("database.server.id",     String.valueOf(src.getServerId()));
                p.setProperty("topic.prefix",           "dbsync");
                p.setProperty("database.include.list",  src.getDatabase());
                // MariaDB JDBC driver uses different SSL mode values from MySQL connector/J.
                // Debezium's MariaDB connector builds its internal URL with MySQL-style params
                // (e.g. sslMode=preferred) which the MariaDB driver rejects; disable SSL to avoid this.
                p.setProperty("database.ssl.mode",      "disabled");
                if (!opts.getTables().isEmpty()) {
                    String list = opts.getTables().stream()
                            .map(t -> src.getDatabase() + "." + t)
                            .collect(Collectors.joining(","));
                    p.setProperty("table.include.list", list);
                }
                // Schema history (required for MariaDB connector)
                p.setProperty("schema.history.internal",
                        "io.debezium.storage.file.history.FileSchemaHistory");
                p.setProperty("schema.history.internal.file.filename",
                        opts.getSchemaHistoryPath());
            }
            case "postgresql" -> {
                p.setProperty("connector.class",     "io.debezium.connector.postgresql.PostgresConnector");
                p.setProperty("database.hostname",   src.getHost());
                p.setProperty("database.port",       String.valueOf(src.getPort()));
                p.setProperty("database.user",       src.getUsername());
                p.setProperty("database.password",   src.getPassword());
                p.setProperty("database.dbname",     src.getDatabase());
                p.setProperty("topic.prefix",        "dbsync");
                p.setProperty("slot.name",           src.getSlotName());
                p.setProperty("plugin.name",         src.getPluginName());
                if (!opts.getTables().isEmpty()) {
                    String list = opts.getTables().stream()
                            .map(t -> "public." + t)
                            .collect(Collectors.joining(","));
                    p.setProperty("table.include.list", list);
                }
            }
            // KingbaseES is PostgreSQL-compatible; uses the Debezium PostgreSQL connector.
            // KingbaseES V9's decoderbufs plugin produces binary messages with non-standard
            // type codes that Debezium cannot decode.  Neither test_decoding nor wal2json is
            // available in the bundled image.  As a result CDC streaming is not supported;
            // we force snapshot.mode=initial_only so Debezium completes a clean full snapshot
            // and then stops rather than hanging indefinitely waiting for replication events.
            case "kingbase" -> {
                p.setProperty("connector.class",     "io.debezium.connector.postgresql.PostgresConnector");
                p.setProperty("database.hostname",   src.getHost());
                p.setProperty("database.port",       String.valueOf(src.getPort()));
                p.setProperty("database.user",       src.getUsername());
                p.setProperty("database.password",   src.getPassword());
                p.setProperty("database.dbname",     src.getDatabase());
                p.setProperty("topic.prefix",        "dbsync");
                p.setProperty("slot.name",           src.getSlotName());
                p.setProperty("plugin.name",         src.getPluginName()); // decoderbufs by default
                if (!opts.getTables().isEmpty()) {
                    String list = opts.getTables().stream()
                            .map(t -> "public." + t)
                            .collect(Collectors.joining(","));
                    p.setProperty("table.include.list", list);
                }
            }
            default -> throw new IllegalArgumentException("Unsupported source type: " + src.getType());
        }

        // For Kingbase source, override with snapshot-only (CDC streaming not supported);
        // for all other sources use the configured snapshot mode.
        if ("kingbase".equals(src.getType())) {
            p.setProperty("snapshot.mode", "initial_only");
        } else {
            p.setProperty("snapshot.mode", opts.getSnapshotMode());
        }
        // Use Kafka Connect's built-in file offset store (debezium-storage-file 2.7.x
        // only ships FileSchemaHistory; the offset store lives in connect-runtime)
        p.setProperty("offset.storage",               "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        p.setProperty("offset.storage.file.filename", opts.getOffsetStorePath());
        p.setProperty("offset.flush.interval.ms",     "1000");

        return p;
    }
}
