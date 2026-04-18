package io.dbsync.cli;

import io.quarkus.arc.Unremovable;
import io.dbsync.config.ConfigLoader;
import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.schema.SchemaReader;
import io.dbsync.schema.TableDef;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CDI service that performs pre-flight validation for the {@code dbsync test} command.
 * <p>
 * Checks (in order):
 * <ol>
 *   <li>Config file can be loaded and passes {@link SyncConfig#validate()}</li>
 *   <li>Source database is reachable via JDBC</li>
 *   <li>Target database is reachable via JDBC</li>
 *   <li>Every table listed in {@code sync.tables} exists in the source
 *       (only when the list is non-empty)</li>
 *   <li>Target table presence is checked and a WARN is issued for each
 *       table that does not yet exist (it will be created by schema-sync)</li>
 * </ol>
 * Returns a list of {@link CheckResult}s that the caller can render however it likes.
 */
@ApplicationScoped
@Unremovable
public class TestCommand {

    private static final Logger LOG = Logger.getLogger(TestCommand.class);

    @Inject ConfigLoader configLoader;
    @Inject SchemaReader schemaReader;

    public List<CheckResult> run(String configFile, Map<String, String> overrides) {
        List<CheckResult> results = new ArrayList<>();

        // ── 1. Config load & validation ───────────────────────────────────
        SyncConfig config = checkConfig(configFile, overrides, results);
        if (config == null) return results;   // fatal – cannot continue

        // ── 2. Source connection ──────────────────────────────────────────
        boolean sourceOk = checkConnection("source", config.getSource(), results);

        // ── 3. Target connection ──────────────────────────────────────────
        boolean targetOk = checkConnection("target", config.getTarget(), results);

        // ── 4 & 5. Table mapping (requires both connections to be alive) ──
        List<String> configured = config.getSync().getTables();
        if (!configured.isEmpty()) {
            checkTableMappingSource(config, configured, results, sourceOk);
            checkTableMappingTarget(config, configured, results, targetOk);
        } else {
            results.add(CheckResult.pass("tables",
                    "No specific tables configured — all source tables will be synced"));
        }

        return results;
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /** Load + validate config; adds check results and returns the config or null on failure. */
    SyncConfig checkConfig(String configFile, Map<String, String> overrides,
                                   List<CheckResult> out) {
        try {
            SyncConfig cfg = configLoader.load(configFile, overrides);
            DatabaseConfig src = cfg.getSource();
            DatabaseConfig tgt = cfg.getTarget();

            List<String> summary = new ArrayList<>();
            summary.add("source: " + src.getType() + " @ " + src.getHost()
                    + ":" + src.getPort() + "/" + src.getDatabase());
            summary.add("target: " + tgt.getType() + " @ " + tgt.getHost()
                    + ":" + tgt.getPort() + "/" + tgt.getDatabase());

            List<String> tables = cfg.getSync().getTables();
            summary.add("tables: " + (tables.isEmpty() ? "(all)" : String.join(", ", tables)));
            summary.add("schema sync: " + (cfg.getSync().isSchemaSync() ? "ON" : "OFF"));
            summary.add("snapshot mode: " + cfg.getSync().getSnapshotMode());

            out.add(CheckResult.pass("config", "Config loaded and validated: " + configFile,
                    summary.toArray(new String[0])));
            return cfg;
        } catch (java.io.FileNotFoundException e) {
            out.add(CheckResult.fail("config", "Config file not found: " + configFile));
            return null;
        } catch (IllegalStateException e) {
            out.add(CheckResult.fail("config", "Config validation failed: " + e.getMessage()));
            return null;
        } catch (Exception e) {
            LOG.warnf(e, "Config load error");
            out.add(CheckResult.fail("config", "Config error: " + e.getMessage()));
            return null;
        }
    }

    /** Attempt JDBC connection; adds a PASS or FAIL result and returns success flag. */
    boolean checkConnection(String label, DatabaseConfig db, List<CheckResult> out) {
        long start = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(
                db.jdbcUrl(), db.getUsername(), db.getPassword())) {
            long ms = System.currentTimeMillis() - start;
            String version = conn.getMetaData().getDatabaseProductVersion();
            out.add(CheckResult.pass("connection." + label,
                    label + " connection OK (" + ms + "ms)",
                    "version: " + version,
                    "url: " + db.jdbcUrl()));
            return true;
        } catch (Exception e) {
            out.add(CheckResult.fail("connection." + label,
                    label + " connection FAILED: " + e.getMessage(),
                    "url: " + db.jdbcUrl()));
            return false;
        }
    }

    /** Check that every configured table exists in the source database. */
    private void checkTableMappingSource(SyncConfig config, List<String> configured,
                                         List<CheckResult> out, boolean sourceOk) {
        if (!sourceOk) {
            out.add(CheckResult.warn("tables.source",
                    "Source table check skipped (connection unavailable)"));
            return;
        }
        try {
            List<TableDef> found = schemaReader.readTables(config.getSource(), configured);
            Set<String> foundNames = found.stream()
                    .map(t -> t.getName().toLowerCase())
                    .collect(Collectors.toSet());

            List<String> missing = configured.stream()
                    .filter(t -> !foundNames.contains(t.toLowerCase()))
                    .collect(Collectors.toList());

            if (missing.isEmpty()) {
                List<String> details = found.stream()
                        .map(t -> t.getName() + " (" + t.getColumns().size() + " columns)")
                        .collect(Collectors.toList());
                out.add(CheckResult.pass("tables.source",
                        "All " + found.size() + " configured table(s) found in source",
                        details.toArray(new String[0])));
            } else {
                out.add(CheckResult.fail("tables.source",
                        "Missing in source: " + missing,
                        missing.stream().map(t -> "not found: " + t).toArray(String[]::new)));
            }
        } catch (Exception e) {
            out.add(CheckResult.fail("tables.source", "Source table check error: " + e.getMessage()));
        }
    }

    /** Check target table existence and WARN for tables that will need to be created. */
    private void checkTableMappingTarget(SyncConfig config, List<String> configured,
                                         List<CheckResult> out, boolean targetOk) {
        if (!targetOk) {
            out.add(CheckResult.warn("tables.target",
                    "Target table check skipped (connection unavailable)"));
            return;
        }
        try {
            // Use empty filter to read all target tables, then compare
            List<TableDef> found = schemaReader.readTables(config.getTarget(), configured);
            Set<String> foundNames = found.stream()
                    .map(t -> t.getName().toLowerCase())
                    .collect(Collectors.toSet());

            List<String> missing = configured.stream()
                    .filter(t -> !foundNames.contains(t.toLowerCase()))
                    .collect(Collectors.toList());

            if (missing.isEmpty()) {
                out.add(CheckResult.pass("tables.target",
                        "All " + found.size() + " configured table(s) already exist in target"));
            } else {
                boolean schemaSync = config.getSync().isSchemaSync();
                String msg = missing.size() + " table(s) missing in target"
                        + (schemaSync ? " (will be created by schema sync)" : " — schema sync is OFF");
                CheckResult.Status status = schemaSync
                        ? CheckResult.Status.WARN : CheckResult.Status.FAIL;
                out.add(new CheckResultBuilder(status, "tables.target", msg,
                        missing.stream().map(t -> "not in target: " + t).toArray(String[]::new)).build());
            }
        } catch (Exception e) {
            out.add(CheckResult.fail("tables.target", "Target table check error: " + e.getMessage()));
        }
    }

    /** Tiny builder to construct a CheckResult with arbitrary Status (avoids switch). */
    private static final class CheckResultBuilder {
        private final CheckResult.Status status;
        private final String id, msg;
        private final String[] details;

        CheckResultBuilder(CheckResult.Status status, String id, String msg, String[] details) {
            this.status = status; this.id = id; this.msg = msg; this.details = details;
        }

        CheckResult build() {
            return switch (status) {
                case PASS -> CheckResult.pass(id, msg, details);
                case WARN -> CheckResult.warn(id, msg, details);
                case FAIL -> CheckResult.fail(id, msg, details);
            };
        }
    }
}
