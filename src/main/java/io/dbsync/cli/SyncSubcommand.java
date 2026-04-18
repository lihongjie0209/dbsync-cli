package io.dbsync.cli;

import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * {@code dbsync sync} — run a full snapshot + CDC sync as described in a config file.
 */
@Command(
    name        = "sync",
    description = "Run database sync (snapshot + CDC) using the supplied config file.",
    mixinStandardHelpOptions = true
)
@Dependent
@Unremovable
public class SyncSubcommand implements Callable<Integer> {

    // ── Config file ───────────────────────────────────────────────────────
    @Option(names = {"-c", "--config"},
            description = "Path to sync-config.yaml (default: sync-config.yaml)",
            defaultValue = "sync-config.yaml")
    String configFile;

    // ── Source overrides ──────────────────────────────────────────────────
    @Option(names = "--source-host",     description = "Override source host")
    String sourceHost;
    @Option(names = "--source-port",     description = "Override source port")
    Integer sourcePort;
    @Option(names = "--source-database", description = "Override source database name")
    String sourceDatabase;
    @Option(names = "--source-user",     description = "Override source username")
    String sourceUser;
    @Option(names = "--source-password", description = "Override source password")
    String sourcePassword;

    // ── Target overrides ──────────────────────────────────────────────────
    @Option(names = "--target-host",     description = "Override target host")
    String targetHost;
    @Option(names = "--target-port",     description = "Override target port")
    Integer targetPort;
    @Option(names = "--target-database", description = "Override target database name")
    String targetDatabase;
    @Option(names = "--target-user",     description = "Override target username")
    String targetUser;
    @Option(names = "--target-password", description = "Override target password")
    String targetPassword;

    @Inject
    SyncCommand syncCommand;

    @Override
    public Integer call() throws Exception {
        return syncCommand.execute(configFile, buildOverrides());
    }

    private Map<String, String> buildOverrides() {
        Map<String, String> m = new HashMap<>();
        if (sourceHost     != null) m.put("source.host",     sourceHost);
        if (sourcePort     != null) m.put("source.port",     String.valueOf(sourcePort));
        if (sourceDatabase != null) m.put("source.database", sourceDatabase);
        if (sourceUser     != null) m.put("source.username", sourceUser);
        if (sourcePassword != null) m.put("source.password", sourcePassword);
        if (targetHost     != null) m.put("target.host",     targetHost);
        if (targetPort     != null) m.put("target.port",     String.valueOf(targetPort));
        if (targetDatabase != null) m.put("target.database", targetDatabase);
        if (targetUser     != null) m.put("target.username", targetUser);
        if (targetPassword != null) m.put("target.password", targetPassword);
        return m;
    }
}
