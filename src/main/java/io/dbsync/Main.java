package io.dbsync;

import io.dbsync.cli.SyncSubcommand;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Root entry point — routes to {@code sync} or {@code test} subcommands.
 * <pre>
 *   dbsync sync  -c config.yaml   # snapshot + CDC
 *   dbsync test  -c config.yaml   # validate config &amp; connections
 * </pre>
 */
@QuarkusMain
@Command(
    name        = "dbsync",
    description = "Sync databases using Debezium Embedded (snapshot + CDC). Supports MySQL ↔ PostgreSQL.",
    mixinStandardHelpOptions = true,
    version     = "1.0.0",
    subcommands = {
        SyncSubcommand.class,
        io.dbsync.cli.TestSubcommand.class
    }
)
public class Main implements QuarkusApplication {

    @Inject
    CommandLine.IFactory factory;

    @Override
    public int run(String... args) throws Exception {
        CommandLine cmd = new CommandLine(this, factory);
        if (args.length == 0) {
            cmd.usage(System.out);
            return 0;
        }
        return cmd.execute(args);
    }
}

