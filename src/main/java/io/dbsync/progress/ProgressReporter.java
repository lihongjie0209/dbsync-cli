package io.dbsync.progress;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically writes a sync-status summary to the log.
 * Replaces the TUI dashboard — works in any environment without
 * terminal or ANSI support.
 *
 * <pre>
 * [SYNC]  12:34:56  tables: 3
 *   orders    SNAPSHOT  1,234 / 5,678 (21%)  ins=0  upd=0  del=0
 *   products  CDC       done                 ins=42 upd=8  del=2  last=3s ago
 *   users     DONE      done                 ins=100 upd=0 del=0
 * </pre>
 *
 * Call {@link #start} to begin reporting; it blocks until the JVM
 * receives a shutdown signal (Ctrl+C / SIGTERM).
 */
@ApplicationScoped
public class ProgressReporter {

    private static final Logger log = Logger.getLogger(ProgressReporter.class);

    /** How often to emit a status line (seconds). */
    private static final int INTERVAL_SEC = 15;

    /**
     * Start periodic reporting and block until shutdown.
     * Registers a JVM shutdown hook so Ctrl+C triggers a clean stop.
     */
    public void start(SyncProgressRegistry registry) throws InterruptedException {
        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown, "progress-shutdown"));

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "progress-reporter");
            t.setDaemon(true);
            return t;
        });

        // Emit immediately, then every INTERVAL_SEC seconds
        scheduler.scheduleAtFixedRate(
                () -> report(registry), 0, INTERVAL_SEC, TimeUnit.SECONDS);

        // Block until Ctrl+C / SIGTERM
        stop.await();

        scheduler.shutdown();
        // Final report on exit
        report(registry);
        log.info("[SYNC] Shutting down — final status above.");
    }

    // ------------------------------------------------------------------

    private void report(SyncProgressRegistry registry) {
        Collection<TableSyncState> all = registry.getAll();
        if (all.isEmpty()) {
            log.info("[SYNC] No tables registered yet.");
            return;
        }

        List<TableSyncState> sorted = new ArrayList<>(all);
        sorted.sort(Comparator.comparing(TableSyncState::getTableName));

        String now = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        long totalCdc = sorted.stream().mapToLong(TableSyncState::totalCdcEvents).sum();

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[SYNC]  %s  tables: %d  total-cdc: %,d%n",
                now, sorted.size(), totalCdc));

        for (TableSyncState s : sorted) {
            sb.append("  ").append(formatTableLine(s)).append('\n');
        }

        log.info(sb.toString().stripTrailing());
    }

    // Package-private for unit tests ────────────────────────────────────

    String formatTableLine(TableSyncState s) {
        String snap = s.getPhase() == SyncPhase.INITIALIZING ? "-" : s.snapshotLabel();
        String last = s.getLastEventMs() > 0
                ? "last=" + formatElapsed(System.currentTimeMillis() - s.getLastEventMs())
                : "";
        return String.format("  %-28s %-12s %-22s ins=%-6d upd=%-6d del=%-6d %s",
                s.getTableName(),
                s.getPhase().name(),
                snap,
                s.getCdcInserts().get(),
                s.getCdcUpdates().get(),
                s.getCdcDeletes().get(),
                last).stripTrailing();
    }

    String formatElapsed(long ms) {
        if (ms < 1_000)     return ms + "ms";
        if (ms < 60_000)    return (ms / 1_000) + "s ago";
        if (ms < 3_600_000) return (ms / 60_000) + "m ago";
        return (ms / 3_600_000) + "h ago";
    }
}
