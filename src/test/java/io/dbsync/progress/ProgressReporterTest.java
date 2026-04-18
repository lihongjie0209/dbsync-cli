package io.dbsync.progress;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class ProgressReporterTest {

    @Inject ProgressReporter reporter;

    // ── formatElapsed ──────────────────────────────────────────────────────────

    @Test
    void formatElapsed_milliseconds() {
        assertEquals("500ms", reporter.formatElapsed(500));
    }

    @Test
    void formatElapsed_seconds() {
        assertEquals("3s ago", reporter.formatElapsed(3_500));
    }

    @Test
    void formatElapsed_minutes() {
        assertEquals("2m ago", reporter.formatElapsed(2 * 60_000 + 30_000));
    }

    @Test
    void formatElapsed_hours() {
        assertEquals("2h ago", reporter.formatElapsed(2 * 3_600_000L + 100));
    }

    // ── formatTableLine ────────────────────────────────────────────────────────

    @Test
    void formatTableLine_initializing() {
        TableSyncState s = new TableSyncState("users");
        s.setPhase(SyncPhase.INITIALIZING);

        String line = reporter.formatTableLine(s);
        assertTrue(line.contains("users"), "should contain table name");
        assertTrue(line.contains("INITIALIZING"), "should contain phase");
        assertTrue(line.contains("ins=0"), "should contain zero inserts");
    }

    @Test
    void formatTableLine_snapshot_with_progress() {
        TableSyncState s = new TableSyncState("orders");
        s.setPhase(SyncPhase.SNAPSHOT);
        s.setSnapshotTotal(1000);
        s.getSnapshotScanned().set(250);

        String line = reporter.formatTableLine(s);
        assertTrue(line.contains("SNAPSHOT"), "should contain phase");
        assertTrue(line.contains("25.0%"), "should contain snapshot %");
    }

    @Test
    void formatTableLine_cdc_counts() {
        TableSyncState s = new TableSyncState("products");
        s.setPhase(SyncPhase.CDC);
        s.getCdcInserts().set(42);
        s.getCdcUpdates().set(7);
        s.getCdcDeletes().set(3);

        String line = reporter.formatTableLine(s);
        assertTrue(line.contains("CDC"),   "phase");
        assertTrue(line.contains("ins=42"), "inserts");
        assertTrue(line.contains("upd=7"),  "updates");
        assertTrue(line.contains("del=3"),  "deletes");
    }

    @Test
    void formatTableLine_done_no_last_event() {
        TableSyncState s = new TableSyncState("audit");
        s.setPhase(SyncPhase.DONE);
        s.getCdcInserts().set(100);

        String line = reporter.formatTableLine(s);
        assertTrue(line.contains("DONE"),   "phase");
        assertTrue(line.contains("ins=100"), "inserts");
        // last event not set → no "last=" suffix
        assertFalse(line.contains("last="), "should not have last= when not set");
    }

    @Test
    void formatTableLine_includes_last_event_when_set() {
        TableSyncState s = new TableSyncState("events");
        s.setPhase(SyncPhase.CDC);
        // set lastEventMs to 5 seconds ago
        s.setLastEventMs(System.currentTimeMillis() - 5_000);

        String line = reporter.formatTableLine(s);
        assertTrue(line.contains("last="), "should include last= when event was seen");
    }
}
