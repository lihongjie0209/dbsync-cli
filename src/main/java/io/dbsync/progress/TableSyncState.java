package io.dbsync.progress;

import java.util.concurrent.atomic.AtomicLong;

public class TableSyncState {

    private final String tableName;
    private volatile SyncPhase phase = SyncPhase.INITIALIZING;

    private final AtomicLong snapshotScanned = new AtomicLong(0);
    private volatile long snapshotTotal = -1; // -1 = unknown

    private final AtomicLong cdcInserts = new AtomicLong(0);
    private final AtomicLong cdcUpdates = new AtomicLong(0);
    private final AtomicLong cdcDeletes = new AtomicLong(0);

    private volatile long lastEventMs = 0;
    private volatile String errorMessage;

    public TableSyncState(String tableName) {
        this.tableName = tableName;
    }

    // --- snapshot helpers -------------------------------------------------

    public double snapshotProgressPct() {
        if (snapshotTotal <= 0) return -1;
        return (double) snapshotScanned.get() / snapshotTotal * 100.0;
    }

    public String snapshotLabel() {
        long scanned = snapshotScanned.get();
        if (snapshotTotal <= 0) {
            return scanned + " rows";
        }
        return String.format("%.1f%% (%,d/%,d)", snapshotProgressPct(), scanned, snapshotTotal);
    }

    // --- CDC throughput ---------------------------------------------------

    public long totalCdcEvents() {
        return cdcInserts.get() + cdcUpdates.get() + cdcDeletes.get();
    }

    // --- accessors --------------------------------------------------------

    public String getTableName() { return tableName; }

    public SyncPhase getPhase() { return phase; }
    public void setPhase(SyncPhase phase) { this.phase = phase; }

    public AtomicLong getSnapshotScanned() { return snapshotScanned; }

    public long getSnapshotTotal() { return snapshotTotal; }
    public void setSnapshotTotal(long snapshotTotal) { this.snapshotTotal = snapshotTotal; }

    public AtomicLong getCdcInserts() { return cdcInserts; }
    public AtomicLong getCdcUpdates() { return cdcUpdates; }
    public AtomicLong getCdcDeletes() { return cdcDeletes; }

    public long getLastEventMs() { return lastEventMs; }
    public void setLastEventMs(long lastEventMs) { this.lastEventMs = lastEventMs; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
