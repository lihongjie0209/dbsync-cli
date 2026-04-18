package io.dbsync.progress;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableSyncStateTest {

    @Test
    void initialStateIsInitializing() {
        TableSyncState s = new TableSyncState("users");
        assertEquals(SyncPhase.INITIALIZING, s.getPhase());
    }

    @Test
    void tableNameIsPreserved() {
        assertEquals("orders", new TableSyncState("orders").getTableName());
    }

    @Test
    void snapshotProgressTracking() {
        TableSyncState s = new TableSyncState("t");
        s.setSnapshotTotal(1000L);
        s.setPhase(SyncPhase.SNAPSHOT);

        s.getSnapshotScanned().addAndGet(250);
        s.getSnapshotScanned().addAndGet(250);

        assertEquals(SyncPhase.SNAPSHOT, s.getPhase());
        assertEquals(500L,  s.getSnapshotScanned().get());
        assertEquals(1000L, s.getSnapshotTotal());
    }

    @Test
    void cdcEventCountTracking() {
        TableSyncState s = new TableSyncState("t");
        s.setPhase(SyncPhase.CDC);
        s.getCdcInserts().incrementAndGet();
        s.getCdcUpdates().incrementAndGet();
        s.getCdcDeletes().incrementAndGet();
        assertEquals(3L, s.totalCdcEvents());
    }

    @Test
    void phaseTransitions() {
        TableSyncState s = new TableSyncState("t");
        s.setPhase(SyncPhase.SNAPSHOT);
        assertEquals(SyncPhase.SNAPSHOT, s.getPhase());
        s.setPhase(SyncPhase.CDC);
        assertEquals(SyncPhase.CDC, s.getPhase());
        s.setPhase(SyncPhase.DONE);
        assertEquals(SyncPhase.DONE, s.getPhase());
        s.setPhase(SyncPhase.ERROR);
        assertEquals(SyncPhase.ERROR, s.getPhase());
    }

    @Test
    void lastEventMsTracking() {
        long before = System.currentTimeMillis();
        TableSyncState s = new TableSyncState("t");
        s.setLastEventMs(before);
        assertEquals(before, s.getLastEventMs());
    }

    @Test
    void errorMessageGetterSetter() {
        TableSyncState s = new TableSyncState("t");
        s.setErrorMessage("some error");
        assertEquals("some error", s.getErrorMessage());
    }

    @Test
    void zeroInitialCounters() {
        TableSyncState s = new TableSyncState("t");
        assertEquals(0L, s.getSnapshotScanned().get());
        assertEquals(0L, s.totalCdcEvents());
        assertEquals(-1L, s.getSnapshotTotal()); // -1 means unknown
    }

    @Test
    void snapshotProgressPctUnknownWhenTotalNegative() {
        TableSyncState s = new TableSyncState("t");
        assertEquals(-1.0, s.snapshotProgressPct(), 0.001);
    }

    @Test
    void snapshotProgressPctCalculation() {
        TableSyncState s = new TableSyncState("t");
        s.setSnapshotTotal(100L);
        s.getSnapshotScanned().set(50L);
        assertEquals(50.0, s.snapshotProgressPct(), 0.001);
    }

    @Test
    void snapshotLabelUnknownTotal() {
        TableSyncState s = new TableSyncState("t");
        s.getSnapshotScanned().set(42L);
        assertEquals("42 rows", s.snapshotLabel());
    }

    @Test
    void snapshotLabelWithKnownTotal() {
        TableSyncState s = new TableSyncState("t");
        s.setSnapshotTotal(1000L);
        s.getSnapshotScanned().set(500L);
        assertTrue(s.snapshotLabel().contains("50.0%"));
    }

    @Test
    void totalCdcEventsAllOps() {
        TableSyncState s = new TableSyncState("t");
        s.getCdcInserts().set(10);
        s.getCdcUpdates().set(5);
        s.getCdcDeletes().set(3);
        assertEquals(18L, s.totalCdcEvents());
    }
}

