package io.dbsync.progress;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class SyncProgressRegistryTest {

    @Test
    void registerAndRetrieveTable() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("users");
        TableSyncState s = r.get("users");
        assertNotNull(s);
        assertEquals("users", s.getTableName());
    }

    @Test
    void registerSameTableReturnsSameInstance() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("orders");
        TableSyncState s1 = r.get("orders");
        r.register("orders");
        TableSyncState s2 = r.get("orders");
        assertSame(s1, s2);
    }

    @Test
    void getNonexistentReturnsNull() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        assertNull(r.get("no_such_table"));
    }

    @Test
    void getAllTablesReturnsAllRegistered() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("a");
        r.register("b");
        r.register("c");
        Collection<TableSyncState> all = r.getAll();
        assertEquals(3, all.size());
    }

    @Test
    void markSnapshotRowIncrementsCounter() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("t");
        r.markSnapshotRow("t");
        r.markSnapshotRow("t");
        assertEquals(2L, r.get("t").getSnapshotScanned().get());
    }

    @Test
    void markSnapshotRowSetsPhase() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.markSnapshotRow("t");
        assertEquals(SyncPhase.SNAPSHOT, r.get("t").getPhase());
    }

    @Test
    void markCdcEventInsert() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.markCdcEvent("t", "INSERT");
        r.markCdcEvent("t", "INSERT");
        assertEquals(2L, r.get("t").getCdcInserts().get());
        assertEquals(SyncPhase.CDC, r.get("t").getPhase());
    }

    @Test
    void markCdcEventUpdate() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.markCdcEvent("t", "UPDATE");
        assertEquals(1L, r.get("t").getCdcUpdates().get());
    }

    @Test
    void markCdcEventDelete() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.markCdcEvent("t", "DELETE");
        assertEquals(1L, r.get("t").getCdcDeletes().get());
    }

    @Test
    void markErrorSetsPhaseAndMessage() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("t");
        r.markError("t", "oops");
        assertEquals(SyncPhase.ERROR, r.get("t").getPhase());
        assertEquals("oops", r.get("t").getErrorMessage());
    }

    @Test
    void markDoneSetsPhase() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("t");
        r.markDone("t");
        assertEquals(SyncPhase.DONE, r.get("t").getPhase());
    }

    @Test
    void setSnapshotTotal() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("t");
        r.setSnapshotTotal("t", 500L);
        assertEquals(500L, r.get("t").getSnapshotTotal());
    }

    @Test
    void markSnapshotRowAutoCreatesEntry() {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.markSnapshotRow("new_table");
        assertNotNull(r.get("new_table"));
        assertEquals(1L, r.get("new_table").getSnapshotScanned().get());
    }

    @Test
    void concurrentMarkCdcEventsSumCorrectly() throws InterruptedException {
        SyncProgressRegistry r = new SyncProgressRegistry();
        r.register("t");
        int threads = 10, perThread = 1000;
        var latch = new CountDownLatch(threads);
        var pool  = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                for (int j = 0; j < perThread; j++) r.markCdcEvent("t", "INSERT");
                latch.countDown();
            });
        }
        latch.await();
        pool.shutdown();
        assertEquals((long) threads * perThread, r.get("t").getCdcInserts().get());
    }
}

