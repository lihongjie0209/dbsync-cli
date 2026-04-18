package io.dbsync.progress;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class SyncProgressRegistry {

    private final ConcurrentMap<String, TableSyncState> states = new ConcurrentHashMap<>();

    public void register(String tableName) {
        states.putIfAbsent(tableName, new TableSyncState(tableName));
    }

    public void setSnapshotTotal(String tableName, long total) {
        getOrCreate(tableName).setSnapshotTotal(total);
    }

    public void markSnapshotRow(String tableName) {
        TableSyncState s = getOrCreate(tableName);
        s.getSnapshotScanned().incrementAndGet();
        s.setPhase(SyncPhase.SNAPSHOT);
        s.setLastEventMs(System.currentTimeMillis());
    }

    public void markCdcEvent(String tableName, String op) {
        TableSyncState s = getOrCreate(tableName);
        s.setPhase(SyncPhase.CDC);
        switch (op) {
            case "INSERT" -> s.getCdcInserts().incrementAndGet();
            case "UPDATE" -> s.getCdcUpdates().incrementAndGet();
            case "DELETE" -> s.getCdcDeletes().incrementAndGet();
        }
        s.setLastEventMs(System.currentTimeMillis());
    }

    public void markError(String tableName, String message) {
        TableSyncState s = getOrCreate(tableName);
        s.setPhase(SyncPhase.ERROR);
        s.setErrorMessage(message);
    }

    public void markDone(String tableName) {
        getOrCreate(tableName).setPhase(SyncPhase.DONE);
    }

    public Collection<TableSyncState> getAll() {
        return states.values();
    }

    public TableSyncState get(String tableName) {
        return states.get(tableName);
    }

    private TableSyncState getOrCreate(String tableName) {
        return states.computeIfAbsent(tableName, TableSyncState::new);
    }
}
