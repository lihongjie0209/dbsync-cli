package io.dbsync.progress;

public enum SyncPhase {
    INITIALIZING,
    SNAPSHOT,
    CDC,
    DONE,
    ERROR
}
