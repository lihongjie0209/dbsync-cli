package io.dbsync.config;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.ArrayList;
import java.util.List;

@RegisterForReflection
public class SyncOptions {

    /** Tables to sync. Empty = sync all tables. */
    private List<String> tables = new ArrayList<>();

    /** Automatically create/alter target tables to match source schema. */
    private boolean schemaSync = true;

    /** Debezium snapshot mode: initial | never | schema_only | always */
    private String snapshotMode = "initial";

    /** Batch size for bulk writes to target. */
    private int batchSize = 500;

    /** Path to Debezium offset storage file. */
    private String offsetStorePath = "./offsets.dat";

    /** Path to Debezium schema history file (MySQL only). */
    private String schemaHistoryPath = "./schema-history.dat";

    /**
     * After snapshot completes, delete rows from target that no longer exist in source
     * ("zombie rows" left from previous sync runs). Defaults to true.
     */
    private boolean cleanupOrphans = true;

    public List<String> getTables() { return tables; }
    public void setTables(List<String> tables) { this.tables = tables; }

    public boolean isSchemaSync() { return schemaSync; }
    public void setSchemaSync(boolean schemaSync) { this.schemaSync = schemaSync; }

    public String getSnapshotMode() { return snapshotMode; }
    public void setSnapshotMode(String snapshotMode) { this.snapshotMode = snapshotMode; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public String getOffsetStorePath() { return offsetStorePath; }
    public void setOffsetStorePath(String offsetStorePath) { this.offsetStorePath = offsetStorePath; }

    public String getSchemaHistoryPath() { return schemaHistoryPath; }
    public void setSchemaHistoryPath(String schemaHistoryPath) { this.schemaHistoryPath = schemaHistoryPath; }

    public boolean isCleanupOrphans() { return cleanupOrphans; }
    public void setCleanupOrphans(boolean cleanupOrphans) { this.cleanupOrphans = cleanupOrphans; }
}
