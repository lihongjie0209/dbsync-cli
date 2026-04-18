package io.dbsync.config;

public class SyncConfig {

    private DatabaseConfig source;
    private DatabaseConfig target;
    private SyncOptions sync = new SyncOptions();

    public DatabaseConfig getSource() { return source; }
    public void setSource(DatabaseConfig source) { this.source = source; }

    public DatabaseConfig getTarget() { return target; }
    public void setTarget(DatabaseConfig target) { this.target = target; }

    public SyncOptions getSync() { return sync; }
    public void setSync(SyncOptions sync) { this.sync = sync; }

    public void validate() {
        if (source == null) throw new IllegalStateException("source configuration is required");
        if (target == null) throw new IllegalStateException("target configuration is required");
        if (source.getType() == null) throw new IllegalStateException("source.type is required (mysql|postgresql)");
        if (target.getType() == null) throw new IllegalStateException("target.type is required (mysql|postgresql)");
        if (source.getDatabase() == null) throw new IllegalStateException("source.database is required");
        if (target.getDatabase() == null) throw new IllegalStateException("target.database is required");
    }
}
