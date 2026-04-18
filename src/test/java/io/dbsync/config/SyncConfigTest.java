package io.dbsync.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SyncConfigTest {

    @Test
    void validateThrowsWhenSourceNull() {
        SyncConfig c = new SyncConfig();
        assertThrows(IllegalStateException.class, c::validate);
    }

    @Test
    void validateThrowsWhenTargetNull() {
        SyncConfig c = new SyncConfig();
        c.setSource(new DatabaseConfig());
        assertThrows(IllegalStateException.class, c::validate);
    }

    @Test
    void validateThrowsWhenSourceTypeNull() {
        SyncConfig c = new SyncConfig();
        c.setSource(new DatabaseConfig());
        c.setTarget(new DatabaseConfig());
        // source.type is null
        assertThrows(IllegalStateException.class, c::validate);
    }

    @Test
    void validateThrowsWhenTargetTypeNull() {
        SyncConfig c = new SyncConfig();
        DatabaseConfig src = new DatabaseConfig();
        src.setType("mysql");
        src.setDatabase("db");
        c.setSource(src);
        c.setTarget(new DatabaseConfig()); // no type
        assertThrows(IllegalStateException.class, c::validate);
    }

    @Test
    void validateThrowsWhenSourceDatabaseNull() {
        SyncConfig c = new SyncConfig();
        DatabaseConfig src = new DatabaseConfig();
        src.setType("mysql");
        // no database
        c.setSource(src);
        DatabaseConfig tgt = new DatabaseConfig();
        tgt.setType("postgresql");
        tgt.setDatabase("dest");
        c.setTarget(tgt);
        assertThrows(IllegalStateException.class, c::validate);
    }

    @Test
    void validatePassesWithAllRequired() {
        SyncConfig c = buildValid();
        assertDoesNotThrow(c::validate);
    }

    @Test
    void defaultSyncOptionsNotNull() {
        SyncConfig c = new SyncConfig();
        assertNotNull(c.getSync());
        assertTrue(c.getSync().isSchemaSync());
        assertEquals("initial", c.getSync().getSnapshotMode());
    }

    @Test
    void gettersSetters() {
        SyncConfig c = buildValid();
        assertNotNull(c.getSource());
        assertNotNull(c.getTarget());
        assertNotNull(c.getSync());
    }

    static SyncConfig buildValid() {
        SyncConfig c = new SyncConfig();
        DatabaseConfig src = new DatabaseConfig();
        src.setType("mysql"); src.setDatabase("src");
        c.setSource(src);
        DatabaseConfig tgt = new DatabaseConfig();
        tgt.setType("postgresql"); tgt.setDatabase("tgt");
        c.setTarget(tgt);
        return c;
    }
}
