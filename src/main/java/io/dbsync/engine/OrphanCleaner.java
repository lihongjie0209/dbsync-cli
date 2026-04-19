package io.dbsync.engine;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import org.jboss.logging.Logger;

import java.sql.*;
import java.util.*;

/**
 * After a snapshot completes, rows may exist in the target that were deleted from the source
 * while the sync was not running ("zombie rows"). This cleaner detects and removes them
 * by comparing primary-key sets between source and target.
 *
 * <p>Only runs when {@code options.cleanupOrphans = true} (the default).
 * Uses single-column PK per table; tables without a discoverable PK are skipped.
 */
public class OrphanCleaner {

    private static final Logger log = Logger.getLogger(OrphanCleaner.class);
    private static final int BATCH_SIZE = 500;

    private final SyncConfig config;

    public OrphanCleaner(SyncConfig config) {
        this.config = config;
    }

    /**
     * Clean all configured tables. Blocks until complete.
     * Returns total number of orphan rows deleted across all tables.
     */
    public int cleanAll() {
        List<String> tables = config.getSync().getTables();
        int total = 0;
        for (String table : tables) {
            try {
                total += cleanTable(table);
            } catch (Exception e) {
                log.warnf(e, "Orphan cleanup failed for table '%s' — skipping", table);
            }
        }
        return total;
    }

    // ── per-table cleanup ────────────────────────────────────────────────────

    private int cleanTable(String tableName) throws Exception {
        DatabaseConfig src = config.getSource();
        DatabaseConfig dst = config.getTarget();

        src.loadDriver();
        dst.loadDriver();

        try (Connection srcConn = DriverManager.getConnection(src.jdbcUrl(), src.getUsername(), src.getPassword());
             Connection dstConn = DriverManager.getConnection(dst.jdbcUrl(), dst.getUsername(), dst.getPassword())) {

            srcConn.setAutoCommit(true);
            dstConn.setAutoCommit(true);

            // Discover PK columns from destination metadata
            List<String> pkCols = discoverPks(dstConn, dst, tableName);
            if (pkCols.isEmpty()) {
                log.infof("Orphan cleanup: no PK found for table '%s' — skipping", tableName);
                return 0;
            }

            // Load all source PK tuples
            Set<List<Object>> srcPks = loadPkTuples(srcConn, src, tableName, pkCols);
            log.debugf("Orphan cleanup: %s source has %d PK tuple(s)", tableName, srcPks.size());

            // Find and delete orphans from target
            List<List<Object>> orphans = findOrphanTuples(dstConn, dst, tableName, pkCols, srcPks);
            if (orphans.isEmpty()) {
                return 0;
            }

            log.infof("Orphan cleanup: found %d zombie row(s) in '%s' — deleting", orphans.size(), tableName);
            int deleted = deleteOrphanTuples(dstConn, dst, tableName, pkCols, orphans);
            log.infof("Orphan cleanup: deleted %d row(s) from '%s'", deleted, tableName);
            return deleted;
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** Returns ordered list of PK column names (empty = no PK). */
    private List<String> discoverPks(Connection conn, DatabaseConfig cfg, String tableName) throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        boolean pgFamily = "postgresql".equals(cfg.getType()) || "kingbase".equals(cfg.getType());
        String schema = pgFamily ? "public" : null;
        Map<Short, String> bySeq = new TreeMap<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, tableName)) {
            while (rs.next()) {
                bySeq.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME").toLowerCase());
            }
        }
        return new ArrayList<>(bySeq.values());
    }

    private Set<List<Object>> loadPkTuples(Connection conn, DatabaseConfig cfg,
                                            String tableName, List<String> pkCols) throws Exception {
        String cols = pkCols.stream().map(c -> qi(cfg, c)).collect(java.util.stream.Collectors.joining(", "));
        String sql = "SELECT " + cols + " FROM " + qi(cfg, tableName);
        Set<List<Object>> tuples = new HashSet<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                List<Object> tuple = new ArrayList<>(pkCols.size());
                for (int i = 1; i <= pkCols.size(); i++) {
                    tuple.add(normalizeKey(rs.getObject(i)));
                }
                tuples.add(tuple);
            }
        }
        return tuples;
    }

    private List<List<Object>> findOrphanTuples(Connection conn, DatabaseConfig cfg,
                                                  String tableName, List<String> pkCols,
                                                  Set<List<Object>> srcTuples) throws Exception {
        String cols = pkCols.stream().map(c -> qi(cfg, c)).collect(java.util.stream.Collectors.joining(", "));
        String sql = "SELECT " + cols + " FROM " + qi(cfg, tableName);
        List<List<Object>> orphans = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                List<Object> tuple = new ArrayList<>(pkCols.size());
                for (int i = 1; i <= pkCols.size(); i++) {
                    tuple.add(normalizeKey(rs.getObject(i)));
                }
                if (!srcTuples.contains(tuple)) {
                    orphans.add(tuple);
                }
            }
        }
        return orphans;
    }

    private int deleteOrphanTuples(Connection conn, DatabaseConfig cfg,
                                    String tableName, List<String> pkCols,
                                    List<List<Object>> orphans) throws Exception {
        String where = pkCols.stream()
                .map(c -> qi(cfg, c) + " = ?")
                .collect(java.util.stream.Collectors.joining(" AND "));
        String sql = "DELETE FROM " + qi(cfg, tableName) + " WHERE " + where;
        int deleted = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int batch = 0;
            for (List<Object> tuple : orphans) {
                for (int i = 0; i < tuple.size(); i++) {
                    ps.setObject(i + 1, tuple.get(i));
                }
                ps.addBatch();
                batch++;
                if (batch >= BATCH_SIZE) {
                    deleted += Arrays.stream(ps.executeBatch()).sum();
                    batch = 0;
                }
            }
            if (batch > 0) {
                deleted += Arrays.stream(ps.executeBatch()).sum();
            }
        }
        return deleted;
    }

    /** Normalize integer PK types (Integer, Short, Byte → Long) to handle source/target type differences. */
    private static Object normalizeKey(Object pk) {
        if (pk instanceof Number n && !(pk instanceof Long) && !(pk instanceof Double)
                && !(pk instanceof Float)) {
            return n.longValue();
        }
        return pk;
    }

    private static String qi(DatabaseConfig cfg, String name) {
        boolean pgFamily = "postgresql".equals(cfg.getType()) || "kingbase".equals(cfg.getType());
        return pgFamily ? "\"" + name + "\"" : "`" + name + "`";
    }
}
