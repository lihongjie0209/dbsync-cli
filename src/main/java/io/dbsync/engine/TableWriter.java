package io.dbsync.engine;

import com.fasterxml.jackson.databind.JsonNode;
import io.dbsync.config.DatabaseConfig;
import org.jboss.logging.Logger;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Writes INSERT / UPSERT / DELETE operations to the target database for a single table.
 * One instance per table, reuses a single JDBC connection.
 *
 * <p>On the first write, the column SQL types are loaded from the target schema
 * ({@link DatabaseMetaData#getColumns}) and used to coerce each value to the
 * correct Java type before {@link PreparedStatement#setObject}.  This handles
 * Debezium's logical-type mismatches such as:
 * <ul>
 *   <li>MySQL {@code TINYINT(1)} → PG {@code BOOLEAN}: integer 0/1 → boolean</li>
 *   <li>Debezium {@code io.debezium.time.Timestamp}: epoch-millis long → {@link java.sql.Timestamp}</li>
 *   <li>Debezium {@code io.debezium.time.Date}: epoch-days int → {@link java.sql.Date}</li>
 * </ul>
 */
public class TableWriter {

    private static final Logger log = Logger.getLogger(TableWriter.class);

    private final DatabaseConfig targetConfig;
    private final String tableName;
    private Connection connection;

    /** Lazily populated on first write: column name (lowercase) → java.sql.Types constant. */
    private Map<String, Integer> columnTypes;

    /** Lazily populated: ordered list of primary-key column names (lowercase). */
    private List<String> primaryKeys;

    public TableWriter(DatabaseConfig targetConfig, String tableName) {
        this.targetConfig = targetConfig;
        this.tableName = tableName;
    }

    /** Package-private: allows tests to inject a pre-built connection (e.g., H2). */
    TableWriter(DatabaseConfig targetConfig, String tableName, Connection testConnection) {
        this.targetConfig = targetConfig;
        this.tableName = tableName;
        this.connection = testConnection;
    }

    public void insert(JsonNode record) throws Exception {
        List<String> cols = new ArrayList<>();
        List<Object> vals = new ArrayList<>();
        extractFields(record, cols, vals);
        if (cols.isEmpty()) return;

        String sql = buildUpsertSql(cols);
        try (PreparedStatement ps = conn().prepareStatement(sql)) {
            bindValues(ps, cols, vals);
            ps.executeUpdate();
        }
    }

    public void upsert(JsonNode after, JsonNode before) throws Exception {
        insert(after); // UPSERT SQL handles conflict
    }

    public void delete(JsonNode record) throws Exception {
        List<String> cols = new ArrayList<>();
        List<Object> vals = new ArrayList<>();
        extractFields(record, cols, vals);
        if (cols.isEmpty()) return;

        // Prefer using only PK columns for the WHERE clause (works regardless of
        // REPLICA IDENTITY setting — with DEFAULT, non-PK before-values are null).
        List<String> pkCols;
        try {
            pkCols = getPrimaryKeys();
        } catch (Exception e) {
            log.warnf("Could not load PK for DELETE on %s; falling back to all non-null columns", tableName);
            pkCols = List.of();
        }

        List<String> whereCols;
        List<Object> whereVals;

        if (!pkCols.isEmpty()) {
            // Build parallel lists for PK columns only
            whereCols = new ArrayList<>();
            whereVals = new ArrayList<>();
            for (int i = 0; i < cols.size(); i++) {
                if (pkCols.contains(cols.get(i).toLowerCase())) {
                    whereCols.add(cols.get(i));
                    whereVals.add(vals.get(i));
                }
            }
        } else {
            // No PK info — fall back to non-null columns
            whereCols = new ArrayList<>();
            whereVals = new ArrayList<>();
            for (int i = 0; i < cols.size(); i++) {
                if (vals.get(i) != null) {
                    whereCols.add(cols.get(i));
                    whereVals.add(vals.get(i));
                }
            }
        }

        if (whereCols.isEmpty()) {
            log.warnf("DELETE on '%s' had no usable WHERE columns — skipping", tableName);
            return;
        }

        String where = whereCols.stream()
                .map(c -> qi(c) + " = ?")
                .collect(Collectors.joining(" AND "));
        String sql = "DELETE FROM " + qi(tableName) + " WHERE " + where;

        try (PreparedStatement ps = conn().prepareStatement(sql)) {
            bindValues(ps, whereCols, whereVals);
            ps.executeUpdate();
        }
    }

    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ------------------------------------------------------------------

    private String buildUpsertSql(List<String> cols) {
        String colList      = cols.stream().map(this::qi).collect(Collectors.joining(", "));
        String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
        String base = "INSERT INTO " + qi(tableName) + " (" + colList + ") VALUES (" + placeholders + ")";

        return switch (targetConfig.getType()) {
            case "postgresql", "kingbase" -> buildPgUpsert(base, cols);
            case "mysql", "mariadb" -> {
                String updates = cols.stream()
                        .map(c -> qi(c) + " = VALUES(" + qi(c) + ")")
                        .collect(Collectors.joining(", "));
                yield base + " ON DUPLICATE KEY UPDATE " + updates;
            }
            default -> base;
        };
    }

    private String buildPgUpsert(String base, List<String> cols) {
        List<String> pks;
        try {
            pks = getPrimaryKeys();
        } catch (Exception e) {
            log.warnf("Could not load PK for %s: %s — falling back to ON CONFLICT DO NOTHING", tableName, e.getMessage());
            return base + " ON CONFLICT DO NOTHING";
        }
        if (pks.isEmpty()) {
            return base + " ON CONFLICT DO NOTHING";
        }
        String pkClause = pks.stream().map(this::qi).collect(Collectors.joining(", "));
        List<String> nonPkCols = cols.stream()
                .filter(c -> !pks.contains(c.toLowerCase()))
                .collect(Collectors.toList());
        if (nonPkCols.isEmpty()) {
            return base + " ON CONFLICT (" + pkClause + ") DO NOTHING";
        }
        String setClause = nonPkCols.stream()
                .map(c -> qi(c) + " = EXCLUDED." + qi(c))
                .collect(Collectors.joining(", "));
        return base + " ON CONFLICT (" + pkClause + ") DO UPDATE SET " + setClause;
    }

    private void extractFields(JsonNode node, List<String> cols, List<Object> vals) {
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            cols.add(entry.getKey());
            vals.add(toJava(entry.getValue()));
        }
    }

    /**
     * Binds values using target-column SQL types to coerce Debezium logical-type
     * representations (epoch millis, TINYINT booleans, etc.) to the correct Java types.
     */
    private void bindValues(PreparedStatement ps, List<String> cols, List<Object> vals)
            throws Exception {
        Map<String, Integer> types = getColumnTypes();
        for (int i = 0; i < vals.size(); i++) {
            Object coerced = coerce(vals.get(i), types.get(cols.get(i).toLowerCase()));
            ps.setObject(i + 1, coerced);
        }
    }

    /**
     * Coerces a value to match the actual target column SQL type.
     *
     * <ul>
     *   <li>{@code BOOLEAN}/{@code BIT}: int → boolean (MySQL TINYINT(1) → PG BOOLEAN)</li>
     *   <li>{@code TIMESTAMP}: long (epoch ms) → {@link java.sql.Timestamp}</li>
     *   <li>{@code DATE}: int (epoch days) → {@link java.sql.Date}</li>
     *   <li>{@code TIME}: long (millis since midnight) → {@link java.sql.Time}</li>
     * </ul>
     */
    Object coerce(Object val, Integer sqlType) {
        if (val == null || sqlType == null) return val;
        return switch (sqlType) {
            case Types.BOOLEAN, Types.BIT ->
                    val instanceof Number n ? n.intValue() != 0 : val;
            case Types.TIMESTAMP ->
                    val instanceof Long l ? new java.sql.Timestamp(l) :
                    val instanceof String s ? parseTimestampString(s) : val;
            case Types.TIMESTAMP_WITH_TIMEZONE ->
                    val instanceof Long l ? new java.sql.Timestamp(l) :
                    val instanceof String s ? java.sql.Timestamp.from(
                            java.time.OffsetDateTime.parse(s).toInstant()) : val;
            case Types.DATE ->
                    val instanceof Integer d
                            ? java.sql.Date.valueOf(LocalDate.ofEpochDay(d))
                            : val instanceof Long l
                            ? java.sql.Date.valueOf(LocalDate.ofEpochDay(l))
                            : val;
            case Types.TIME ->
                    val instanceof Long l ? new java.sql.Time(l)
                            : val instanceof Integer ms ? new java.sql.Time(ms)
                            : val;
            default -> val;
        };
    }

    /** Parses an ISO-8601 timestamp string, with or without timezone offset. */
    private static java.sql.Timestamp parseTimestampString(String s) {
        try {
            return java.sql.Timestamp.from(java.time.OffsetDateTime.parse(s).toInstant());
        } catch (java.time.format.DateTimeParseException e) {
            return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(s));
        }
    }


    private Map<String, Integer> getColumnTypes() throws Exception {
        if (columnTypes == null) {
            columnTypes = new LinkedHashMap<>();
            DatabaseMetaData meta = conn().getMetaData();
            // catalog/schema null = all; tableName exact match
            try (ResultSet rs = meta.getColumns(null, null, tableName, null)) {
                while (rs.next()) {
                    columnTypes.put(
                            rs.getString("COLUMN_NAME").toLowerCase(),
                            rs.getInt("DATA_TYPE"));
                }
            }
            if (columnTypes.isEmpty()) {
                log.warnf("No column metadata found for table '%s' — type coercions disabled", tableName);
            }
        }
        return columnTypes;
    }

    private List<String> getPrimaryKeys() throws Exception {
        if (primaryKeys == null) {
            primaryKeys = new ArrayList<>();
            DatabaseMetaData meta = conn().getMetaData();
            boolean pgFamily = "postgresql".equals(targetConfig.getType())
                    || "kingbase".equals(targetConfig.getType());
            String schema = pgFamily ? "public" : null;
            try (ResultSet rs = meta.getPrimaryKeys(null, schema, tableName)) {
                // Sort by KEY_SEQ to preserve composite PK order
                Map<Short, String> bySeq = new TreeMap<>();
                while (rs.next()) {
                    bySeq.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME").toLowerCase());
                }
                primaryKeys.addAll(bySeq.values());
            }
        }
        return primaryKeys;
    }

    private Object toJava(JsonNode n) {
        if (n == null || n.isNull()) return null;
        if (n.isBoolean())    return n.booleanValue();
        if (n.isBigDecimal()) return n.decimalValue();   // decoded from Debezium Decimal bytes
        if (n.isInt())        return n.intValue();
        if (n.isLong())       return n.longValue();
        if (n.isDouble())     return n.doubleValue();
        if (n.isTextual())    return n.textValue();
        if (n.isObject() || n.isArray()) return n.toString();
        return n.asText();
    }

    private String qi(String name) {
        return ("postgresql".equals(targetConfig.getType()) || "kingbase".equals(targetConfig.getType()))
                ? "\"" + name + "\"" : "`" + name + "`";
    }

    private Connection conn() throws Exception {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(
                    targetConfig.jdbcUrl(), targetConfig.getUsername(), targetConfig.getPassword());
            connection.setAutoCommit(true);
        }
        return connection;
    }
}
