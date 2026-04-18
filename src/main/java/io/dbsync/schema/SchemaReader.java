package io.dbsync.schema;

import io.dbsync.config.DatabaseConfig;
import jakarta.enterprise.context.ApplicationScoped;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class SchemaReader {

    public List<TableDef> readTables(DatabaseConfig dbConfig, List<String> tableFilter) throws Exception {
        dbConfig.loadDriver();
        try (Connection conn = DriverManager.getConnection(dbConfig.jdbcUrl(),
                dbConfig.getUsername(), dbConfig.getPassword())) {

            DatabaseMetaData meta = conn.getMetaData();
            List<TableDef> tables = new ArrayList<>();

            boolean pgFamily = "postgresql".equals(dbConfig.getType()) || "kingbase".equals(dbConfig.getType());
            String catalog = pgFamily ? null : dbConfig.getDatabase();
            String schema  = pgFamily ? "public" : null;

            try (ResultSet rs = meta.getTables(catalog, schema, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (!tableFilter.isEmpty() && !tableFilter.contains(tableName)) continue;
                    tables.add(readTable(meta, catalog, schema, tableName));
                }
            }
            return tables;
        }
    }

    private TableDef readTable(DatabaseMetaData meta, String catalog, String schema, String tableName) throws Exception {
        TableDef table = new TableDef(tableName);

        try (ResultSet cols = meta.getColumns(catalog, schema, tableName, null)) {
            while (cols.next()) {
                ColumnDef col = new ColumnDef();
                col.setName(cols.getString("COLUMN_NAME"));
                col.setTypeName(cols.getString("TYPE_NAME"));
                col.setSize(cols.getInt("COLUMN_SIZE"));
                col.setDecimalDigits(cols.getInt("DECIMAL_DIGITS"));
                col.setNullable(cols.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                col.setDefaultValue(cols.getString("COLUMN_DEF"));
                col.setAutoIncrement("YES".equalsIgnoreCase(cols.getString("IS_AUTOINCREMENT")));
                col.setOrdinalPosition(cols.getInt("ORDINAL_POSITION"));
                table.addColumn(col);
            }
        }

        // Primary keys — PK_SEQ is 1-based ordinal within PK
        try (ResultSet pks = meta.getPrimaryKeys(catalog, schema, tableName)) {
            List<String[]> pkEntries = new ArrayList<>();
            while (pks.next()) {
                pkEntries.add(new String[]{
                    pks.getString("COLUMN_NAME"),
                    String.valueOf(pks.getShort("KEY_SEQ"))
                });
            }
            pkEntries.sort((a, b) -> Integer.compare(Integer.parseInt(a[1]), Integer.parseInt(b[1])));
            pkEntries.forEach(e -> table.addPrimaryKey(e[0]));
        }

        return table;
    }
}
