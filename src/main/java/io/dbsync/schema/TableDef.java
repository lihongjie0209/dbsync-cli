package io.dbsync.schema;

import java.util.ArrayList;
import java.util.List;

public class TableDef {

    private final String name;
    private final List<ColumnDef> columns = new ArrayList<>();
    private final List<String> primaryKeys = new ArrayList<>();

    public TableDef(String name) {
        this.name = name;
    }

    public String getName() { return name; }

    public List<ColumnDef> getColumns() { return columns; }

    public void addColumn(ColumnDef col) { columns.add(col); }

    public List<String> getPrimaryKeys() { return primaryKeys; }

    public void addPrimaryKey(String columnName) { primaryKeys.add(columnName); }

    /** Find a column by name (case-insensitive). */
    public ColumnDef getColumn(String colName) {
        return columns.stream()
                .filter(c -> c.getName().equalsIgnoreCase(colName))
                .findFirst().orElse(null);
    }
}
