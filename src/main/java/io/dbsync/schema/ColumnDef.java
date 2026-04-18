package io.dbsync.schema;

public class ColumnDef {

    private String name;
    private String typeName;
    private int size;
    private int decimalDigits;
    private boolean nullable = true;
    private String defaultValue;
    private boolean autoIncrement;
    private int ordinalPosition;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getTypeName() { return typeName; }
    public void setTypeName(String typeName) { this.typeName = typeName; }

    public int getSize() { return size; }
    public void setSize(int size) { this.size = size; }

    public int getDecimalDigits() { return decimalDigits; }
    public void setDecimalDigits(int decimalDigits) { this.decimalDigits = decimalDigits; }

    public boolean isNullable() { return nullable; }
    public void setNullable(boolean nullable) { this.nullable = nullable; }

    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }

    public boolean isAutoIncrement() { return autoIncrement; }
    public void setAutoIncrement(boolean autoIncrement) { this.autoIncrement = autoIncrement; }

    public int getOrdinalPosition() { return ordinalPosition; }
    public void setOrdinalPosition(int ordinalPosition) { this.ordinalPosition = ordinalPosition; }
}
