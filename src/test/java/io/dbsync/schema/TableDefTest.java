package io.dbsync.schema;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableDefTest {

    @Test
    void emptyTableHasNoPrimaryKeys() {
        TableDef t = new TableDef("users");
        assertTrue(t.getPrimaryKeys().isEmpty());
        assertTrue(t.getColumns().isEmpty());
    }

    @Test
    void addColumnAndRetrieve() {
        TableDef t = new TableDef("orders");
        ColumnDef col = DdlTranslatorTest.col("id", "BIGINT", 0, 0);
        col.setNullable(false);
        t.addColumn(col);

        assertEquals(1, t.getColumns().size());
        assertSame(col, t.getColumns().get(0));
    }

    @Test
    void addMultipleColumnsPreservesOrder() {
        TableDef t = new TableDef("orders");
        t.addColumn(DdlTranslatorTest.col("id",    "BIGINT",  0, 0));
        t.addColumn(DdlTranslatorTest.col("name",  "VARCHAR", 100, 0));
        t.addColumn(DdlTranslatorTest.col("price", "DECIMAL", 10, 2));

        assertEquals(List.of("id","name","price"),
                t.getColumns().stream().map(ColumnDef::getName).toList());
    }

    @Test
    void addPrimaryKey() {
        TableDef t = new TableDef("users");
        t.addPrimaryKey("id");
        assertEquals(List.of("id"), t.getPrimaryKeys());
    }

    @Test
    void addMultiplePrimaryKeys() {
        TableDef t = new TableDef("order_items");
        t.addPrimaryKey("order_id");
        t.addPrimaryKey("item_id");
        assertEquals(List.of("order_id","item_id"), t.getPrimaryKeys());
    }

    @Test
    void tableNameIsPreserved() {
        assertEquals("customers", new TableDef("customers").getName());
    }

    @Test
    void columnNullableFlag() {
        ColumnDef c = new ColumnDef();
        c.setNullable(true);
        assertTrue(c.isNullable());
        c.setNullable(false);
        assertFalse(c.isNullable());
    }

    @Test
    void columnAutoIncrementFlag() {
        ColumnDef c = new ColumnDef();
        c.setAutoIncrement(true);
        assertTrue(c.isAutoIncrement());
        c.setAutoIncrement(false);
        assertFalse(c.isAutoIncrement());
    }

    @Test
    void columnGettersSetters() {
        ColumnDef c = DdlTranslatorTest.col("amount", "DECIMAL", 12, 4);
        assertEquals("amount",  c.getName());
        assertEquals("DECIMAL", c.getTypeName());
        assertEquals(12,        c.getSize());
        assertEquals(4,         c.getDecimalDigits());
    }
}
