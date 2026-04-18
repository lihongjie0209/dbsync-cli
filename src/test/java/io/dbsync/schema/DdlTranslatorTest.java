package io.dbsync.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdlTranslatorTest {

    private DdlTranslator translator;

    @BeforeEach
    void setUp() {
        translator = new DdlTranslator();
    }

    // ── MySQL → PostgreSQL : integer types ───────────────────────────────

    @Test void mysql2pg_tinyint()          { assertTrans("mysql","postgresql","TINYINT",0,0,       "SMALLINT"); }
    @Test void mysql2pg_smallint()         { assertTrans("mysql","postgresql","SMALLINT",0,0,      "SMALLINT"); }
    @Test void mysql2pg_mediumint()        { assertTrans("mysql","postgresql","MEDIUMINT",0,0,     "INTEGER"); }
    @Test void mysql2pg_int()              { assertTrans("mysql","postgresql","INT",0,0,           "INTEGER"); }
    @Test void mysql2pg_integer()          { assertTrans("mysql","postgresql","INTEGER",0,0,       "INTEGER"); }
    @Test void mysql2pg_bigint()           { assertTrans("mysql","postgresql","BIGINT",0,0,        "BIGINT"); }

    @Test void mysql2pg_tinyint_unsigned() { assertTrans("mysql","postgresql","TINYINT UNSIGNED",0,0,   "SMALLINT"); }
    @Test void mysql2pg_smallint_unsigned(){ assertTrans("mysql","postgresql","SMALLINT UNSIGNED",0,0,  "INTEGER"); }
    @Test void mysql2pg_mediumint_unsigned(){ assertTrans("mysql","postgresql","MEDIUMINT UNSIGNED",0,0,"INTEGER"); }
    @Test void mysql2pg_int_unsigned()     { assertTrans("mysql","postgresql","INT UNSIGNED",0,0,       "BIGINT"); }
    @Test void mysql2pg_integer_unsigned() { assertTrans("mysql","postgresql","INTEGER UNSIGNED",0,0,   "BIGINT"); }

    @Test void mysql2pg_bigint_unsigned() {
        // BIGINT UNSIGNED max > PG BIGINT range → NUMERIC(20,0)
        String result = translator.translate("mysql", "postgresql", col("n", "BIGINT UNSIGNED", 0, 0));
        assertTrue(result.toUpperCase().startsWith("NUMERIC"), "Expected NUMERIC but got " + result);
    }

    // ── MySQL → PostgreSQL : boolean / bit ───────────────────────────────

    @Test void mysql2pg_boolean()          { assertTrans("mysql","postgresql","BOOLEAN",0,0,  "BOOLEAN"); }
    @Test void mysql2pg_bit()              { assertTrans("mysql","postgresql","BIT",0,0,      "BOOLEAN"); }

    @Test void mysql2pg_tinyint1_is_boolean() {
        // MySQL TINYINT(1) = boolean by convention; JDBC returns TYPE_NAME="TINYINT", COLUMN_SIZE=1
        assertEquals("BOOLEAN", translator.translate("mysql", "postgresql", col("flag", "TINYINT", 1, 0)));
    }

    // ── MySQL → PostgreSQL : floating-point ──────────────────────────────

    @Test void mysql2pg_float()            { assertTrans("mysql","postgresql","FLOAT",0,0,          "REAL"); }
    @Test void mysql2pg_float_unsigned()   { assertTrans("mysql","postgresql","FLOAT UNSIGNED",0,0, "REAL"); }
    @Test void mysql2pg_double()           { assertTrans("mysql","postgresql","DOUBLE",0,0,         "DOUBLE PRECISION"); }
    @Test void mysql2pg_double_unsigned()  { assertTrans("mysql","postgresql","DOUBLE UNSIGNED",0,0,"DOUBLE PRECISION"); }
    @Test void mysql2pg_double_precision() { assertTrans("mysql","postgresql","DOUBLE PRECISION",0,0,"DOUBLE PRECISION"); }

    // ── MySQL → PostgreSQL : fixed-point ─────────────────────────────────

    @Test void mysql2pg_decimal()          { assertTrans("mysql","postgresql","DECIMAL",0,0,         "NUMERIC"); }
    @Test void mysql2pg_decimal_unsigned() { assertTrans("mysql","postgresql","DECIMAL UNSIGNED",0,0,"NUMERIC"); }
    @Test void mysql2pg_numeric()          { assertTrans("mysql","postgresql","NUMERIC",0,0,         "NUMERIC"); }

    @Test void mysql2pg_decimal_with_precision() {
        assertEquals("NUMERIC(10, 2)", translator.translate("mysql", "postgresql", col("price", "DECIMAL", 10, 2)));
    }

    // ── MySQL → PostgreSQL : string types ────────────────────────────────

    @Test void mysql2pg_char()             { assertTrans("mysql","postgresql","CHAR",5,0,    "CHAR"); }
    @Test void mysql2pg_varchar()          { assertTrans("mysql","postgresql","VARCHAR",255,0,"VARCHAR"); }
    @Test void mysql2pg_tinytext()         { assertTrans("mysql","postgresql","TINYTEXT",0,0,"TEXT"); }
    @Test void mysql2pg_text()             { assertTrans("mysql","postgresql","TEXT",0,0,    "TEXT"); }
    @Test void mysql2pg_mediumtext()       { assertTrans("mysql","postgresql","MEDIUMTEXT",0,0,"TEXT"); }
    @Test void mysql2pg_longtext()         { assertTrans("mysql","postgresql","LONGTEXT",0,0,"TEXT"); }
    @Test void mysql2pg_enum()             { assertTrans("mysql","postgresql","ENUM",0,0,    "TEXT"); }
    @Test void mysql2pg_set()              { assertTrans("mysql","postgresql","SET",0,0,     "TEXT"); }

    @Test void mysql2pg_varchar_with_size() {
        assertEquals("VARCHAR(255)", translator.translate("mysql", "postgresql", col("name", "VARCHAR", 255, 0)));
    }

    @Test void mysql2pg_char_with_size() {
        assertEquals("CHAR(5)", translator.translate("mysql", "postgresql", col("code", "CHAR", 5, 0)));
    }

    // ── MySQL → PostgreSQL : date/time ───────────────────────────────────

    @Test void mysql2pg_date()             { assertTrans("mysql","postgresql","DATE",0,0,      "DATE"); }
    @Test void mysql2pg_time()             { assertTrans("mysql","postgresql","TIME",0,0,      "TIME"); }
    @Test void mysql2pg_year()             { assertTrans("mysql","postgresql","YEAR",0,0,      "SMALLINT"); }
    @Test void mysql2pg_datetime()         { assertTrans("mysql","postgresql","DATETIME",0,0,  "TIMESTAMP"); }
    @Test void mysql2pg_timestamp()        { assertTrans("mysql","postgresql","TIMESTAMP",0,0, "TIMESTAMPTZ"); }

    // ── MySQL → PostgreSQL : binary types ────────────────────────────────

    @Test void mysql2pg_binary()           { assertTrans("mysql","postgresql","BINARY",0,0,    "BYTEA"); }
    @Test void mysql2pg_varbinary()        { assertTrans("mysql","postgresql","VARBINARY",0,0, "BYTEA"); }
    @Test void mysql2pg_tinyblob()         { assertTrans("mysql","postgresql","TINYBLOB",0,0,  "BYTEA"); }
    @Test void mysql2pg_blob()             { assertTrans("mysql","postgresql","BLOB",0,0,      "BYTEA"); }
    @Test void mysql2pg_mediumblob()       { assertTrans("mysql","postgresql","MEDIUMBLOB",0,0,"BYTEA"); }
    @Test void mysql2pg_longblob()         { assertTrans("mysql","postgresql","LONGBLOB",0,0,  "BYTEA"); }

    // ── MySQL → PostgreSQL : structured / spatial ────────────────────────

    @Test void mysql2pg_json()             { assertTrans("mysql","postgresql","JSON",0,0,           "JSONB"); }
    @Test void mysql2pg_geometry()         { assertTrans("mysql","postgresql","GEOMETRY",0,0,       "TEXT"); }
    @Test void mysql2pg_point()            { assertTrans("mysql","postgresql","POINT",0,0,          "TEXT"); }
    @Test void mysql2pg_linestring()       { assertTrans("mysql","postgresql","LINESTRING",0,0,     "TEXT"); }
    @Test void mysql2pg_polygon()          { assertTrans("mysql","postgresql","POLYGON",0,0,        "TEXT"); }
    @Test void mysql2pg_multipoint()       { assertTrans("mysql","postgresql","MULTIPOINT",0,0,     "TEXT"); }
    @Test void mysql2pg_multilinestring()  { assertTrans("mysql","postgresql","MULTILINESTRING",0,0,"TEXT"); }
    @Test void mysql2pg_multipolygon()     { assertTrans("mysql","postgresql","MULTIPOLYGON",0,0,   "TEXT"); }
    @Test void mysql2pg_geometrycollection(){ assertTrans("mysql","postgresql","GEOMETRYCOLLECTION",0,0,"TEXT"); }

    // ── PostgreSQL → MySQL : integer types ───────────────────────────────

    @Test void pg2mysql_int2()             { assertTrans("postgresql","mysql","INT2",0,0,    "SMALLINT"); }
    @Test void pg2mysql_int4()             { assertTrans("postgresql","mysql","INT4",0,0,    "INT"); }
    @Test void pg2mysql_int8()             { assertTrans("postgresql","mysql","INT8",0,0,    "BIGINT"); }
    @Test void pg2mysql_smallint()         { assertTrans("postgresql","mysql","SMALLINT",0,0,"SMALLINT"); }
    @Test void pg2mysql_integer()          { assertTrans("postgresql","mysql","INTEGER",0,0, "INT"); }
    @Test void pg2mysql_bigint()           { assertTrans("postgresql","mysql","BIGINT",0,0,  "BIGINT"); }
    @Test void pg2mysql_oid()              { assertTrans("postgresql","mysql","OID",0,0,     "BIGINT"); }
    @Test void pg2mysql_smallserial()      { assertTrans("postgresql","mysql","SMALLSERIAL",0,0,"SMALLINT"); }
    @Test void pg2mysql_serial()           { assertTrans("postgresql","mysql","SERIAL",0,0,  "INT"); }
    @Test void pg2mysql_bigserial()        { assertTrans("postgresql","mysql","BIGSERIAL",0,0,"BIGINT"); }

    // ── PostgreSQL → MySQL : floating-point ──────────────────────────────

    @Test void pg2mysql_float4()           { assertTrans("postgresql","mysql","FLOAT4",0,0,         "FLOAT"); }
    @Test void pg2mysql_float8()           { assertTrans("postgresql","mysql","FLOAT8",0,0,         "DOUBLE"); }
    @Test void pg2mysql_real()             { assertTrans("postgresql","mysql","REAL",0,0,           "FLOAT"); }
    @Test void pg2mysql_double_precision() { assertTrans("postgresql","mysql","DOUBLE PRECISION",0,0,"DOUBLE"); }

    // ── PostgreSQL → MySQL : fixed-point ─────────────────────────────────

    @Test void pg2mysql_numeric()          { assertTrans("postgresql","mysql","NUMERIC",0,0, "DECIMAL"); }
    @Test void pg2mysql_decimal()          { assertTrans("postgresql","mysql","DECIMAL",0,0, "DECIMAL"); }
    @Test void pg2mysql_money()            { assertTrans("postgresql","mysql","MONEY",0,0,   "DECIMAL"); }

    @Test void pg2mysql_numeric_with_precision() {
        assertEquals("DECIMAL(12, 4)", translator.translate("postgresql", "mysql", col("amount", "NUMERIC", 12, 4)));
    }

    // ── PostgreSQL → MySQL : string types ────────────────────────────────

    @Test void pg2mysql_bpchar()            { assertTrans("postgresql","mysql","BPCHAR",10,0,  "CHAR"); }
    @Test void pg2mysql_character()         { assertTrans("postgresql","mysql","CHARACTER",5,0,"CHAR"); }
    @Test void pg2mysql_char()              { assertTrans("postgresql","mysql","CHAR",5,0,     "CHAR"); }
    @Test void pg2mysql_varchar()           { assertTrans("postgresql","mysql","VARCHAR",100,0,"VARCHAR"); }
    @Test void pg2mysql_character_varying() { assertTrans("postgresql","mysql","CHARACTER VARYING",200,0,"VARCHAR"); }
    @Test void pg2mysql_text()              { assertTrans("postgresql","mysql","TEXT",0,0,     "LONGTEXT"); }
    @Test void pg2mysql_xml()               { assertTrans("postgresql","mysql","XML",0,0,      "LONGTEXT"); }

    @Test void pg2mysql_varchar_with_size() {
        assertEquals("VARCHAR(100)", translator.translate("postgresql", "mysql", col("name", "VARCHAR", 100, 0)));
    }

    // ── PostgreSQL → MySQL : date/time ───────────────────────────────────

    @Test void pg2mysql_date()              { assertTrans("postgresql","mysql","DATE",0,0,                    "DATE"); }
    @Test void pg2mysql_time()              { assertTrans("postgresql","mysql","TIME",0,0,                    "TIME"); }
    @Test void pg2mysql_time_without_tz()   { assertTrans("postgresql","mysql","TIME WITHOUT TIME ZONE",0,0,  "TIME"); }
    @Test void pg2mysql_timetz()            { assertTrans("postgresql","mysql","TIMETZ",0,0,                  "TIME"); }
    @Test void pg2mysql_time_with_tz()      { assertTrans("postgresql","mysql","TIME WITH TIME ZONE",0,0,     "TIME"); }
    @Test void pg2mysql_timestamp()         { assertTrans("postgresql","mysql","TIMESTAMP",0,0,               "DATETIME"); }
    @Test void pg2mysql_timestamp_without_tz(){ assertTrans("postgresql","mysql","TIMESTAMP WITHOUT TIME ZONE",0,0,"DATETIME"); }
    @Test void pg2mysql_timestamptz()       { assertTrans("postgresql","mysql","TIMESTAMPTZ",0,0,             "TIMESTAMP"); }
    @Test void pg2mysql_timestamp_with_tz() { assertTrans("postgresql","mysql","TIMESTAMP WITH TIME ZONE",0,0,"TIMESTAMP"); }
    @Test void pg2mysql_interval()          { assertTrans("postgresql","mysql","INTERVAL",0,0,                "VARCHAR"); }

    // ── PostgreSQL → MySQL : boolean ─────────────────────────────────────

    @Test void pg2mysql_bool()              { assertTrans("postgresql","mysql","BOOL",0,0,   "TINYINT(1)"); }
    @Test void pg2mysql_boolean()           { assertTrans("postgresql","mysql","BOOLEAN",0,0,"TINYINT(1)"); }

    // ── PostgreSQL → MySQL : binary ──────────────────────────────────────

    @Test void pg2mysql_bytea()             { assertTrans("postgresql","mysql","BYTEA",0,0,"LONGBLOB"); }

    // ── PostgreSQL → MySQL : bit types ───────────────────────────────────

    @Test void pg2mysql_bit()               { assertTrans("postgresql","mysql","BIT",0,0,        "BIT"); }
    @Test void pg2mysql_varbit()            { assertTrans("postgresql","mysql","VARBIT",0,0,     "VARBINARY"); }
    @Test void pg2mysql_bit_varying()       { assertTrans("postgresql","mysql","BIT VARYING",0,0,"VARBINARY"); }

    // ── PostgreSQL → MySQL : structured ──────────────────────────────────

    @Test void pg2mysql_json()              { assertTrans("postgresql","mysql","JSON",0,0,  "JSON"); }
    @Test void pg2mysql_jsonb()             { assertTrans("postgresql","mysql","JSONB",0,0, "JSON"); }
    @Test void pg2mysql_uuid()              { assertTrans("postgresql","mysql","UUID",0,0,  "CHAR(36)"); }

    // ── PostgreSQL → MySQL : network address types ───────────────────────

    @Test void pg2mysql_inet()              { assertTrans("postgresql","mysql","INET",0,0,   "VARCHAR"); }
    @Test void pg2mysql_cidr()              { assertTrans("postgresql","mysql","CIDR",0,0,   "VARCHAR"); }
    @Test void pg2mysql_macaddr()           { assertTrans("postgresql","mysql","MACADDR",0,0,"VARCHAR"); }

    // ── PostgreSQL → MySQL : full-text search types ──────────────────────

    @Test void pg2mysql_tsvector()          { assertTrans("postgresql","mysql","TSVECTOR",0,0,"TEXT"); }
    @Test void pg2mysql_tsquery()           { assertTrans("postgresql","mysql","TSQUERY",0,0, "TEXT"); }

    // ── PostgreSQL → MySQL : geometric types ─────────────────────────────

    @Test void pg2mysql_point()             { assertTrans("postgresql","mysql","POINT",0,0,  "TEXT"); }
    @Test void pg2mysql_line()              { assertTrans("postgresql","mysql","LINE",0,0,   "TEXT"); }
    @Test void pg2mysql_lseg()              { assertTrans("postgresql","mysql","LSEG",0,0,   "TEXT"); }
    @Test void pg2mysql_box()               { assertTrans("postgresql","mysql","BOX",0,0,    "TEXT"); }
    @Test void pg2mysql_path()              { assertTrans("postgresql","mysql","PATH",0,0,   "TEXT"); }
    @Test void pg2mysql_polygon()           { assertTrans("postgresql","mysql","POLYGON",0,0,"TEXT"); }
    @Test void pg2mysql_circle()            { assertTrans("postgresql","mysql","CIRCLE",0,0, "TEXT"); }

    // ── Homogeneous sync ─────────────────────────────────────────────────

    @Test void sameType_mysqlToMysql_preservesType() {
        assertEquals("VARCHAR(64)", translator.translate("mysql", "mysql", col("name", "VARCHAR", 64, 0)));
    }

    @Test void sameType_pgToPg_preservesType() {
        assertEquals("TIMESTAMPTZ", translator.translate("postgresql", "postgresql", col("ts", "TIMESTAMPTZ", 0, 0)));
    }

    // ── Fallback behaviour ───────────────────────────────────────────────

    @Test void unknownMysqlType_fallsBackToText() {
        // A fictional MySQL type has no mapping → TEXT on PostgreSQL target
        String result = translator.translate("mysql", "postgresql", col("x", "FOOBARTYPE", 0, 0));
        assertEquals("TEXT", result);
    }

    @Test void unknownPgType_fallsBackToLongtext() {
        // A fictional PG type has no mapping → LONGTEXT on MySQL target
        String result = translator.translate("postgresql", "mysql", col("x", "FOOBARTYPE", 0, 0));
        assertEquals("LONGTEXT", result);
    }

    @Test void pgArrayType_fallsBackToLongtext() {
        // PostgreSQL array types have pg_typname prefixed with '_' (e.g. _int4)
        String result = translator.translate("postgresql", "mysql", col("tags", "_INT4", 0, 0));
        assertEquals("LONGTEXT", result);
    }

    @Test void unsignedWithNoSpecificMapping_fallsBackViaBaseType() {
        // FLOAT UNSIGNED has an explicit mapping → should resolve to REAL
        String result = translator.translate("mysql", "postgresql", col("v", "FLOAT UNSIGNED", 0, 0));
        assertEquals("REAL", result);
    }

    // ── Default size behaviour ───────────────────────────────────────────

    @Test void varcharWithNoSizeDefaultsTo255() {
        assertEquals("VARCHAR(255)", translator.translate("mysql", "postgresql", col("x", "VARCHAR", 0, 0)));
    }

    @Test void numericWithNoSizeDefaultsToPrec18() {
        String result = translator.translate("mysql", "postgresql", col("x", "DECIMAL", 0, 0));
        assertTrue(result.startsWith("NUMERIC(18"), "Expected NUMERIC(18,...) but got " + result);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private void assertTrans(String src, String tgt, String rawType, int size, int decimal, String expected) {
        ColumnDef c = col("col", rawType, size, decimal);
        String result = translator.translate(src, tgt, c);
        assertTrue(result.toUpperCase().startsWith(expected.toUpperCase()),
                "Expected result starting with '" + expected + "' but got '" + result + "'");
    }

    static ColumnDef col(String name, String typeName, int size, int decimalDigits) {
        ColumnDef c = new ColumnDef();
        c.setName(name);
        c.setTypeName(typeName);
        c.setSize(size);
        c.setDecimalDigits(decimalDigits);
        return c;
    }
}
