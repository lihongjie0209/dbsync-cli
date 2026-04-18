package io.dbsync.schema;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * Maps column type names between MySQL and PostgreSQL when schemas are heterogeneous.
 * <p>
 * Type names are derived from JDBC {@code DatabaseMetaData.getColumns()} {@code TYPE_NAME},
 * normalised to upper-case before lookup.  When no mapping is found the translator falls
 * back to a safe default ({@code TEXT} for PostgreSQL targets, {@code LONGTEXT} for MySQL
 * targets) and emits a WARN log so operators can add an explicit mapping if needed.
 */
@ApplicationScoped
public class DdlTranslator {

    private static final Logger LOG = Logger.getLogger(DdlTranslator.class);

    // ── MySQL / MariaDB → PostgreSQL ──────────────────────────────────────
    private static final Map<String, String> MYSQL_TO_PG = Map.ofEntries(
        // Integer types (signed)
        Map.entry("TINYINT",              "SMALLINT"),
        Map.entry("SMALLINT",             "SMALLINT"),
        Map.entry("MEDIUMINT",            "INTEGER"),
        Map.entry("INT",                  "INTEGER"),
        Map.entry("INTEGER",              "INTEGER"),
        Map.entry("BIGINT",               "BIGINT"),
        // Integer types (unsigned) – promote to avoid overflow
        Map.entry("TINYINT UNSIGNED",     "SMALLINT"),
        Map.entry("SMALLINT UNSIGNED",    "INTEGER"),
        Map.entry("MEDIUMINT UNSIGNED",   "INTEGER"),
        Map.entry("INT UNSIGNED",         "BIGINT"),
        Map.entry("INTEGER UNSIGNED",     "BIGINT"),
        Map.entry("BIGINT UNSIGNED",      "NUMERIC(20, 0)"),
        // Boolean / Bit
        Map.entry("BIT",                  "BOOLEAN"),
        Map.entry("BOOLEAN",              "BOOLEAN"),
        // Floating-point
        Map.entry("FLOAT",                "REAL"),
        Map.entry("FLOAT UNSIGNED",       "REAL"),
        Map.entry("DOUBLE",               "DOUBLE PRECISION"),
        Map.entry("DOUBLE UNSIGNED",      "DOUBLE PRECISION"),
        Map.entry("DOUBLE PRECISION",     "DOUBLE PRECISION"),
        // Fixed-point
        Map.entry("DECIMAL",              "NUMERIC"),
        Map.entry("DECIMAL UNSIGNED",     "NUMERIC"),
        Map.entry("NUMERIC",              "NUMERIC"),
        Map.entry("NUMERIC UNSIGNED",     "NUMERIC"),
        // String types
        Map.entry("CHAR",                 "CHAR"),
        Map.entry("VARCHAR",              "VARCHAR"),
        Map.entry("TINYTEXT",             "TEXT"),
        Map.entry("TEXT",                 "TEXT"),
        Map.entry("MEDIUMTEXT",           "TEXT"),
        Map.entry("LONGTEXT",             "TEXT"),
        // Date / Time
        Map.entry("DATE",                 "DATE"),
        Map.entry("TIME",                 "TIME"),
        Map.entry("YEAR",                 "SMALLINT"),
        Map.entry("DATETIME",             "TIMESTAMP"),
        Map.entry("TIMESTAMP",            "TIMESTAMPTZ"),
        // Binary
        Map.entry("BINARY",               "BYTEA"),
        Map.entry("VARBINARY",            "BYTEA"),
        Map.entry("TINYBLOB",             "BYTEA"),
        Map.entry("BLOB",                 "BYTEA"),
        Map.entry("MEDIUMBLOB",           "BYTEA"),
        Map.entry("LONGBLOB",             "BYTEA"),
        // Structured / semi-structured
        Map.entry("JSON",                 "JSONB"),
        Map.entry("ENUM",                 "TEXT"),
        Map.entry("SET",                  "TEXT"),
        // MariaDB-specific native types
        Map.entry("UUID",                 "UUID"),
        Map.entry("INET4",                "INET"),
        Map.entry("INET6",                "VARCHAR(45)"),
        // Spatial – no PostGIS assumed; store as text
        Map.entry("GEOMETRY",             "TEXT"),
        Map.entry("POINT",                "TEXT"),
        Map.entry("LINESTRING",           "TEXT"),
        Map.entry("POLYGON",              "TEXT"),
        Map.entry("MULTIPOINT",           "TEXT"),
        Map.entry("MULTILINESTRING",      "TEXT"),
        Map.entry("MULTIPOLYGON",         "TEXT"),
        Map.entry("GEOMETRYCOLLECTION",   "TEXT")
    );

    // ── PostgreSQL → MySQL / MariaDB ──────────────────────────────────────
    private static final Map<String, String> PG_TO_MYSQL = Map.ofEntries(
        // Integer types (pg_typname aliases returned by JDBC)
        Map.entry("INT2",                        "SMALLINT"),
        Map.entry("INT4",                        "INT"),
        Map.entry("INT8",                        "BIGINT"),
        Map.entry("SMALLINT",                    "SMALLINT"),
        Map.entry("INTEGER",                     "INT"),
        Map.entry("BIGINT",                      "BIGINT"),
        Map.entry("OID",                         "BIGINT"),
        // Serial pseudo-types
        Map.entry("SMALLSERIAL",                 "SMALLINT"),
        Map.entry("SERIAL",                      "INT"),
        Map.entry("BIGSERIAL",                   "BIGINT"),
        // Floating-point
        Map.entry("FLOAT4",                      "FLOAT"),
        Map.entry("FLOAT8",                      "DOUBLE"),
        Map.entry("REAL",                        "FLOAT"),
        Map.entry("DOUBLE PRECISION",            "DOUBLE"),
        // Fixed-point
        Map.entry("NUMERIC",                     "DECIMAL"),
        Map.entry("DECIMAL",                     "DECIMAL"),
        Map.entry("MONEY",                       "DECIMAL(19, 2)"),
        // String types (bpchar = blank-padded char, PostgreSQL internal name)
        Map.entry("BPCHAR",                      "CHAR"),
        Map.entry("CHARACTER",                   "CHAR"),
        Map.entry("CHAR",                        "CHAR"),
        Map.entry("VARCHAR",                     "VARCHAR"),
        Map.entry("CHARACTER VARYING",           "VARCHAR"),
        Map.entry("TEXT",                        "LONGTEXT"),
        // Date / Time
        Map.entry("DATE",                        "DATE"),
        Map.entry("TIME",                        "TIME"),
        Map.entry("TIME WITHOUT TIME ZONE",      "TIME"),
        Map.entry("TIMETZ",                      "TIME"),
        Map.entry("TIME WITH TIME ZONE",         "TIME"),
        Map.entry("TIMESTAMP",                   "DATETIME"),
        Map.entry("TIMESTAMP WITHOUT TIME ZONE", "DATETIME"),
        Map.entry("TIMESTAMPTZ",                 "TIMESTAMP"),
        Map.entry("TIMESTAMP WITH TIME ZONE",    "TIMESTAMP"),
        Map.entry("INTERVAL",                    "VARCHAR(64)"),
        // Boolean
        Map.entry("BOOL",                        "TINYINT(1)"),
        Map.entry("BOOLEAN",                     "TINYINT(1)"),
        // Binary
        Map.entry("BYTEA",                       "LONGBLOB"),
        // Bit types
        Map.entry("BIT",                         "BIT"),
        Map.entry("VARBIT",                      "VARBINARY"),
        Map.entry("BIT VARYING",                 "VARBINARY"),
        // JSON
        Map.entry("JSON",                        "JSON"),
        Map.entry("JSONB",                       "JSON"),
        // Structured
        Map.entry("UUID",                        "CHAR(36)"),
        Map.entry("XML",                         "LONGTEXT"),
        // Network address types
        Map.entry("INET",                        "VARCHAR(45)"),
        Map.entry("CIDR",                        "VARCHAR(18)"),
        Map.entry("MACADDR",                     "VARCHAR(17)"),
        // Full-text search types
        Map.entry("TSVECTOR",                    "TEXT"),
        Map.entry("TSQUERY",                     "TEXT"),
        // Geometric types – no MySQL GIS counterpart without extension
        Map.entry("POINT",                       "TEXT"),
        Map.entry("LINE",                        "TEXT"),
        Map.entry("LSEG",                        "TEXT"),
        Map.entry("BOX",                         "TEXT"),
        Map.entry("PATH",                        "TEXT"),
        Map.entry("POLYGON",                     "TEXT"),
        Map.entry("CIRCLE",                      "TEXT")
    );

    /**
     * Translate a column's type from {@code sourceDbType} to {@code targetDbType}.
     * Returns a full SQL type string suitable for use in DDL (e.g., {@code VARCHAR(255)}).
     */
    public String translate(String sourceDbType, String targetDbType, ColumnDef col) {
        if (sourceDbType.equalsIgnoreCase(targetDbType)) {
            return buildTypeWithSize(col.getTypeName(), col);
        }
        // Treat mariadb and mysql as the same family for cross-DB translation
        String normalizedSrc = normalize(sourceDbType);
        String normalizedTgt = normalize(targetDbType);
        if (normalizedSrc.equalsIgnoreCase(normalizedTgt)) {
            return buildTypeWithSize(col.getTypeName(), col);
        }

        boolean srcMySQL = "mysql".equals(normalizedSrc);
        Map<String, String> mapping = srcMySQL ? MYSQL_TO_PG : PG_TO_MYSQL;

        String rawType = col.getTypeName().toUpperCase().trim();
        int parenIdx = rawType.indexOf('(');
        String baseType = parenIdx >= 0 ? rawType.substring(0, parenIdx).trim() : rawType;

        // MySQL TINYINT(1) is conventionally a boolean flag
        if (srcMySQL && "TINYINT".equals(baseType) && col.getSize() == 1) {
            return "BOOLEAN";
        }

        // PostgreSQL array types (JDBC typname prefixed with '_', e.g. _int4, _varchar)
        if (!srcMySQL && baseType.startsWith("_")) {
            LOG.warnf("PostgreSQL array type '%s' has no MySQL equivalent; using LONGTEXT", col.getTypeName());
            return "LONGTEXT";
        }

        String targetType = mapping.get(baseType);

        if (targetType == null) {
            // Last-chance: an UNSIGNED variant not in the map → retry without qualifier
            if (baseType.endsWith(" UNSIGNED")) {
                String withoutUnsigned = baseType.substring(0, baseType.length() - " UNSIGNED".length());
                targetType = mapping.get(withoutUnsigned);
            }
        }

        if (targetType == null) {
            String fallback = srcMySQL ? "TEXT" : "LONGTEXT";
            LOG.warnf("No type mapping for %s→%s of source type '%s'; falling back to %s",
                    sourceDbType, targetDbType, col.getTypeName(), fallback);
            return fallback;
        }

        return buildTypeWithSize(targetType, col);
    }

    private String buildTypeWithSize(String targetType, ColumnDef col) {
        String upper = targetType.toUpperCase();
        // Only append size when the type declaration doesn't already include it
        if (!upper.contains("(")) {
            if (upper.contains("VARCHAR") || upper.equals("CHAR") || upper.equals("BPCHAR")) {
                int sz = col.getSize() > 0 ? col.getSize() : 255;
                return targetType + "(" + sz + ")";
            }
            if (upper.contains("NUMERIC") || upper.contains("DECIMAL")) {
                int precision = col.getSize() > 0 ? col.getSize() : 18;
                int scale = col.getDecimalDigits();
                return targetType + "(" + precision + ", " + scale + ")";
            }
        }
        return targetType;
    }

    /** Normalises database type names: mariadb is treated as mysql; kingbase is treated as postgresql for type mapping. */
    private static String normalize(String dbType) {
        if ("mariadb".equalsIgnoreCase(dbType)) return "mysql";
        if ("kingbase".equalsIgnoreCase(dbType)) return "postgresql";
        return dbType.toLowerCase();
    }
}
