package io.dbsync.schema;

import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class SchemaApplier {

    private static final Logger log = Logger.getLogger(SchemaApplier.class);

    @Inject
    SchemaReader schemaReader;

    @Inject
    DdlTranslator ddlTranslator;

    public SchemaApplier() {}  // CDI requires explicit no-arg once we add the parameterized one below

    /** For use outside the CDI container (tests, CLI orchestration). */
    public SchemaApplier(SchemaReader reader, DdlTranslator translator) {
        this.schemaReader = reader;
        this.ddlTranslator = translator;
    }

    /**
     * Reads all configured tables from the source and creates / updates them in the target.
     * Returns the list of table definitions that will be synced.
     */
    public List<TableDef> sync(SyncConfig config) throws Exception {
        List<TableDef> sourceTables = schemaReader.readTables(
                config.getSource(), config.getSync().getTables());

        if (!config.getSync().isSchemaSync()) {
            log.info("schemaSync disabled — skipping DDL synchronisation");
            return sourceTables;
        }

        DatabaseConfig tgt = config.getTarget();
        try (Connection conn = DriverManager.getConnection(tgt.jdbcUrl(),
                tgt.getUsername(), tgt.getPassword())) {
            conn.setAutoCommit(true);
            for (TableDef table : sourceTables) {
                syncTable(conn, table, config.getSource().getType(), tgt.getType());
            }
        }
        return sourceTables;
    }

    private void syncTable(Connection conn, TableDef table, String srcType, String tgtType) throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        boolean pgFamily = "postgresql".equals(tgtType) || "kingbase".equals(tgtType);
        String catalog = pgFamily ? null : conn.getCatalog();
        String schema  = pgFamily ? "public" : null;

        try (ResultSet rs = meta.getTables(catalog, schema, table.getName(), new String[]{"TABLE"})) {
            if (!rs.next()) {
                String ddl = generateCreateDdl(table, srcType, tgtType);
                log.infof("Creating table %s", table.getName());
                log.debugf("DDL: %s", ddl);
                conn.createStatement().execute(ddl);
            } else {
                alterTableIfNeeded(conn, meta, catalog, schema, table, srcType, tgtType);
            }
        }
    }

    private String generateCreateDdl(TableDef table, String srcType, String tgtType) {
        List<String> colDefs = new ArrayList<>();
        for (ColumnDef col : table.getColumns()) {
            colDefs.add(buildColDdl(col, srcType, tgtType));
        }

        if (!table.getPrimaryKeys().isEmpty()) {
            String pkList = table.getPrimaryKeys().stream()
                    .map(k -> quoteId(k, tgtType))
                    .collect(Collectors.joining(", "));
            colDefs.add("  PRIMARY KEY (" + pkList + ")");
        }

        return "CREATE TABLE IF NOT EXISTS " + quoteId(table.getName(), tgtType)
                + " (\n" + String.join(",\n", colDefs) + "\n)";
    }

    private String buildColDdl(ColumnDef col, String srcType, String tgtType) {
        StringBuilder sb = new StringBuilder("  ");
        sb.append(quoteId(col.getName(), tgtType)).append(" ");

        // Auto-increment columns get DB-native serial type
        if (col.isAutoIncrement()) {
            boolean pgFamily = "postgresql".equals(tgtType) || "kingbase".equals(tgtType);
            sb.append(pgFamily ? "BIGSERIAL" : "BIGINT AUTO_INCREMENT");
        } else {
            sb.append(ddlTranslator.translate(srcType, tgtType, col));
        }

        if (!col.isNullable() && !col.isAutoIncrement()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    private void alterTableIfNeeded(Connection conn, DatabaseMetaData meta,
            String catalog, String schema, TableDef srcTable, String srcType, String tgtType) throws Exception {

        // Collect existing target columns
        List<String> existingCols = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(catalog, schema, srcTable.getName(), null)) {
            while (rs.next()) {
                existingCols.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
        }

        for (ColumnDef col : srcTable.getColumns()) {
            if (!existingCols.contains(col.getName().toLowerCase())) {
                String addColDdl = "ALTER TABLE " + quoteId(srcTable.getName(), tgtType)
                        + " ADD COLUMN " + buildColDdl(col, srcType, tgtType);
                log.infof("Adding column %s.%s", srcTable.getName(), col.getName());
                conn.createStatement().execute(addColDdl);
            }
        }
    }

    private String quoteId(String name, String dbType) {
        return ("postgresql".equals(dbType) || "kingbase".equals(dbType))
                ? "\"" + name + "\"" : "`" + name + "`";
    }
}
