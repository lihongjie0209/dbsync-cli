package io.dbsync.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.progress.SyncProgressRegistry;
import io.debezium.engine.ChangeEvent;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Receives Debezium change events (JSON-formatted) and routes them to per-table writers.
 *
 * <p>Debezium represents MySQL {@code DECIMAL}/{@code NUMERIC} columns as
 * {@code org.apache.kafka.connect.data.Decimal} — a Base64-encoded, big-endian,
 * two's-complement byte array.  This class decodes those values to {@link BigDecimal}
 * before they reach {@link TableWriter}, so PostgreSQL {@code NUMERIC} columns receive
 * a proper numeric value instead of a raw Base64 string.
 */
public class ChangeEventHandler implements Consumer<ChangeEvent<String, String>> {

    private static final Logger log = Logger.getLogger(ChangeEventHandler.class);
    static final String DEBEZIUM_DECIMAL = "org.apache.kafka.connect.data.Decimal";

    private final SyncConfig config;
    private final SyncProgressRegistry registry;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, TableWriter> writers = new ConcurrentHashMap<>();
    private volatile boolean streamingStarted = false;
    /** Guards orphan-cleanup so it runs at most once per engine lifecycle. */
    private final AtomicBoolean orphanCleanupFired = new AtomicBoolean(false);

    private final java.util.function.BiFunction<DatabaseConfig, String, TableWriter> writerFactory;

    public ChangeEventHandler(SyncConfig config, SyncProgressRegistry registry) {
        this(config, registry, TableWriter::new);
    }

    /** Package-private constructor allowing injection of a mock writer factory in tests. */
    ChangeEventHandler(SyncConfig config, SyncProgressRegistry registry,
                       java.util.function.BiFunction<DatabaseConfig, String, TableWriter> writerFactory) {
        this.config = config;
        this.registry = registry;
        this.writerFactory = writerFactory;
    }

    @Override
    public void accept(ChangeEvent<String, String> event) {
        if (event.value() == null) return; // tombstone / schema-only event

        try {
            JsonNode root    = mapper.readTree(event.value());
            JsonNode payload = root.path("payload");
            if (payload.isMissingNode()) return;

            String op        = payload.path("op").asText("");
            String tableName = payload.path("source").path("table").asText("");

            if (tableName.isEmpty()) return;

            // Detect transition from snapshot → streaming so empty tables
            // (0 rows snapshotted) leave INITIALIZING state.
            String snapshotField = payload.path("source").path("snapshot").asText("false");
            boolean isStreaming  = "false".equals(snapshotField) || snapshotField.isEmpty();
            boolean isSnapshotLast = "last".equals(snapshotField);

            // "last" is emitted once for the very last row of the complete snapshot.
            // Trigger orphan cleanup here so zombie rows are cleaned even if no CDC
            // events arrive after snapshot (quiescent source).
            if (isSnapshotLast) {
                maybeRunOrphanCleaner();
            }

            if (isStreaming && !streamingStarted) {
                streamingStarted = true;
                registry.markStreamingStarted();
                maybeRunOrphanCleaner();
            }

            TableWriter writer = writers.computeIfAbsent(tableName,
                    t -> writerFactory.apply(config.getTarget(), t));

            // Build per-field schema maps so we can decode Decimal bytes
            Map<String, JsonNode> afterSchema  = extractFieldSchemas(root, "after");
            Map<String, JsonNode> beforeSchema = extractFieldSchemas(root, "before");

            JsonNode after  = decodeRecord(payload.path("after"),  afterSchema);
            JsonNode before = decodeRecord(payload.path("before"), beforeSchema);

            switch (op) {
                case "r" -> { // snapshot read
                    if (!after.isNull() && !after.isMissingNode()) writer.insert(after);
                    registry.markSnapshotRow(tableName);
                }
                case "c" -> { // create
                    if (!after.isNull() && !after.isMissingNode()) writer.insert(after);
                    registry.markCdcEvent(tableName, "INSERT");
                }
                case "u" -> { // update
                    if (!after.isNull() && !after.isMissingNode()) writer.upsert(after, before);
                    registry.markCdcEvent(tableName, "UPDATE");
                }
                case "d" -> { // delete
                    if (!before.isNull() && !before.isMissingNode()) {
                        writer.delete(before);
                    } else {
                        log.warnf("DELETE skipped for table=%s — before is null/missing", tableName);
                    }
                    registry.markCdcEvent(tableName, "DELETE");
                }
            }
        } catch (Exception e) {
            log.errorf(e, "Failed to handle event: %s", event.value());
        }
    }

    public void close() {
        writers.values().forEach(w -> {
            try { w.close(); } catch (Exception ignored) {}
        });
    }

    /** Runs the orphan cleaner exactly once (idempotent). */
    private void maybeRunOrphanCleaner() {
        if (!config.getSync().isCleanupOrphans()) return;
        if (!orphanCleanupFired.compareAndSet(false, true)) return;
        Thread.ofVirtual().name("orphan-cleaner").start(() -> {
            try {
                log.info("Orphan cleanup starting (zombie rows from previous sync runs)…");
                int deleted = new OrphanCleaner(config).cleanAll();
                if (deleted > 0) {
                    log.infof("Orphan cleanup complete — removed %d zombie row(s) from target", deleted);
                } else {
                    log.info("Orphan cleanup complete — no zombie rows found");
                }
            } catch (Exception e) {
                log.warnf(e, "Orphan cleanup failed");
            }
        });
    }

    // ── schema helpers ────────────────────────────────────────────────────────

    /**
     * Walks {@code root.schema.fields} to find the struct field named {@code structField}
     * ("after" or "before") and returns a map of child field name → child field schema node.
     */
    Map<String, JsonNode> extractFieldSchemas(JsonNode root, String structField) {
        Map<String, JsonNode> result = new HashMap<>();
        JsonNode topFields = root.path("schema").path("fields");
        for (JsonNode sf : topFields) {
            if (structField.equals(sf.path("field").asText())) {
                for (JsonNode f : sf.path("fields")) {
                    result.put(f.path("field").asText(), f);
                }
                break;
            }
        }
        return result;
    }

    /**
     * Returns a copy of {@code record} with any Debezium Decimal fields converted
     * from Base64 bytes to {@link BigDecimal}.  Non-Decimal fields are left unchanged.
     */
    JsonNode decodeRecord(JsonNode record, Map<String, JsonNode> fieldSchemas) {
        if (record == null || record.isNull() || record.isMissingNode()) return record;

        ObjectNode out = mapper.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> it = record.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            String  col       = entry.getKey();
            JsonNode val      = entry.getValue();
            JsonNode colSchema = fieldSchemas.getOrDefault(col, mapper.nullNode());

            if (DEBEZIUM_DECIMAL.equals(colSchema.path("name").asText())) {
                out.set(col, decodeDecimalNode(val, colSchema));
            } else {
                out.set(col, val);
            }
        }
        return out;
    }

    /**
     * Decodes a Base64-encoded, big-endian two's-complement byte array
     * (Debezium Decimal wire format) into a {@link BigDecimal} Jackson node.
     */
    JsonNode decodeDecimalNode(JsonNode val, JsonNode fieldSchema) {
        if (val == null || val.isNull()) return val;
        try {
            int scale = Integer.parseInt(
                    fieldSchema.path("parameters").path("scale").asText("0"));
            byte[] bytes = Base64.getDecoder().decode(val.asText());
            BigDecimal decimal = new BigDecimal(new BigInteger(bytes), scale);
            return new DecimalNode(decimal);
        } catch (Exception e) {
            log.warnf("Failed to decode Decimal field value '%s'; using raw value", val.asText());
            return val;
        }
    }
}
