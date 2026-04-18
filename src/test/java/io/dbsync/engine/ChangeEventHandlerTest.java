package io.dbsync.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dbsync.config.DatabaseConfig;
import io.dbsync.config.SyncConfig;
import io.dbsync.config.SyncOptions;
import io.dbsync.progress.SyncProgressRegistry;
import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ChangeEventHandlerTest {

    private SyncConfig config;
    private SyncProgressRegistry registry;
    private TableWriter mockWriter;
    private ChangeEventHandler handler;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        config   = buildConfig();
        registry = new SyncProgressRegistry();
        mockWriter = Mockito.mock(TableWriter.class);
        // factory always returns the same mock for every table
        handler  = new ChangeEventHandler(config, registry, (cfg, table) -> mockWriter);
    }

    @Test
    void tombstoneEventIsIgnored() {
        handler.accept(event(null, null));
        Mockito.verifyNoInteractions(mockWriter);
    }

    @Test
    void missingPayloadIsIgnored() throws Exception {
        handler.accept(event(null, "{\"no_payload\": true}"));
        Mockito.verifyNoInteractions(mockWriter);
    }

    @Test
    void snapshotReadCallsInsert() throws Exception {
        String value = changeEventJson("r", "users", null, Map.of("id", 1, "name", "Alice"));
        handler.accept(event(null, value));
        Mockito.verify(mockWriter, Mockito.times(1)).insert(Mockito.any());
    }

    @Test
    void insertCallsInsert() throws Exception {
        String value = changeEventJson("c", "users", null, Map.of("id", 2, "name", "Bob"));
        handler.accept(event(null, value));
        Mockito.verify(mockWriter, Mockito.times(1)).insert(Mockito.any());
    }

    @Test
    void updateCallsUpsert() throws Exception {
        String value = changeEventJson("u", "orders",
                Map.of("id", 5, "total", 10.0),
                Map.of("id", 5, "total", 20.0));
        handler.accept(event(null, value));
        Mockito.verify(mockWriter, Mockito.times(1)).upsert(Mockito.any(), Mockito.any());
    }

    @Test
    void deleteCallsDelete() throws Exception {
        String value = changeEventJson("d", "orders",
                Map.of("id", 5, "total", 10.0), null);
        handler.accept(event(null, value));
        Mockito.verify(mockWriter, Mockito.times(1)).delete(Mockito.any());
    }

    @Test
    void snapshotReadUpdatesRegistry() throws Exception {
        String value = changeEventJson("r", "users", null, Map.of("id", 1));
        handler.accept(event(null, value));
        assertNotNull(registry.get("users"));
        assertEquals(1L, registry.get("users").getSnapshotScanned().get());
    }

    @Test
    void insertUpdatesCdcRegistry() throws Exception {
        String value = changeEventJson("c", "users", null, Map.of("id", 2));
        handler.accept(event(null, value));
        assertNotNull(registry.get("users"));
        assertEquals(1L, registry.get("users").getCdcInserts().get());
    }

    @Test
    void updateUpdatesCdcRegistry() throws Exception {
        String value = changeEventJson("u", "users", Map.of("id", 1), Map.of("id", 1));
        handler.accept(event(null, value));
        assertEquals(1L, registry.get("users").getCdcUpdates().get());
    }

    @Test
    void deleteUpdatesCdcRegistry() throws Exception {
        String value = changeEventJson("d", "users", Map.of("id", 1), null);
        handler.accept(event(null, value));
        assertEquals(1L, registry.get("users").getCdcDeletes().get());
    }

    @Test
    void emptyTableNameInEventIsIgnored() throws Exception {
        String value = changeEventJson("c", "", null, Map.of("id", 1));
        handler.accept(event(null, value));
        Mockito.verifyNoInteractions(mockWriter);
    }

    @Test
    void writerExceptionIsHandledGracefully() throws Exception {
        Mockito.doThrow(new RuntimeException("DB down")).when(mockWriter).insert(Mockito.any());
        String value = changeEventJson("c", "users", null, Map.of("id", 1));
        // Should not throw
        assertDoesNotThrow(() -> handler.accept(event(null, value)));
    }

    @Test
    void closeCallsCloseOnAllWriters() throws Exception {
        // trigger writer creation by sending an event
        String value = changeEventJson("c", "users", null, Map.of("id", 1));
        handler.accept(event(null, value));

        handler.close();
        Mockito.verify(mockWriter, Mockito.atLeastOnce()).close();
    }

    // ── Decimal decoding tests ─────────────────────────────────────────────────

    /**
     * Debezium encodes DECIMAL(10,2) value 9.99 as Base64 "A+c=" (bytes 0x03, 0xE7 = 999, scale=2).
     */
    @Test
    void decodeDecimalNode_decodesBase64ToCorrectBigDecimal() {
        // "A+c=" → bytes [0x03, 0xE7] = 999, scale=2 → 9.99
        var fieldSchema = mapper.createObjectNode();
        fieldSchema.put("name", "org.apache.kafka.connect.data.Decimal");
        var params = mapper.createObjectNode();
        params.put("scale", "2");
        fieldSchema.set("parameters", params);

        var val = mapper.createObjectNode();
        val.put("price", "A+c=");
        JsonNode decoded = handler.decodeDecimalNode(val.get("price"), fieldSchema);

        assertTrue(decoded.isBigDecimal(), "should be a BigDecimal node");
        assertEquals(new java.math.BigDecimal("9.99"), decoded.decimalValue());
    }

    @Test
    void decodeDecimalNode_nullValueReturnsNull() {
        var fieldSchema = mapper.createObjectNode();
        fieldSchema.put("name", ChangeEventHandler.DEBEZIUM_DECIMAL);
        var params = mapper.createObjectNode();
        params.put("scale", "2");
        fieldSchema.set("parameters", params);

        var nullNode = mapper.nullNode();
        JsonNode result = handler.decodeDecimalNode(nullNode, fieldSchema);
        assertTrue(result.isNull());
    }

    @Test
    void decodeRecord_convertsDecimalFieldsAndLeavesOthersAlone() throws Exception {
        // Build a field schema map: "price" is Decimal(2), "name" is plain string
        var priceSchema = mapper.createObjectNode();
        priceSchema.put("name", ChangeEventHandler.DEBEZIUM_DECIMAL);
        var params = mapper.createObjectNode();
        params.put("scale", "2");
        priceSchema.set("parameters", params);

        Map<String, JsonNode> schemas = new java.util.HashMap<>();
        schemas.put("price", priceSchema);

        var record = mapper.createObjectNode();
        record.put("id", 1);
        record.put("name", "Widget A");
        record.put("price", "A+c="); // 9.99

        JsonNode result = handler.decodeRecord(record, schemas);

        assertEquals(1, result.path("id").intValue());
        assertEquals("Widget A", result.path("name").textValue());
        assertTrue(result.path("price").isBigDecimal(), "price should be decoded to BigDecimal");
        assertEquals(new java.math.BigDecimal("9.99"), result.path("price").decimalValue());
    }

    @Test
    void decodeRecord_nullRecordReturnsAsIs() {
        var nullNode = mapper.nullNode();
        JsonNode result = handler.decodeRecord(nullNode, Map.of());
        assertTrue(result.isNull());
    }

    @Test
    void extractFieldSchemas_extractsAfterFields() throws Exception {
        // Minimal schema JSON matching Debezium envelope format
        String schemaJson = """
                {
                  "schema": {
                    "fields": [
                      {"field": "before", "fields": []},
                      {"field": "after", "fields": [
                        {"field": "price", "name": "org.apache.kafka.connect.data.Decimal",
                         "parameters": {"scale": "2"}}
                      ]}
                    ]
                  },
                  "payload": {}
                }
                """;
        JsonNode root = mapper.readTree(schemaJson);
        Map<String, JsonNode> schemas = handler.extractFieldSchemas(root, "after");

        assertEquals(1, schemas.size());
        assertTrue(schemas.containsKey("price"));
        assertEquals("org.apache.kafka.connect.data.Decimal",
                schemas.get("price").path("name").asText());
    }

    @Test
    void fullEvent_decimalFieldIsWrittenAsDecimal() throws Exception {
        // Build a realistic Debezium JSON event with schema + payload
        String eventJson = """
                {
                  "schema": {
                    "fields": [
                      {"type":"struct","field":"after","fields":[
                        {"type":"bytes","name":"org.apache.kafka.connect.data.Decimal",
                         "field":"price","parameters":{"scale":"2","connect.decimal.precision":"10"}}
                      ]},
                      {"type":"struct","field":"before","fields":[]}
                    ]
                  },
                  "payload": {
                    "op": "r",
                    "source": {"table": "products", "db": "sourcedb"},
                    "before": null,
                    "after": {"price": "A+c="}
                  }
                }
                """;
        // Capture what was actually written
        var writtenNodes = new java.util.ArrayList<JsonNode>();
        Mockito.doAnswer(inv -> { writtenNodes.add(inv.getArgument(0)); return null; })
               .when(mockWriter).insert(Mockito.any());

        handler.accept(event(null, eventJson));

        assertEquals(1, writtenNodes.size());
        JsonNode written = writtenNodes.get(0);
        assertTrue(written.path("price").isBigDecimal(),
                "price should arrive at writer as BigDecimal, not Base64 string");
        assertEquals(new java.math.BigDecimal("9.99"), written.path("price").decimalValue());
    }


    private SyncConfig buildConfig() {
        SyncConfig c = new SyncConfig();
        DatabaseConfig src = new DatabaseConfig();
        src.setType("mysql"); src.setDatabase("srcdb");
        c.setSource(src);
        DatabaseConfig tgt = new DatabaseConfig();
        tgt.setType("postgresql"); tgt.setDatabase("tgtdb");
        tgt.setUrl("jdbc:h2:mem:unused");
        tgt.setUsername("sa"); tgt.setPassword("");
        c.setTarget(tgt);
        c.setSync(new SyncOptions());
        return c;
    }

    private String changeEventJson(String op, String table,
                                    Map<String, Object> before,
                                    Map<String, Object> after) throws Exception {
        Map<String, Object> source = Map.of("table", table, "db", "testdb");
        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        payload.put("op", op);
        payload.put("source", source);
        payload.put("before", before);
        payload.put("after",  after);
        return mapper.writeValueAsString(Map.of("payload", payload));
    }

    private ChangeEvent<String, String> event(String key, String value) {
        @SuppressWarnings("unchecked")
        ChangeEvent<String, String> e = Mockito.mock(ChangeEvent.class);
        Mockito.when(e.key()).thenReturn(key);
        Mockito.when(e.value()).thenReturn(value);
        Mockito.when(e.destination()).thenReturn("test");
        return e;
    }
}
