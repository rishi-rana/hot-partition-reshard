package com.hotpartition.transport.rpc;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RpcBatchEnvelopeTest {

    @Test
    void encodeThenDecodeRoundTripsMultipleItems() {
        List<RpcBatchEnvelope.Item> items = List.of(
                new RpcBatchEnvelope.Item("k1", 10L, "v1".getBytes(StandardCharsets.UTF_8)),
                new RpcBatchEnvelope.Item("k2", 11L, "a-longer-value-2".getBytes(StandardCharsets.UTF_8)),
                new RpcBatchEnvelope.Item(null, 12L, new byte[0])
        );

        byte[] encoded = RpcBatchEnvelope.encode(items);
        List<RpcBatchEnvelope.Item> decoded = RpcBatchEnvelope.decode(encoded);

        assertEquals(3, decoded.size());
        assertEquals("k1", decoded.get(0).key());
        assertEquals(10L, decoded.get(0).offset());
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), decoded.get(0).value());
        assertEquals("k2", decoded.get(1).key());
        assertArrayEquals("a-longer-value-2".getBytes(StandardCharsets.UTF_8), decoded.get(1).value());
        assertEquals(null, decoded.get(2).key());
        assertEquals(0, decoded.get(2).value().length);
    }

    @Test
    void emptyBatchRoundTrips() {
        byte[] encoded = RpcBatchEnvelope.encode(List.of());
        List<RpcBatchEnvelope.Item> decoded = RpcBatchEnvelope.decode(encoded);
        assertEquals(0, decoded.size());
    }
}
