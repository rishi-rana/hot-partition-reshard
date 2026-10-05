package com.hotpartition.transport.rpc;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RpcEnvelopeTest {

    @Test
    void encodeThenDecodeRoundTrips() {
        byte[] value = "hello world".getBytes(StandardCharsets.UTF_8);
        byte[] encoded = RpcEnvelope.encode("my-key", 42L, value);

        RpcEnvelope.Decoded decoded = RpcEnvelope.decode(encoded);

        assertEquals("my-key", decoded.key());
        assertEquals(42L, decoded.offset());
        assertArrayEquals(value, decoded.value());
    }

    @Test
    void nullKeyRoundTrips() {
        byte[] value = "v".getBytes(StandardCharsets.UTF_8);
        byte[] encoded = RpcEnvelope.encode(null, 7L, value);

        RpcEnvelope.Decoded decoded = RpcEnvelope.decode(encoded);

        assertNull(decoded.key());
        assertEquals(7L, decoded.offset());
    }

    @Test
    void emptyValueRoundTrips() {
        byte[] encoded = RpcEnvelope.encode("k", 0L, new byte[0]);

        RpcEnvelope.Decoded decoded = RpcEnvelope.decode(encoded);

        assertEquals("k", decoded.key());
        assertEquals(0, decoded.value().length);
    }
}
