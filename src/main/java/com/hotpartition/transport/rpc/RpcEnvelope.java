package com.hotpartition.transport.rpc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Wire format for transport Option A (direct RPC): a record's key, original offset, and
 * value, framed as [4-byte key length][key utf8][8-byte offset][value bytes]. Deliberately
 * hand-rolled binary framing rather than a serialization library -- the record's value is
 * an arbitrary byte[] (already-serialized application payload), so a text format would
 * need escaping for no benefit, and this prototype has no other use for a JSON/protobuf
 * dependency.
 */
final class RpcEnvelope {

    private RpcEnvelope() {
    }

    record Decoded(String key, long offset, byte[] value) {
    }

    static byte[] encode(String key, long offset, byte[] value) {
        byte[] keyBytes = key == null ? new byte[0] : key.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(4 + keyBytes.length + 8 + value.length);
        out.writeBytes(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
        out.writeBytes(keyBytes);
        out.writeBytes(ByteBuffer.allocate(8).putLong(offset).array());
        out.writeBytes(value);
        return out.toByteArray();
    }

    static Decoded decode(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        int keyLen = buf.getInt();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        long offset = buf.getLong();
        byte[] value = new byte[buf.remaining()];
        buf.get(value);
        String key = keyLen == 0 ? null : new String(keyBytes, StandardCharsets.UTF_8);
        return new Decoded(key, offset, value);
    }
}
