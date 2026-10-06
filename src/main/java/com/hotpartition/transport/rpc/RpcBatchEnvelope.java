package com.hotpartition.transport.rpc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Wire format for the batched variant of transport Option A: several records in one HTTP
 * request body, framed as [4-byte count], then per record
 * [4-byte key length][key utf8][8-byte offset][4-byte value length][value bytes]. Unlike
 * {@link RpcEnvelope} (one record per request, value is "the rest of the body"), a batch
 * needs an explicit value length per record since multiple records are concatenated.
 */
final class RpcBatchEnvelope {

    private RpcBatchEnvelope() {
    }

    record Item(String key, long offset, byte[] value) {
    }

    static byte[] encode(List<Item> items) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(4).putInt(items.size()).array());
        for (Item item : items) {
            byte[] keyBytes = item.key() == null ? new byte[0] : item.key().getBytes(StandardCharsets.UTF_8);
            out.writeBytes(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
            out.writeBytes(keyBytes);
            out.writeBytes(ByteBuffer.allocate(8).putLong(item.offset()).array());
            out.writeBytes(ByteBuffer.allocate(4).putInt(item.value().length).array());
            out.writeBytes(item.value());
        }
        return out.toByteArray();
    }

    static List<Item> decode(byte[] body) {
        ByteBuffer buf = ByteBuffer.wrap(body);
        int count = buf.getInt();
        List<Item> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int keyLen = buf.getInt();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            long offset = buf.getLong();
            int valueLen = buf.getInt();
            byte[] value = new byte[valueLen];
            buf.get(value);
            String key = keyLen == 0 ? null : new String(keyBytes, StandardCharsets.UTF_8);
            items.add(new Item(key, offset, value));
        }
        return items;
    }
}
