package ddia.bitcask.Service.Impl;

import java.nio.ByteBuffer;
import java.util.Map;

import ddia.bitcask.model.Key;

public class RecordConverter {

    public static final int keySizeBytes = 2, valueSizeBytes = 4, offestSizeBytes = 8;

    public static byte[] toRecord(Key key, byte[] value) {
        // record: keySize|valueSize|key|value

        final int size = keySizeBytes + valueSizeBytes + key.getBytes().length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putShort((short) key.getBytes().length)
                .putInt(value.length)
                .put(key.getBytes())
                .put(value);

        return buffer.array();
    }

    public static Map.Entry<Key, byte[]> toKeyValuePair(byte[] record) {
        ByteBuffer buffer = ByteBuffer.wrap(record);

        int ksz = buffer.getShort() & 0xffff;
        int vsz = buffer.getInt();
        byte[] keyBytes = new byte[ksz];
        byte[] value = new byte[vsz];
        buffer.get(keyBytes, 0, ksz);
        buffer.get(value, 0, vsz);

        return Map.entry(new Key(keyBytes), value);
    }

    public static byte[] toHintRecord(Key key, int recordSize, long offset) {
        // record: keySize|recordSize|offset|key

        final int size = keySizeBytes + valueSizeBytes + offestSizeBytes + key.getBytes().length;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putShort((short) key.getBytes().length)
                .putInt(recordSize)
                .putLong(offset)
                .put(key.getBytes());

        return buffer.array();
    }

}
