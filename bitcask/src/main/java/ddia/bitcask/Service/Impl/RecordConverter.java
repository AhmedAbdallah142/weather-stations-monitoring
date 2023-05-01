package ddia.bitcask.Service.Impl;

import java.nio.ByteBuffer;
import java.util.Map;

import ddia.bitcask.model.Key;

public class RecordConverter {

    public static final int keySizeBytes = 2, valueSizeBytes = 4;

    public static byte[] toRecord(Key key, byte[] value) {
        // record: keySize|valueSize|key|value

        ByteBuffer buffer = ByteBuffer
                .allocate(keySizeBytes + valueSizeBytes + key.getBytes().length + value.length);

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

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.putShort((short) 1);
        buffer.putShort((short) 1);

        System.out.println(buffer.position());
    }

}
