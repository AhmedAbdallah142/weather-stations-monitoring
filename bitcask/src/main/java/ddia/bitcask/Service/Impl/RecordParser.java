package ddia.bitcask.service.Impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import ddia.bitcask.model.Key;
import ddia.bitcask.model.RecordRef;

public class RecordParser {

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

    public static Map.Entry<Key, RecordRef> readNextRecordHint(InputStream hintFilStream) throws IOException {
        // record: keySize|recordSize|offset|key
        var recordFixedSize = keySizeBytes + valueSizeBytes + offestSizeBytes;

        var recordHead = hintFilStream.readNBytes(recordFixedSize);
        if(recordHead.length < recordFixedSize)
            throw new RuntimeException("Failed to read the record");

        ByteBuffer buffer = ByteBuffer.wrap(recordHead);

        int ksz = buffer.getShort() & 0xffff;
        int rsz = buffer.getInt();
        long offset = buffer.getLong();

        byte[] keyBytes = hintFilStream.readNBytes(ksz);
        if(keyBytes.length < ksz)
            throw new RuntimeException("Failed to read the record");

        var recordRef = RecordRef.builder().offset(offset).recordLength(rsz).build();
        return Map.entry(new Key(keyBytes), recordRef);
    }

    public static Map.Entry<Key, RecordRef> readNextRecordData(InputStream dataFilStream) throws IOException {
        // record: keySize|valueSize|key|value
        var recordFixedSize = keySizeBytes + valueSizeBytes;

        var recordHead = dataFilStream.readNBytes(recordFixedSize);
        if(recordHead.length < recordFixedSize)
            throw new RuntimeException("Failed to read the record");

        ByteBuffer buffer = ByteBuffer.wrap(recordHead);

        int ksz = buffer.getShort() & 0xffff;
        int vsz = buffer.getInt();

        byte[] keyBytes = dataFilStream.readNBytes(ksz);
        if(keyBytes.length < ksz)
            throw new RuntimeException("Failed to read the record");
        byte[] valueBytes = dataFilStream.readNBytes(vsz);
        if(valueBytes.length < vsz)
            throw new RuntimeException("Failed to read the record");

        var recordRef = RecordRef.builder().recordLength(recordFixedSize + ksz + vsz).build();
        return Map.entry(new Key(keyBytes), recordRef);
    }

}
