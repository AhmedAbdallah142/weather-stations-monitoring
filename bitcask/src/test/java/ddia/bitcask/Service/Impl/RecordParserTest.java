package ddia.bitcask.service.Impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

import ddia.bitcask.model.Key;

public class RecordParserTest {

    @Test
    void testRecordParser() {
        byte[] key = { 1, 2, 3, 4, 5 };
        byte[] value = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        var record = RecordParser.toRecord(new Key(key), value);
        var pair = RecordParser.toKeyValuePair(record);

        assertArrayEquals(key, pair.getKey().getBytes());
        assertArrayEquals(value, pair.getValue());
    }
}
