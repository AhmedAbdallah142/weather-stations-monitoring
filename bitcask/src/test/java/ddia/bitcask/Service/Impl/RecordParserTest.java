package ddia.bitcask.service.Impl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import ddia.bitcask.model.Key;

public class RecordParserTest {

    @Test
    void testRecordParser() {
        byte[] key = { 1, 2, 3, 4, 5 };
        byte[] value = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        var record = RecordParser.toRecord(new Key(key), value);
        var pair = RecordParser.toKeyValuePair(record);

        assertTrue(Arrays.equals(key, pair.getKey().getBytes()));
        assertTrue(Arrays.equals(value, pair.getValue()));
    }
}
