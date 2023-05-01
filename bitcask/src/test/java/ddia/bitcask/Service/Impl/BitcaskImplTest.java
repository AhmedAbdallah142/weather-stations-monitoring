package ddia.bitcask.Service.Impl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import ddia.bitcask.Service.Bitcask;

public class BitcaskImplTest {

    @Test
    void testBitcask() throws IOException {
        Bitcask bitcask = new BitcaskImpl("data/test-dir");
        
        byte[] key1 = {1}, value1 = {1, 2, 3};
        byte[] key2 = {2}, value2 = {2, 4, 7}, value3 = {3, 5, 8, 9};
        bitcask.put(key1, value1);
        bitcask.put(key2, value2);
        bitcask.put(key2, value3);

        byte[] returnedValue = bitcask.get(key1);
        assertTrue(Arrays.equals(value1, returnedValue));

        returnedValue = bitcask.get(key2);
        assertTrue(Arrays.equals(value3, returnedValue));

        bitcask.close();
    }
}
