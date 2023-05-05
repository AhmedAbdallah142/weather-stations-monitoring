package ddia.bitcask.service.Impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import ddia.bitcask.service.Bitcask;

public class BitcaskImplTest {

    private final String testDir = "test/";

    String bitcaskBasic() throws IOException {
        String dir = testDir + RandomStringUtils.randomAlphanumeric(7);
        System.out.println(dir);
        Bitcask bitcask = new BitcaskImpl(dir, 64 * 1024 * 1024);

        byte[] key1 = { 1 }, value1 = { 1, 2, 3 };
        byte[] key2 = { 2 }, value2 = { 2, 4, 7 }, value3 = { 3, 5, 8, 9 };
        bitcask.put(key1, value1);
        bitcask.put(key2, value2);
        bitcask.put(key2, value3);

        assertArrayEquals(value1, bitcask.get(key1));
        assertArrayEquals(value3, bitcask.get(key2));

        bitcask.close();
        return dir;
    }

    String bitcaskMerge() throws IOException {
        String dir = testDir + RandomStringUtils.randomAlphanumeric(7);
        System.out.println(dir);
        BitcaskImpl bitcask = new BitcaskImpl(dir, 1);

        byte[] key1 = { 1 }, key2 = { 2 };
        byte[] value1 = { 1, 2, 3 }, value2 = { 2, 4, 7 }, value3 = { 3, 5, 8, 9 };
        bitcask.put(key1, value1);
        bitcask.put(key2, value2);
        bitcask.put(key1, value3);

        bitcask.doMerge();

        assertArrayEquals(value3, bitcask.get(key1));
        assertArrayEquals(value2, bitcask.get(key2));

        bitcask.close();
        return dir;
    }

    @Test
    void testBitcaskStartWithoutMerge() throws IOException {
        String dir = bitcaskBasic();
        BitcaskImpl bitcask = new BitcaskImpl(dir, 1024);

        byte[] key1 = { 1 }, value1 = { 1, 2, 3 };
        byte[] key2 = { 2 }, value3 = { 3, 5, 8, 9 };

        assertArrayEquals(value1, bitcask.get(key1));
        assertArrayEquals(value3, bitcask.get(key2));

        bitcask.close();
    }

    @Test
    void testBitcaskStartAfterMerge() throws IOException {
        String dir = bitcaskMerge();
        BitcaskImpl bitcask = new BitcaskImpl(dir, 1024);

        byte[] key1 = { 1 }, key2 = { 2 };
        byte[] value2 = { 2, 4, 7 }, value3 = { 3, 5, 8, 9 };

        assertArrayEquals(value3, bitcask.get(key1));
        assertArrayEquals(value2, bitcask.get(key2));

        bitcask.close();
    }

}
