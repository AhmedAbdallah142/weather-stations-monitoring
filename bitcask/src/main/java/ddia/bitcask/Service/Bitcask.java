package ddia.bitcask.Service;

import java.io.IOException;
import java.util.List;

public interface Bitcask {
    byte[] get(byte[] key) throws IOException;
    void put(byte[] key, byte[] value) throws IOException;
    List<byte[]> listKeys();
    void merge();
    void close() throws IOException;
}
