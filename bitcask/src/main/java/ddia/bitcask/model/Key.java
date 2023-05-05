package ddia.bitcask.model;

import java.util.Arrays;

public class Key {
    public static final int MAX_LENGTH = (1 << 16) - 1;

    private final byte[] bytes;

    public Key(byte[] bytes) {
        if (bytes.length > MAX_LENGTH)
            throw new RuntimeException("Key is too long");
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public int length() {
        return bytes.length;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof Key other) {
            return Arrays.equals(this.bytes, other.bytes);
        }
        return false;
    }
}
