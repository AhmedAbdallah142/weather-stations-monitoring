package ddia.bitcask.model;

import java.util.Arrays;

import lombok.Getter;

@Getter
public class Key {
    
    public static final int MAX_LENGTH = (1 << 16) - 1;

    public static void main(String[] args) {
        System.out.println(MAX_LENGTH);
    }

    private byte[] bytes;

    public Key(byte[] bytes) {
        if(bytes.length > MAX_LENGTH)
            throw new RuntimeException("Key is too long");
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key) {
            Key other = (Key) obj;
            return Arrays.equals(this.bytes, other.bytes);
        }
        return false;
    }
}
