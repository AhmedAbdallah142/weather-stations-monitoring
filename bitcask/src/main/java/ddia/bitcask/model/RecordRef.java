package ddia.bitcask.model;

import java.util.Objects;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
public class RecordRef {
    private String filePath;
    private long offset;
    private int recordLength;

    @Override
    public int hashCode() {
        return Objects.hash(filePath, offset, recordLength);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        if (obj instanceof RecordRef other) {
            return Objects.equals(this.filePath, other.filePath)
                    && offset == other.offset
                    && recordLength == other.recordLength;
        }
        return false;
    }

    public static void main(String[] args) {
        RecordRef r1 = RecordRef.builder().filePath(null).build();
        RecordRef r2 = RecordRef.builder().filePath(null).build();

        System.out.println(r1.equals(r2));
    }
}
