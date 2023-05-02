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
        if (this == obj)
            return true;
        if (!(obj instanceof RecordRef))
            return false;
        
        RecordRef other = (RecordRef) obj;
        if(filePath == null)
            if(other.filePath != null)
                return false;
        else if(!filePath.equals(other.filePath))
            return false;

        return offset == other.offset && recordLength == other.recordLength;
    }
}
