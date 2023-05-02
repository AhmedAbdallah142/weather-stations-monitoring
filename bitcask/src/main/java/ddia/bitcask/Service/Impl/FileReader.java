package ddia.bitcask.service.Impl;

import java.io.IOException;
import java.io.RandomAccessFile;

import ddia.bitcask.model.RecordRef;

public class FileReader {

    public byte[] getRecord (RecordRef recordRef) throws IOException {
        RandomAccessFile file = new RandomAccessFile(recordRef.getFilePath(), "r");
        file.seek(recordRef.getOffset());
        byte[] record = new byte[recordRef.getRecordLength()];
        var read = file.read(record);
        file.close();

        if(read != recordRef.getRecordLength())
            throw new RuntimeException(String.format("Failed to read the record, expected length %d, but read %d", recordRef.getRecordLength(), read));

        return record;
    }

}
