package ddia.bitcask.Service.Impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ddia.bitcask.Service.Bitcask;
import ddia.bitcask.model.Key;
import ddia.bitcask.model.RecordRef;

public class BitcaskImpl implements Bitcask{

    public final String directory;
    private Map<Key, RecordRef> keydir;
    private FileWriter fileWriter;
    private FileManager fileManager;

    public BitcaskImpl(String directory) {
        this.directory = directory;
        this.keydir = new HashMap<Key, RecordRef>();
        this.fileWriter = new FileWriter(directory);
        this.fileManager = new FileManager();
    }

    @Override
    public byte[] get(byte[] keyBytes) throws IOException {
        var key = new Key(keyBytes);
        var record = fileManager.getRecord(keydir.get(key));
        return RecordConverter.toKeyValuePair(record).getValue();
    }

    @Override
    public void put(byte[] keyBytes, byte[] value) throws IOException {
        var key = new Key(keyBytes);
        var recordRef = fileWriter.append(RecordConverter.toRecord(key, value));
        keydir.put(key, recordRef);
    }

    @Override
    public List<byte[]> listKeys() {
        return keydir.keySet().stream().map(e -> e.getBytes()).toList();
    }

    @Override
    public void close() throws IOException {
        fileWriter.closeOpenedFile();
    }
    
}
