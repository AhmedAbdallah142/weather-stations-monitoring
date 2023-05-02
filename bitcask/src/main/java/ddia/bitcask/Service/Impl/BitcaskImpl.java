package ddia.bitcask.Service.Impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ddia.bitcask.Service.Bitcask;
import ddia.bitcask.model.Key;
import ddia.bitcask.model.RecordRef;

public class BitcaskImpl implements Bitcask {

    public final String directory;
    private final Map<Key, RecordRef> keydir;
    private final FileWriter fileWriter;
    private final FileManager fileManager;
    private boolean mergeRunning = false;

    public BitcaskImpl(String directory) {
        this.directory = directory;
        this.keydir = new ConcurrentHashMap<Key, RecordRef>();
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

    public synchronized void merge() {
        if (mergeRunning)
            return;
        mergeRunning = true;
        Thread thread = new Thread(() -> {
            try {
                doMerge();
            } catch (IOException e) {
                throw new RuntimeException("Merge failed", e);
            }
            mergeRunning = false;
        });
        thread.start();
    }

    protected long extractTime(String filePath) {
        String time = new File(filePath).getName().substring(5, 5 + 19);
        return Long.valueOf(time);
    }

    private class ToUpdateRef {
        RecordRef recordRef;
        long offsetMerge;

        ToUpdateRef(RecordRef recordRef, long offsetMerge) {
            this.recordRef = recordRef;
            this.offsetMerge = offsetMerge;
        }
    }

    private class myFileFilter implements FilenameFilter {
        String mergeFileName;

        myFileFilter(String mergeFileName) {
            this.mergeFileName = mergeFileName;
        }

        @Override
        public boolean accept(File dir, String name) {
            return mergeFileName.compareTo(name) > 0;
        }

    }

    protected void doMerge() throws IOException {
        String activePath = fileWriter.getActiveFile().getAbsolutePath();
        String dataFilePath = String.format("%s/temp/data-%019d.data", directory, extractTime(activePath) - 1);
        String hintFilePath = dataFilePath.replace(".data", ".hint");

        var dataFile = new File(dataFilePath);
        var hintFile = new File(hintFilePath);

        var updateMap = mergeFiles(activePath, dataFile, hintFile);

        if (!moveFiles(dataFile, hintFile))
            return;

        updateKeydir(updateMap, dataFile);
        removeOldFiles(dataFile);
    }

    protected Map<Key, ToUpdateRef> mergeFiles(String activePath, File dataFile, File hintFile) throws IOException {
        dataFile.getParentFile().mkdirs();
        hintFile.getParentFile().mkdirs();

        var dataFileWriter = new BufferedOutputStream(new FileOutputStream(dataFile.getAbsolutePath()));
        var hintFileWriter = new BufferedOutputStream(new FileOutputStream(hintFile.getAbsolutePath()));

        var updateMap = new HashMap<Key, ToUpdateRef>();
        long offset = 0;

        for (var entry : keydir.entrySet()) {
            var key = entry.getKey();
            var recordRef = entry.getValue();
            if (activePath.compareTo(recordRef.getFilePath()) > 0) {
                var record = fileManager.getRecord(recordRef);
                dataFileWriter.write(record);
                hintFileWriter.write(RecordConverter.toHintRecord(key, record.length, offset));
                updateMap.put(key, new ToUpdateRef(recordRef, offset));
                offset += record.length;
            }
        }
        dataFileWriter.close();
        hintFileWriter.close();

        return updateMap;
    }

    // return false if dataFile is empty
    protected boolean moveFiles(File dataFile, File hintFile) {
        if (dataFile.length() <= 0) {
            dataFile.delete();
            hintFile.delete();
            return false;
        }

        dataFile.renameTo(new File(String.format("%s/%s", directory, dataFile.getName())));
        hintFile.renameTo(new File(String.format("%s/%s", directory, hintFile.getName())));
        return true;
    }

    protected void updateKeydir(Map<Key, ToUpdateRef> updateMap, File dataFile) {
        String dataFilePath = dataFile.getAbsolutePath();

        for (var entry : updateMap.entrySet()) {
            var key = entry.getKey();
            var val = entry.getValue();

            var recordRefMerge = RecordRef.builder()
                    .filePath(dataFilePath)
                    .offset(val.offsetMerge)
                    .recordLength(val.recordRef.getRecordLength())
                    .build();

            keydir.replace(key, val.recordRef, recordRefMerge);
        }
    }

    protected void removeOldFiles(File dataFile) {
        var dir = new File(directory);
        var files = dir.listFiles(new myFileFilter(dataFile.getName()));
        for (var file : files) {
            file.delete();
        }
    }

}
