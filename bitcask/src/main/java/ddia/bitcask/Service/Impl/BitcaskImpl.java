package ddia.bitcask.service.Impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ddia.bitcask.service.Bitcask;
import ddia.bitcask.model.Key;
import ddia.bitcask.model.RecordRef;

public class BitcaskImpl implements Bitcask {

    private Logger logger = LoggerFactory.getLogger(BitcaskImpl.class);

    public final String directory;
    private final Map<Key, RecordRef> keydir;
    private final FileWriter fileWriter;
    private final FileReader fileManager;
    private boolean mergeRunning = false;

    public BitcaskImpl(String directory) throws IOException {
        this.directory = directory;
        this.keydir = new ConcurrentHashMap<Key, RecordRef>();
        this.fileWriter = new FileWriter(directory);
        this.fileManager = new FileReader();
        start();
    }

    @Override
    public byte[] get(byte[] keyBytes) throws IOException {
        var key = new Key(keyBytes);
        var recordRef = keydir.get(key);
        if(recordRef == null)
            return null;
        var record = fileManager.getRecord(keydir.get(key));
        return RecordParser.toKeyValuePair(record).getValue();
    }

    @Override
    public void put(byte[] keyBytes, byte[] value) throws IOException {
        var key = new Key(keyBytes);
        var recordRef = fileWriter.append(RecordParser.toRecord(key, value));
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
            long start = System.currentTimeMillis();
            logger.info("Merge operation started");
            try {
                doMerge();
            } catch (IOException e) {
                logger.error("Merge Failed", e);
            }
            logger.info(String.format("Merge operation finished in total time of %d ms",
                    System.currentTimeMillis() - start));
            mergeRunning = false;
        });
        thread.start();
    }

    private class olderFileFilter implements FilenameFilter {
        String fileName;

        olderFileFilter(String fileName) {
            this.fileName = fileName.substring(0, fileName.lastIndexOf("."));
        }

        @Override
        public boolean accept(File dir, String name) {
            int t = name.lastIndexOf(".");
            if (t < 0)
                return false;
            return fileName.compareTo(name.substring(0, t)) > 0;
        }
    }

    private class newerFileFilter implements FilenameFilter {
        String fileName;

        newerFileFilter(String fileName) {
            this.fileName = fileName.substring(0, fileName.lastIndexOf("."));
        }

        @Override
        public boolean accept(File dir, String name) {
            int t = name.lastIndexOf(".");
            if (t < 0)
                return false;
            return fileName.compareTo(name.substring(0, t)) < 0;
        }
    }

    private class HintFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".hint");
        }
    }

    private class DataFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".data");
        }
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

    protected void doMerge() throws IOException {
        String activePath = fileWriter.getActiveFile().getAbsolutePath();
        String dataFilePath = String.format("%s/temp/data-%019d.data", directory, extractTime(activePath) - 1);
        String hintFilePath = dataFilePath.replace(".data", ".hint");

        var dataFile = new File(dataFilePath);
        var hintFile = new File(hintFilePath);

        var updateMap = mergeFiles(activePath, dataFile, hintFile);

        dataFile = moveFiles(dataFile, hintFile);
        if (dataFile == null)
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
                hintFileWriter.write(RecordParser.toHintRecord(key, record.length, offset));
                updateMap.put(key, new ToUpdateRef(recordRef, offset));
                offset += record.length;
            }
        }
        dataFileWriter.close();
        hintFileWriter.close();

        return updateMap;
    }

    protected File moveFiles(File dataFile, File hintFile) {
        if (dataFile.length() <= 0) {
            dataFile.delete();
            hintFile.delete();
            return null;
        }

        var newDataFile = new File(String.format("%s/%s", directory, dataFile.getName()));
        var newHintFile = new File(String.format("%s/%s", directory, hintFile.getName()));

        dataFile.renameTo(newDataFile);
        hintFile.renameTo(newHintFile);
        return newDataFile;
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

    protected void removeOldFiles(File file) {
        var dir = new File(directory);
        var files = dir.listFiles(new olderFileFilter(file.getName()));
        for (var f : files) {
            f.delete();
        }
    }

    protected void start() throws IOException {
        var dir = new File(directory);
        if (!dir.exists())
            return;

        File[] hintFiles = dir.listFiles(new HintFileFilter());
        File[] dataFiles = null;

        if (hintFiles.length > 1)
            throw new RuntimeException("More than one hint file!");
        if (hintFiles.length == 1) {
            var hintFile = hintFiles[0];
            removeOldFiles(hintFile);
            loadFromHintFile(hintFile);
            dataFiles = dir.listFiles(new newerFileFilter(hintFile.getName()));
        }

        if (dataFiles == null)
            dataFiles = dir.listFiles(new DataFileFilter());
        loadFromDataFiles(dataFiles);
    }

    protected void loadFromHintFile(File hinFile) throws IOException {
        String dataFilePath = hinFile.getAbsolutePath().replace(".hint", ".data");
        var inputStream = new BufferedInputStream(new FileInputStream(hinFile));

        while (inputStream.available() > 0) {
            var pair = RecordParser.readNextRecordHint(inputStream);
            if (pair == null) {
                logger.error("Failed to read record from hint file");
                break;
            }
            pair.getValue().setFilePath(dataFilePath);
            keydir.put(pair.getKey(), pair.getValue());
        }

        inputStream.close();
    }

    protected void loadFromDataFiles(File[] dataFiles) throws IOException {
        if (dataFiles == null || dataFiles.length <= 0)
            return;

        Arrays.sort(dataFiles);

        for (var file : dataFiles) {
            var inputStream = new BufferedInputStream(new FileInputStream(file));
            long offset = 0;
            while (inputStream.available() > 0) {
                var pair = RecordParser.readNextRecordData(inputStream);
                if (pair == null) {
                    logger.error("Failed to read record from data file");
                    break;
                }
                var recordRef = pair.getValue();
                recordRef.setFilePath(file.getAbsolutePath());
                recordRef.setOffset(offset);
                offset += recordRef.getRecordLength();
                keydir.put(pair.getKey(), pair.getValue());
            }
            inputStream.close();
        }
    }

}
