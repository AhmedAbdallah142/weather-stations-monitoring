package ddia.bitcask.service.Impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ddia.bitcask.service.Bitcask;
import ddia.bitcask.model.Key;
import ddia.bitcask.model.RecordRef;

public class BitcaskImpl implements Bitcask {

    private final Logger logger = LoggerFactory.getLogger(BitcaskImpl.class);

    public final String directory;
    private final Map<Key, RecordRef> keyDir;
    private final FileWriter fileWriter;
    private boolean mergeRunning = false;

    public BitcaskImpl(String directory) throws IOException {
        this.directory = directory;
        this.keyDir = new ConcurrentHashMap<>();
        this.fileWriter = new FileWriter(directory, 64 * 1024 * 1024);
        start();
    }

    public BitcaskImpl(String directory, long sizeThresholdBytes) throws IOException {
        this.directory = directory;
        this.keyDir = new ConcurrentHashMap<>();
        this.fileWriter = new FileWriter(directory, sizeThresholdBytes);
        start();
    }

    @Override
    public byte[] get(byte[] keyBytes) throws IOException {
        var key = new Key(keyBytes);
        var recordRef = keyDir.get(key);
        if (recordRef == null)
            return null;
        var record = FileUtils.getRecord(recordRef);
        return RecordParser.toKeyValuePair(record).getValue();
    }

    @Override
    public void put(byte[] keyBytes, byte[] value) throws IOException {
        var key = new Key(keyBytes);
        var recordRef = fileWriter.append(RecordParser.toRecord(key, value));
        keyDir.put(key, recordRef);
    }

    @Override
    public List<byte[]> listKeys() {
        return keyDir.keySet().stream().map(Key::getBytes).collect(Collectors.toList());
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

    protected void doMerge() throws IOException {
        File activeFile = fileWriter.getActiveFile();
        fileWriter.addToFileTime(activeFile.getName(), -1);
        String dataFilePath = String.format("%s/temp/%s", directory,
                fileWriter.addToFileTime(activeFile.getName(), -1));
        String hintFilePath = dataFilePath.replace(".data", ".hint");

        var dataFile = new File(dataFilePath);
        var hintFile = new File(hintFilePath);

        var updateMap = mergeFiles(activeFile.getAbsolutePath(), dataFile, hintFile);

        dataFile = moveFiles(dataFile, hintFile);
        if (dataFile == null)
            return;

        updateKeyDir(updateMap, dataFile);
        FileUtils.removeOlderFiles(dataFile);
    }

    protected Map<Key, ToUpdateRef> mergeFiles(String activePath, File dataFile, File hintFile) throws IOException {
        dataFile.getParentFile().mkdirs();
        hintFile.getParentFile().mkdirs();

        var dataFileWriter = new BufferedOutputStream(new FileOutputStream(dataFile.getAbsolutePath()));
        var hintFileWriter = new BufferedOutputStream(new FileOutputStream(hintFile.getAbsolutePath()));

        var updateMap = new HashMap<Key, ToUpdateRef>();
        long offset = 0;

        for (var entry : keyDir.entrySet()) {
            var key = entry.getKey();
            var recordRef = entry.getValue();
            if (activePath.compareTo(recordRef.getFilePath()) > 0) {
                var record = FileUtils.getRecord(recordRef);
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

    protected void updateKeyDir(Map<Key, ToUpdateRef> updateMap, File dataFile) {
        String dataFilePath = dataFile.getAbsolutePath();

        for (var entry : updateMap.entrySet()) {
            var key = entry.getKey();
            var val = entry.getValue();

            var recordRefMerge = RecordRef.builder()
                    .filePath(dataFilePath)
                    .offset(val.offsetMerge)
                    .recordLength(val.recordRef.getRecordLength())
                    .build();

            keyDir.replace(key, val.recordRef, recordRefMerge);
        }
    }

    protected void start() throws IOException {
        var dir = new File(directory);
        if (!dir.exists())
            return;

        File[] hintFiles = FileUtils.getHintFiles(dir);
        File[] dataFiles = null;

        if (hintFiles.length > 1)
            throw new RuntimeException("More than one hint file!");
        if (hintFiles.length == 1) {
            var hintFile = hintFiles[0];
            FileUtils.removeOlderFiles(hintFile);
            loadFromHintFile(hintFile);
            dataFiles = FileUtils.getNewerFiles(hintFile);
        }

        if (dataFiles == null)
            dataFiles = FileUtils.getDataFiles(dir);
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
            keyDir.put(pair.getKey(), pair.getValue());
        }

        inputStream.close();
    }

    protected void loadFromDataFiles(File[] dataFiles) throws IOException {
        if (dataFiles == null || dataFiles.length == 0)
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
                keyDir.put(pair.getKey(), pair.getValue());
            }
            inputStream.close();
        }
    }

    private static class ToUpdateRef {
        RecordRef recordRef;
        long offsetMerge;

        ToUpdateRef(RecordRef recordRef, long offsetMerge) {
            this.recordRef = recordRef;
            this.offsetMerge = offsetMerge;
        }
    }

}
