package ddia.bitcask.service.Impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import ddia.bitcask.model.RecordRef;

public class FileWriter {

    private final String directory;
    private File file;
    private FileOutputStream fileStream;
    private long fileSize;

    private final long sizeThreshold;

    public FileWriter(String directory, long sizeThreshold) {
        if (sizeThreshold < 1)
            throw new RuntimeException("File size threshold must be positive number");

        this.directory = directory;
        this.sizeThreshold = sizeThreshold;
    }

    public String addToFileTime(String fileName, int timeToAdd) {
        String time = fileName.substring(5, 5 + 19);
        long newFileTime = Long.valueOf(time) + timeToAdd;
        return String.format("data-%019d.data", newFileTime);
    }

    public void createNewFile() throws IOException {
        if (fileStream != null)
            fileStream.close();

        file = new File(String.format("%s/data-%017d00.data", directory, System.currentTimeMillis()));
        file.getParentFile().mkdirs();

        if (file.exists())
            file = new File(String.format("%s/%s", directory, addToFileTime(file.getName(), 1)));
        file.createNewFile();

        fileStream = new FileOutputStream(file, true);
        fileSize = 0;
    }

    public RecordRef append(byte[] record) throws IOException {
        if (file == null) {
            createNewFile();
        }

        long offset = fileSize;
        String currentFilePath = file.getAbsolutePath();
        fileStream.write(record);
        fileStream.flush();
        fileSize += record.length;

        if (fileSize > sizeThreshold) {
            closeOpenedFile();
            createNewFile();
        }

        return RecordRef.builder()
                .filePath(currentFilePath)
                .offset(offset)
                .recordLength(record.length)
                .build();
    }

    public File getActiveFile() {
        return file;
    }

    public void closeOpenedFile() throws IOException {
        if (fileStream != null)
            fileStream.close();
        file = null;
    }
}
