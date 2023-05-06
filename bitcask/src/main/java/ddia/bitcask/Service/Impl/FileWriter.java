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

    private final long sizeThreshold = 64 * 1024 * 1024;

    public FileWriter(String directory) {
        this.directory = directory;
    }

    public void createNewFile() throws IOException {
        if (fileStream != null)
            fileStream.close();

        file = new File(String.format("%s/data-%019d.data", directory, System.currentTimeMillis()));
        file.getParentFile().mkdirs();
        boolean created = file.createNewFile();

        if (!created)
            throw new RuntimeException("File already exists");

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
        fileStream.close();
        file = null;
    }
}
