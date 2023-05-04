package ddia.bitcask.service.Impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;

import ddia.bitcask.model.RecordRef;

public class FileUtils {

    public static byte[] getRecord(RecordRef recordRef) throws IOException {
        RandomAccessFile file = new RandomAccessFile(recordRef.getFilePath(), "r");
        file.seek(recordRef.getOffset());
        byte[] record = new byte[recordRef.getRecordLength()];
        var read = file.read(record);
        file.close();

        if (read != recordRef.getRecordLength())
            throw new RuntimeException(String.format("Failed to read the record, expected length %d, but read %d",
                    recordRef.getRecordLength(), read));

        return record;
    }

    public static void removeOldFiles(File file) {
        var files = file.getParentFile().listFiles(new olderFileFilter(file.getName()));
        for (var f : files) {
            f.delete();
        }
    }

    public static File[] getNewerFiles(File file) {
        return file.getParentFile().listFiles(new newerFileFilter(file.getName()));
    }

    public static File[] getDataFiles(File dir) {
        return dir.listFiles(new FileUtils.DataFileFilter());
    }

    public static File[] getHintFiles(File dir) {
        return dir.listFiles(new FileUtils.HintFileFilter());
    }

    private static class olderFileFilter implements FilenameFilter {
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

    private static class newerFileFilter implements FilenameFilter {
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

    public static class HintFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".hint");
        }
    }

    public static class DataFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".data");
        }
    }

}
