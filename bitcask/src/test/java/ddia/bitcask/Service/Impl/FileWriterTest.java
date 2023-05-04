package ddia.bitcask.service.Impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class FileWriterTest {

    private final String testDir = "test/";

    @Test
    void testFileWriter() {
        try {
            var fileWriter = new FileWriter(testDir + "testfileWriter", 1024);

            var recordRef = fileWriter.append("Hello my name is ahmed".getBytes());
            assertEquals(0L, recordRef.getOffset());

            recordRef = fileWriter.append("Hello my name is ahmed".getBytes());
            assertEquals(22, recordRef.getOffset());
        } catch (IOException e) {
            fail(e);
        }
    }
}
