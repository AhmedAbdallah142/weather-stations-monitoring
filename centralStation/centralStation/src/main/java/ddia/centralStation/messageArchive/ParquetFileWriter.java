package ddia.centralStation.messageArchive;

import ddia.centralStation.config.Props;
import ddia.centralStation.models.StationStatusMessage;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;

public class ParquetFileWriter {

    private static final String PARQUET_EXTENSION = ".parquet";
    private static final String DESTINATION_PATH = Props.DATA_PATH + "/elastic";

    public static void writeListToParquetFile(List<StationStatusMessage> messages, String filePath) throws IOException {
        String fileName = System.nanoTime() + PARQUET_EXTENSION;
        Path path = new Path(filePath, fileName);
        Configuration conf = new Configuration();
        OutputFile outputFile = HadoopOutputFile.fromPath(path, conf);
        try (ParquetWriter<StationStatusMessage> writer = AvroParquetWriter.<StationStatusMessage>builder(outputFile)
                .withSchema(ReflectData.AllowNull.get().getSchema(StationStatusMessage.class))
                .withDataModel(ReflectData.get())
                .withConf(new Configuration())
                .withCompressionCodec(SNAPPY)
                .withWriteMode(Mode.CREATE)
                .build()) {
            System.out.printf("start writing %d messages into %s ...%n", messages.size(), path);
            for (StationStatusMessage message : messages) {
                writer.write(message);
            }
        }

        FileSystem fs = FileSystem.get(conf);
        // Define the source and destination paths for the file
        Path dstPath = new Path(DESTINATION_PATH, fileName);
        // Move the file using the HDFS API
        FileUtil.copy(fs, path, fs, dstPath, false, conf);
    }
}
