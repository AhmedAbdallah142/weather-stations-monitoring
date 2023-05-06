package ddia.centralStation.messageArchive;

import ddia.centralStation.models.StationStatusMessage;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
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

    public static void writeListToParquetFile(List<StationStatusMessage> messages, String filePath) throws IOException {
        String fileName = System.nanoTime() + PARQUET_EXTENSION;
        Path path = new Path(filePath, fileName);
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
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
    }
}
