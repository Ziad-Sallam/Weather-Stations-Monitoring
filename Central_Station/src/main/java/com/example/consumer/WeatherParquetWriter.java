package com.example.consumer;

import com.example.repo.WeatherMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class WeatherParquetWriter {

    private static final int BATCH_SIZE = 10_000;
    private static final String BASE_DIR = "parquet-data";

    private final List<WeatherMessage> buffer = new ArrayList<>();

    private static final String SCHEMA_JSON = """
        {
          "type": "record",
          "name": "WeatherMessage",
          "fields": [
            {"name": "station_id",        "type": "long"},
            {"name": "s_no",              "type": "long"},
            {"name": "battery_status",    "type": "string"},
            {"name": "status_timestamp",  "type": "long"},
            {"name": "humidity",          "type": "int"},
            {"name": "temperature",       "type": "int"},
            {"name": "wind_speed",        "type": "int"}
          ]
        }
        """;

    private final Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

    public synchronized void add(WeatherMessage message) {
        buffer.add(message);
        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
    }

    public synchronized void flush() {
        if (buffer.isEmpty()) return;

        String timestamp = DateTimeFormatter
            .ofPattern("yyyyMMdd_HHmmss")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());

        buffer.stream()
            .collect(java.util.stream.Collectors.groupingBy(m -> m.station_id))
            .forEach((stationId, messages) -> {
                String path = String.format(
                    "%s/station_id=%d/%s.parquet",
                    BASE_DIR, stationId, timestamp
                );
                writePartition(path, messages);
            });

        System.out.printf("[Parquet] Flushed %d records%n", buffer.size());
        buffer.clear();
    }

    private void writePartition(String filePath, List<WeatherMessage> messages) {
        Path path = new Path(filePath);
        Configuration conf = new Configuration();

        try {
            path.getFileSystem(conf).mkdirs(path.getParent());

            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf))
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {

                for (WeatherMessage msg : messages) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("station_id",       msg.station_id);
                    record.put("s_no",             msg.s_no);
                    record.put("battery_status",   msg.battery_status);
                    record.put("status_timestamp", msg.status_timestamp);
                    record.put("humidity",         msg.weather.humidity);
                    record.put("temperature",      msg.weather.temperature);
                    record.put("wind_speed",       msg.weather.wind_speed);
                    writer.write(record);
                }

            }
        } catch (IOException e) {
            System.err.println("[Parquet] Failed to write: " + filePath);
            e.printStackTrace();
        }
    }
}