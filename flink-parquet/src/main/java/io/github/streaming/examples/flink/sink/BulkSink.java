package io.github.streaming.examples.flink.sink;

import static io.github.streaming.examples.flink.utils.ParquetUtils.applyCommonConfig;
import static io.github.streaming.examples.flink.utils.ParquetUtils.createParquetConfig;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import io.github.streaming.examples.flink.utils.ParquetConfig;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkSink {

  private static final Logger LOG = LoggerFactory.getLogger(BulkSink.class);

  public static StreamingFileSink createParquetBulkSink(Path basePath, Class type,
      ParameterTool params) {

    // 创建 Parquet 参数
    final ParquetConfig parquetConfig = createParquetConfig(params.getProperties());

    // 创建
    ParquetWriterFactory writerFactory = createWriterFactory(type, parquetConfig);
    StreamingFileSink sink = StreamingFileSink.
        forBulkFormat(basePath, writerFactory)
        .withBucketAssigner(new DateBucketAssigner("yyyyMMdd"))
//        .withBucketCheckInterval()
        .build();

    return sink;
  }

  /**
   * Creates a ParquetWriterFactory for the given type. The Parquet writers will use Avro to
   * reflectively create a schema for the type and use that schema to write the columnar data.
   *
   * @param type The class of the type to write.
   * @param parquetConfig Parquet config.
   */
  public static <T> ParquetWriterFactory<T> createWriterFactory(Class<T> type,
      ParquetConfig parquetConfig) {
    final String schemaString = ReflectData.get().getSchema(type).toString();
    final ParquetBuilder<T> builder = (out) -> createAvroParquetWriter(schemaString,
        ReflectData.get(), parquetConfig, out);

    return new ParquetWriterFactory<>(builder);
  }

  private static <T> ParquetWriter<T> createAvroParquetWriter(
      String schemaString,
      GenericData dataModel, ParquetConfig parquetConfig,
      OutputFile out) throws IOException {

    final Schema schema = new Schema.Parser().parse(schemaString);

    AvroParquetWriter.Builder<T> writerBuilder = AvroParquetWriter.<T>builder(out)
        .withSchema(schema)
        .withDataModel(dataModel);

    // 设置 parquet 参数
    applyCommonConfig(writerBuilder, parquetConfig);

    return writerBuilder.build();
  }
}
