package io.github.streaming.examples.flink;

import static org.apache.nifi.parquet.utils.ParquetUtils.applyCommonConfig;
import static org.apache.nifi.parquet.utils.ParquetUtils.createParquetConfig;

import io.github.streaming.examples.flink.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.avro.generated.Address;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetWriter.Builder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkToParquetJob {

  private static final Logger LOG = LoggerFactory.getLogger(HeapMonitorSource.class);

  public static void main(String[] args) throws Exception {

    // Read the parameters from the commandline
    ParameterTool params = Utils.parseArgs(args);

    // create env
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 并行数
    env.setParallelism(1);
    // 检查点
    env.enableCheckpointing(100);
    File folder = File.createTempFile("out-", "", new File("/tmp"));
    folder.delete();
    folder.mkdir();
    LOG.info("Output dir: {}", folder.getPath());

//    final List<Address> data = Arrays.asList(
//        new Address(1, "a", "b", "c", "12345"),
//        new Address(2, "p", "q", "r", "12345"),
//        new Address(3, "x", "y", "z", "12345")
//    );

    // add source
    DataStream<HeapMetrics> stream = env
        .addSource(new HeapMonitorSource(100), TypeInformation.of(HeapMetrics.class))
        .name("Heap Monitor Source");
//    DataStream<Address> stream = env.addSource(
//        new FiniteTestSource<>(data), TypeInformation.of(Address.class));
    // transform

    // add sink
    final Configuration conf = new Configuration();

    // 构建 parquetWriter
    final ParquetConfig parquetConfig = createParquetConfig(params.getProperties());
    ParquetWriterFactory writerFactory = createWriterFactory(Address.class, parquetConfig);
    StreamingFileSink sink = StreamingFileSink.
        forBulkFormat(Path.fromLocalFile(folder), writerFactory)
        .build();

    stream.addSink(sink).name("Write Parquet Sink");

    env.execute("");
    LOG.info("finished job!");
  }

  /**
   * Creates a ParquetWriterFactory for the given type. The Parquet writers will use Avro to
   * reflectively create a schema for the type and use that schema to write the columnar data.
   *
   * @param type The class of the type to write.
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
