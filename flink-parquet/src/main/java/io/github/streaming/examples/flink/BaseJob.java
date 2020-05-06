package io.github.streaming.examples.flink;

import static io.github.streaming.examples.flink.utils.Constants.CK_POINT_DATA_LOC;
import static io.github.streaming.examples.flink.utils.Constants.EVENT_TIME_KEY;
import static io.github.streaming.examples.flink.utils.Constants.K_HDFS_OUTPUT;
import static io.github.streaming.examples.flink.utils.Constants.K_KAFKA_TOPIC;
import static io.github.streaming.examples.flink.utils.Constants.MAX_PART_SIZE;
import static io.github.streaming.examples.flink.utils.Constants.OUTPUT_USE_LOC;
import static io.github.streaming.examples.flink.utils.ParquetUtils.applyCommonConfig;
import static io.github.streaming.examples.flink.utils.ParquetUtils.createParquetConfig;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.TransactionSchema;
import io.github.streaming.examples.flink.sink.filesystem.bucketassigners.DateBucketAssigner;
import io.github.streaming.examples.flink.sink.filesystem.rollingpolicies.CCheckpointRollingPolicy;
import io.github.streaming.examples.flink.sink.filesystem.rollingpolicies.CCheckpointRollingPolicy.RollingPolicyBuilder;
import io.github.streaming.examples.flink.utils.ParquetConfig;
import io.github.streaming.examples.flink.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

/**
 * 基础 Job 分装了通用方法
 *
 * @author chufucun
 */
public abstract class BaseJob {

  /**
   * 创建执行环境
   *
   * @param params 命令行参数
   */
  protected static StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) {
    StreamExecutionEnvironment env;
    // 判断是否本地环境
    if (params.has(OUTPUT_USE_LOC) && params.getBoolean(OUTPUT_USE_LOC)) {
      Configuration conf = new Configuration();
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
      if (params.has(CK_POINT_DATA_LOC)) {
        env.setStateBackend(new FsStateBackend(params.get(CK_POINT_DATA_LOC)));
      }
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    // We set max parallelism to a number with a lot of divisors
    env.setMaxParallelism(360);

    // Configure checkpointing if interval is set
    long cpInterval = params.getLong("checkpoint.interval.millis", TimeUnit.MINUTES.toMillis(1));
    if (cpInterval > 0) {
      CheckpointConfig checkpointConf = env.getCheckpointConfig();
      checkpointConf.setCheckpointInterval(cpInterval);
      checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
      checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
      checkpointConf.enableExternalizedCheckpoints(
          CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
      env.getConfig().setUseSnapshotCompression(true);
    }

    if (params.getBoolean(EVENT_TIME_KEY, false)) {
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    return env;
  }

  /**
   * 创建读取 Transaction 数据流，指定时间和水位线
   *
   * @param params 命令行参数
   * @param env    执行环境
   */
  protected static DataStream<ItemTransaction> readTransactionStream(ParameterTool params,
      StreamExecutionEnvironment env) {
    // topic name
    String topic = params.getRequired(K_KAFKA_TOPIC);
    // We read the ItemTransaction objects directly using the schema
    FlinkKafkaConsumer<ItemTransaction> transactionSource = new FlinkKafkaConsumer<>(
        topic, new TransactionSchema(),
        Utils.readKafkaProperties(params));

    transactionSource.setCommitOffsetsOnCheckpoints(true);
    transactionSource.setStartFromEarliest();

    // In case event time processing is enabled we assign trailing watermarks for each partition
    transactionSource
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ItemTransaction>(
            Time.minutes(1)) {
          @Override
          public long extractTimestamp(ItemTransaction transaction) {
            return transaction.ts;
          }
        });

    final String name = "Kafka " + topic + " Source";
    return env.addSource(transactionSource)
        .name(name)
        .uid(name);
  }

  /**
   * 创建 Parquet Bulk Sink
   *
   * @param basePath 基础路径
   * @param type     写入的类的类型
   * @param params   参数
   */
  public static StreamingFileSink createParquetBulkSink(Path basePath, Class type,
      ParameterTool params) {
    // 创建 Parquet 参数
    final ParquetConfig parquetConfig = createParquetConfig(params.getProperties());
    // 创建工厂
    ParquetWriterFactory writerFactory = createWriterFactory(type, parquetConfig);
    // 自定义滚动策略
    RollingPolicyBuilder rollingPolicyBuilder = CCheckpointRollingPolicy.builder();
    // 默认 128M
    long maxPartSize = params.getLong(MAX_PART_SIZE, 1024L * 1024L * 128L);
    rollingPolicyBuilder.withMaxPartSize(maxPartSize);// 128M

    StreamingFileSink sink = StreamingFileSink.
        forBulkFormat(basePath, writerFactory)
        .withBucketAssigner(new DateBucketAssigner("yyyyMMdd"))
        .withRollingPolicy(rollingPolicyBuilder.build())
        .build();

    return sink;
  }

  /**
   * 为给定类型创建一个 ParquetWriterFactory，Parquet Writer 将使用Avro来反射性地创建该类型的 schema，并使用该模式来编写列类型数据
   *
   * @param type          写入的类的类型.
   * @param parquetConfig Parquet 配置.
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

  /**
   * 获得输出路径
   *
   * @param param 命令行参数
   */
  protected static Path getOutPath(ParameterTool param) throws IOException {
    final String hdfsOutput = param.getRequired(K_HDFS_OUTPUT);
    Path basePath;
    if (param.has(OUTPUT_USE_LOC) && param.getBoolean(OUTPUT_USE_LOC)) {
      basePath = Path.fromLocalFile(new File(hdfsOutput));
    } else {
      basePath = new Path(hdfsOutput);
    }
    final FileSystem fs = basePath.getFileSystem();
    if (!fs.exists(basePath)) {
      fs.mkdirs(basePath);
    }
    return basePath;
  }
}
