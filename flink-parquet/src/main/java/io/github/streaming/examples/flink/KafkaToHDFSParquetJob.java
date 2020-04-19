package io.github.streaming.examples.flink;

import static io.github.streaming.examples.flink.utils.Constants.EVENT_TIME_KEY;
import static io.github.streaming.examples.flink.utils.Constants.K_HDFS_OUTPUT;
import static io.github.streaming.examples.flink.utils.Constants.K_KAFKA_TOPIC;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.TransactionSchema;
import io.github.streaming.examples.flink.sink.BulkSink;
import io.github.streaming.examples.flink.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费 Kafka 数据写 Parquet 文件
 *
 * @author chufucun
 */
public class KafkaToHDFSParquetJob {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToHDFSParquetJob.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new RuntimeException("Path to the properties file is expected as the only argument.");
    }
    // read properties params
    ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

    // 1. Create and configure the StreamExecutionEnvironment
    StreamExecutionEnvironment env = createExecutionEnvironment(params);

    // 2. Read transaction stream
    DataStream<ItemTransaction> source = readTransactionStream(params, env);

    // 3. Add Sink
    // 注意： Bulk方式默认是按检查点策略滚动的
    Path basePath = getOutPath(params);
    source.keyBy("itemId")
        .addSink(BulkSink.createParquetBulkSink(basePath, ItemTransaction.class, params))
        .name("Transaction HDFS Sink");

    source.print();

    env.execute("Flink Streaming Parquet Job");
  }

  /**
   * 创建执行环境
   *
   * @param params 命令行参数
   * @return
   */
  private static StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
   * 创建读取数据流
   *
   * @param params 命令行参数
   * @param env    执行环境
   * @return
   */
  private static DataStream<ItemTransaction> readTransactionStream(ParameterTool params,
      StreamExecutionEnvironment env) {
    // We read the ItemTransaction objects directly using the schema
    FlinkKafkaConsumer<ItemTransaction> transactionSource = new FlinkKafkaConsumer<>(
        params.getRequired(K_KAFKA_TOPIC), new TransactionSchema(),
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

    return env.addSource(transactionSource)
        .name("Kafka Transaction Source")
        .uid("Kafka Transaction Source");
  }

  /**
   * 获得输出路径
   *
   * @param param 命令行参数
   * @return
   * @throws IOException
   */
  private static Path getOutPath(ParameterTool param) throws IOException {
    final String hdfsOutput = param.getRequired(K_HDFS_OUTPUT);
    Path basePath;
    if (param.getBoolean("output.local")) {
      basePath = Path.fromLocalFile(new File(hdfsOutput));
    } else {
      basePath = new Path(hdfsOutput);
    }
    final FileSystem fs = basePath.getFileSystem();
    if (!fs.exists(basePath)) {
      fs.mkdirs(basePath);
    }
    LOG.info("Output: {}", basePath);
    return basePath;
  }
}
