package io.github.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费 Kafka 数据写 Parquet 文件
 *
 * @author chufucun
 */
public class KafkaToHDFSParquetJob extends BaseJob {

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

    // 3. Write Parquet
    // 注意： Bulk方式默认是按检查点策略滚动的
    Path basePath = getOutPath(params);
    source
        .keyBy("itemId")
        .addSink(createParquetBulkSink(basePath, ItemTransaction.class, params))
        .name("Transaction HDFS Sink");

//    source.print();

    env.execute("Flink Streaming Parquet Job");
  }


}
