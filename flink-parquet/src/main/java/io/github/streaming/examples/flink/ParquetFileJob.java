package io.github.streaming.examples.flink;

import static io.github.streaming.examples.flink.utils.Constants.K_HDFS_OUTPUT;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import io.github.streaming.examples.flink.sink.BulkSink;
import io.github.streaming.examples.flink.utils.Utils;
import java.io.File;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileJob {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileJob.class);

  public static void main(String[] args) throws Exception {

    // 读取命令行参数
    ParameterTool params = Utils.parseArgs(args);

    // 参数检查
    final File folder = new File(params.getRequired(K_HDFS_OUTPUT));
    Path basePath = Path.fromLocalFile(folder);
    final FileSystem fs = basePath.getFileSystem();
    if (!fs.exists(basePath)) {
      fs.mkdirs(basePath);
    }
    LOG.info("basePath: {}", basePath);

    // 1. 执行环境
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 并行数
    env.setParallelism(2);
    // 检查点
    env.enableCheckpointing(60000);

    // 2. Add Source
    DataStream<ItemTransaction> source =
        env.addSource(new ItemTransactionGeneratorSource(params))
            .name("Item Transaction Generator");

    // 3. Add Sink
    // 注意： Bulk方式默认是按检查点策略滚动的
    source.keyBy("itemId")
        .addSink(BulkSink.createParquetBulkSink(basePath, ItemTransaction.class, params))
        .name("Transaction HDFS Sink");

//    source.print();

    env.execute("Flink Streaming Parquet Job");
    LOG.info("Flink Streaming Parquet Job");
  }


}
