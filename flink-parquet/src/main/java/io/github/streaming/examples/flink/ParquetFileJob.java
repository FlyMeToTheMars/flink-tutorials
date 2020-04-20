package io.github.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import io.github.streaming.examples.flink.utils.Utils;
import java.util.PriorityQueue;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileJob extends BaseJob {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileJob.class);

  public static void main(String[] args) throws Exception {

    // 读取命令行参数
    ParameterTool params = Utils.parseArgs(args);

    // 1. 执行环境
    final StreamExecutionEnvironment env = createExecutionEnvironment(params);
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // 2. 自定义数据生成器
    DataStream<ItemTransaction> source = env
        .addSource(new ItemTransactionGeneratorSource(params))
        .name("Item Transaction Generator")
        .assignTimestampsAndWatermarks(new TransactionAssigner());

    // 3. Add Sink
    // 注意： Bulk方式默认是按检查点策略滚动的
    Path basePath = getOutPath(params);
    source
        .keyBy("itemId")
        .addSink(createParquetBulkSink(basePath, ItemTransaction.class, params))
        .name("Transaction HDFS Sink");

    env.execute("Flink Streaming Parquet Job");
    LOG.info("Flink Streaming Parquet Job");
  }

  public static class SortFunction extends
      KeyedProcessFunction<String, ItemTransaction, ItemTransaction> {

    private ValueState<PriorityQueue<ItemTransaction>> queueState = null;

    @Override
    public void open(Configuration config) {
      ValueStateDescriptor<PriorityQueue<ItemTransaction>> descriptor = new ValueStateDescriptor<>(
          // state name
          "sorted-events",
          // type information of state
          TypeInformation.of(new TypeHint<PriorityQueue<ItemTransaction>>() {
          }));
      queueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ItemTransaction event, Context context,
        Collector<ItemTransaction> out) throws Exception {
      TimerService timerService = context.timerService();

      if (context.timestamp() > timerService.currentWatermark()) {
        PriorityQueue<ItemTransaction> queue = queueState.value();
        if (queue == null) {
          queue = new PriorityQueue<>(10);
        }
        queue.add(event);
        queueState.update(queue);
        timerService.registerEventTimeTimer(event.ts);
      }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<ItemTransaction> out)
        throws Exception {
      PriorityQueue<ItemTransaction> queue = queueState.value();
      Long watermark = context.timerService().currentWatermark();
      ItemTransaction head = queue.peek();
      while (head != null && head.ts <= watermark) {
        out.collect(head);
        queue.remove(head);
        head = queue.peek();
      }
    }
  }

  public static class TransactionAssigner implements
      AssignerWithPunctuatedWatermarks<ItemTransaction> {

    @Override
    public long extractTimestamp(ItemTransaction event, long previousElementTimestamp) {
      return event.ts;
    }

    @Override
    public Watermark checkAndGetNextWatermark(ItemTransaction event, long extractedTimestamp) {
      // simply emit a watermark with every event
      return new Watermark(extractedTimestamp - 30000);
    }
  }
}
