package io.github.streaming.examples.flink;

import static io.github.streaming.examples.flink.utils.ParquetUtils.PAGE_ROW_COUNT_LIMIT;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import io.github.streaming.examples.flink.triggers.CountTriggerWithTimeout;
import io.github.streaming.examples.flink.utils.Utils;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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
    env.setParallelism(2);

    // 2. 自定义数据生成器
    DataStream<ItemTransaction> source = env
        .addSource(new ItemTransactionGeneratorSource(params))
        .name("Item Transaction Generator");

    // 3. 添加 Sink
    long cpInterval = params.getLong("checkpoint.interval.millis", TimeUnit.MINUTES.toMillis(1));
    // 页行数限制
    int pageRowCountLimit = params.getInt(PAGE_ROW_COUNT_LIMIT, 20000);
    Path basePath = getOutPath(params);
    //  Hash -> Window -> sink
    source
        .keyBy("itemId")
        .timeWindow(Time.milliseconds(cpInterval))
        // 自定义触发器，防止窗口积压数据，这里以 pageRowCountLimit为阈值
        .trigger(
            new CountTriggerWithTimeout<>(pageRowCountLimit, TimeCharacteristic.ProcessingTime)
        )
        .process(new SortWindowFunction())
        .name("Hash by itemId")
        .addSink(createParquetBulkSink(basePath, ItemTransaction.class, params))
        .name("Transaction HDFS Sink");

    env.execute("Flink Streaming Parquet Job");
    LOG.info("Flink Streaming Parquet Job");
  }


  /**
   * 窗口内计算，这种方式会会降低 page index的范围（大部分情况下一个itemid占用一个page index） 兼顾性能要求
   *
   * ProcessAllWindowFunction 能实现排序，有瓶颈
   */
  private static class SortWindowFunction extends
      ProcessWindowFunction<ItemTransaction, Object, Tuple, TimeWindow> {

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param tuple The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     * @throws Exception The function may throw exceptions to fail the program and trigger
     * recovery.
     */
    @Override
    public void process(Tuple tuple, Context context, Iterable<ItemTransaction> elements,
        Collector<Object> out) throws Exception {
      elements.forEach(out::collect);
    }
  }

  /**
   * 窗口内全排序
   */
  private static class SortAllFunction extends
      ProcessAllWindowFunction<ItemTransaction, Object, TimeWindow> {

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param context The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     * @throws Exception The function may throw exceptions to fail the program and trigger
     * recovery.
     */
    @Override
    public void process(Context context, Iterable<ItemTransaction> elements,
        Collector<Object> out) throws Exception {
      List<ItemTransaction> list = Lists.newArrayList();
      elements.forEach(list::add);
      Collections.sort(list, new Comparator<ItemTransaction>() {
        @Override
        public int compare(ItemTransaction o1, ItemTransaction o2) {
          if (o1 != null && o2 != null) {
            return o1.itemId.compareTo(o2.itemId);
          }
          return 0;
        }
      });
      list.forEach(out::collect);
    }
  }
}
