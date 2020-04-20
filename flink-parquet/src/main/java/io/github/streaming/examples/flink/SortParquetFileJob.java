package io.github.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import io.github.streaming.examples.flink.utils.Utils;
import java.io.File;
import java.util.PriorityQueue;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

public class SortParquetFileJob extends BaseJob {

  private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

  public static void main(String[] args) throws Exception {

    // 读取命令行参数
    ParameterTool params = Utils.parseArgs(args);

    // 1. 执行环境
//    final StreamExecutionEnvironment env = createExecutionEnvironment(params);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    ;
    env.setParallelism(1);
    String basepath = "/tmp/flink/dt=20200420/";
    File partFile = new File(basepath, "part-0-0");

    final String schemaString = ReflectData.get().getSchema(ItemTransaction.class).toString();
    final Schema avroSchema = new Schema.Parser().parse(schemaString);

    MessageType msgType = SCHEMA_CONVERTER.convert(avroSchema);

    FileInputFormat format = new ParquetRowInputFormat(Path.fromLocalFile(partFile), msgType);

    DataSource source = env
        .readFile(format, partFile.getPath());

    source
        .sortPartition("itemId", Order.ASCENDING)
        .print();

    env.execute("Flink Streaming Sort Parquet Job");
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
