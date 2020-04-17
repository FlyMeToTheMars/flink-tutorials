package com.cloudera.streaming.examples.flink.clickhouse;

import com.cloudera.streaming.examples.flink.HeapMonitorSource;
import com.cloudera.streaming.examples.flink.types.HeapMetrics;
import com.cloudera.streaming.examples.flink.utils.Utils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HeapMonitorToCHBySinkJob {

    public static void main(String[] args) throws Exception {

        // Read the parameters from the commandline
        ParameterTool params = Utils.parseArgs(args);
        final String divername = params.get("ch.divername", "ru.yandex.clickhouse.ClickHouseDriver");
        final String dBUrl = params.get("ch.dBUrl", "jdbc:clickhouse://clickhouse-dev:8001/test");
        final String username = params.get("ch.username", "root");
        final String password = params.get("ch.password", "");
        if ("".equals(password)) {
            System.err.println("require --ch.password parameter !");
            System.exit(-1);
        }
        // Create and configure the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.enableCheckpointing(10_000);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Map<String, String> globalParameters = new HashMap<>();

        // clickhouse cluster properties
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "clickhouse-dev:8001");
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_USER, username);
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, password);

        // sink common
        globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, "5");
        globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, "/tmp");
        globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, "2");
        globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, "2");
        globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, "10");

        // set global paramaters
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(globalParameters));
        // Define our source
        DataStream<HeapMetrics> heapStats = env.addSource(new HeapMonitorSource(100))
                .name("Heap Monitor Source");

        // create props for sink
        Properties props = new Properties();
        props.put(ClickhouseSinkConsts.TARGET_TABLE_NAME, "test.heap_metrics");
        props.put(ClickhouseSinkConsts.MAX_BUFFER_SIZE, "1000");

        // build chain
        heapStats.map(new RichMapFunction<HeapMetrics, String>() {
            @Override
            public String map(HeapMetrics a) throws Exception {
                StringBuilder builder = new StringBuilder();
//                builder.append("insert into heap_metrics(time,area,used,max,ratio,jobId,hostname) values ");
                builder.append("( ");

                builder.append(a.time);
                builder.append(", ");
                // add a.str
                builder.append("'");
                builder.append(a.area);
                builder.append("', ");

                // add a.intger
                builder.append(String.valueOf(a.used));
                builder.append(", ");
                builder.append(String.valueOf(a.max));
                builder.append(", ");
                builder.append(String.valueOf(a.ratio));
                builder.append(", ");

                builder.append(a.jobId);
                builder.append(", ");

                builder.append("'");
                builder.append(a.hostname);
                builder.append("'");

                builder.append(" )");
                return builder.toString();
            }
        }).name("convert YourEvent to Clickhouse table format")
                .addSink(new ClickhouseSink(props))
                .name("your_table clickhouse sink");

        env.execute("HeapMonitor to ClickHouse");
    }

}
