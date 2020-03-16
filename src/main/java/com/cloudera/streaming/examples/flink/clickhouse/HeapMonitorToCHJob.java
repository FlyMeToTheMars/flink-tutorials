package com.cloudera.streaming.examples.flink.clickhouse;

import com.cloudera.streaming.examples.flink.HeapMonitorSource;
import com.cloudera.streaming.examples.flink.types.HeapMetrics;
import com.cloudera.streaming.examples.flink.utils.Utils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * 堆监控信息入 ClickHouse
 *
 * @author chufucun
 */
public class HeapMonitorToCHJob {

    public static void main(String[] args) throws Exception {

        // Read the parameters from the commandline
        ParameterTool params = Utils.parseArgs(args);
        final String divername = params.get("ch.divername", "ru.yandex.clickhouse.ClickHouseDriver");
        final String dBUrl = params.get("ch.dBUrl", "jdbc:clickhouse://clickhouse-dev:8001/test");
        final String username = params.get("ch.username", "root");
        final String password = params.get("ch.password", "");
        final int batchSize = params.getInt("ch.batchSize", 15);

        if ("".equals(password)) {
            System.err.println("require --ch.password parameter !");
            System.exit(-1);
        }

        final boolean clusterExec = params.getBoolean("cluster", true);

        // Create and configure the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设定检查点
        env.enableCheckpointing(10_000);
        // 设定 eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Define our source
        DataStream<HeapMetrics> heapStats = env.addSource(new HeapMonitorSource(100))
                .name("Heap Monitor Source");

        // 注意： 传入参数 Row 字段顺序 , fieldTypes 类型，一定要与sql语句的参数类型顺序保持一致。
        // data struct convert
        DataStream<Row> ds = heapStats.map(new RichMapFunction<HeapMetrics, Row>() {
            @Override
            public Row map(HeapMetrics metrics) throws Exception {
                return Row.of(
                        metrics.time,
                        metrics.area, metrics.used, metrics.max, metrics.ratio,
                        metrics.jobId, metrics.hostname
                );
            }
        });

        // define rows type
        TypeInformation[] fieldTypes = {
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };

        // define clickhouse JDBC sink
        JDBCAppendTableSink jdbcSink = JDBCAppendTableSink.builder()
                .setDrivername(divername)
                .setDBUrl(dBUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery("insert into heap_metrics(time,area,used,max,ratio,jobId,hostname) values(?, ?, ?, ?, ?, ?, ?)")
                .setParameterTypes(fieldTypes)
                .setBatchSize(batchSize)
                .build();

        jdbcSink.consumeDataStream(ds).name("ClickHouse Sink");

        env.execute("HeapMonitor to ClickHouse");
    }

}
