# Flink Parquet Tutorial

## 开发环境
### 启用 webui
a. 添加依赖：
```xml
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-runtime-web_2.11</artifactId>
        <version>${flink.version}</version>
    </dependency>
```
b. 创建一个包括 WebUI 的本地执行环境:
```java
    Configuration conf = new Configuration();
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
```
参考：
[ “baby” steps to develop a Flink application](https://www.ververica.com/blog/5-steps-flink-application-development)

## Sink
`StreamingFileSink` 同时支持逐行编码 (row-wise) 格式和块编码 (bulk-encoding) 格式，如 Apache Parquet。

`StreamingFileSink.forRowFormat()` 行编码格式，默认策略根据文件大小和超时滚动文件

`StreamingFileSink.forBulkFormat()` 批量编码格式只能与 `OnCheckpointRollingPolicy` 结合使用，会在每个检查点上滚动正在进行的部分文件。

## 测试方法：
1. 启动 `KafkaDataGeneratorJob` 生成测试数据入kafka
2. 启动 `KafkaToHDFSParquetJob` 消费数据生成Parquet文件

## Parquet 元数据查看

使用 `parquet-tool` 工具查看

文档：[Parquet 资料](http://note.youdao.com/noteshare?id=1c5028c4f5a1e47b7dc64e2d72d2a30f)

## 写入注意事项
+ Row group size: An optimized read setup would be: 1GB row groups, 1GB HDFS block size, 1 HDFS block per HDFS file.
+ Data page size: We recommend 8KB for page sizes.
https://parquet.apache.org/documentation/latest/
+ 第一列放置查询常用查询字段，这块是连续的

### How Parquet Data Files Are Organized
在数据文件中，对一组行的数据进行重新排列，以便将第一列中的所有值组织在一个连续的块中，然后将第二列中的所有值组织在一起，依此类推。将来自同一列的值放在一起，使得Impala可以对该列中的值使用有效的压缩技术。
https://docs.cloudera.com/runtime/7.1.0/impala-reference/topics/impala-parquet.html

## Page Index 加速查询
+ [Speeding Up SELECT Queries with Parquet Page Indexes](https://blog.cloudera.com/speeding-up-select-queries-with-parquet-page-indexes/)
+ [Impala2.9-更快的条件查询性能](https://mp.weixin.qq.com/s/OeYxv9CYrTiH8ifS-lIYLA)
