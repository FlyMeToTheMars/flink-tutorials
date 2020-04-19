# Flink Parquet Tutorial

`StreamingFileSink` 同时支持逐行编码 (row-wise) 格式和块编码 (bulk-encoding) 格式，如 Apache Parquet。

`StreamingFileSink.forRowFormat()` 行编码格式，默认策略根据文件大小和超时滚动文件

`StreamingFileSink.forBulkFormat()` 批量编码格式只能与 `OnCheckpointRollingPolicy` 结合使用，会在每个检查点上滚动正在进行的部分文件。

## 测试方法：
1. 启动 `KafkaDataGeneratorJob` 生成测试数据入kafka
2. 启动 `KafkaToHDFSParquetJob` 消费数据生成Parquet文件

## Parquet 元数据查看

使用 `parquet-tool` 工具查看

文档：[Parquet 资料](http://note.youdao.com/noteshare?id=1c5028c4f5a1e47b7dc64e2d72d2a30f)



## Page Index 加速查询
+ [Speeding Up SELECT Queries with Parquet Page Indexes](https://blog.cloudera.com/speeding-up-select-queries-with-parquet-page-indexes/)
+ [Impala2.9-更快的条件查询性能](https://mp.weixin.qq.com/s/OeYxv9CYrTiH8ifS-lIYLA)
