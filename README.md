# Flink Tutorial

## Flink clickhouse
1. [CH建表语句](./src/main/resources/clickhouse_ddl.sql)
2. 打包注意事项：
    修改 pom.xml 中的 main 类，见 `<mainClass/>` 标签
3. 要直接从 IntelliJ 中运行应用程序，必须启用 `add-dependencies-for-IDEA` profile

## Flink parquet

Page Index 加速查询
+ [Speeding Up SELECT Queries with Parquet Page Indexes](https://blog.cloudera.com/speeding-up-select-queries-with-parquet-page-indexes/)
+ [Impala2.9-更快的条件查询性能](https://mp.weixin.qq.com/s/OeYxv9CYrTiH8ifS-lIYLA)

The foundation project comes from [flink-tutorials](https://github.com/cloudera/flink-tutorials)
 