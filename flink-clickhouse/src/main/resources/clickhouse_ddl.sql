-- 建表
create database test;
CREATE TABLE test.heap_metrics (
time UInt64,
area String,
used  Int64,
max Int64,
ratio Float32,
jobId Int32,
hostname String
)
ENGINE = MergeTree() PARTITION BY toYYYYMM(toDate(time)) ORDER BY (jobId, area, time);

