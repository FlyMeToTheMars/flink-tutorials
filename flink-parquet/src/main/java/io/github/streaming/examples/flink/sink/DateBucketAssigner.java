package io.github.streaming.examples.flink.sink;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public class DateBucketAssigner<IN> extends DateTimeBucketAssigner<IN> {


  public DateBucketAssigner(String formatString) {
    super(formatString);
  }

  @Override
  public String getBucketId(IN element, Context context) {
    return "dt=" + super.getBucketId(element, context);
  }
}
