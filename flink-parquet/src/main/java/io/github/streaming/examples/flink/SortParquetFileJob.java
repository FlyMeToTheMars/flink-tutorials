package io.github.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import io.github.streaming.examples.flink.utils.Utils;
import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.table.expressions.E;
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

    FileInputFormat inputFormat = new ParquetRowInputFormat(Path.fromLocalFile(partFile), msgType);

    inputFormat = new ParquetPojoInputFormat(Path.fromLocalFile(partFile), msgType,
        (PojoTypeInfo<ItemTransaction>) PojoTypeInfo.of(ItemTransaction.class));

    DataSource source = env
        .createInput(inputFormat, TypeInformation.of(ItemTransaction.class));

    source
        .sortPartition("itemId", Order.ASCENDING)
        .writeAsCsv("/tmp/flink/item.csv", WriteMode.OVERWRITE);

    env.execute("Flink Streaming Sort Parquet Job");
  }

}
