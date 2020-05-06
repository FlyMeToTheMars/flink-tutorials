package io.github.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.github.streaming.examples.flink.utils.parquet.Parquet;
import io.github.streaming.examples.flink.utils.parquet.ParquetReaderUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.tools.Main;
import org.checkerframework.checker.units.qual.Temperature;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ParquetFileTest {

  private Configuration conf = new Configuration();
  private Path basepath;
  private Path inputPath;

  @Before
  public void pre() throws IOException {
    String basedir = "/tmp/flink/dt=20200506/";
    basepath = new Path(new File(basedir).getPath());
    File partFile = new File(basedir, "part-0-0");
    inputPath = new Path(partFile.getPath());

    if (!inputPath.getFileSystem(conf).exists(inputPath)) {
      Assert.fail("File Not Found!" + inputPath.toUri());
    }
    System.out.println("basepath: " + basepath);
    System.out.println("inputPath: " + inputPath);
  }

  @Test
  public void restRead() {
  }

  @Test
  public void allmeta() throws IOException {

    FileStatus[] filelist = basepath.getFileSystem(conf).listStatus(basepath);
    for (FileStatus fileStatus : filelist) {
      if (StringUtils.endsWith(fileStatus.getPath().getName(), "-0-0")) {

        Parquet parquet = ParquetReaderUtils.getParquetData(fileStatus.getPath().toUri().getPath());
        ParquetFileReader.readFooters(conf, fileStatus, false);
//        System.out.println("Schema: " + parquet.getSchema());

        List<SimpleGroup> data = parquet.getData();
        Set<String> itemids = Sets.newHashSet();
        for (SimpleGroup group : data) {
          String itemId = group.getString(2, 0);
          itemids.add(itemId);
          System.out.println(fileStatus.getPath().getName() + ": " + itemId);
        }

        List<String> itemidList = Lists.newArrayList(itemids);
        Collections.sort(itemidList);
        System.out.println(fileStatus.getPath().getName() + " : " + itemidList);
      }
    }
  }

  @Test
  public void showUsage() throws IOException {
    String[] args = Lists.newArrayList("-h").toArray(new String[]{});
    Main.main(args);
  }

  @Test
  public void showSchema() throws IOException {
    String[] args = Lists.newArrayList("schema", "-d", inputPath.toUri().getPath())
        .toArray(new String[]{});
    Main.main(args);
  }

  @Test
  public void showMeta() throws IOException {
    String[] args = Lists.newArrayList("meta", "-o", inputPath.toUri().getPath())
        .toArray(new String[]{});
    Main.main(args);
  }

  @Test
  public void rowCount() throws IOException {
    String[] args = Lists.newArrayList("rowcount", "-d", inputPath.toUri().getPath())
        .toArray(new String[]{});
    Main.main(args);
  }

  @Test
  public void columnIndex() throws IOException {
    String[] args = Lists
        .newArrayList("column-index", "-c", "itemId", "-i", inputPath.toUri().getPath())
        .toArray(new String[]{});
    Main.main(args);
  }

  @Test
  public void dump() throws IOException {
    String[] args = Lists.newArrayList("dump", inputPath.toUri().getPath())
        .toArray(new String[]{});
    Main.main(args);
  }

}
