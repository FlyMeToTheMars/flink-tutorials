package io.github.streaming.examples.flink;

import com.google.common.collect.Lists;
import io.github.streaming.examples.flink.utils.parquet.Parquet;
import io.github.streaming.examples.flink.utils.parquet.ParquetReaderUtils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.tools.Main;
import org.junit.Before;
import org.junit.Test;


public class ParquetFileTest {

  private Configuration conf = new Configuration();
  private Path inputPath;

  @Before
  public void pre() throws IOException {
    String basepath = "/tmp/flink/dt=20200419/";
    File partFile = new File(basepath, "part-0-0");
    inputPath = new Path(partFile.getPath());

    if (!inputPath.getFileSystem(conf).exists(inputPath)) {
      System.out.println("File Not Found!" + inputPath.toUri());
    }
  }


  @Test
  public void allmeta() throws IOException {
    Parquet parquet = ParquetReaderUtils.getParquetData(inputPath.toUri().getPath());

    System.out.println("Schema: " + parquet.getSchema());

    List<SimpleGroup> data = parquet.getData();
    for (SimpleGroup group : data) {

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
  public void dump() throws IOException {
    String[] args = Lists.newArrayList("dump", inputPath.toUri().getPath())
        .toArray(new String[]{});
    Main.main(args);
  }

}
