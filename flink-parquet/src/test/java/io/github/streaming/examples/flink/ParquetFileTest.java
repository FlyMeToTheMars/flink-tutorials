package io.github.streaming.examples.flink;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
      System.exit(-1);
    }

  }

  @Test
  public void showSchema() throws IOException {
    String[] args = Lists.newArrayList("schema", "-d", inputPath.toUri().getPath())
        .toArray(new String[]{});
    org.apache.parquet.tools.Main.main(args);
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

}
