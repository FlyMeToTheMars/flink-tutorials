//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.github.streaming.examples.flink.utils;

import java.io.IOException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
public class EncryptTool {
  private static final Logger LOG = LoggerFactory.getLogger(EncryptTool.class);
  private static final String HDFS_ROOT = "hdfs://";
  private static final String KEY_FILE_NAME = ".flink-master-key.base64";
  private final Path keyPath;
  private final String keyAlgorithm;
  private final Key encryptionKey;
  private final String cipherTransformation;
  private static EncryptTool encryptTool;

  private EncryptTool(Path keyPath, String keyAlgorithm, String cipherTransformation) {
    this.keyPath = keyPath;
    this.keyAlgorithm = keyAlgorithm;
    this.encryptionKey = new SecretKeySpec(readKey(keyPath), keyAlgorithm);
    this.cipherTransformation = cipherTransformation;
  }

  public static synchronized EncryptTool getInstance(Configuration config) {
    Path keyPath = new Path(config.getString(EncryptToolOptions.ENCRYPT_TOOL_KEY_LOCATION));
    String keyAlgorithm = config.getString(EncryptToolOptions.ENCRYPT_TOOL_KEY_ALGORITHM);
    String cipherTransformation = config.getString(EncryptToolOptions.ENCRYPT_TOOL_CIPHER_TRANSFORMATION);
    if (encryptTool == null) {
      LOG.info("Initializing new EncrpytTool instance.");
      encryptTool = new EncryptTool(keyPath, keyAlgorithm, cipherTransformation);
    } else if (!keyPath.equals(encryptTool.keyPath) || !keyAlgorithm.equals(encryptTool.keyAlgorithm) || !cipherTransformation.equals(encryptTool.cipherTransformation)) {
      LOG.warn("Reloading EncryptTool with changed configuration.");
      encryptTool = new EncryptTool(keyPath, keyAlgorithm, cipherTransformation);
    }

    return encryptTool;
  }

  public static void writeKey(String seed, int keyBytes, Path keyPath) throws IOException {
    SecureRandom rnd = new SecureRandom(seed.getBytes());
    byte[] key = new byte[keyBytes];
    rnd.nextBytes(key);
    FileSystem fs = keyPath.getFileSystem();
    FSDataOutputStream outStream = fs.create(keyPath, WriteMode.OVERWRITE);
    Throwable var7 = null;

    try {
      byte[] keyBase64 = Base64.getEncoder().encode(key);
      outStream.write(keyBase64);
    } catch (Throwable var16) {
      var7 = var16;
      throw var16;
    } finally {
      if (outStream != null) {
        if (var7 != null) {
          try {
            outStream.close();
          } catch (Throwable var15) {
            var7.addSuppressed(var15);
          }
        } else {
          outStream.close();
        }
      }

    }

  }

  public String encrypt(String input) {
    try {
      Cipher cipher = Cipher.getInstance(this.cipherTransformation);
      cipher.init(1, this.encryptionKey);
      return new String(Base64.getEncoder().encode(cipher.doFinal(input.getBytes())));
    } catch (Exception var3) {
      throw new RuntimeException("Unable to encrypt input", var3);
    }
  }

  public String decrypt(String input) {
    try {
      Cipher cipher = Cipher.getInstance(this.cipherTransformation);
      cipher.init(2, this.encryptionKey);
      return new String(cipher.doFinal(Base64.getDecoder().decode(input.getBytes())));
    } catch (Exception var3) {
      throw new RuntimeException("Unable to decrypt input", var3);
    }
  }

  public static Path determineDefaultKeyPath() {
    Path hdfsPath = new Path("hdfs://");

    Path keyPath;
    try {
      FileSystem hdfsFs = hdfsPath.getFileSystem();
      keyPath = new Path(hdfsFs.getHomeDirectory() + "/" + ".flink-master-key.base64");
    } catch (IOException var4) {
      FileSystem localFs = FileSystem.getLocalFileSystem();
      keyPath = new Path(localFs.getHomeDirectory() + "/" + ".flink-master-key.base64");
    }

    LOG.debug("Determined default master key location to be {}.", keyPath);
    return keyPath;
  }

  private static byte[] readKey(Path keyPath) {
    FileSystem fs;
    try {
      fs = keyPath.getFileSystem();
    } catch (IOException var16) {
      throw new RuntimeException("Unable to read the encryption key", var16);
    }

    try {
      FSDataInputStream keyStream = fs.open(keyPath);
      Throwable var4 = null;

      byte[] key;
      try {
        byte[] keyBase64 = new byte[keyStream.available()];
        IOUtils.readFully(keyStream, keyBase64, 0, keyStream.available());
        key = Base64.getDecoder().decode(keyBase64);
      } catch (Throwable var15) {
        var4 = var15;
        throw var15;
      } finally {
        if (keyStream != null) {
          if (var4 != null) {
            try {
              keyStream.close();
            } catch (Throwable var14) {
              var4.addSuppressed(var14);
            }
          } else {
            keyStream.close();
          }
        }

      }

      return key;
    } catch (IOException var18) {
      throw new RuntimeException("Unable to read the encryption key", var18);
    }
  }
}
