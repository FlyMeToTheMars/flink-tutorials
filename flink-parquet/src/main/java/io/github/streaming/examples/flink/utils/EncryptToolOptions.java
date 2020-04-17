//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.github.streaming.examples.flink.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@PublicEvolving
public class EncryptToolOptions {
  public static final ConfigOption<Boolean> ENCRYPT_TOOL_ENABLED = ConfigOptions.key("security.encrypt-tool.enabled").defaultValue(false).withDescription("Enables encryption-tool to protect sensitive entries in config files");
  public static final ConfigOption<String> ENCRYPT_TOOL_KEY_LOCATION = ConfigOptions.key("security.encrypt-tool.key.location").defaultValue(EncryptTool.determineDefaultKeyPath().toString()).withDescription("Location of the encryption key for the EncryptTool. This location should be protected and only readable by your own user.");
  public static final ConfigOption<String> ENCRYPT_TOOL_KEY_SEED = ConfigOptions.key("security.encrypt-tool.key.seed").defaultValue("Apache Flink").withDescription("Optional seed value for the secure random generator used to generate the encryption key.");
  public static final ConfigOption<String> ENCRYPT_TOOL_KEY_ALGORITHM = ConfigOptions.key("security.encrypt-tool.key.algorithm").defaultValue("AES").withDescription("Algorithm passed to javax.crypto.spec.SecretKeySpec to create the key.");
  public static final ConfigOption<Integer> ENCRYPT_TOOL_KEY_BYTES = ConfigOptions.key("security.encrypt-tool.key.bytes").defaultValue(32).withDescription("Length of the encryption key in bytes. We recommend using 32 byte or longer keys.");
  public static final ConfigOption<String> ENCRYPT_TOOL_CIPHER_TRANSFORMATION = ConfigOptions.key("security.encrypt-tool.cipher.algorithm").defaultValue("AES").withDescription("Transformation passed to javax.crypto.Cipher to describe the crypthographic transformation.");

  private EncryptToolOptions() {
  }
}
