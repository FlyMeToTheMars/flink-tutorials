/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.streaming.examples.flink.utils;

import static io.github.streaming.examples.flink.utils.Constants.*;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class Utils {

  private static Logger LOG = LoggerFactory.getLogger(Utils.class);


  public static ParameterTool parseArgs(String[] args) throws IOException {

    // Processing job properties
    ParameterTool params = ParameterTool.fromArgs(args);
    if (params.has(K_PROPERTIES_FILE)) {
      params = ParameterTool.fromPropertiesFile(params.getRequired(K_PROPERTIES_FILE))
          .mergeWith(params);
    }

    LOG.info("### Job parameters:");
    for (String key : params.getProperties().stringPropertyNames()) {
      LOG.info("Job Param: {}={}", key, isSensitive(key, params) ? MASK : params.get(key));
    }
    return params;
  }

  public static Properties readKafkaProperties(ParameterTool params) {
    Properties properties = new Properties();
    for (String key : params.getProperties().stringPropertyNames()) {
      if (key.startsWith(KAFKA_PREFIX)) {
        properties.setProperty(key.substring(KAFKA_PREFIX.length()),
            isSensitive(key, params) ? decrypt(params.get(key)) : params.get(key));
      }
    }

    LOG.info("### Kafka parameters:");
    for (String key : properties.stringPropertyNames()) {
      LOG.info("Loading configuration property: {}, {}", key,
          isSensitive(key, params) ? MASK : properties.get(key));
    }
    return properties;
  }


  public static boolean isSensitive(String key, ParameterTool params) {
    Preconditions.checkNotNull(key, "key is null");
    final String value = params.get(SENSITIVE_KEYS_KEY);
    if (value == null) {
      return false;
    }
    String keyInLower = key.toLowerCase();
    String[] sensitiveKeys = value.split(",");

    for (int i = 0; i < sensitiveKeys.length; ++i) {
      String hideKey = sensitiveKeys[i];
      if (keyInLower.length() >= hideKey.length() && keyInLower.contains(hideKey)) {
        return true;
      }
    }
    return false;
  }

  public static String decrypt(String input) {
    Preconditions.checkNotNull(input, "key is null");
    return EncryptTool.getInstance(getConfiguration()).decrypt(input);
  }

  public static Configuration getConfiguration() {
    return ConfigHolder.INSTANCE;
  }

  private static class ConfigHolder {

    static final Configuration INSTANCE = GlobalConfiguration
        .loadConfiguration(CliFrontend.getConfigurationDirectoryFromEnv());
  }

}
