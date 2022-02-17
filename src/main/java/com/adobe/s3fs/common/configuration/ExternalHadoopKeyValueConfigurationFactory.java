/*
Copyright 2021 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

package com.adobe.s3fs.common.configuration;

import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Factory that loads a Hadoop configuration located in S3 and applies the properties to the file system configuration.
 */
public class ExternalHadoopKeyValueConfigurationFactory implements KeyValueConfigurationFactory, Configurable {

  public static final String EXTERNAL_S3_HADOOP_CONFIGURATION_LOCATION = "fs.s3k.external.hadoop.config.factory.config.location";

  private static final Logger LOG = LoggerFactory.getLogger(ExternalHadoopKeyValueConfigurationFactory.class);

  private Configuration hadoopConf;

  @Override
  public void setConf(Configuration configuration) {
    this.hadoopConf = configuration;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf;
  }

  @Override
  public KeyValueConfiguration create() {
    String fullPathToConfig = hadoopConf.get(EXTERNAL_S3_HADOOP_CONFIGURATION_LOCATION);
    if (Strings.isNullOrEmpty(fullPathToConfig)) {
      throw new IllegalStateException(EXTERNAL_S3_HADOOP_CONFIGURATION_LOCATION + " must be set");
    }

    LOG.info("Loading external config from {}", fullPathToConfig);

    Configuration finalConfiguration = new Configuration(hadoopConf);
    try (InputStream in = new FileInputStream(fullPathToConfig)) {
      Configuration externalConfiguration = new Configuration(false);
      externalConfiguration.addResource(in);
      for (Map.Entry<String, String> entry : externalConfiguration) {
        finalConfiguration.set(entry.getKey(), entry.getValue());
      }
    } catch (IOException e) {
      throw new UncheckedException("Failed to read config", e);
    }
    return new HadoopKeyValueConfiguration(finalConfiguration);
  }
}
