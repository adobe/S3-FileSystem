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

package com.adobe.s3fs.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

import java.util.concurrent.atomic.AtomicReference;

public class S3FsMetricsSystemFactory {

  public static final String S3_FS_METRICS_SYSTEM_NAME = "S3FS";

  private S3FsMetricsSystemFactory() {
    // Nothing to do for the moment
  }

  public static S3FsMetricsSystem getS3FsMetricsSystem() {
    return S3FsMetricsSystemImpl.INSTANCE;
  }

  enum S3FsMetricsSystemImpl implements S3FsMetricsSystem {
    INSTANCE;

    private final AtomicReference<MetricsSystem> metricsSystemImpl;

    S3FsMetricsSystemImpl() {
      metricsSystemImpl = new AtomicReference<>(new MetricsSystemImpl());
      getImpl().init(S3_FS_METRICS_SYSTEM_NAME);
    }

    @Override
    public <T> T registerSource(String name, String description, T source) {
      return getImpl().register(name, description, source);
    }

    @Override
    public void shutdown() {
      getImpl().shutdown();
    }

    MetricsSystem getImpl() {
      return metricsSystemImpl.get();
    }
  }
}
