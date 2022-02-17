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

import static com.adobe.s3fs.metrics.S3FsMetricsSystemFactory.S3FsMetricsSystemImpl.INSTANCE;

public class S3FsMetricsSystemPackagePrivateAccessor {
  public static <T> T register(String name, String description, T source) {
    return INSTANCE.registerSource(name, description, source);
  }

  public static void unregister(String name) {
    INSTANCE.getImpl().unregisterSource(name);
  }
}
