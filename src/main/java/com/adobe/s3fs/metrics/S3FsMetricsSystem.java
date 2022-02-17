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

public interface S3FsMetricsSystem {
  /**
   * Register a metrics source
   * @param name of the source. Must be unique or null (then extracted from
   *             the annotations of the source object, see org.apache.hadoop.metrics2.annotation.Metric)
   * @param description the description of the source (or null. See above)
   * @param source object to register
   * @param <T> the actual type of the source object
   * @return source objects
   */
  public <T> T registerSource(String name, String description, T source);

  /**
   * Register a metrics source (deriving name and description from the object)
   * @param source object to register
   * @param <T> the actual type of the source object
   * @return
   */
  public default <T> T registerSource(T source) {
    return registerSource(null, null, source);
  }

  /**
   * Shutdown the metrics system
   */
  public void shutdown();
}
