/*

 Copyright today.year Adobe. All rights reserved.
 This file is licensed to you under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License. You may obtain a copy
 of the License at http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under
 the License is distributed on an "AS IS" BASIS, WITHOUT
 WARRANTIES OR REPRESENTATIONS
 OF ANY KIND, either express or implied. See the License for the specific language
 governing permissions and limitations under the License.
 */

package com.adobe.s3fs.utils.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Serialization;

import java.util.Arrays;
import java.util.stream.Stream;

public final class HadoopConfigUtils {

  public static final String IO_SERIALIZATIONS_PROP = "io.serializations";

  private HadoopConfigUtils() {}

  public static <T> void addSerialization(Configuration conf, Class<? extends Serialization<T>> clazz) {
    Stream<String> currentSerializations = Arrays.stream(conf.getTrimmedStrings(IO_SERIALIZATIONS_PROP));
    String[] combinedSerializations = Stream.concat(currentSerializations, Stream.of(clazz.getName()))
        .distinct()
        .toArray(String[]::new);
    conf.setStrings(IO_SERIALIZATIONS_PROP, combinedSerializations);
  }
}
