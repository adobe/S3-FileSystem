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

package com.adobe.s3fs.metastore.internal.dynamodb.hashing;

import java.util.function.ToIntFunction;
import org.apache.hadoop.fs.Path;

public class DefaultHashFunction implements ToIntFunction<Path> {

  @Override
  public int applyAsInt(Path value) {
    return hashCode(value);
  }

  private int hashCode(Path value) {
    int h = 0;
    char[] string = value.getName().toCharArray();
    if (string.length > 0) {
      char[] val = string;
      for (int i = 0; i < string.length; i++) {
        h = 31 * h + val[i];
      }
    }
    return h;
  }
}
