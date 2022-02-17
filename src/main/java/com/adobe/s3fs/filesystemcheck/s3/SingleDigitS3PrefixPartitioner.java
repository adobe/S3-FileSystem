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

package com.adobe.s3fs.filesystemcheck.s3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SingleDigitS3PrefixPartitioner implements S3Partitioner {

  private static final String[] singleDigits = {
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"
  };

  private final List<String> prefixes;

  public SingleDigitS3PrefixPartitioner() {
    prefixes =
        Collections.unmodifiableList(Arrays.stream(singleDigits).collect(Collectors.toList()));
  }

  @Override
  public List<String> prefixes() {
    return prefixes;
  }
}
