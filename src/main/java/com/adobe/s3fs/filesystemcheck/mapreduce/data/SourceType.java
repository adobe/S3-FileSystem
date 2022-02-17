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

package com.adobe.s3fs.filesystemcheck.mapreduce.data;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public enum SourceType {
  FROM_S3((short)0),
  FROM_METASTORE((short)1),
  FROM_OPLOG((short)2);

  private static final Map<Short, SourceType> codeToEnum =
      Stream.of(values()).collect(toMap(SourceType::getCode, e -> e));

  public final short code;

  SourceType(short code) {
    this.code = code;
  }

  public short getCode() {
    return code;
  }

  public static SourceType fromCode(short code) {
    return codeToEnum.get(code);
  }
}
