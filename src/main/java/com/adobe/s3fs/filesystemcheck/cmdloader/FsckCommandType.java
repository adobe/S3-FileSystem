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

package com.adobe.s3fs.filesystemcheck.cmdloader;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public enum FsckCommandType {
  S3("s3"),
  METASTORE("metastore"),
  OPLOG("oplog");

  private static final Map<String, FsckCommandType> stringToEnum =
      Stream.of(values()).collect(toMap(FsckCommandType::getType, e -> e));

  private final String type;

  FsckCommandType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }

  @Override
  public String toString() {
    return type;
  }

  /**
   *
   * @param type String representation
   * @return enum type associated with the String representation
   * @throws IllegalStateException if the String argument doesn't have an enum associated
   */
  public static FsckCommandType fromString(String type) {
    FsckCommandType enumType = stringToEnum.get(type);
    if (enumType == null) {
      throw new IllegalStateException(type + " isn't attached to a know enum type!");
    }
    return stringToEnum.get(type);
  }
}
