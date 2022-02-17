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

package com.adobe.s3fs.metastore.internal.dynamodb.utils;

import com.adobe.s3fs.metastore.internal.dynamodb.storage.DynamoDBItem;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;

public final class DynamoUtils {

  private DynamoUtils() {
    // no nop
  }

  public static DynamoDBItem objectToDynamoItem(VersionedObject object, String hashKey, String sortKey) {
    return DynamoDBItem.builder()
        .hashKey(hashKey)
        .sortKey(sortKey)
        .isDirectory(object.metadata().isDirectory())
        .creationTime(object.metadata().getCreationTime())
        .size(object.metadata().getSize())
        .physicalPath(object.metadata().getPhysicalPath())
        .physicalDataCommitted(object.metadata().physicalDataCommitted())
        .version(object.version())
        .id(object.id())
        .build();
  }
}
