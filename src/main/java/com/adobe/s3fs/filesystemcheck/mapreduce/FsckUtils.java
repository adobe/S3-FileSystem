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

package com.adobe.s3fs.filesystemcheck.mapreduce;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.util.Optional;
import java.util.UUID;

import static com.adobe.s3fs.filesystem.FileSystemImplementation.UNCOMMITED_PATH_MARKER;
import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;

public final class FsckUtils {

  private FsckUtils() {
    throw new IllegalStateException("Non-instantiable class!");
  }

  public static VersionedObject writableToVersionedObject(
      LogicalObjectWritable logicalObjectWritable, String objectHandleId) {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path(logicalObjectWritable.getLogicalPath()))
            .size(logicalObjectWritable.getSize())
            .creationTime(logicalObjectWritable.getCreationTime())
            .isDirectory(logicalObjectWritable.isDirectory())
            .physicalPath(
                logicalObjectWritable.isDirectory()
                    ? Optional.empty()
                    : Optional.of(logicalObjectWritable.getPhysicalPath()))
            .physicalDataCommitted(
                !logicalObjectWritable.getPhysicalPath().equals(UNCOMMITED_PATH_MARKER))
            .build();

    return VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.fromString(objectHandleId))
        .version(logicalObjectWritable.getVersion())
        .build();
  }

  public static TextArrayWritable mosS3ValueWritable(String bucket, String prefix) {
    return new TextArrayWritable(new Text[] {new Text(bucket), new Text(prefix)});
  }

  public static String operationLogPrefix(String objectHandleId) {
    return String.format("%s%s", objectHandleId, INFO_SUFFIX);
  }
}
