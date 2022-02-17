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

package com.adobe.s3fs.shell.commands.tools;

import com.adobe.s3fs.operationlog.LogicalFileMetadataV2;
import com.adobe.s3fs.operationlog.ObjectMetadataSerialization;
import com.adobe.s3fs.utils.aws.s3.S3Helpers;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;

import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;
import static com.adobe.s3fs.shell.CommandGroups.TOOLS;

@Command(
    name = "operationLogReader",
    description = "Resolves a logical path having as input a physical path from S3. Command example: " +
        "tools operationLogReader --path s3://<path_to_physical_path_or_operation_log>",
    groupNames = TOOLS)
public class OperationLogReader implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(OperationLogReader.class);
  private static final String PHYSICAL_DATA_MARKER = ".id=";

  @Option(
      name = {"--path"},
      description =
          "Physical path from S3 or operation log path. Needs to adhere to one of the following formats:" +
              " s3://<bucket>/<randomUUID>.id=<objectHandle.id> or " +
              " s3://<bucket>/<objectHandle.id>.info")
  @Required
  private String physicalPath;

  @Override
  public void run() {
    Preconditions.checkState(!Strings.isNullOrEmpty(physicalPath));

    AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard().build();

    // Retrieve the id from the physical path
    LOG.info("Determining logical path for physical path {}", physicalPath);
    UUID objHandleId =
        getIdFromPhysicalPath(S3Helpers.getPrefix(physicalPath))
            .orElseThrow(() -> new IllegalStateException("Physical path doesn't respect format!"));

    String operationLogPrefix = String.format("%s%s", objHandleId.toString(), INFO_SUFFIX);
    // Let's retrieve the operation log entry associated with this id
    LogicalFileMetadataV2 opLogMetadata =
        downloadOperationLog(amazonS3, S3Helpers.getBucket(physicalPath), operationLogPrefix)
            .orElseThrow(() -> new IllegalStateException("Failed to retrieve operation log!"));

    if (!opLogMetadata.getId().equals(objHandleId.toString())) {
      throw new IllegalStateException(
          "Mismatch between id from physical path and id from operation log");
    }

    LOG.info("Operation log is {}", opLogMetadata);
  }

  private Optional<UUID> convertToUUIDFromString(String uuid) {
    try {
      return Optional.of(UUID.fromString(uuid));
    } catch (RuntimeException ex) {
      LOG.error("{} doesn't respect UUID format", uuid);
      return Optional.empty();
    }
  }

  private Optional<UUID> getIdFromPhysicalPath(String physicalPath) {
    int lastIndexOfPhysicalDataMarker = physicalPath.lastIndexOf(PHYSICAL_DATA_MARKER);
    if (lastIndexOfPhysicalDataMarker > 0) {
      String objHandleId = physicalPath.substring(lastIndexOfPhysicalDataMarker + PHYSICAL_DATA_MARKER.length());
      return convertToUUIDFromString(objHandleId);
    }

    int lastIndexOfOpLogMarker = physicalPath.lastIndexOf(INFO_SUFFIX);
    if (lastIndexOfOpLogMarker > 0) {
      String objHandleId = physicalPath.substring(0, lastIndexOfOpLogMarker);
      return convertToUUIDFromString(objHandleId);
    }

    return Optional.empty();
  }

  private Optional<LogicalFileMetadataV2> downloadOperationLog(
      AmazonS3 amazonS3, String bucket, String prefix) {
    try (InputStream inputStream = amazonS3.getObject(bucket, prefix).getObjectContent()) {
      return Optional.ofNullable(ObjectMetadataSerialization.deserializeFromV2(inputStream));
    } catch (IOException e) {
      LOG.error("Exception thrown while getting operation log", e);
      return Optional.empty();
    }
  }
}
