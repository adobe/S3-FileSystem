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

package com.adobe.s3fs.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.operationlog.LogicalFileMetadataV2;
import com.adobe.s3fs.operationlog.ObjectMetadataSerialization;
import com.adobe.s3fs.operationlog.S3MetadataOperationLog;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class OperationLogStateChecker {

  public static void checkOperationLogState(AmazonS3 s3, String bucket, ExpectedFSObject... expectedOperationLog) {
    checkOperationLogState(s3, bucket, Arrays.asList(expectedOperationLog));
  }

  public static void checkOperationLogState(AmazonS3 s3, String bucket, Collection<ExpectedFSObject> expectedOperationLog) {
    List<LogicalFileMetadataV2> operationLog = ITUtils.listFully(s3, bucket)
        .stream()
        .filter(it -> it.getKey().endsWith(S3MetadataOperationLog.INFO_SUFFIX))
        .map(it -> readMetadata(s3, new GetObjectRequest(bucket, it.getKey())))
        .collect(Collectors.toList());

    assertEquals(expectedOperationLog.size(), operationLog.size());

    for (ExpectedFSObject expectedFSObject : expectedOperationLog) {
      List<LogicalFileMetadataV2> objectVersions = operationLog.stream()
          .filter(it -> samePathsIgnoringScheme(new Path(it.getLogicalPath()), expectedFSObject.path))
          .sorted(Comparator.comparingInt(LogicalFileMetadataV2::getVersion))
          .collect(Collectors.toList());
      assertFalse(objectVersions.isEmpty());
      LogicalFileMetadataV2 logicalFileMetadata = objectVersions.get(objectVersions.size() - 1);

      assertEquals(expectedFSObject.length, logicalFileMetadata.getSize());
      if (!expectedFSObject.isDir) {
        assertTrue(logicalFileMetadata.isPhysicalDataCommitted());
      } else {
        assertFalse(logicalFileMetadata.isPhysicalDataCommitted());
      }
      assertEquals(expectedFSObject.version, logicalFileMetadata.getVersion());
      assertEquals(OperationLogEntryState.COMMITTED, logicalFileMetadata.getState());
    }
  }

  private static LogicalFileMetadataV2 readMetadata(AmazonS3 s3, GetObjectRequest getObjectRequest) {
    try {
      return ObjectMetadataSerialization.deserializeFromV2(s3.getObject(getObjectRequest).getObjectContent());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean samePathsIgnoringScheme(Path first, Path second) {
    try {
      URI firstURI = first.toUri();
      URI secondURI = second.toUri();
      Path sameURIFirst = new Path(new URI("scheme",
                                           firstURI.getUserInfo(),
                                           firstURI.getHost(),
                                           firstURI.getPort(),
                                           firstURI.getPath(),
                                           firstURI.getQuery(),
                                           firstURI.getFragment()));
      Path sameURISecond = new Path(new URI("scheme",
                                           secondURI.getUserInfo(),
                                           secondURI.getHost(),
                                           secondURI.getPort(),
                                           secondURI.getPath(),
                                           secondURI.getQuery(),
                                           secondURI.getFragment()));
      return sameURIFirst.equals(sameURISecond);
    } catch (Exception e) {
      return false;
    }
  }
}
