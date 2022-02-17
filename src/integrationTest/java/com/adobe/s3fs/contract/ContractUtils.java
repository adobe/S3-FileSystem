/*

 Copyright 2021 Adobe. All rights reserved.
 This file is licensed to you under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License. You may obtain a copy
 of the License at http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under
 the License is distributed on an "AS IS" BASIS, WITHOUT
 WARRANTIES OR REPRESENTATIONS
 OF ANY KIND, either express or implied. See the License for the specific language
 governing permissions and limitations under the License.
 */

package com.adobe.s3fs.contract;

import com.adobe.s3fs.filesystem.HadoopFileSystemAdapter;
import com.adobe.s3fs.utils.ITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testcontainers.containers.localstack.LocalStackContainer;

public final class ContractUtils {

  private ContractUtils() {}

  public static void configureFullyFunctionalFileSystem(Configuration configuration,
                                                        LocalStackContainer localStackContainer,
                                                        String tmpFolder) {
    ITUtils.createBucketIfNotExists(ITUtils.amazonS3(localStackContainer), S3KFileSystemContract.BUCKET);
    ITUtils.createBucketIfNotExists(ITUtils.amazonS3(localStackContainer), S3KFileSystemContract.OPLOG_BUCKET);
    ITUtils.createMetaTableIfNotExists(ITUtils.amazonDynamoDB(localStackContainer), S3KFileSystemContract.DYNAMO_TABLE);

    configuration.setClass("fs.s3k.impl", HadoopFileSystemAdapter.class, FileSystem.class);
    configuration.setBoolean("fs.s3k.impl.disable.cache", true);
    ITUtils.configureAsyncOperations(configuration, S3KFileSystemContract.BUCKET, "ctx");

    ITUtils.configureDynamoAccess(localStackContainer, configuration, S3KFileSystemContract.BUCKET);
    ITUtils.mapBucketToTable(configuration, S3KFileSystemContract.BUCKET, S3KFileSystemContract.DYNAMO_TABLE);

    ITUtils.configureS3OperationLog(configuration, S3KFileSystemContract.BUCKET, S3KFileSystemContract.OPLOG_BUCKET);
    ITUtils.configureS3OperationLogAccess(localStackContainer, configuration, S3KFileSystemContract.BUCKET);

    ITUtils.configureS3AAsUnderlyingFileSystem(localStackContainer, configuration, S3KFileSystemContract.BUCKET, tmpFolder);

    ITUtils.configureSuffixCount(configuration, S3KFileSystemContract.BUCKET, 10);

    ITUtils.setFileSystemContext("ctx");
  }
}
