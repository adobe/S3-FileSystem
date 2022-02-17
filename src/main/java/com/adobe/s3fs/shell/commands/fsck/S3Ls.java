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

package com.adobe.s3fs.shell.commands.fsck;

import com.adobe.s3fs.filesystemcheck.s3.CartesianS3PrefixPartitioner;
import com.adobe.s3fs.filesystemcheck.s3.S3Lister;
import com.adobe.s3fs.filesystemcheck.s3.S3Partitioner;
import com.adobe.s3fs.filesystemcheck.s3.S3PrefixLister;
import com.adobe.s3fs.filesystemcheck.s3.SingleDigitS3PrefixPartitioner;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.adobe.s3fs.shell.CommandGroups.FSCK;

@Command(
    name = "ls",
    description =
        "List S3 bucket. Command example: " +
            "s3fs fsck ls --bucket some-bucket " +
            "--buffer-dir /path/to/buffer",
    groupNames = FSCK)
public class S3Ls implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(S3Ls.class);
  private final Configuration configuration = new Configuration(true);

  @Option(
      name = "--bucket",
      description = "S3 bucket to be listed")
  @Required
  private String bucket;

  @Option(
      name = "--buffer-dir",
      description = "Absolute path in local HDFS from the EMR used for placing S3 prefixes for each partition")
  @Required
  private String bufferDir;

  @Override
  public void run() {
    Preconditions.checkState(!Strings.isNullOrEmpty(bucket));
    Preconditions.checkState(!Strings.isNullOrEmpty(bufferDir));

    LOG.info("Listing bucket {}", bucket);
    LOG.info("Writing prefixes to {}", bufferDir);

    // List all prefixes from the bucket in files rooted in directory pointed by bufferDir
    S3Partitioner s3Partitioner =
        new CartesianS3PrefixPartitioner(new SingleDigitS3PrefixPartitioner());
    AmazonS3 s3 =
        AmazonS3ClientBuilder.standard()
            .withClientConfiguration(
                new ClientConfiguration().withMaxConnections(s3Partitioner.size()))
            .build();
    S3Lister s3Lister = new S3PrefixLister(s3Partitioner, configuration, s3);
    LOG.info("Starting bucket listing");
    s3Lister.list(bucket, bufferDir);

    LOG.info("Listing finished successfully!");
  }
}
