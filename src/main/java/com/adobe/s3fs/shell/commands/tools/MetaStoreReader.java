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

import com.adobe.s3fs.filesystemcheck.utils.FileSystemServices;
import com.adobe.s3fs.metastore.api.MetadataStoreExtended;
import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.adobe.s3fs.shell.CommandGroups.TOOLS;

@Command(
    name = "metaStoreReader",
    description = "Resolves a physical path having as input a logical(meta) path from DynamoDB. Command example: " +
        "tools metaStoreReader --bucket <bucket> --logicalPath <dir1>/<dir2>/.../<dirN>/file",
    groupNames = TOOLS)
public class MetaStoreReader implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreReader.class);
  private final Configuration configuration = new Configuration(true);

  @Option(
      name = "--logicalPath",
      description =
          "Relative Logical(meta) path(prefix) from DynamoDB (<some_path_to_parent_dir>/file)")
  @Required
  private String logicalPath;

  @Option(name = "--bucket", description = "S3 bucket to which this logicalPath belongs")
  @Required
  private String bucket;

  @Override
  public void run() {
    Preconditions.checkState(!Strings.isNullOrEmpty(logicalPath));
    Preconditions.checkState(!Strings.isNullOrEmpty(bucket));

    try (MetadataStoreExtended metadataStoreExt = FileSystemServices.createMetadataStore(configuration, bucket)) {
      Path metaFullPath = new Path("ks://" + bucket + "/" + logicalPath); // NOSONAR
      LOG.info("Determining metadata for logical path {}", metaFullPath);
      Optional<? extends ObjectHandle> existingObjectOptional =
          metadataStoreExt.getObject(metaFullPath);
      ObjectHandle objectHandle = existingObjectOptional.orElseThrow(IllegalArgumentException::new);

      LOG.info("Metadata is {}", objectHandle);
    } catch (IOException e) {
      LOG.error("Exception thrown while processing", e);
      throw new UncheckedIOException(e);
    }
  }
}
