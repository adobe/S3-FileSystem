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

package com.adobe.s3fs.metastore.internal.dynamodb.operations;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class  AsynchronousMetadataTreeOperations implements MetadataTreeOperations {

  private final AsynchronousDFSTreeTraversal treeTraversal;
  private final MetadataOperations metadataOperations;

  private static final Logger LOG = LoggerFactory.getLogger(AsynchronousMetadataTreeOperations.class);

  public AsynchronousMetadataTreeOperations(MetadataOperations metadataOperations) {
    this.metadataOperations = Preconditions.checkNotNull(metadataOperations);
    this.treeTraversal = new AsynchronousDFSTreeTraversal(metadataOperations);
  }

  @Override
  public boolean deleteObjectTree(VersionedObject root, Function<ObjectHandle, Boolean> physicalDeleteCallback) {
    AsynchronousDeleteVisitor deleteVisitor = new AsynchronousDeleteVisitor(metadataOperations, physicalDeleteCallback);

    try {
      return treeTraversal.traverse(root, deleteVisitor)
          .exceptionally(e -> {
            LOG.error("Error deleting " + root.metadata().getKey(), e);
            return Boolean.FALSE;
          }).get();
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean renameObjectTree(VersionedObject source, Path destination) {
    AsynchronousRenameVisitor renameVisitor = new AsynchronousRenameVisitor(destination, metadataOperations);

    try {
      return treeTraversal.traverse(source, renameVisitor)
          .exceptionally(e -> {
            LOG.error("Error renaming " + source.metadata().getKey() + " to " + destination, e);
            return Boolean.FALSE;
          }).get();
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public ContentSummary computeContentSummary(VersionedObject object) {
    AsynchronousContentSummaryVisitor visitor = new AsynchronousContentSummaryVisitor();

    boolean result = false;
    try {
      result = treeTraversal.traverse(object, visitor)
          .exceptionally(e -> {
            LOG.error("Failed to compute content summary");
            return Boolean.FALSE;
          }).get();
    } catch (Exception ignored) {
      // nothing to do
    }

    if (!result) {
      throw new IllegalStateException("Failed to compute content summary for " + object);
    }

    return visitor.toContentSummary();
  }
}
