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

import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

/** Renames an object tree. */
public class RenameVisitor implements ObjectVisitor {

  private final Path destinationPath;
  private final MetadataOperations metadataOps;

  public RenameVisitor(
      Path destinationPath,
      MetadataOperations metadataOps) {
    this.destinationPath = Preconditions.checkNotNull(destinationPath);
    this.metadataOps = Preconditions.checkNotNull(metadataOps);
  }

  @VisibleForTesting
  public Path getDestinationPath() {
    return destinationPath;
  }

  /**
   * Creates the destination directory where the subtree will be moved.
   *
   * @param directory
   * @return
   */
  @Override
  public boolean preVisitDirectoryObject(VersionedObject directory) {
    return metadataOps.store(directory.moveTo(destinationPath));
  }

  /**
   * Delets the source directory of the move.
   *
   * @param directory
   * @return
   */
  @Override
  public boolean postVisitDirectoryObject(VersionedObject directory) {
    return metadataOps.delete(directory);
  }

  /**
   * Moves a single file. This implies deleting the source and creating the destination.
   *
   * @param file
   * @return
   */
  @Override
  public boolean visitFileObject(VersionedObject file) {
    return metadataOps.renameFile(file, destinationPath);
  }

  /**
   * Creates a new visitor for the child subtree.
   *
   * @param parent
   * @param child
   * @return
   */
  @Override
  public ObjectVisitor childVisitor(VersionedObject parent, VersionedObject child) {
    return new RenameVisitor(
        new Path(destinationPath, child.metadata().getKey().getName()),
        metadataOps);
  }
}
