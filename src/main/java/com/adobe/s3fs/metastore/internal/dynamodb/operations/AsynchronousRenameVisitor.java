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
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

import java.util.concurrent.CompletableFuture;

public class AsynchronousRenameVisitor implements AsynchronousObjectVisitor {

  private final Path destinationPath;
  private final MetadataOperations metadataOps;

  public AsynchronousRenameVisitor(Path destinationPath, MetadataOperations metadataOps) {
    this.destinationPath = Preconditions.checkNotNull(destinationPath);
    this.metadataOps = Preconditions.checkNotNull(metadataOps);
  }

  @Override
  public CompletableFuture<Boolean> preVisitDirectoryObject(VersionedObject directory) {
    return metadataOps.storeAsync(directory.moveTo(destinationPath));
  }

  @Override
  public CompletableFuture<Boolean> postVisitDirectoryObject(VersionedObject directory) {
    return metadataOps.deleteAsync(directory);
  }

  @Override
  public CompletableFuture<Boolean> visitFileObject(VersionedObject file) {

    return metadataOps.renameFileAsync(file, destinationPath);
  }

  @Override
  public AsynchronousObjectVisitor childVisitor(VersionedObject parent, VersionedObject child) {
    return new AsynchronousRenameVisitor(new Path(destinationPath, child.metadata().getKey().getName()), metadataOps);
  }
}
