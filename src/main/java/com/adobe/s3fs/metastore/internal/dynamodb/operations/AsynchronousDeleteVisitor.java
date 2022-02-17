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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AsynchronousDeleteVisitor implements AsynchronousObjectVisitor {

  private final MetadataOperations metadataOperations;
  private final Function<ObjectHandle, Boolean> callback;

  public AsynchronousDeleteVisitor(MetadataOperations metadataOperations, Function<ObjectHandle, Boolean> callback) {
    this.metadataOperations = Preconditions.checkNotNull(metadataOperations);
    this.callback = Preconditions.checkNotNull(callback);
  }

  @Override
  public CompletableFuture<Boolean> preVisitDirectoryObject(VersionedObject directory) {
    return CompletableFuture.completedFuture(Boolean.TRUE);
  }

  @Override
  public CompletableFuture<Boolean> visitFileObject(VersionedObject file) {
    return metadataOperations.deleteAsync(file)
        .thenApply(it -> it && callback.apply(file));
  }

  @Override
  public CompletableFuture<Boolean> postVisitDirectoryObject(VersionedObject directory) {
    return metadataOperations.deleteAsync(directory)
        .thenApply(it -> it && callback.apply(directory));
  }
}
