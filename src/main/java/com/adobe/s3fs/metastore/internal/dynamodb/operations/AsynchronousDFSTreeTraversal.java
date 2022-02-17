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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsynchronousDFSTreeTraversal {

  private final MetadataOperations metadataOperations;

  public AsynchronousDFSTreeTraversal(MetadataOperations metadataOperations) {
    this.metadataOperations = Preconditions.checkNotNull(metadataOperations);
  }

  public CompletableFuture<Boolean> traverse(VersionedObject root, AsynchronousObjectVisitor visitor) {
    if (!root.metadata().isDirectory()) {
      return visitor.visitFileObject(root);
    }

    CompletableFuture<Boolean> previsitParent = visitor.preVisitDirectoryObject(root);

    return previsitParent.thenCompose(success -> {
      if (!success) {
        return CompletableFuture.completedFuture(Boolean.FALSE);
      }

      CompletableFuture<Boolean> subtreeFuture = metadataOperations.listAsync(root)
          .thenCompose(children -> {
            AtomicBoolean someChildrenFailed = new AtomicBoolean(false);

            List<CompletableFuture<Boolean>> childrenFutures = new ArrayList<>();

            for (VersionedObject child : children) {
              AsynchronousObjectVisitor childVisitor = visitor.childVisitor(root, child);
              childrenFutures.add(traverse(child, childVisitor).thenApply(childSuccess -> {
                if (!childSuccess) {
                  someChildrenFailed.set(true);
                }
                return childSuccess;
              }));
            }

            return CompletableFuture.allOf((CompletableFuture[]) childrenFutures.toArray(new CompletableFuture[0]))
                .thenApply(it -> !someChildrenFailed.get());
          });

      return subtreeFuture
          .thenCompose(subtreeResult -> subtreeResult ? visitor.postVisitDirectoryObject(root) : CompletableFuture.completedFuture(Boolean.FALSE));
    });
  }
}
