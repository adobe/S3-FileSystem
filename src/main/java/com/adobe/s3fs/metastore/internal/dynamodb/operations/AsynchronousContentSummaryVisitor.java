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
import org.apache.hadoop.fs.ContentSummary;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class AsynchronousContentSummaryVisitor implements AsynchronousObjectVisitor {

  private final AtomicLong length;
  private final AtomicLong dirCount;
  private final AtomicLong fileCount;


  public AsynchronousContentSummaryVisitor() {
    this(new AtomicLong(), new AtomicLong(), new AtomicLong());
  }

  public AsynchronousContentSummaryVisitor(AtomicLong length, AtomicLong dirCount, AtomicLong fileCount) {
    this.length = length;
    this.dirCount = dirCount;
    this.fileCount = fileCount;
  }

  @Override
  public CompletableFuture<Boolean> preVisitDirectoryObject(VersionedObject directory) {
    dirCount.incrementAndGet();
    return CompletableFuture.completedFuture(Boolean.TRUE);
  }

  @Override
  public CompletableFuture<Boolean> postVisitDirectoryObject(VersionedObject directory) {
    return CompletableFuture.completedFuture(Boolean.TRUE);
  }

  @Override
  public CompletableFuture<Boolean> visitFileObject(VersionedObject file) {
    fileCount.incrementAndGet();
    length.addAndGet(file.metadata().getSize());
    return CompletableFuture.completedFuture(Boolean.TRUE);
  }

  public ContentSummary toContentSummary() {
    return new ContentSummary.Builder()
        .length(length.get())
        .directoryCount(dirCount.get())
        .fileCount(fileCount.get())
        .build();
  }
}
