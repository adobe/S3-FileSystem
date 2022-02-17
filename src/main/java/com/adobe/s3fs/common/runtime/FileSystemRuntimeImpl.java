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

package com.adobe.s3fs.common.runtime;

import com.google.common.base.Preconditions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class FileSystemRuntimeImpl implements FileSystemRuntime, AutoCloseable {

  private final ExecutorService executorService;

  public FileSystemRuntimeImpl(ExecutorService executorService) {
    this.executorService = Preconditions.checkNotNull(executorService);
  }

  @Override
  public <T> CompletableFuture<T> async(Supplier<T> supplier) {
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  @Override
  public void close() throws Exception {
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Executor could not shutdown");
    }
  }
}
