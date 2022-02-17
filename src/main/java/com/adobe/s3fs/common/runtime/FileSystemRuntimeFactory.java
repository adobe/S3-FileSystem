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

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class FileSystemRuntimeFactory {

  public static final String THREAD_POOL_SIZE = "fs.s3k.thread.pool.size";
  public static final String MAX_PENDING_TASKS = "fs.s3k.thread.pool.max.tasks";

  public static final int DEFAULT_THREAD_POOL_SIZE = 10;
  public static final int DEFAULT_MAX_PENDING_TASKS = 50;

  private FileSystemRuntimeFactory() {}

  public static FileSystemRuntimeImpl create(FileSystemConfiguration configuration) {
    return new FileSystemRuntimeImpl(createExecutor(configuration));
  }

  private static ExecutorService createExecutor(FileSystemConfiguration configuration) {
    int threadCount = configuration.contextAware().getInt(THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
    int maxPendingTasks = configuration.contextAware().getInt(MAX_PENDING_TASKS, DEFAULT_MAX_PENDING_TASKS);

    return new ThreadPoolExecutor(threadCount,
                                  threadCount,
                                  Integer.MAX_VALUE,
                                  TimeUnit.MILLISECONDS,
                                  new LinkedBlockingQueue<>(maxPendingTasks),
                                  new WorkerFactory(),
                                  new ThreadPoolExecutor.CallerRunsPolicy());
  }

  private static class WorkerFactory implements ThreadFactory {

    private final AtomicInteger count = new AtomicInteger();

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName("S3KWorker-" + count.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
