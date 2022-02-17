package com.adobe.s3fs.utils.threading;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class BlockingExecutor implements Executor {

  private final Semaphore semaphore;
  private final ExecutorService delegate;

  public BlockingExecutor(int concurrentTasksLimit, ExecutorService delegate) {
    this.semaphore = new Semaphore(concurrentTasksLimit);
    this.delegate = delegate;
  }

  @Override
  public void execute(final Runnable runnable) {
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted waiting for tasks to finish", e);
    }
    final Runnable wrapped = () -> {
      try {
        runnable.run();
      } finally {
        semaphore.release();
      }
    };
    delegate.execute(wrapped);
  }

  public void shutdownAndAwaitTermination() {
    delegate.shutdown();
    try {
      delegate.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted awaiting termination", e);
    }
  }
}
