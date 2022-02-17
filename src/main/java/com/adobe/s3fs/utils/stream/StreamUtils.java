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

package com.adobe.s3fs.utils.stream;

import com.adobe.s3fs.utils.exceptions.UncheckedException;

import java.util.function.Consumer;
import java.util.function.Function;

public final class StreamUtils {

  private StreamUtils() {}

  public static <T, E extends Exception> Consumer<T> uncheckedConsumer(ThrowingConsumer<T, E> consumer) {
    return it -> {
      try {
        consumer.accept(it);
      } catch (Exception e) {
        throw new UncheckedException(e);
      }
    };
  }

  public static <T, U, E extends Exception> Function<T, U> uncheckedFunction(ThrowingFunction<T,U, E> function) {
    return it -> {
      try {
        return function.apply(it);
      } catch (Exception e) {
        throw new UncheckedException(e);
      }
    };
  }

  public static <E extends Exception> Runnable uncheckedRunnable(ThrowingRunnable<E> runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (Exception e) {
        throw new UncheckedException(e);
      }
    };
  }
}
