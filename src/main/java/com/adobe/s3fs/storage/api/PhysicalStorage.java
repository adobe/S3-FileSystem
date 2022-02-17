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

package com.adobe.s3fs.storage.api;

import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface modelling a persistent physical storage (e.g. S3 blob store bucket)
 */
public interface PhysicalStorage extends Closeable {

  /**
   * Creates the given key in the storage. If the key already exists it will be overwritten.
   * @param key
   * @return An {@link OutputStream} that the client can use to write data.
   * @throws IOException in case of IO error.
   */
  OutputStream createKey(Path key) throws IOException;

  /**
   * Opens the key for reading. If the key does not exist an error will be thrown.
   * @param key
   * @return and {@link InputStream} that the client can use to read data.
   * @throws IOException in case of IO error.
   */
  InputStream openKey(Path key) throws IOException;

  /**
   * Check whether the given key exists.
   * @param key
   * @return
   * @throws IOException in case of IO error.
   */
  boolean exists(Path key) throws IOException;

  /**
   * Deletes the given key. If the key does not exist in the storage this is a NOOP.
   * @param key
   * @throws IOException in case of IO error.
   */
  void deleteKey(Path key) throws IOException;
}
