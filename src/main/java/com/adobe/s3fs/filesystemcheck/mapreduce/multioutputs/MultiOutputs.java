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

package com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs;

import java.io.IOException;

/**
 * An interface defining a subset of the methods present in MultipleOutputs (the ones currently used in this package),
 * with the purpose of abstracting the usage of MultipleOutputs (especially when not in a Map-Reduce context).
 *
 * @see MultiOutputsAdapter
 */
public interface MultiOutputs extends AutoCloseable {

  <K, V> void write(String namedOutput, K key, V value, String baseOutputPath) throws IOException, InterruptedException;

  void close() throws IOException, InterruptedException;
}
