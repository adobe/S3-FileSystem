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

import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class MultiOutputsAdapter implements MultiOutputs {

  private final MultipleOutputs<?, ?> mos;

  public static MultiOutputs createInstance(MultipleOutputs<?, ?> mos) {
    return new MultiOutputsAdapter(mos);
  }

  private MultiOutputsAdapter(MultipleOutputs<?, ?> mos) {
    this.mos = Preconditions.checkNotNull(mos);
  }

  public <K, V> void write(String namedOutput, K key, V value, String baseOutputPath)
      throws IOException, InterruptedException {
    mos.write(namedOutput, key, value, baseOutputPath);
  }

  public void close() throws IOException, InterruptedException {
    mos.close();
  }
}
