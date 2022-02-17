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

package com.adobe.s3fs.filesystemcheck.s3;

import java.util.List;

/** Provide a list of prefixes after which to partition the list call */
public interface S3Partitioner {

  /** @return a collection of prefixes to be used for partitioning the list call */
  List<String> prefixes();

  /** @return number of prefixes to partition after */
  default int size() {
    return prefixes().size();
  }
}
