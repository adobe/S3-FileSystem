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

package com.adobe.s3fs.filesystem;

public interface QualifyingFileSystemMetrics {

  /**
   * Record that a create operation failed
   */
  void recordFailedCreate();

  /**
   * Record that a overwrite operation failed
   */
  void recordFailedOverwrite();

  /**
   * Record that a rename operation completed successfully
   */
  void recordSuccessfulRename();

  /**
   * Record that a rename operation failed
   */
  void recordFailedRename();

  /**
   * Record that a delete operation completed successfully
   */
  void recordSuccessfulDelete();

  /**
   * Record that a delete operation failed
   */
  void recordFailedDelete();

  /**
   * Record that a mkdir operation completed successfully
   */
  void recordSuccessfulMkdir();

  /**
   * Record that a mkdir operation failed
   */
  void recordFailedMkdir();
}
