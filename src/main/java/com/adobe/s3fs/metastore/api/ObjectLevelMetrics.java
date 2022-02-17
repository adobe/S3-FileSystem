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

package com.adobe.s3fs.metastore.api;

/**
 * Interface used for recording erros when interacting with
 * Operation Log and DynamoDB. The implementing class
 * must provide thread safety for the exposed methods
 * because these methods will be called concurrently
 */
public interface ObjectLevelMetrics {
  /**
   * Record failed PENDING Operation log
   */
  void incrFailedPendingOpLog();

  /**
   * Record failed COMMIT Operation log
   */
  void incrFailedCommitOpLog();

  /**
   * Record failed DynamoDB operation
   */
  void incrFailedDynamoDB();

  /**
   * Record a failure of the operation log rollback that occurs when a DynamoDB operation fails.
   */
  void incrOpLogFailedRollback();

  /**
   * Record a success of the operation log rollback that occurs when a DynamoDB operation fails.
   */
  void incrOpLogSuccessfulRollback();
}
