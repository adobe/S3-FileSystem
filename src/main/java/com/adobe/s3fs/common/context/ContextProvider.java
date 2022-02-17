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

package com.adobe.s3fs.common.context;

import java.util.Optional;

/**
 * Exposes the context in which the filesystem runs and for which it needs configuration.
 * For example we could use different configuration when running as a standalone java app vs a M/R task.
 */
public interface ContextProvider {

  /**
   *
   * @return The context identifier.
   */
  Optional<String> getContextID();
}
