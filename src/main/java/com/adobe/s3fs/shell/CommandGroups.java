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

package com.adobe.s3fs.shell;

/**
 * This class provides a list of command group names that can be checked at compile time. Please use
 * the variables found here for referencing the group names in the Airline annotations. By doing so,
 * we minimize the chance of mistyping them and we can change them easily if we want to.
 */
public final class CommandGroups {

  public static final String FSCK = "fsck";
  public static final String TOOLS = "tools";

  private CommandGroups() {
    throw new IllegalStateException("Non instantiable class");
  }
}
