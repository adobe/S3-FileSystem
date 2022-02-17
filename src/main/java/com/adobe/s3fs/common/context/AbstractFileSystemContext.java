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

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;
import com.adobe.s3fs.common.runtime.FileSystemRuntime;

import org.immutables.value.Value;

/**
 * Contain information specific for a file system instance.
 * In any given process any number of file system instances may exist (one for each bucket) and each one has a specific context.
 */
@Value.Immutable
@Value.Style(strictBuilder = true, typeImmutable = "*")
public interface AbstractFileSystemContext {

  String bucket();

  FileSystemConfiguration configuration();

  FileSystemRuntime runtime();
}
