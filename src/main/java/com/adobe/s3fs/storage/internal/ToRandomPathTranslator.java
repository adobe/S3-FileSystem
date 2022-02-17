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

package com.adobe.s3fs.storage.internal;

import com.adobe.s3fs.common.exceptions.PathTranslationException;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.storage.api.PathTranslator;
import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

/**
 * Translates a Path to random {@link UUID} stored directly in the root bucket of the input path.
 */
public class ToRandomPathTranslator implements PathTranslator {

  private final String targetScheme;

  public ToRandomPathTranslator(String targetScheme) {
    this.targetScheme = Preconditions.checkNotNull(targetScheme);
  }

  @Override
  public Path newUniquePath(ObjectHandle objectHandle) {
    Path path = objectHandle.metadata().getKey();
    URI uri = path.toUri();

    try {
      return new Path(new URI(targetScheme,
              uri.getUserInfo(),
              uri.getHost(),
              uri.getPort(),
              "/" + UUID.randomUUID().toString() + ".id=" + objectHandle.id(),
              null,
              null));
    } catch (URISyntaxException e) {
      throw new PathTranslationException(path, e);
    }
  }
}
