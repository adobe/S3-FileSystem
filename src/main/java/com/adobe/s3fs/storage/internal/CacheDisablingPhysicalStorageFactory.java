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

import com.adobe.s3fs.storage.internal.exceptions.PhysicalStorageCreationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.Map;

/**
 * Factory for {@link FileSystemPhysicalStorage} that ensures each created instance has its own
 * {@link org.apache.hadoop.fs.FileSystem} instance.
 */
public class CacheDisablingPhysicalStorageFactory {

  private final FileSystemPhysicalStorageConfiguration storageConfiguration;
  private final Configuration hadoopConfiguration;

  public CacheDisablingPhysicalStorageFactory(FileSystemPhysicalStorageConfiguration storageConfiguration,
                                              Configuration hadoopConfiguration) {
    this.storageConfiguration = Preconditions.checkNotNull(storageConfiguration);

    this.hadoopConfiguration = applyUnderlyingFileSystemProperties(Preconditions.checkNotNull(hadoopConfiguration), this.storageConfiguration);
  }

  @VisibleForTesting
  public Configuration getHadoopConfiguration() {
    return hadoopConfiguration;
  }

  public FileSystemPhysicalStorage create(String bucket) {
    try {
      Path path = new Path(new URI(storageConfiguration.getUnderlyingFileSystemScheme(),
                                   null,
                                   bucket,
                                   -1,
                                   null,
                                   null,
                                   null));

      return new FileSystemPhysicalStorage(path.getFileSystem(hadoopConfiguration), storageConfiguration);
    } catch (Exception e) {
      throw new PhysicalStorageCreationException("Unable to create FileSystemPhysicalStorage", e);
    }
  }

  private static Configuration applyUnderlyingFileSystemProperties(Configuration initialConfig, FileSystemPhysicalStorageConfiguration storageConfiguration) {
    // clone the config so we don't break stuff
    Configuration finalConfiguration = new Configuration(initialConfig);

    // forcefully disable caching, so we can cleanly tear down our FS object tree and
    // allow the underlying filesystem scheme to be used elsewhere in the process
    finalConfiguration.setBoolean(String.format("fs.%s.impl.disable.cache", storageConfiguration.getUnderlyingFileSystemScheme()),
        true);

    // apply custom properties for our instance of the underlying fs
    for (Map.Entry<String, String> entry : storageConfiguration.getUnderlyingFileSystemProperties()) {
      finalConfiguration.set(entry.getKey(), entry.getValue());
    }

    return finalConfiguration;
  }
}
