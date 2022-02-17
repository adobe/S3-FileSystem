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

package com.adobe.s3fs.common.configuration;

import com.adobe.s3fs.common.context.ContextProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class FileSystemConfigurationTest {

  @Mock
  private KeyValueConfiguration mockKeyValueConfig;

  private static final String BUCKET = "bucket";

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPropertyIsReturnedWithContextPresent() {
    ContextProvider contextProvider = () -> Optional.of("ctx");
    when(mockKeyValueConfig.getInt("prop.bucket.ctx")).thenReturn(2);
    FileSystemConfiguration fileSystemConfiguration = new FileSystemConfiguration(BUCKET, mockKeyValueConfig, contextProvider);

    assertEquals(2, fileSystemConfiguration.contextAware().getInt("prop"));
  }

  @Test
  public void testPropertyIsReturnedWithContextNotPresent() {
    ContextProvider contextProvider = () -> Optional.empty();
    when(mockKeyValueConfig.getInt("prop.bucket")).thenReturn(2);
    FileSystemConfiguration fileSystemConfiguration = new FileSystemConfiguration(BUCKET, mockKeyValueConfig, contextProvider);

    assertEquals(2, fileSystemConfiguration.contextAware().getInt("prop"));
  }
}
