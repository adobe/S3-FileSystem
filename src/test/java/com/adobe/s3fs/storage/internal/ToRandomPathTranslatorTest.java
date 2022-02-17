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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.UUID;

@RunWith(DataProviderRunner.class)
public class ToRandomPathTranslatorTest {

  private ToRandomPathTranslator pathTranslator;

  @DataProvider
  public static Object[][] inputPaths() {
    return new Object[][] {
        new Object[] {new Path("s3://bucket/f")},
        new Object[] {new Path("s3://bucket/d/f")},
        new Object[] {new Path("s3://bucket/d/d1/f")},
        new Object[] {new Path("s3://bucket/d/d1/d2/f")},
    };
  }

  @Before
  public void setup() {
    pathTranslator = new ToRandomPathTranslator("s3t");
  }

  @Test
  @UseDataProvider("inputPaths")
  public void testRandomPathsAreGenerated(Path inputPath) {
    ObjectMetadata metadata = ObjectMetadata.builder()
            .key(inputPath)
            .isDirectory(false)
            .creationTime(100)
            .size(50)
            .physicalDataCommitted(false)
            .physicalPath("uncommitted")
            .build();
    ObjectHandle mockHandle = mock(ObjectHandle.class);
    UUID id = UUID.randomUUID();
    when(mockHandle.metadata()).thenReturn(metadata);
    when(mockHandle.id()).thenReturn(id);

    Path translated = pathTranslator.newUniquePath(mockHandle);

    URI uri = translated.toUri();

    assertEquals("s3t", uri.getScheme());
    assertEquals("bucket", uri.getHost());
    assertTrue(translated.getName().endsWith(id.toString()));
    assertTrue(validUUUID(translated.getName().substring(0, translated.getName().indexOf(".id"))));
    assertTrue(translated.getParent().isRoot());
  }

  @Test
  public void testNewPathsAreGeneratedForTheSameInputPath() {
    ObjectMetadata metadata = ObjectMetadata.builder()
            .key(new Path("s3://bucket/f"))
            .isDirectory(false)
            .creationTime(100)
            .size(50)
            .physicalDataCommitted(false)
            .physicalPath("uncommitted")
            .build();
    ObjectHandle mockHandle = mock(ObjectHandle.class);
    UUID id = UUID.randomUUID();
    when(mockHandle.metadata()).thenReturn(metadata);
    when(mockHandle.id()).thenReturn(id);

    Path translated = pathTranslator.newUniquePath(mockHandle);
    Path anotherTranslated = pathTranslator.newUniquePath(mockHandle);

    assertNotEquals(translated, anotherTranslated);
  }

  private static boolean validUUUID(String str) {
    try{
      UUID uuid = UUID.fromString(str);
      return true;
    } catch (IllegalArgumentException e){
      return false;
    }
  }
}
