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

package com.adobe.s3fs.filesystemcheck.mapreduce;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class ParentShuffleCacheTest {

  @Test
  public void testCacheWithOneElementAndSamePath() {
    ParentShuffleCache parentShuffleCache = new ParentShuffleCache(1);

    Path path = new Path("ks://bucket/dir1/dir2/dir3/dir4/file");
    Path parentPath = path.getParent();
    // Element not present
    Assert.assertTrue(parentShuffleCache.shuffleParent(parentPath));
    // Element present
    Assert.assertFalse(parentShuffleCache.shuffleParent(parentPath));
  }

  @Test
  public void testCacheWithOneElementAndDifferentPaths() {
    ParentShuffleCache parentShuffleCache = new ParentShuffleCache(1);

    Path path = new Path("ks://bucket/dir1/dir2/dir3/dir4/file");
    Path parentPath = path.getParent();

    while (!parentPath.isRoot()) {
      // Element not present
      Assert.assertTrue(parentShuffleCache.shuffleParent(parentPath));
      // Element present
      Assert.assertFalse(parentShuffleCache.shuffleParent(parentPath));
      parentPath = parentPath.getParent();
    }
  }

  @Test
  public void testCacheWithTwoElements() {
    ParentShuffleCache parentShuffleCache = new ParentShuffleCache(2);

    Path path1 = new Path("ks://bucket/dir1/dir2/dir3/dir4");
    Path path2 = new Path("ks://bucket/dir1/dir2/dir3");
    Path path3 = new Path("ks://bucket/dir1/dir2");
    Path path4 = new Path("ks://bucket/dir1");

    // Nothing present
    Assert.assertTrue(parentShuffleCache.shuffleParent(path1));
    Assert.assertTrue(parentShuffleCache.shuffleParent(path2));
    // Path1 is retouched and becomes the newest element
    Assert.assertFalse(parentShuffleCache.shuffleParent(path1));
    // Path3 not present; will evict path2 (oldest element)
    Assert.assertTrue(parentShuffleCache.shuffleParent(path3));
    // Path1 still present (retouched and it becomes newest element)
    Assert.assertFalse(parentShuffleCache.shuffleParent(path1));
    // Path4 not present; will evict path3 (oldest element)
    Assert.assertTrue(parentShuffleCache.shuffleParent(path4));

    Assert.assertFalse(parentShuffleCache.hasParent(path2));
    Assert.assertFalse(parentShuffleCache.hasParent(path3));
  }
}
