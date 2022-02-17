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

package com.adobe.s3fs.metastore.internal.dynamodb.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SynchronousDFSTreeTraversalTest {

  private SynchronousDFSTreeTraversal treeTraversal = new SynchronousDFSTreeTraversal();

  @Mock private Function<VersionedObject, Iterable<VersionedObject>> mockChildExpansion;

  @Mock private ObjectVisitor mockObjectVisitor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockObjectVisitor.childVisitor(any(), any())).thenReturn(mockObjectVisitor);
  }

  @Test
  public void testVisitorIsCalledInCorrectOrderWhenNoErrors() {
    VersionedObject root = newDirectory();
    VersionedObject d1 = newDirectory();
    VersionedObject f1 = newFile();
    VersionedObject f2 = newFile();
    VersionedObject f3 = newFile();
    when(mockChildExpansion.apply(root)).thenReturn(Arrays.asList(d1, f1));
    when(mockChildExpansion.apply(d1)).thenReturn(Arrays.asList(f2, f3));
    List<ObjectWithVisit> capturedVisits = new ArrayList<>();
    new MockVisitorStubber().stub(capturedVisits);

    assertTrue(treeTraversal.traverse(root, mockChildExpansion, mockObjectVisitor));

    List<ObjectWithVisit> expected =
        Lists.newArrayList(
            new ObjectWithVisit(root, Visit.PreVisitDir),
            new ObjectWithVisit(d1, Visit.PreVisitDir),
            new ObjectWithVisit(f2, Visit.VisitFile),
            new ObjectWithVisit(f3, Visit.VisitFile),
            new ObjectWithVisit(d1, Visit.PostVisitDir),
            new ObjectWithVisit(f1, Visit.VisitFile),
            new ObjectWithVisit(root, Visit.PostVisitDir));
    assertEquals(expected, capturedVisits);
  }

  @Test
  public void testVisitorIsCalledInCorrectOrderWhenPreVisitDirectoryReturnsFalse() {
    VersionedObject root = newDirectory();
    VersionedObject d1 = newDirectory();
    VersionedObject f1 = newFile();
    VersionedObject f2 = newFile();
    VersionedObject f3 = newFile();
    when(mockChildExpansion.apply(root)).thenReturn(Arrays.asList(d1, f1));
    when(mockChildExpansion.apply(d1)).thenReturn(Arrays.asList(f2, f3));
    List<ObjectWithVisit> capturedVisits = new ArrayList<>();
    new MockVisitorStubber().withVisitToReturnFalse(d1, Visit.PreVisitDir).stub(capturedVisits);

    assertFalse(treeTraversal.traverse(root, mockChildExpansion, mockObjectVisitor));

    List<ObjectWithVisit> expected =
        Lists.newArrayList(new ObjectWithVisit(root, Visit.PreVisitDir));
    assertEquals(expected, capturedVisits);
  }

  @Test
  public void testVisitorIsCalledInCorrectOrderWhenPreVisitDirectoryThrowsError() {
    VersionedObject root = newDirectory();
    VersionedObject d1 = newDirectory();
    VersionedObject f1 = newFile();
    VersionedObject f2 = newFile();
    VersionedObject f3 = newFile();
    when(mockChildExpansion.apply(root)).thenReturn(Arrays.asList(d1, f1));
    when(mockChildExpansion.apply(d1)).thenReturn(Arrays.asList(f2, f3));
    List<ObjectWithVisit> capturedVisits = new ArrayList<>();
    new MockVisitorStubber().withVisitToThrowError(d1, Visit.PreVisitDir).stub(capturedVisits);

    assertFalse(treeTraversal.traverse(root, mockChildExpansion, mockObjectVisitor));

    List<ObjectWithVisit> expected =
        Lists.newArrayList(new ObjectWithVisit(root, Visit.PreVisitDir));
    assertEquals(expected, capturedVisits);
  }

  @Test
  public void testVisitorIsCalledInCorrectOrderWhenVisitFileReturnsFalse() {
    VersionedObject root = newDirectory();
    VersionedObject d1 = newDirectory();
    VersionedObject f1 = newFile();
    VersionedObject f2 = newFile();
    VersionedObject f3 = newFile();
    when(mockChildExpansion.apply(root)).thenReturn(Arrays.asList(f1, d1));
    when(mockChildExpansion.apply(d1)).thenReturn(Arrays.asList(f2, f3));
    List<ObjectWithVisit> capturedVisits = new ArrayList<>();
    new MockVisitorStubber().withVisitToReturnFalse(f1, Visit.VisitFile).stub(capturedVisits);

    assertFalse(treeTraversal.traverse(root, mockChildExpansion, mockObjectVisitor));

    List<ObjectWithVisit> expected =
        Lists.newArrayList(new ObjectWithVisit(root, Visit.PreVisitDir));
    assertEquals(expected, capturedVisits);
  }

  @Test
  public void testVisitorIsCalledInCorrectOrderWhenVisitFileThrowsError() {
    VersionedObject root = newDirectory();
    VersionedObject d1 = newDirectory();
    VersionedObject f1 = newFile();
    VersionedObject f2 = newFile();
    VersionedObject f3 = newFile();
    when(mockChildExpansion.apply(root)).thenReturn(Arrays.asList(f1, d1));
    when(mockChildExpansion.apply(d1)).thenReturn(Arrays.asList(f2, f3));
    List<ObjectWithVisit> capturedVisits = new ArrayList<>();
    new MockVisitorStubber().withVisitToThrowError(f1, Visit.VisitFile).stub(capturedVisits);

    assertFalse(treeTraversal.traverse(root, mockChildExpansion, mockObjectVisitor));

    List<ObjectWithVisit> expected =
        Lists.newArrayList(new ObjectWithVisit(root, Visit.PreVisitDir));
    assertEquals(expected, capturedVisits);
  }

  @Test
  public void testVisitorIsCalledInCorrectOrderWhenPostVisitDirectoryReturnsFalse() {
    VersionedObject root = newDirectory();
    VersionedObject d1 = newDirectory();
    VersionedObject f1 = newFile();
    VersionedObject f2 = newFile();
    VersionedObject f3 = newFile();
    when(mockChildExpansion.apply(root)).thenReturn(Arrays.asList(d1, f1));
    when(mockChildExpansion.apply(d1)).thenReturn(Arrays.asList(f2, f3));
    List<ObjectWithVisit> capturedVisits = new ArrayList<>();
    new MockVisitorStubber().withVisitToReturnFalse(d1, Visit.PostVisitDir).stub(capturedVisits);

    assertFalse(treeTraversal.traverse(root, mockChildExpansion, mockObjectVisitor));

    List<ObjectWithVisit> expected =
        Lists.newArrayList(
            new ObjectWithVisit(root, Visit.PreVisitDir),
            new ObjectWithVisit(d1, Visit.PreVisitDir),
            new ObjectWithVisit(f2, Visit.VisitFile),
            new ObjectWithVisit(f3, Visit.VisitFile));
    assertEquals(expected, capturedVisits);
  }

  @Test
  public void testVisitorIsCalledInCorrectOrderWhenPostVisitDirectoryThrowsError() {
    VersionedObject root = newDirectory();
    VersionedObject d1 = newDirectory();
    VersionedObject f1 = newFile();
    VersionedObject f2 = newFile();
    VersionedObject f3 = newFile();
    when(mockChildExpansion.apply(root)).thenReturn(Arrays.asList(d1, f1));
    when(mockChildExpansion.apply(d1)).thenReturn(Arrays.asList(f2, f3));
    List<ObjectWithVisit> capturedVisits = new ArrayList<>();
    new MockVisitorStubber().withVisitToThrowError(d1, Visit.PostVisitDir).stub(capturedVisits);

    assertFalse(treeTraversal.traverse(root, mockChildExpansion, mockObjectVisitor));

    List<ObjectWithVisit> expected =
        Lists.newArrayList(
            new ObjectWithVisit(root, Visit.PreVisitDir),
            new ObjectWithVisit(d1, Visit.PreVisitDir),
            new ObjectWithVisit(f2, Visit.VisitFile),
            new ObjectWithVisit(f3, Visit.VisitFile));
    assertEquals(expected, capturedVisits);
  }

  private VersionedObject newDirectory() {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(new Path("ks://bucket/" + UUID.randomUUID().toString()))
        .isDirectory(true)
        .size(0)
        .creationTime(Instant.now().getEpochSecond())
        .physicalPath(Optional.empty())
        .build();
    return VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
  }

  private VersionedObject newFile() {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(new Path("ks://bucket/" + UUID.randomUUID().toString()))
        .isDirectory(false)
        .size(100)
        .creationTime(Instant.now().getEpochSecond())
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    return VersionedObject.builder()
        .metadata(metadata)
        .version(1)
        .id(UUID.randomUUID())
        .build();
  }

  private class MockVisitorStubber {
    private Visit visitToReturnFalse;
    private Visit visitToThrowError;
    private VersionedObject failedObject;

    MockVisitorStubber withVisitToReturnFalse(VersionedObject object, Visit visit) {
      this.visitToReturnFalse = visit;
      this.failedObject = object;
      return this;
    }

    MockVisitorStubber withVisitToThrowError(VersionedObject object, Visit visit) {
      this.visitToThrowError = visit;
      this.failedObject = object;
      return this;
    }

    void stub(List<ObjectWithVisit> captureList) {
      when(mockObjectVisitor.preVisitDirectoryObject(any()))
          .thenAnswer(
              it -> {
                VersionedObject object = (VersionedObject) it.getArguments()[0];
                if (Visit.PreVisitDir == visitToReturnFalse && object.equals(failedObject)) {
                  return false;
                }
                if (Visit.PreVisitDir == visitToThrowError && object.equals(failedObject)) {
                  throw new RuntimeException();
                }
                captureList.add(new ObjectWithVisit(object, Visit.PreVisitDir));
                return true;
              });

      when(mockObjectVisitor.postVisitDirectoryObject(any()))
          .thenAnswer(
              it -> {
                VersionedObject object = (VersionedObject) it.getArguments()[0];
                if (Visit.PostVisitDir == visitToReturnFalse && object.equals(failedObject)) {
                  return false;
                }
                if (Visit.PostVisitDir == visitToThrowError && object.equals(failedObject)) {
                  throw new RuntimeException();
                }
                captureList.add(new ObjectWithVisit(object, Visit.PostVisitDir));
                return true;
              });

      when(mockObjectVisitor.visitFileObject(any()))
          .thenAnswer(
              it -> {
                VersionedObject object = (VersionedObject) it.getArguments()[0];
                if (Visit.VisitFile == visitToReturnFalse && object.equals(failedObject)) {
                  return false;
                }
                if (Visit.VisitFile == visitToThrowError && object.equals(failedObject)) {
                  throw new RuntimeException();
                }
                captureList.add(new ObjectWithVisit(object, Visit.VisitFile));
                return true;
              });
    }
  }

  private enum Visit {
    PreVisitDir,
    VisitFile,
    PostVisitDir
  }

  private static class ObjectWithVisit {
    final VersionedObject object;
    final Visit visit;

    public ObjectWithVisit(VersionedObject object, Visit visit) {
      this.object = object;
      this.visit = visit;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ObjectWithVisit that = (ObjectWithVisit) o;

      if (!object.equals(that.object)) {
        return false;
      }
      return visit == that.visit;
    }

    @Override
    public int hashCode() {
      int result = object.hashCode();
      result = 31 * result + visit.hashCode();
      return result;
    }
  }
}
