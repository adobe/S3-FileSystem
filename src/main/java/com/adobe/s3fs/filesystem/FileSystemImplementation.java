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

package com.adobe.s3fs.filesystem;

import com.adobe.s3fs.common.exceptions.PathTranslationException;
import com.adobe.s3fs.common.exceptions.UncommittedFileException;
import com.adobe.s3fs.metastore.api.MetadataStore;
import com.adobe.s3fs.metastore.api.MetadataStoreExtended;
import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.storage.api.PathTranslator;
import com.adobe.s3fs.storage.api.PhysicalStorage;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.io.CountingOutputStream;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileSystemImplementation implements Closeable {

  public static final FsPermission FILE_PERMISSIONS = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE, false);
  public static final FsPermission DIRECTORY_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, false);

  public static final String METADATA_PATH_SCHEME = "ks";

  public static final String UNCOMMITED_PATH_MARKER = "<uncommitted>";

  private final MetadataStore metadataStore;
  private final PhysicalStorage physicalStorage;
  private final PathTranslator pathTranslator;
  private final UserGroupInformation userGroupInformation;
  private final FileSystemMetrics fsMetrics;

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemImplementation.class);

  public FileSystemImplementation(MetadataStore metadataStore,
                                  PhysicalStorage physicalStorage,
                                  PathTranslator pathTranslator,
                                  FileSystemMetrics fsMetrics) {
    this.metadataStore = Preconditions.checkNotNull(metadataStore);
    this.physicalStorage = Preconditions.checkNotNull(physicalStorage);
    this.pathTranslator = Preconditions.checkNotNull(pathTranslator);
    this.userGroupInformation = currentUser();
    this.fsMetrics = Preconditions.checkNotNull(fsMetrics);
  }

  public FSDataInputStream open(Path path) throws IOException {
    ObjectHandle objectHandle = checkObjectPresent(path);

    if (objectHandle.metadata().isDirectory()) {
      throw new FileNotFoundException("Can't open a directory:" + path);
    }

    if (!objectHandle.metadata().physicalDataCommitted()) {
      throw new UncommittedFileException(path);
    }

    String physicalPath = objectHandle.metadata().getPhysicalPath()
        .orElseThrow(IllegalStateException::new);
    return new FSDataInputStream(physicalStorage.openKey(new Path(physicalPath)));
  }

  /**
   * Creates the file and assumes parent exists (error will be thrown if parent does not exist).
   * @param path
   * @param flags
   * @return
   * @throws IOException
   */
  public FSDataOutputStream createNonRecursive(Path path, EnumSet<CreateFlag> flags) throws IOException {
    throwIfOperationIsOnRoot(path, "create");
    if (!exists(path.getParent())) {
      throw new FileNotFoundException("Parent " + path.getParent() + " does not exist");
    }

    boolean overwrite = flags.equals(EnumSet.of(CreateFlag.OVERWRITE));
    boolean createOrOverwrite = flags.equals(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
    if (overwrite && !exists(path)) {
      throw new FileNotFoundException(path.toString());
    }

    return createInternal(path, overwrite || createOrOverwrite);
  }

  /**
   * Creates the file as well as parent if it does not exist.
   * @param path
   * @param overwrite
   * @return
   * @throws IOException
   */
  public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
    throwIfOperationIsOnRoot(path, "create");
    if (!mkdirs(path.getParent())) {
      throw new IOException("Failed to create parent for " + path);
    }
    return createInternal(path, overwrite);
  }

  /**
   * Creates the file assuming parent already exists.
   * @param path
   * @param overwrite
   * @return
   * @throws IOException
   */
  private FSDataOutputStream createInternal(Path path, boolean overwrite) throws IOException {
    Path metaPath = translateToMetastorePath(path);
    Optional<? extends ObjectHandle> existingObjectOptional = metadataStore.getObject(metaPath);

    if (!overwrite && existingObjectOptional.isPresent()) {
      throw new FileAlreadyExistsException("File " + path + " already exists");
    }

    boolean destinationExistsAndIsDirectory = existingObjectOptional.map(it -> it.metadata().isDirectory())
        .orElse(Boolean.FALSE);
    if (destinationExistsAndIsDirectory) {
      throw new FileAlreadyExistsException(String.format("Destination %s already exists and is a directory", path));
    }

    ObjectMetadata initialMetadata = ObjectMetadata.builder()
        .key(metaPath)
        .isDirectory(false)
        .creationTime(Instant.now().getEpochSecond())
        .size(0)
        .physicalPath(UNCOMMITED_PATH_MARKER)
        .build();

    ObjectHandle handle = existingObjectOptional.isPresent() ?
        existingObjectOptional.get() : metadataStore.createObject(initialMetadata);

    Path storagePath = pathTranslator.newUniquePath(handle);

    OutputStream storageStream = physicalStorage.createKey(storagePath);

    CountingOutputStream countingStream = new CountingOutputStream(storageStream);

    return new FSDataOutputStream(
        countingStream, new FileSystem.Statistics(path.toUri().getScheme())) {
      private final AtomicBoolean close = new AtomicBoolean(false);

      private final Object lock = new Object();

      @Override
      public void close() throws IOException {
        synchronized (lock) {
          if (close.get()) {
            return;
          }
          // commit the data to physical storage first
          // if this fails, the client will not see the file at all
          // if it succeeds but persisting the metadata fails
          // we just have some junk left over in the storage bucket that fsck will cleanup
          super.close();
          ObjectMetadata finalMetadata = initialMetadata
                  .withPhysicalPath(storagePath.toString())
                  .withSize(countingStream.getCount())
                  .withPhysicalDataCommitted(true);
          metadataStore.updateObject(handle, finalMetadata);
          if (overwrite) {
            boolean previousPhysicalFileDeleted =
                !existingObjectOptional.isPresent() || deleteOverwrittenMetadata(existingObjectOptional.get().metadata());
            if (previousPhysicalFileDeleted) {
              fsMetrics.recordSuccessfulOverwrite();
            } else {
              fsMetrics.recordFailedOverwrittenFilePhysicalDelete();
            }
          } else {
            fsMetrics.recordSuccessfulCreate();
          }
          close.set(true);
        }
      }
    };
  }

  public boolean mkdirs(Path path) throws IOException {
    return createDirectoryChain(path, Instant.now().getEpochSecond());
  }

  public DeleteOutcome delete(Path path, boolean recursive) throws IOException {
    if (path.isRoot()) {
      return DeleteOutcome.Failed;
    }

    Path metaPath = translateToMetastorePath(path);

    Optional<? extends ObjectHandle> objectOptional = metadataStore.getObject(metaPath);

    if (!objectOptional.isPresent()) {
      LOG.warn("Received call to delete {}, but object is not present in metastore", path);
      return DeleteOutcome.ObjectAlreadyDeleted; // file/directory does not exist
    }

    ObjectHandle objectHandle = objectOptional.get();

    if (objectHandle.metadata().isDirectory() && !recursive) {
      boolean emptyDirectory = !hasChildObjects(objectHandle);
      if (!emptyDirectory) {
        throw new IOException("Cannot delete directory " + path + " w/o recursive flag");
      }
    }

    boolean deletedSuccessfully = metadataStore.deleteObject(
        objectHandle, new PhysicalFileDeleteCallback(physicalStorage, fsMetrics));
    return deletedSuccessfully ? DeleteOutcome.Successful : DeleteOutcome.Failed;
  }

  public boolean rename(Path src, Path dst) throws IOException {
    if (src.equals(dst)) {
      return true;
    }
    throwIfOperationIsOnRoot(src, "rename");
    throwIfDestinationIsADescendantOfSource(src, dst);

    ObjectHandle srcObject = checkObjectPresent(src);

    if (!dst.isRoot()) { // for non-root dst enforce parent exists and is not a file
      ObjectHandle dstParentObject = checkObjectPresent(dst.getParent());
      if (!dstParentObject.metadata().isDirectory()) {
        throw new IOException("Parent of " + dst + " is a file");
      }
    }

    Path dstMeta = translateToMetastorePath(dst);
    Optional<? extends ObjectHandle> dstObjectOptional = metadataStore.getObject(dstMeta);

    if (dstObjectOptional.isPresent()) {
      ObjectHandle dstObject = dstObjectOptional.get();
      // cannot rename a file or directory onto an already existing file
      if (!dstObject.metadata().isDirectory()) {
        return false;
      }
      // allowing renaming of files/directories onto directories
      // as long as the directory does not have any children with the same name as the source
      dstMeta = new Path(dstMeta, src.getName());
      dstObjectOptional = metadataStore.getObject(dstMeta);
      if (dstObjectOptional.isPresent()) {
        return false;
      }
    }

    return metadataStore.renameObject(srcObject, dstMeta);
  }

  public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException();
  }

  public FileStatus getFileStatus(Path path) throws IOException {
    return objectToFileStatus(checkObjectPresent(path), path);
  }

  public boolean exists(Path path) throws IOException {
    return metadataStore.getObject(translateToMetastorePath(path)).isPresent();
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    ObjectHandle objectHandle = checkObjectPresent(path);

    if (!objectHandle.metadata().isDirectory()) {
      return new FileStatus[] {objectToFileStatus(objectHandle, path)};
    }

    String scheme = path.toUri().getScheme();

    List<FileStatus> result = FluentIterable.from(metadataStore.listChildObjects(objectHandle))
        .transform(it -> objectToFileStatus(it, translateFromMetastorePath(it.metadata().getKey(), scheme)))
        .toList();

    return result.toArray(new FileStatus[result.size()]);
  }

  public boolean supportsSpecificContentSummaryComputation() {
    return metadataStore instanceof MetadataStoreExtended;
  }

  public ContentSummary getContentSummary(Path path) throws IOException {
    if (!supportsSpecificContentSummaryComputation()) {
      throw new UnsupportedOperationException();
    }

    ObjectHandle objectHandle = checkObjectPresent(path);
    if (!objectHandle.metadata().isDirectory()) {
      ObjectMetadata metadata = objectHandle.metadata();
      return new ContentSummary.Builder()
          .length(metadata.getSize())
          .fileCount(1)
          .directoryCount(0)
          .spaceConsumed(metadata.getSize())
          .build();
    }

    return ((MetadataStoreExtended) metadataStore).computeContentSummary(objectHandle);
  }

  @Override
  public void close() throws IOException {
    metadataStore.close();
    physicalStorage.close();
  }

  private void throwIfOperationIsOnRoot(Path path, String operation) throws IOException {
    if (path.isRoot()) {
      throw new IOException(String.format("Cannot do %s on root", operation));
    }
  }

  private void throwIfDestinationIsADescendantOfSource(Path src, Path dst) throws IOException {
    String srcString = src.toUri().toString();
    String dstString = dst.toUri().toString();
    if (dstString.startsWith(srcString)) {
      throw new IOException("Cannot rename a file/directory as a child of itself");
    }
  }

  private FileStatus objectToFileStatus(ObjectHandle objectHandle, Path path) {
    return new FileStatus(objectHandle.metadata().getSize(),
                          objectHandle.metadata().isDirectory(),
                          0,
                          0,
                          objectHandle.metadata().getCreationTime() * 1000,
                          0,
                          objectHandle.metadata().isDirectory() ? DIRECTORY_PERMISSIONS : FILE_PERMISSIONS,
                          userGroupInformation.getShortUserName(),
                          userGroupInformation.getShortUserName(),
                          path);
  }

  private boolean createDirectoryChain(Path path, long now) throws IOException {
    int missingTailPartCount = 0;
    List<Path> reversedPathParts = reversedPathParts(path);

    for (Path part : reversedPathParts) {
      Optional<? extends ObjectHandle> objectMetadataOptional = metadataStore.getObject(translateToMetastorePath(part));

      if (objectMetadataOptional.isPresent()) {
        if (!objectMetadataOptional.get().metadata().isDirectory()) {
          throw new FileAlreadyExistsException(part.toString());
        }
        break;
      } else {
        missingTailPartCount++;
      }
    }

    if (missingTailPartCount == 0) {
      return true;
    }

    List<Path> inOrderPathParts = Lists.reverse(reversedPathParts);
    for (Path tailPathToCreate : inOrderPathParts.subList(inOrderPathParts.size() - missingTailPartCount, inOrderPathParts.size())) {
      ObjectMetadata metadataToCreate = ObjectMetadata.builder()
          .key(translateToMetastorePath(tailPathToCreate))
          .isDirectory(true)
          .creationTime(now)
          .size(0)
          .physicalPath(Optional.empty())
          .build();
      metadataStore.createObject(metadataToCreate);
    }

    return true;
  }

  private boolean hasChildObjects(ObjectHandle objectHandle) {
    Iterable<? extends ObjectHandle> children = metadataStore.listChildObjects(objectHandle);
    return children != null && children.iterator().hasNext();
  }

  private List<Path> reversedPathParts(Path path) {
    List<Path> parts = new LinkedList<>();
    Path current = path;
    while (!current.isRoot()) {
      parts.add(current);
      current = current.getParent();
    }
    return parts;
  }

  private boolean deleteOverwrittenMetadata(ObjectMetadata metadata) {
    try {
      if (!metadata.physicalDataCommitted()) {
        return true;
      }
      String physicalPath = metadata.getPhysicalPath().get();
      physicalStorage.deleteKey(new Path(physicalPath));
      return true;
    } catch (Exception e) {
      LOG.warn("Failed to delete overwritten key", e);
      return false;
    }
  }

  private Path translateFromMetastorePath(Path path, String scheme) {
    URI originalUri = path.toUri();

    try {
      return new Path(new URI(scheme,
                              originalUri.getUserInfo(),
                              originalUri.getHost(),
                              originalUri.getPort(),
                              originalUri.getPath(),
                              originalUri.getQuery(),
                              originalUri.getFragment()));
    } catch (URISyntaxException e) {
      throw new PathTranslationException(path, e);
    }
  }

  private Path translateToMetastorePath(Path path) {
    URI originalUri = path.toUri();

    try {
      return new Path(new URI(METADATA_PATH_SCHEME,
                              originalUri.getUserInfo(),
                              originalUri.getHost(),
                              originalUri.getPort(),
                              originalUri.getPath(),
                              originalUri.getQuery(),
                              originalUri.getFragment()));
    } catch (URISyntaxException e) {
      throw new PathTranslationException(path, e);
    }
  }

  private ObjectHandle checkObjectPresent(Path path) throws IOException {
    Optional<? extends ObjectHandle> optionalObjectMetadata = metadataStore.getObject(translateToMetastorePath(path));

    return optionalObjectMetadata
        .orElseThrow(() -> new FileNotFoundException(path.toString()));
  }

  private static UserGroupInformation currentUser() {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException("Could not get current user", e);
    }
  }
}
