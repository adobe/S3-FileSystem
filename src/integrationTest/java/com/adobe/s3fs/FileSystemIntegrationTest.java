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

package com.adobe.s3fs;

import static com.adobe.s3fs.utils.FileSystemStateChecker.checkFileSystemState;
import static com.adobe.s3fs.utils.FileSystemStateChecker.expectedDirectory;
import static com.adobe.s3fs.utils.FileSystemStateChecker.expectedFile;
import static com.adobe.s3fs.utils.OperationLogStateChecker.checkOperationLogState;
import static com.adobe.s3fs.utils.stream.StreamUtils.uncheckedRunnable;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.adobe.s3fs.common.runtime.FileSystemRuntimeFactory;
import com.adobe.s3fs.filesystem.HadoopFileSystemAdapter;
import com.adobe.s3fs.utils.DynamoTable;
import com.adobe.s3fs.utils.ExpectedFSObject;
import com.adobe.s3fs.utils.ITUtils;
import com.adobe.s3fs.utils.S3Bucket;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileSystemIntegrationTest {

  static {
    LogManager.getRootLogger().setLevel(Level.INFO);
  }

  @ClassRule
  public static Network network = Network.newNetwork();

  @ClassRule
  public static LocalStackContainer localStackContainer = new LocalStackContainer()
      .withNetwork(network)
      .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3)
      .withNetworkAliases("localstack");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public DynamoTable dynamoTable1 = new DynamoTable(ITUtils.amazonDynamoDB(localStackContainer));

  @Rule
  public S3Bucket bucket1 = new S3Bucket(ITUtils.amazonS3(localStackContainer));

  @Rule
  public S3Bucket operationLogBucket = new S3Bucket(ITUtils.amazonS3(localStackContainer));

  private Configuration configuration;

  private FileSystem fileSystem;

  private AmazonS3 s3;

  @Before
  public void setup() throws IOException {
    s3 = ITUtils.amazonS3(localStackContainer);

    configuration = new Configuration(false);
    configuration.setClass("fs.s3.impl", HadoopFileSystemAdapter.class, FileSystem.class);
    configuration.setBoolean("fs.s3.impl.disable.cache", true);
    ITUtils.configureAsyncOperations(configuration, bucket1.getBucket(), "ctx");

    ITUtils.configureDynamoAccess(localStackContainer, configuration, bucket1.getBucket());
    ITUtils.mapBucketToTable(configuration, bucket1.getBucket(), dynamoTable1.getTable());

    ITUtils.configureS3OperationLog(configuration, bucket1.getBucket(), operationLogBucket.getBucket());
    ITUtils.configureS3OperationLogAccess(localStackContainer, configuration, bucket1.getBucket());

    ITUtils.configureS3AAsUnderlyingFileSystem(localStackContainer, configuration, bucket1.getBucket(), temporaryFolder.getRoot().toString());

    ITUtils.configureSuffixCount(configuration, bucket1.getBucket(), 10);

    ITUtils.setFileSystemContext("ctx");

    fileSystem = pathInBucket(bucket1.getBucket(), "").getFileSystem(configuration);
  }

  @Test(expected = FileNotFoundException.class)
  public void testGetFileStatusThrowsErrorForInexistentFile() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");

    FileStatus ignored = fileSystem.getFileStatus(path);
  }

  @Test
  public void testGetFileStatusReturnsPreviouslyCreatedFile() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");
    byte[] data = randomBytes(100);
    createFile(path, data);

    FileStatus fileStatus = fileSystem.getFileStatus(path);

    assertFalse(fileStatus.isDirectory());
    assertEquals(path, fileStatus.getPath());
    assertEquals(100, fileStatus.getLen());
    checkOperationLogState(s3, operationLogBucket.getBucket(), expectedFile(path, 100, 2));
  }

  @Test
  public void testGetFileStatusForDirectory() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d");

    assertTrue(fileSystem.mkdirs(path));

    FileStatus fileStatus = fileSystem.getFileStatus(path);
    assertTrue(fileStatus.isDirectory());
    assertEquals(path, fileStatus.getPath());
  }

  @Test
  public void testListStatus() throws IOException {
    Path folderPath = pathInBucket(bucket1.getBucket(), "root/folder");
    Path filePath = pathInBucket(bucket1.getBucket(), "root/file");
    assertTrue(fileSystem.mkdirs(folderPath));
    try (FSDataOutputStream stream = fileSystem.create(filePath)) {
      IOUtils.write(randomBytes(100), stream);
    }

    ExpectedFSObject expectedRoot = expectedDirectory(pathInBucket(bucket1.getBucket(), ""))
        .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "root"))
                         .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "root/folder")))
                         .addChildren(expectedFile(pathInBucket(bucket1.getBucket(), "root/file"), 100, 2)));
    checkFileSystemState(fileSystem, expectedRoot);
    checkOperationLogState(s3, operationLogBucket.getBucket(), expectedFile(pathInBucket(bucket1.getBucket(), "root/file"), 100, 2));
  }

  @Test(expected = FileNotFoundException.class)
  public void testListStatusThrowsErrorForInexistentDirectory() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d");

    FileStatus[] ignored = fileSystem.listStatus(path);
  }

  @Test
  public void testListStatusOnFileReturnsTheFile() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");
    try (FSDataOutputStream stream = fileSystem.create(path)) {
      IOUtils.write(randomBytes(100), stream);
    }

    List<FileStatus> listing = Lists.newArrayList(fileSystem.listStatus(path));
    assertEquals(1, listing.size());
    FileStatus fileStatus = listing.get(0);
    assertFalse(fileStatus.isDirectory());
    assertEquals(path, fileStatus.getPath());
    assertEquals(100, fileStatus.getLen());
  }

  @Test(expected = FileNotFoundException.class)
  public void testOpenFileThrowsErrorFileInexistentFile() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");

    FSDataInputStream ignored = fileSystem.open(path);
  }

  @Test(expected = FileNotFoundException.class)
  public void testOpenFileThrowsErrorForDirectoryObject() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d");
    fileSystem.mkdirs(path);

    FSDataInputStream ignored = fileSystem.open(path);
  }

  @Test(expected = IOException.class)
  public void testCreateFileThrowsErrorIfFileExists() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");
    createFile(path);

    try (FSDataOutputStream stream = fileSystem.create(path, false)) {
      fail("Error should be thrown");
    }
  }

  @Test(expected = IOException.class)
  public void testCreateFileWithOverwriteThrowsErrorIfDestinationIsExistingDirectory() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");
    fileSystem.mkdirs(path);

    try (FSDataOutputStream stream = fileSystem.create(path, true)) {
      fail("Error should be thrown");
    }
  }

  @Test
  public void testCreateFileCreatesDirectoryChain() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d/f");
    byte[] data = randomBytes(100);
    createFile(path, data);

    ExpectedFSObject expectedRoot = expectedDirectory(pathInBucket(bucket1.getBucket(), ""))
        .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "d"))
                         .addChildren(expectedFile(pathInBucket(bucket1.getBucket(), "d/f"), 100, 2)));

    checkFileSystemState(fileSystem, expectedRoot);
    checkOperationLogState(s3, operationLogBucket.getBucket(), expectedFile(pathInBucket(bucket1.getBucket(), "d/f"), 100, 2));
  }

  @Test
  public void testPreviouslyCreatedFileCanBeRead() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");
    byte[] data = randomBytes(100);

    createFile(path, data);

    assertDataInFile(path, data);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAppendThrowsError() throws IOException {
    FSDataOutputStream ignored = fileSystem.append(pathInBucket(bucket1.getBucket(), "f"));
  }

  @Test
  public void testMkdirCreatesDirectoryChain() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d/d1/d2");

    assertTrue(fileSystem.mkdirs(path));

    ExpectedFSObject expectedRoot = expectedDirectory(pathInBucket(bucket1.getBucket(), ""))
        .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "d"))
                         .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "d/d1"))
                                          .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "d/d1/d2")))));

    checkFileSystemState(fileSystem, expectedRoot);
    checkOperationLogState(s3, operationLogBucket.getBucket());
  }

  @Test
  public void testMkdirReturnsTrueForExistingDirectories() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d");
    assertTrue(fileSystem.mkdirs(path));

    assertTrue(fileSystem.mkdirs(path));
  }

  @Test
  public void testDeleteOnInexistentFileReturnsFalse() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");

    assertFalse(fileSystem.delete(path, true));
  }

  @Test(expected = IOException.class)
  public void testDeleteNonEmptyDirectoryWithoutRecursiveThrowsError() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d");
    Path file = pathInBucket(bucket1.getBucket(), "d/f");
    createFile(file, randomBytes(10));

    assertTrue(fileSystem.mkdirs(path));

    boolean ignored = fileSystem.delete(path, false);
  }

  @Test
  public void testDeleteEmptyDirectoryWithoutRecursiveSucceeds() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "d");

    assertTrue(fileSystem.mkdirs(path));

    assertTrue(fileSystem.delete(path, false));

    assertPathsDontExist(path);
    checkFileSystemState(fileSystem, expectedDirectory(pathInBucket(bucket1.getBucket(), "")));
    checkOperationLogState(s3, operationLogBucket.getBucket());
  }

  @Test
  public void testDeleteFileIsSuccessful() throws IOException {
    Path path = pathInBucket(bucket1.getBucket(), "f");
    createFile(path);

    assertTrue(fileSystem.delete(path, false));

    assertPathsDontExist(path);
    checkFileSystemState(fileSystem, expectedDirectory(pathInBucket(bucket1.getBucket(), "")));
    checkOperationLogState(s3, operationLogBucket.getBucket());
  }

  @Test
  public void testDeleteDirectoryIsSuccessful() throws IOException {
    /*
      Prepare
     */
    Path root = pathInBucket(bucket1.getBucket(), "root/d");
    Path fileChild = pathInBucket(bucket1.getBucket(), "root/d/file");
    Path directoryChild = pathInBucket(bucket1.getBucket(), "root/d/dir");
    assertTrue(fileSystem.mkdirs(directoryChild));
    createFile(fileChild);

    /*
      Act
     */
    assertTrue(fileSystem.delete(root, true));

    /*
      Assert
     */
    assertPathsDontExist(root, fileChild, directoryChild);
    ExpectedFSObject expectedRoot = expectedDirectory(pathInBucket(bucket1.getBucket(), ""))
        .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "root")));
    checkFileSystemState(fileSystem, expectedRoot);
    checkOperationLogState(s3, operationLogBucket.getBucket());
  }

  @Test
  public void testRenameFileInSameBucketIsSuccessful() throws IOException {
    /*
     * Prepare
     */
    Path src = pathInBucket(bucket1.getBucket(), "d/src");
    Path dst = pathInBucket(bucket1.getBucket(), "d/dst");
    byte[] data = randomBytes(100);
    createFile(src, data);

    /*
     * Act
     */
    assertTrue(fileSystem.rename(src, dst));

    /*
     * Assert
     */
    assertPathsDontExist(src);
    ExpectedFSObject expectedRoot = expectedDirectory(pathInBucket(bucket1.getBucket(), ""))
        .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "d"))
                         .addChildren(expectedFile(pathInBucket(bucket1.getBucket(), "d/dst"), 100, 3)));
    checkFileSystemState(fileSystem, expectedRoot);
    checkOperationLogState(s3, operationLogBucket.getBucket(), expectedFile(pathInBucket(bucket1.getBucket(), "d/dst"), 100, 3));

    assertDataInFile(dst, data);
  }

  @Test()
  public void testRenameFileThrowsErrorIfDestinationExists() throws IOException {
    Path src = pathInBucket(bucket1.getBucket(), "d/src");
    createFile(src);
    Path dst = pathInBucket(bucket1.getBucket(), "d/dst");
    createFile(dst);

    assertFalse(fileSystem.rename(src, dst));
  }

  @Test
  public void testRenameDirectoryInSameBucketIsSuccessful() throws IOException {
    /*
     * Prepare
     */
    Path src = pathInBucket(bucket1.getBucket(), "root-src/src");
    Path srcFileChild = pathInBucket(bucket1.getBucket(), "root-src/src/file");
    Path srcDirectoryChild = pathInBucket(bucket1.getBucket(), "root-src/src/dir");
    Path srcFileGrandson = pathInBucket(bucket1.getBucket(), "root-src/src/dir/file1");
    Path dst = pathInBucket(bucket1.getBucket(), "root-dst/dst");

    assertTrue(fileSystem.mkdirs(srcDirectoryChild));
    assertTrue(fileSystem.mkdirs(pathInBucket(bucket1.getBucket(), "root-dst")));

    byte[] fileChildData = randomBytes(100);
    createFile(srcFileChild, fileChildData);

    byte[] fileGrandsonData = randomBytes(100);
    createFile(srcFileGrandson, fileGrandsonData);

    /*
     * Act
     */
    assertTrue(fileSystem.rename(src, dst));

    /*
     * Assert
     */
    assertPathsDontExist(src, srcFileChild, srcDirectoryChild, srcFileGrandson);

    ExpectedFSObject expectedRoot = expectedDirectory(pathInBucket(bucket1.getBucket(), ""))
        .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "root-src")))
        .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "root-dst"))
                         .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "root-dst/dst"))
                                          .addChildren(expectedFile(pathInBucket(bucket1.getBucket(), "root-dst/dst/file"), 100, 3))
                                          .addChildren(expectedDirectory(pathInBucket(bucket1.getBucket(), "root-dst/dst/dir"))
                                                           .addChildren(expectedFile(pathInBucket(bucket1.getBucket(), "root-dst/dst/dir/file1"), 100, 3)))));
    checkFileSystemState(fileSystem, expectedRoot);

    checkOperationLogState(s3, operationLogBucket.getBucket(), expectedFile(pathInBucket(bucket1.getBucket(), "root-dst/dst/file"), 100, 3),
                           expectedFile(pathInBucket(bucket1.getBucket(), "root-dst/dst/dir/file1"), 100, 3));

    assertDataInFile(pathInBucket(bucket1.getBucket(), "root-dst/dst/file"), fileChildData);
    assertDataInFile(pathInBucket(bucket1.getBucket(), "root-dst/dst/dir/file1"), fileGrandsonData);
  }

  @Test
  public void testParallelDirectoryRename() throws IOException {
    /*
     * Prepare
     */
    configuration.setLong(String.format("%s.%s.%s", FileSystemRuntimeFactory.THREAD_POOL_SIZE, bucket1.getBucket(), "ctx"), 5);
    configuration.setLong(String.format("%s.%s.%s",FileSystemRuntimeFactory.MAX_PENDING_TASKS, bucket1.getBucket(), "ctx"), 10);

    fileSystem = pathInBucket(bucket1.getBucket(), "").getFileSystem(configuration);

    int numDirectories = 5;
    int numFilesPerDirectory = 10;
    Map<Path, byte[]> filesToCreate = new HashMap<>();
    Map<Path, byte[]> expectedDataInFiles = new HashMap<>();
    List<ExpectedFSObject> expectedMovedFiles = new ArrayList<>();
    Iterator<byte[]> dataIterator = Iterators.cycle(randomBytes(10), randomBytes(10), randomBytes(10));

    Path root = pathInBucket(bucket1.getBucket(), "root");
    Path movedRoot = pathInBucket(bucket1.getBucket(), "moved-root");

    ExpectedFSObject expectedMovedRoot = expectedDirectory(movedRoot);

    for (int i = 0; i < numDirectories; i++) {
      String directoryName = "dir" + i;
      ExpectedFSObject expectedDirectory = expectedDirectory(new Path(movedRoot, directoryName));
      for (int j = 0; j < numFilesPerDirectory; j++) {
        String fileName = "file" + j;
        byte[] data = dataIterator.next();

        filesToCreate.put(new Path(root, directoryName + "/" + fileName), data);

        Path expectedFilePath = new Path(movedRoot, directoryName + "/" +fileName);
        expectedDataInFiles.put(expectedFilePath, data);

        ExpectedFSObject expectedFile = expectedFile(expectedFilePath, 10, 3);
        expectedMovedFiles.add(expectedFile);
        expectedDirectory.addChildren(expectedFile);
      }
      expectedMovedRoot.addChildren(expectedDirectory);
    }
    parallel(filesToCreate.entrySet().stream()
        .map(it -> uncheckedRunnable(() -> createFile(it.getKey(), it.getValue()))));

    /*
     * Act
     */
    assertTrue(fileSystem.rename(root, movedRoot));

    /*
     * Assert
     */
    checkFileSystemState(fileSystem, expectedMovedRoot);
    checkOperationLogState(s3, operationLogBucket.getBucket(), expectedMovedFiles);
    parallel(expectedDataInFiles.entrySet().stream()
                 .map(it -> uncheckedRunnable(() -> assertDataInFile(it.getKey(), it.getValue()))));
  }

  @Test
  public void testParallelDirectoryDelete() throws IOException {
    /*
     * Prepare
     */
    configuration.setLong(String.format("%s.%s.%s", FileSystemRuntimeFactory.THREAD_POOL_SIZE, bucket1.getBucket(), "ctx"), 5);
    configuration.setLong(String.format("%s.%s.%s", FileSystemRuntimeFactory.MAX_PENDING_TASKS, bucket1.getBucket(), "ctx"), 10);

    fileSystem = pathInBucket(bucket1.getBucket(), "").getFileSystem(configuration);

    int numDirectories = 5;
    int numFilesPerDirectory = 10;
    Map<Path, byte[]> filesToCreate = new HashMap<>();
    byte[] data = randomBytes(10);

    Path root = pathInBucket(bucket1.getBucket(), "root");
    Path toBeDeleted = new Path(root, "dir");

    for (int i = 0; i < numDirectories; i++) {
      String directoryName = "dir" + i;
      for (int j = 0; j < numFilesPerDirectory; j++) {
        String fileName = "file" + j;

        filesToCreate.put(new Path(toBeDeleted, directoryName + "/" + fileName), data);
      }
    }
    parallel(filesToCreate.entrySet().stream()
                 .map(it -> uncheckedRunnable(() -> createFile(it.getKey(), it.getValue()))));

    /*
     * Act
     */
    assertTrue(fileSystem.delete(toBeDeleted, true));

    /*
     * Assert
     */
    checkFileSystemState(fileSystem, expectedDirectory(root));
    checkOperationLogState(s3, operationLogBucket.getBucket());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameSourceAndDestinationDifferentBucketThrowsError() throws IOException {
    boolean ignored = fileSystem.rename(new Path("s3://invalid-bucket/src"), new Path("s3://invalid-bucket/dst"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameFromDifferentBucketThrowsError() throws IOException {
    boolean ignored = fileSystem.rename(new Path("s3://invalid-bucket/src"), pathInBucket(bucket1.getBucket(), "dst"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameInDifferentBucketThrowsError() throws IOException {
    boolean ignored = fileSystem.rename(pathInBucket(bucket1.getBucket(), "src"), new Path("s3://invalid-bucket/file"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeleteFileInDifferentBucketThrowsError() throws IOException {
    boolean ingored = fileSystem.delete(new Path("s3://invalid-bucket/file"), true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMkdirsInDifferentBucketThrowsError() throws IOException {
    boolean ingored = fileSystem.mkdirs(new Path("s3://invalid-bucket/dir"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateFileInDifferentBucketThrowsError() throws IOException {
    FSDataOutputStream ingored = fileSystem.create(new Path("s3://invalid-bucket/file"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOpenFileInDifferentBucketThrowsError() throws IOException {
    FSDataInputStream ingored = fileSystem.open(new Path("s3://invalid-bucket/file"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFileStatusInDifferentBucketThrowsError() throws IOException {
    FileStatus ingored = fileSystem.getFileStatus(new Path("s3://invalid-bucket/file"));
  }

  private void assertDataInFile(Path path, byte[] data) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try (FSDataInputStream inputStream = fileSystem.open(path)) {
      IOUtils.copy(inputStream, outputStream);
    }

    assertArrayEquals(data, outputStream.toByteArray());
  }

  private void createFile(Path path) throws IOException {
    try (FSDataOutputStream stream = fileSystem.create(path)) {
      IOUtils.write(randomBytes(100), stream);
    }
  }

  private void createFile(Path path, byte[] data) throws IOException {
    try (FSDataOutputStream stream = fileSystem.create(path)) {
      IOUtils.write(data, stream);
    }
  }

  private void assertPathsDontExist(Path... paths) throws IOException {
    for (Path path : paths) {
      assertFalse(fileSystem.exists(path));
    }
  }

  private static Path pathInBucket(String bucket, String path) {
    return new Path("s3://" + bucket + "/" + path);
  }

  private static byte[] randomBytes(int size) {
    byte[] result = new byte[size];
    new Random().nextBytes(result);
    return result;
  }

  private static void parallel(Stream<Runnable> runnables) {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    try {
      List<Future<?>> futures = runnables.map(executorService::submit).collect(Collectors.toList());
      for (Future<?> f : futures) {
        f.get();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      executorService.shutdown();
    }
  }
}
