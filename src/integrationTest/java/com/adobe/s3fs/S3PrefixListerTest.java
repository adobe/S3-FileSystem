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

import com.adobe.s3fs.filesystemcheck.s3.CartesianS3PrefixPartitioner;
import com.adobe.s3fs.filesystemcheck.s3.S3Partitioner;
import com.adobe.s3fs.filesystemcheck.s3.S3PrefixLister;
import com.adobe.s3fs.filesystemcheck.s3.SingleDigitS3PrefixPartitioner;
import com.adobe.s3fs.utils.ITUtils;
import com.adobe.s3fs.utils.S3Bucket;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3PrefixListerTest {

  @ClassRule public static Network network = Network.newNetwork();

  @ClassRule
  public static LocalStackContainer localStackContainer =
      new LocalStackContainer()
          .withNetwork(network)
          .withServices(LocalStackContainer.Service.S3)
          .withNetworkAliases("localstack");

  static {
    LogManager.getRootLogger().setLevel(Level.INFO);
  }

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public S3Bucket bucket1 = new S3Bucket(ITUtils.amazonS3(localStackContainer));

  private S3PrefixLister s3PrefixLister;

  private Configuration configuration;

  private AmazonS3 s3;

  private AmazonS3 s3Spy;

  private FileSystem fileSystem;

  private String buffer;

  private static Path pathInBucket(String bucket, String path) {
    return new Path("s3://" + bucket + "/" + path);
  }

  private static byte[] randomBytes(int size) {
    byte[] result = new byte[size];
    new Random().nextBytes(result);
    return result;
  }

  private static List<String> readContentsFromPrefixes(
      AmazonS3 s3, String bucket, String startingPrefix) {
    return ITUtils.listFully(s3, bucket).stream()
        .filter(obj -> obj.getKey().startsWith(startingPrefix))
        .flatMap(obj -> readS3Object(s3, obj.getBucketName(), obj.getKey()))
        .collect(Collectors.toList());
  }

  private static Stream<String> readS3Object(AmazonS3 s3, String bucket, String prefix) {
    try (InputStream is = s3.getObject(bucket, prefix).getObjectContent();
        InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(reader)) {
      return IOUtils.readLines(br).stream();
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private static void createFile(FileSystem fs, Path path, byte[] data) throws IOException {
    try (FSDataOutputStream stream = fs.create(path)) {
      IOUtils.write(data, stream);
    }
  }

  private void verifyList(List<String> expected, List<String> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    Assert.assertTrue(expected.containsAll(actual));
  }

  @Before
  public void setup() throws IOException {
    s3 = ITUtils.amazonS3(localStackContainer);

    s3Spy = Mockito.spy(s3);

    configuration = new Configuration(false);
    configuration.setClass("fs.s3.impl", S3AFileSystem.class, FileSystem.class);
    configuration.setBoolean("fs.s3.impl.disable.cache", true);
    // Need below config to overcome an issue when closing zero-bytes streams
    configuration.setBoolean("fs.s3a.multiobjectdelete.enable", false);

    ITUtils.configureS3AAsUnderlyingFileSystem(
        localStackContainer, configuration, bucket1.getBucket(), temporaryFolder.getRoot().toString());

    fileSystem = pathInBucket(bucket1.getBucket(), "").getFileSystem(configuration);

    S3Partitioner s3Partitioner =
        new CartesianS3PrefixPartitioner(new SingleDigitS3PrefixPartitioner());

    s3PrefixLister = new S3PrefixLister(s3Partitioner, configuration, s3Spy);

    buffer = pathInBucket(bucket1.getBucket(), "buffer").toString();
  }

  @Test(expected = UncheckedException.class)
  public void testThrowsExceptionWhenCallingHasNestFromUnderlyingIterable() {
    Mockito.doThrow(new SdkClientException("Failed List!"))
        .when(s3Spy)
        .listObjects(Mockito.any(ListObjectsRequest.class));

    s3PrefixLister.list(bucket1.getBucket(), buffer);
  }

  @Test(expected = UncheckedException.class)
  public void testThrowsExceptionWhenCallingNextFromUnderlyingIterable() {
    // Mock iterator
    Iterator<S3ObjectSummary> mockItrObjSummaries = Mockito.mock(Iterator.class);
    Mockito.when(mockItrObjSummaries.hasNext()).thenReturn(true);
    Mockito.when(mockItrObjSummaries.next()).thenThrow(new SdkClientException("Next failed!"));
    // Mock List
    List<S3ObjectSummary> mockListObjSummaries = Mockito.mock(List.class);
    Mockito.when(mockListObjSummaries.iterator()).thenReturn(mockItrObjSummaries);
    // Mock ObjectListing
    ObjectListing mockListing = Mockito.mock(ObjectListing.class);
    Mockito.when(mockListing.getObjectSummaries()).thenReturn(mockListObjSummaries);
    Mockito.when(mockListing.isTruncated()).thenReturn(false);

    Mockito.doReturn(mockListing)
        .when(s3Spy)
        .listObjects(Mockito.any(ListObjectsRequest.class));

    s3PrefixLister.list(bucket1.getBucket(), buffer);
  }

  @Test
  public void testListObjectsFromSinglePartition() throws IOException {
    // Given
    byte[] data = randomBytes(100);
    // all prefixes starting with f1
    Path f1 = pathInBucket(bucket1.getBucket(), "f1");
    Path f11 = pathInBucket(bucket1.getBucket(), "f11");
    Path f111 = pathInBucket(bucket1.getBucket(), "f111");
    createFile(fileSystem, f1, data);
    createFile(fileSystem, f11, data);
    createFile(fileSystem, f111, data);

    // Act
    boolean status = s3PrefixLister.list(bucket1.getBucket(), buffer);

    // Verify
    Assert.assertTrue("Listing should have finished succesfully!", status);
    List<String> s3Objects = readContentsFromPrefixes(s3, bucket1.getBucket(), "buffer");
    verifyList(Arrays.asList(f1.toString(), f11.toString(), f111.toString()), s3Objects);
  }

  @Test
  public void testListObjectsFromMultiplePartitions() throws IOException {
    // Given
    byte[] data = randomBytes(100);
    Path f1 = pathInBucket(bucket1.getBucket(), "f1"); // all prefixes starting with f1
    Path f2 = pathInBucket(bucket1.getBucket(), "f2"); // all prefixes starting with f2
    Path f3 = pathInBucket(bucket1.getBucket(), "f3"); // all prefixes starting with f3
    Path ff = pathInBucket(bucket1.getBucket(), "ff"); // all prefixes starting with ff
    Path aa1 = pathInBucket(bucket1.getBucket(), "aa1"); // all prefixes starting with aa
    Path aa2 = pathInBucket(bucket1.getBucket(), "aa2"); // all prefixes starting with aa
    Path aa3 = pathInBucket(bucket1.getBucket(), "aa3"); // all prefixes starting with aa
    createFile(fileSystem, f1, data);
    createFile(fileSystem, f2, data);
    createFile(fileSystem, f3, data);
    createFile(fileSystem, ff, data);
    createFile(fileSystem, aa1, data);
    createFile(fileSystem, aa2, data);
    createFile(fileSystem, aa3, data);

    // Act
    boolean status = s3PrefixLister.list(bucket1.getBucket(), buffer);
    Assert.assertTrue("Listing should have finished succesfully!", status);

    List<String> s3Objects = readContentsFromPrefixes(s3, bucket1.getBucket(), "buffer");
    verifyList(
        Arrays.asList(
            f1.toString(), f2.toString(), f3.toString(), ff.toString(),
            aa1.toString(), aa2.toString(), aa3.toString()), s3Objects);
  }
}
