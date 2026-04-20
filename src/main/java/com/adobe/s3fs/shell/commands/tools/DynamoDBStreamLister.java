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

package com.adobe.s3fs.shell.commands.tools;

import com.adobe.s3fs.metastore.internal.dynamodb.storage.AmazonDynamoDBStorage;
import com.adobe.s3fs.utils.threading.BlockingExecutor;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.adobe.s3fs.shell.CommandGroups.TOOLS;

@Command(
    name = "ddbStreamLister",
    description = "Scans a DynamoDB stream and filters out records based on a filter",
    groupNames = TOOLS)
public class DynamoDBStreamLister implements Runnable {

  @Option(name = "--stream-arn")
  @Required
  String streamArn;

  @Option(name = "--filter", description = "The hash or sort key of the item must contain this filter")
  @Required
  String filter;

  @Option(name = "--threads")
  int threads = 200;

  @Option(name = "--debug")
  boolean debug = false;

  private final AtomicLong totalItemCount = new AtomicLong();


  private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStreamLister.class);

  @Override
  public void run() {
    Preconditions.checkNotNull(streamArn);
    Preconditions.checkNotNull(filter);
    totalItemCount.set(0L);

    DynamoDbStreamsClient streamsClient = DynamoDbStreamsClient.builder()
        .credentialsProvider(DefaultCredentialsProvider.builder().build())
        .httpClientBuilder(ApacheHttpClient.builder().maxConnections(threads))
        .region(Region.US_EAST_1)
        .build();

    BlockingExecutor executorService = new BlockingExecutor(
        threads,
        new ThreadPoolExecutor(threads, threads, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10)));

    String lastEvaluatedShardID = null;

    do {
      DescribeStreamResponse describeStreamResult = streamsClient.describeStream(DescribeStreamRequest.builder()
          .streamArn(streamArn)
          .exclusiveStartShardId(lastEvaluatedShardID)
          .build());

      List<Shard> shards = describeStreamResult.streamDescription().shards();

      for (Shard shard : shards) {
        if (Strings.isNullOrEmpty(shard.sequenceNumberRange().endingSequenceNumber())) {
          // shard is still being written so skip it otherwise we loop on it until it closes
          continue;
        }

        executorService.execute(shardProcessor(streamsClient, shard));
      }

      lastEvaluatedShardID = describeStreamResult.streamDescription().lastEvaluatedShardId();
    } while (lastEvaluatedShardID != null);

    executorService.shutdownAndAwaitTermination();
  }

  private Runnable shardProcessor(DynamoDbStreamsClient streamsClient, Shard shard) {
    return () -> {
      try {
        String shardId = shard.shardId();

        GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder()
            .streamArn(streamArn)
            .shardId(shardId)
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build();
        GetShardIteratorResponse getShardIteratorResult =
            streamsClient.getShardIterator(getShardIteratorRequest);

        String currentShardIter = getShardIteratorResult.shardIterator();

        while (currentShardIter != null) {
          GetRecordsResponse getRecordsResult = streamsClient.getRecords(GetRecordsRequest.builder()
              .shardIterator(currentShardIter)
              .build());
          List<Record> records = getRecordsResult.records();
          for (Record it : records) {
            StreamRecord record = it.dynamodb();
            if (debug && totalItemCount.incrementAndGet() % 50000 == 0) {
              LOG.info("Processed {} stream records", totalItemCount.get());
            }

            boolean hasOldImage = record.hasOldImage() && !record.oldImage().isEmpty();
            boolean inOldImageHash = hasOldImage && record.oldImage().get(AmazonDynamoDBStorage.HASH_KEY).s().contains(filter);
            boolean inOldImageSort = hasOldImage && record.oldImage().get(AmazonDynamoDBStorage.SORT_KEY).s().contains(filter);
            boolean hasNewImage = record.hasNewImage() && !record.newImage().isEmpty();
            boolean inNewImageHash = hasNewImage && record.newImage().get(AmazonDynamoDBStorage.HASH_KEY).s().contains(filter);
            boolean inNewImageSort = hasNewImage && record.newImage().get(AmazonDynamoDBStorage.SORT_KEY).s().contains(filter);

            if (inOldImageHash || inOldImageSort || inNewImageHash || inNewImageSort) {
              LOG.info("Stream Entry: {}", record);
            }
          }
          currentShardIter = getRecordsResult.nextShardIterator();
        }
      } catch (Exception e) {
        LOG.error("Error processing shard {}", shard.shardId());
        LOG.error("Error: ", e);
      }
    };
  }
}
