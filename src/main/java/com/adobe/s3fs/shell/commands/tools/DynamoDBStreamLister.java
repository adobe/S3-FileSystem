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
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
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

    AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .withClientConfiguration(new ClientConfiguration().withMaxConnections(threads))
        .withRegion(Regions.US_EAST_1)
        .build();

    BlockingExecutor executorService = new BlockingExecutor(
        threads,
        new ThreadPoolExecutor(threads, threads, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10)));

    String lastEvaluatedShardID = null;

    do {
      DescribeStreamResult describeStreamResult = streamsClient.describeStream(new DescribeStreamRequest()
          .withStreamArn(streamArn)
          .withExclusiveStartShardId(lastEvaluatedShardID));

      List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

      for (Shard shard : shards) {
        if (Strings.isNullOrEmpty(shard.getSequenceNumberRange().getEndingSequenceNumber())) {
          // shard is still being written so skip it otherwise we loop on it until it closes
          continue;
        }

        executorService.execute(shardProcessor(streamsClient, shard));
      }

      lastEvaluatedShardID = describeStreamResult.getStreamDescription().getLastEvaluatedShardId();
    } while (lastEvaluatedShardID != null);

    executorService.shutdownAndAwaitTermination();
  }

  private Runnable shardProcessor(AmazonDynamoDBStreams streamsClient, Shard shard) {
    return () -> {
      try {
        String shardId = shard.getShardId();

        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
        GetShardIteratorResult getShardIteratorResult =
            streamsClient.getShardIterator(getShardIteratorRequest);

        String currentShardIter = getShardIteratorResult.getShardIterator();

        while (currentShardIter != null) {
          GetRecordsResult getRecordsResult = streamsClient.getRecords(new GetRecordsRequest()
              .withShardIterator(currentShardIter));
          List<Record> records = getRecordsResult.getRecords();
          for (Record it : records) {
            StreamRecord record = it.getDynamodb();
            if (debug && totalItemCount.incrementAndGet() % 50000 == 0) {
              LOG.info("Processed {} stream records", totalItemCount.get());
            }

            boolean hasOldImage = record.getOldImage() != null && !record.getOldImage().isEmpty();
            boolean inOldImageHash = hasOldImage && record.getOldImage().get(AmazonDynamoDBStorage.HASH_KEY).getS().contains(filter);
            boolean inOldImageSort = hasOldImage && record.getOldImage().get(AmazonDynamoDBStorage.SORT_KEY).getS().contains(filter);
            boolean hasNewImage = record.getNewImage() != null && !record.getNewImage().isEmpty();
            boolean inNewImageHash = hasNewImage && record.getNewImage().get(AmazonDynamoDBStorage.HASH_KEY).getS().contains(filter);
            boolean inNewImageSort = hasNewImage && record.getNewImage().get(AmazonDynamoDBStorage.SORT_KEY).getS().contains(filter);

            if (inOldImageHash || inOldImageSort || inNewImageHash || inNewImageSort) {
              LOG.info("Stream Entry: {}", record);
            }
          }
          currentShardIter = getRecordsResult.getNextShardIterator();
        }
      } catch (Exception e) {
        LOG.error("Error processing shard {}", shard.getShardId());
        LOG.error("Error: ", e);
      }
    };
  }
}
