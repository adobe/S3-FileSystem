# How to use

## Binaries and dependencies

For gradle projects add: `implementation group: 'com.adobe.s3fs', name: 'S3-FileSystem', version: '1.0'` in
your `build.gradle`

For maven projects:

```
<dependency>
<groupId>com.adobe.s3fs</groupId>
<artifact>IdS3-FileSystem</artifactId>
<version>1.0</version>
</dependency>
```

in your `pom.xml`

The `S3-FileSystem` library comes with most of its dependencies shaded inside a "fat" jar. There are 2 dependencies
which are not shaded and should be provided by users on the classpath:

- `org.apache.hadoop:hadoop-common`
- `org.apache.hadoop:hadoop-mapreduce-client-core`. This must only be provided when running `fsck`.

## External resources

### DynamoDB metadata table

A DynamoDB table with the following schema needs to be created by the user:

```
partitionKey: path(String)
sortKey: children(String)
```

## Configuration

All the configuration properties can be passed via the Hadoop configuration.

Most of the properties also contain a bucket discriminator. This allows the same hadoop config and process to access
different buckets with different configurations.

Additionally, some properties are context aware. This allows different clients that access the same bucket to have
different configurations. It is mostly useful to supply different configurations to workers (M/R tasks, Spark executors
etc.) and coordinators (Spark driver, AppMaster etc.)
A context is specified when a java process is started via the system property `-Dfs.s3k.metastore.context.id=name`.
Afterwards the same property in the same hadoop configuration can be suffixed with two different context ids. For
example, you can have both `fs.s3k.metastore.dynamo.max.http.conn.some-bucket.sparkdriver`
and `fs.s3k.metastore.dynamo.max.http.conn.some-bucket.sparkexecutor` in the same hadoop config

### Configuring the filesystem scheme

Although you can map `S3-FileSystem` to any scheme you wish, we recommend you use a different scheme from  `s3n`
and `s3a`. This is because once you write data using `S3-FileSystem`, you can read it back using **only** `S3-FileSystem`.
On the other hand, the other filesystems that are usually mapped to the other schemes can be
used interchangeably on the same data.

You can configure the scheme like this : `fs.s3k.impl=com.adobe.s3fs.filesystem.HadoopFileSystemAdapter`.

### Configuration properties for the metadata store

| Property                                            |                                          Description               | Default      |   Context aware |
|-----------------------------------------------------|--------------------------------------------------------------------|--------------|------------------|
| fs.s3k.metastore.dynamo.table.&lt;some-bucket>         | Identifies the DynamoDB table used as metadata store for the S3   bucket named `some-bucket`  | No default | No |
| fs.s3k.metastore.dynamo.suffix.count.&lt;some-bucket>  | Identifies the number of suffixes(sub-partitions) that will be used to distribute load in a directory. Please see the "Considerations on performance" section for more details | No Default | No |               
| fs.s3k.metastore.dynamo.base.exponential.delay.&lt;some-bucket>      | Base exponential delay(in milliseconds) used by the DynamoDB client's backoff strategy                |  80 | Yes |
| fs.s3k.metastore.dynamo.max.exponential.delay.&lt;some-bucket>       | Maximum exponential delay(in milliseconds) used by the DynamoDB's client backoff strategy             | 60000 | Yes |
| fs.s3k.metastore.dynamo.backoff.full.jitter.&lt;some-bucket>         | Whether to use full jitter or equal jitter (true for full and false for equal jitter)   | true | Yes |
| fs.s3k.metastore.dynamo.max.retries.&lt;some-bucket>    | Maximum retries used by the DynamoDB's client backoff strategy        | 50 | Yes |
| fs.s3k.metastore.dynamo.max.http.conn.&lt;some-bucket>  | Maximum connection pool size of the DynamoDB client        |  50 | Yes | 
| fs.s3k.metastore.dynamo.access.&lt;some-bucket>  | AWS access key id used by the DynamoDB client. This property is optional, IAM roles can be used instead | No default | No |
| fs.s3k.metastore.dynamo.secret.&lt;some-bucket>  | AWS secret access key used by the DynamoDB client. This property is optional, IAM roles can be used instead | No default | No |
| fs.s3k.metastore.dynamo.endpoint.&lt;some-bucket> | AWS DynamoDB endpoint. This property is optional | No default | No |
| fs.s3k.metastore.dynamo.signing.region.&lt;some-bucket> | AWS DynamoDB signing region. This property is optional | No default | No |

### Configuration properties for the metadata operation log

| Property                                            |                                          Description               | Default      |   Context aware |
|-----------------------------------------------------|--------------------------------------------------------------------|--------------|------------------|
| fs.s3k.metastore.operation.log.factory.class        | Configures the operation log implementation used. You can disable the operation log by setting this to `com.adobe.s3fs.metastore.api.DisabledMetadataOperationLog`. Note that while this will improve performance you will lose all the recovery information | `com.adobe.s3fs.operationlog.S3MetadataOperationLogFactory` | No |
| fs.s3k.operationlog.s3.bucket.&lt;some-bucket>   | The bucket in which the operation log will be written. **Due to a current limitation this property should always be set to the bucket in which the data is stored** | No default | no |
| fs.s3k.operationlog.s3.base.exponential.delay.&lt;some-bucket> | Base exponential delay(in milliseconds) used the S3 client of the operation log |  10 | Yes |
| fs.s3k.operationlog.s3.max.exponential.delay.&lt;some-bucket> | Maximum exponential delay(in milliseconds) used by the S3's client backoff strategy  | 30000 | Yes |
| fs.s3k.operationlog.s3.backoff.full.jitter.&lt;some-bucket> | Whether to use full jitter or equal jitter (true for full and false for equal jitter) | true | Yes |
| fs.s3k.operationlog.s3.max.retries.&lt;some-bucket> | Maximum retries used by the S3's client backoff strategy  | 50 | Yes |
| fs.s3k.operationlog.s3.max.http.conn.&lt;some-bucket> | Maximum connection pool size of the S3 client  | 220 | Yes |
| fs.s3k.operationlog.s3.access.&lt;some-bucket> | AWS access key id used by the S3 client. This property is optional, IAM roles can be used instead | No default | No |
| fs.s3k.operationlog.s3.secret.&lt;some-bucket>  | AWS secret access key used by the S3 client. This property is optional, IAM roles can be used instead | No default | No |
| fs.s3k.operationlog.s3.endpoint.&lt;some-bucket> | AWS S3 endpoint. This property is optional | No default | No |
| fs.s3k.operationlog.s3.signing.region.&lt;some-bucket> | AWS S3 signing region. This property is optional | No default | No |

### Configuring access to the S3 data itself

For writing and reading actual data to and from S3, `S3-FileSystem` relies on a separate implementation of
the `FileSystem`. This can be `emrfs` if you are running your workload in EMR, or it can be `S3A` if you are running
outside EMR.

To configure the underlying filesystem you need to set `fs.s3k.storage.underlying.filesystem.scheme.<some-bucket>` to
the scheme that the other filesystem is using.

To pass configuration to the underlying file system you can set properties in following
manner: `fs.s3k.storage.underlying.fs.prop.marker.actual-property.<some-bucket>=value`

### Configuring parallelism for directory operations (rename and delete)

| Property                                            |                                          Description               | Default      |   Context aware |
|-----------------------------------------------------|--------------------------------------------------------------------|--------------|------------------|
| fs.s3k.thread.pool.size.&lt;some-bucket>     | The thread pool size. Note that this property must be correlated with the HTTP connection pool sizes of the DynamoDB client and S3 operation log client | 10 | Yes |
| fs.s3k.thread.pool.max.tasks.&lt;some-bucket>      |  Maximum pending operations that can be queued if the thread pool is full  | 50 | Yes |

### Considerations on performance

#### Single directory operations performance

Because the first level children of a directory are stored under the same partition key, the operation throughput when
adding/removing/renaming the first level children is limited by the throughput(3,000 RCU/s and 1,000 RCU/s) of a single
partition in DynamoDB.

To improve this, you can use the `fs.s3k.metastore.dynamo.suffix.count.bucket` property. Essentially, this property
will shard the data even further inside a directory giving you first child level operation throughput
of `suffix-count*3,000` RCU/s and `suffix-count*1,000` WCU/s.

Note that increasing this property too much will decrease listing performance. Running `S3-FileSystem` at scale for
Adobe we have found that a value of`10` is good enough for most workloads

### Root path hotspotting

Some operations(e.g. `mkdir` and `create`) of the `FileSystem` contract have the implied semantics that the parent path
may not exist and should be created all the way up to the root of the directory tree. When running these types of
operations in massively parallel context, there will be a large number of requests on the root paths of the directory
tree, which will lead to the DynamoDB partitions hosting the data for those paths to potentially become overloaded.

This behaviour is mostly visible with very flat directory trees. For example: an M/R job creating temporary folders for
attempts `s3://bucket/job-output/task_attempt_[1..N]`

At the moment, the only mitigation we have for this is to configure a very relaxed backoff strategy for the metastore's
DynamoDB client. For example:

```
fs.s3k.metastore.dynamo.suffix.count.<some-bucket>=80
fs.s3k.metastore.dynamo.max.exponential.delay.<some-bucket>=60000
fs.s3k.metastore.dynamo.backoff.full.jitter.<some-bucket>=true
fs.s3k.metastore.dynamo.max.retries.<some-bucket>=50
```
