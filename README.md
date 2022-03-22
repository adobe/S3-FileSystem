S3-FileSystem
======

### Introduction

`S3-FileSystem` is an implementation of the Hadoop file system contract backed by AWS S3.

### Goals
`S3-FileSystem` was created to enable a more efficient usage of AWS S3. This means:
- provide strong read after write consistency (in the meantime AWS has also rolled out native s3 [strong consystency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)).
- provide file `rename` as an atomic O(1) operation. Natively, files cannot be renamed in S3(other file system implementations on top of S3 implement file rename as a copy + delete). 
- avoid S3 partition [hotspot problem](./docs/S3PartitionHotSpotting.md) regardless of client defined file paths. 

### Non-Goals
`S3-FileSystem` does not aim be a drop in replacement for HDFS nor to fully implement the FileSystem specification. 
There are differences between `HDFS` and `S3-FileSystem`, most notably:
 - `S3-FileSystem` does not support atomic rename of directories.
 - `S3-FileSystem` does not support POSIX like permissions.

For a full list of differences between `S3-FileSystem` and the Hadoop API specification see our contract [definition](./src/integrationTest/resources/contract/s3k.xml).

For the full Hadoop API specification please see these [docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html).
For the implicit assumptions(including atomicity and concurrency) of the API please see these [docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/introduction.html).

### Similar projects

A few projects that tackle the same issues:

 - [S3 Guard](https://hadoop.apache.org/docs/r3.0.3/hadoop-aws/tools/hadoop-aws/s3guard.html) tackles S3 consistency issues:
   - Since S3 rolled out native strong consistency, the open source community has decided to deprecate S3 Guard.
 - [S3A committers](https://hadoop.apache.org/docs/r3.1.1/hadoop-aws/tools/hadoop-aws/committers.html) tackles both consistency and S3's rename problems
   - The S3A committers do not attempt to solve these issues at the `FileSystem` level, but at the `OutputCommitter` level. Thus, they are primarily targeted at improving Spark/MR job performance and correctness when running on S3.  

### Contributing

Contributions are welcomed! Read the [Contributing Guide](./docs/Contributing.md) for more information.

### Licensing

This project is licensed under the Apache V2 License. See [LICENSE](LICENSE) for more information.
