S3-FileSystem
======

### Introduction

`S3-FileSystem` is an implementation of the Hadoop file system contract backed by AWS S3.

### Goals
`S3-FileSystem` was created to enable a more efficient usage of AWS S3. This means:
- provide strong read after write consistency (in the meantime AWS has also rolled out native s3 [strong consystency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)).
- provide file `rename` as an atomic O(1) operation.
- avoid S3 hotspotting regardless of client defined file paths. 

### Non-Goals
`S3-FileSystem` does not aim be a drop in replacement for HDFS nor to fully implement the FileSystem specification. 
There are differences between `HDFS` and `S3-FileSystem`, most notably:
 - `S3-FileSystem` does not support atomic rename of directories.
 - `S3-FileSystem` does not support POSIX like permissions.

For a full list of differences between `S3-FileSystem` and the Hadoop API specification see our contract [definition](./src/integrationTest/resources/contract/s3k.xml).

For the full Hadoop API specification please see these [docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html).
For the implicit assumptions(including atomicity and concurrency) of the API please see these [docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/introduction.html). 

### Contributing

Contributions are welcomed! Read the [Contributing Guide](./docs/Contributing.md) for more information.

### Licensing

This project is licensed under the Apache V2 License. See [LICENSE](LICENSE) for more information.
