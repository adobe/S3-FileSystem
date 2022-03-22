## The S3 partition hotspot problem

S3 can achieve [3,500 WRITES/sec and 5,500 READS/sec](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)
per partition. By increasing the number of partitions you can increase the throughput to accommodate even the most high demand workloads(of course with the caveat that
the workload needs to be evenly distributed across the partitions).

### How are partitions determined

The key naming pattern plays a big role in how the data and workload is partitioned by S3.
The key's prefix is used to determine to which partition an object belongs.
The key prefix itself is just an arbitrary number of characters at the beginning of the key name.

### Logical key names vs random key names

S3 is often used to store data in a hierarchical folder structure (logical key names).
For example: `s3://bucket/<month>/<day>/file-[1..N]`.

This structure has its advantages:
 - It allows for easy navigation and listing of the bucket.
 - It allows for objects to be retrieved/filtered based on components of the key name (i.e. list all the files in a given month).
 
These advantages, however, come at the cost of performance:

 - Because all the keys in a given day share the same common prefix, they will belong to the same partition. This will lead to throttling if we exceed the partitions rate limits.
 - S3 will try and scale the partition for a given day. But because when we finish writing to that day we move to another day (and another prefix), we won't benefit from the repartitioning.

This is what we refer to as the "S3 partition hotspot problem".

The solution recommended by AWS is to add some randomness at the beginning of the prefixes.
For example: `s3://bucket/<UUID.random()>`:

 - Even with randomness, at first our workload will be throttled until S3 repartitions.
 - After the repartitioning we will see better performance. This is because our newer objects are evenly distributed across the repartitioned keyspace.