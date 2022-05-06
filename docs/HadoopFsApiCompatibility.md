# Hadoop Contract API compatibility

The Hadoop FileSystem contract in terms of atomicity/consistency/concurrency is detailed [here](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/introduction.html#Core_Expectations_of_a_Hadoop_Compatible_FileSystem).

### Atomicity

Hadoop original requirements:
```
There are some operations that MUST be atomic. This is because they are often used to implement locking/exclusive access between processes in a cluster.
1. Creating a file. If the overwrite parameter is false, the check and creation MUST be atomic.
2. Deleting a file.
3. Renaming a file.
4. Renaming a directory.
5. Creating a single directory with mkdir().
Recursive directory deletion MAY be atomic. Although HDFS offers atomic recursive directory deletion, none of the other Hadoop FileSystems offer such a guarantee (including local FileSystems).
Most other operations come with no requirements or guarantees of atomicity.
```
**We support atomic rename of files (with the caveat that the metastore and operation log can drift).**

**Atomic creation of file with overwrite parameter set to false *can* be implemented (currently it is not).**

### Consistency

Hadoop original requirements:
```
1. Create. Once the close() operation on an output stream writing a newly created file has completed, in-cluster operations querying the file metadata and contents MUST immediately see the file and its data.

2. Update. Once the close() operation on an output stream writing a newly created file has completed, in-cluster operations querying the file metadata and contents MUST immediately see the new data.

3. Delete. once a delete() operation on a path other than “/” has completed successfully, it MUST NOT be visible or accessible. Specifically, listStatus(), open() ,rename() and append() operations MUST fail.

4. Delete then create. When a file is deleted then a new file of the same name created, the new file MUST be immediately visible and its contents accessible via the FileSystem APIs.

5. Rename. After a rename() has completed, operations against the new path MUST succeed; attempts to access the data against the old path MUST fail.

6. The consistency semantics inside of the cluster MUST be the same as outside of the cluster. All clients querying a file that is not being actively manipulated MUST see the same metadata and data irrespective of their location.
```
**We do not support the `append()` operation. Apart from this we are inline with the Hadoop consistency requirements.**

### Concurrency

Hadoop original requirements:
```
There are no guarantees of isolated access to data: if one client is interacting with a remote file and another client changes that file, the changes may or may not be visible.
```
**We are inline with Hadoop concurrency requirements.**
