# S3-FileSystem

`S3-FileSystem` works by storing the data for a single file under a single key in S3.
The keys have an S3 friendly naming pattern (all keys start with a random [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier)).
We refer to these S3 keys as **_physical paths_**.

In order to allow clients to define human-readable hierarchical directory structures, a separate _metadata store_ is used
to maintain an index between the S3 keys and the human-readable paths. We refer to the human-readable paths as **_logical paths_**.

Information about directories _is **only** persisted in the metadata store_.


## Birds eye view of the architecture

![aam s3 fs 3](./diagrams/aam-s3-fs.png)


### MetadataStoreConfiguration API and Implementation

This layer is responsible for providing configuration information that the concrete MetadataStore implementation requires.
It is context aware: for example a metadata heavy client(like a M/R job driver or `LoadIncrementalHFiles`) might have different configuration requirements from a data heavy client (a map task). 
The context is specified using a JAVA system property `fs.s3k.metastore.context.id`.
We currently have one implementation:
 - A Hadoop `Configuration` based one. All the config data will reside in the Hadoop configuration and will be passed by the client directly.

The configuration API will be K/V style for the most part, but more specific interfaces for concrete implementations of `MetadataStore` **_may_** exist.


### MetadataStore API and Implementation

This layer is responsible for:
- Maintaining the association between the logical paths and physical paths.
- Exposing an API for manipulating these associations: basic CRUD operations + recursive delete/rename operations on directory logical paths.
- Exposing an extended API for operations that are not achievable through the Hadoop `FileSystem` contract. For example:
   * An operation of recursive listing that can be implemented efficiently depending on MetaStore internals.
   * An operation for listing all logical paths for a given bucket, regardless of parent<->child relationship validity  (think of `fsck` type scenarios).


#### DynamodDB MetadataStore Implementation.

This is the main implementation of the MetadataStore API and the core part of the FileSystem as an ensemble.

We store all the data for a given logical path in a single DynamoDB `Item`.

Key structure for a path `p`:
```
{
 	hash_key: parent(p)-suffix(child_name(p))
 	sort_key: child_name(p)
 }
```
The hash key is composed from the path's parent to which we apply a suffix. The suffix is chosen from a predefined set of suffixes `suffixes=[s0..sN-1]` this way: `s=suffixes[hash(child_name(path)) % N]`.

**_NOTE_**: 
 - The number of suffixes impacts directly the throughput you can achieve for operations within a single directory. Increasing the number of suffixes theoretically increases throughput but requires data migration. 

In the DynamoDB we also store the following attributes:
```
{
 	is_dir: boolean // directory flag
 	len: long // object length in bytes, 0 for directories
 	ctime: long	// creation time as UNIX epoch in seconds, NOTE: this is the local time of the client that creates the file/folder
 	phy_path: String // S3 key where the data is stored (the physical path)
    physcommitted: boolean // wether the data was committed to physical storage (i.e. close() called and successfull on the OutputStream)
    id: UUID // the unique id of the object. For directories it is not very important, however see `File Object versioning` for files
    ver int // the version of the object
 }
```

To simplify the implementation we have decided that a single instance of the DynamoDB metadata store will work with a single 
table and a FileSystem instance will work with a single bucket. Writing to multiple buckets from the same process will imply multiple FileSystem instances. Moving data between multiple buckets will require copying of the data and will not be a metadata operation.


#### File Object Versioning.

Whenever a new dir/file is created a unique ID is assigned to it. It is also considered to be at version 1.

Whenever and object is mutated (i.e. renamed, physical updated etc.) its version is incremented and the ID remains the same.
At the time of the mutation the updated version and id are checked against DynamoDB(via a ConditionWrite) to ensure consistency.

The version and the unique ID are also persisted by the `MetadataOperationLog` and used by FSCK to determine if the state of the system is consistent.


### MetadataOperationLog

This layer is the equivalent of HDFS's FSEditLog.
It stores all metadata operations related to **_file_** objects.
It is used directly by MetadataStore implementations. 

The following sequence of actions is always followed when logging file operations.
 - All operations **must** be first logged to the operation log and marked as pending.
   * If this fails, the whole operation fails. 
 - The operation is performed in the MetadataStore.
   * If this fails, the whole operation fails. An attempt must be made to rollback the operation log.
 - The operation log entry is marked as committed.
   * If this fails, the operation is **still considered succesfull**.
  

### Hadoop FileSystem Implementation

At this layer we are implementing the Hadoop `FileSystem` contract.
This layer is responsible for:
 - Implementing all Hadoop semantics as outlined [here](https://hadoop.apache.org/docs/r3.1.0/api/org/apache/hadoop/fs/FileSystem.html). For example semantics like: `mkdir()` has `mkdir -p` POSIX semantics, `create()` does not require entire directory structure to exist beforehand etc 
- Ensuring we use a dispersed key space for all S3 buckets.


### PhysicalStorage API

This layer is a simple CRD(create/read/delete no update because files in s3-fs are immutable) API over a blob storage.
The main implementation is backed by EMRFS.
S3 eventual consistency is handled by this layer.


### FileSystem Tooling and FSCK

This layer implements:
 - A CLI similar to `hadoop fs` but with some operations implemented more efficiently. It relies on the MetadataStore Extended API.
 - A FSCK CLI tool that must be able to repair an inconsistent FileSystem.
   * If the `MetadataStore` is completely lost, it will reconstruct it from the operation log.
   * If data is still valid in the `MetadataStore` it will amend the operation log to reflect that state.
   
Details on [usage](../src/main/java/com/adobe/s3fs/shell/commands/fsck/FsckReadme.md).