<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# OSSGuard: Metadata Caching for OSS

**Experimental Feature**

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

## Overview

*OSSGuard* is an experimental feature for the OSS Filesystem of the OSS
object store, which can use a (consistent) database as the store of metadata
about objects in an OSS bucket.

**OSSGuard** will improve performance on directory listing/scanning operations,
including those which take place during the partitioning period of query
execution, the process where files are listed and the work divided up amongst
processes.


*Important*

* OSSGuard is experimental and should be considered unstable.

* While all underlying data is persisted in OSS, if, for some reason,
the OSSGuard-cached metadata becomes inconsistent with that in OSS,
queries on the data may become incorrect.
For example, new datasets may be omitted, objects may be overwritten,
or clients may not be aware that some data has been deleted.
It is essential for all clients writing to an OSSGuard-enabled
OSS Repository to use the feature.

## Setting up OSSGuard

### 1. Choose the Database
A core concept of OSSGuard is that the directory listing data of the object
store, *the metadata* is replicated in a higher-performance, consistent,
database. In OSSGuard, this database is called *The Metadata Store*

By default, OSSGuard is not enabled.

The Metadata Store to use in production is bonded to Aliyun's OTS
table store service. The following setting will enable this Metadata Store:

```xml
<property>
  <name>fs.oss.metadatastore.impl</name>
  <value>org.apache.hadoop.fs.aliyun.oss.guard.OTSMetadataStore</value>
</property>
```

Note that the `NullMetadataStore` store can be explicitly requested if desired.
This offers no metadata storage, and effectively disables OSSGuard.

```xml
<property>
  <name>fs.oss.metadatastore.impl</name>
  <value>org.apache.hadoop.cloud.core.metadata.NullMetadataStore</value>
</property>
```

However, we provide another implementation of MetadataStore(HadoopMetadataStore)
for OSSGuard, we store objects' metadata to HDFS. You can enable it by following
configurations
```xml
<property>
  <name>fs.oss.metadatastore.impl</name>
  <value>org.apache.hadoop.fs.aliyun.oss.guard.HadoopMetadataStore</value>
</property>
```
```xml
<property>
  <name>fs.oss.metadatastore.path</name>
  <description>HDFS path used to persist OSS directories</description>
  <value>/oss/meta</value>
</property>
```

### 2. Configure OSSGuard Settings
More settings will may be added in the future.
Currently the only Metadata Store-independent setting, besides the
implementation class above, is the *allow authoritative* flag.

The _authoritative_ expression in OSSGuard is present in two different layers, for
two different reasons:

* Authoritative OSSGuard
    * OSSGuard can be set as authoritative, which means that an OSS client will
    avoid round-trips to OSS when **getting directory listings** if there is a fully
    cached version of the directory stored in metadata store.
    * This mode can be set as a configuration property
    `fs.oss.metadatastore.authoritative`
    * All interactions with the OSS bucket(s) must be through OSS clients sharing
    the same metadata store.
    * This is independent from which metadata store implementation is used.

* Authoritative directory listings (isAuthoritative bit)
    * Tells if the stored directory listing metadata is complete.
    * This is set by the FileSystem client (e.g. oss) via the `DirListingMetadata`
    class (`org.apache.hadoop.cloud.core.metadata.DirListingMetadata`).
    (The MetadataStore only knows what the FS client tells it.)
    * If set to `TRUE`, we know that the directory listing
    (`DirListingMetadata`) is full, and complete.
    * If set to `FALSE` the listing may not be complete.
    * Metadata store may persist the isAuthoritative bit on the metadata store.
    * Currently `org.apache.hadoop.cloud.core.metadata.LocalMetadataStore`,
    `org.apache.hadoop.fs.aliyun.oss.guard.HadoopMetadataStore` and
    `org.apache.hadoop.fs.aliyun.oss.guard.OTSMetadataStore` implementation
    supports authoritative bit.
    
### 3. Configure the Metadata Store

Here are the `OTSMetadataStore` settings.  Other Metadata Store
implementations will have their own configuration parameters.

### 4. Name Your Table
Set up your OTS endpoint, instance and table name.
```xml
<property>
  <name>fs.ots.endpoint</name>
  <value/>
  <description>Aliyun OTS endpoint to connect to. </description>
</property>
```
```xml
<property>
  <name>fs.ots.instance</name>
  <value/>
  <description>Aliyun OTS instance to manage metadata table(s).</description>
</property>
```
```xml
<property>
  <name>fs.ots.table</name>
  <value/>
  <description>OTS table used to cache oss meta.</description>
</property>
```
 
### 5. Optional: Create your Table
Next, you can choose whether or not the table will be automatically created
(if it doesnt already exist).  If you want this feature, set the
`fs.oss.metadatastore.ots.table.auto.create` option to `true`.

```xml
<property>
  <name>fs.oss.metadatastore.ots.table.auto.create</name>
  <value>true</value>
  <description>
    If true, the OSS client will create the table if it does not already exist.
    Default value is true.
  </description>
</property>
```
### 6. If creating a table: Set your OTS table Capacity Unit
Next, you need to set the OTS table read and write capacity unit requirements
you expect to need for your cluster. Default values are 0.
```xml
<property>
  <name>fs.oss.metadatastore.ots.table.capacity.read</name>
  <value>0</value>
</property>
```
```xml
<property>
  <name>fs.oss.metadatastore.ots.table.capacity.write</name>
  <value>0</value>
</property>
```

### 7. Other configurations
```xml
<property>
  <name>fs.oss.metadatastore.ots.table.attempts.maximum</name>
  <description>Number of times we should retry errors</description>
  <value>3</value>
</property>
```

## Authenticating with OSSGuard
The OTS metadata store takes advantage of the fact that the Aliyun
table store service uses the same authentication mechanisms as OSS.
OSSGuard gets all its credentials from the OSS client that is using it.

All existing OSS authentication mechanisms can be used, except for one
exception. Credentials placed in URIs are not supported for OSSGuard,
for security reasons.

## OSSGuard Command Line Interface (CLI)
Note that in some cases oss:// URI can be provided.

### Create a table: `ossguard init`
```bash
hadoop ossguard init oss://BUCKET/
```
Creates and initializes an empty metadata store.

An OTS metadata store can be initialized with additional parameters pertaining to

```bash
[-write WRITE_CAPACITY_UNITS] [-read READ_CAPACITY_UNITS]
```

Example 1
```bash
[hdfs@master ~]$ hadoop ossguard init oss://ossguard-test
19/04/02 13:55:37 INFO oss.AliyunOSSFileSystem: Using metadata store org.apache.hadoop.fs.aliyun.oss.guard.OTSMetadataStore, authoritative = true
19/04/02 13:55:37 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 13:55:37 CST 2019
19/04/02 13:55:37 INFO util.ExitUtil: Exiting with status 0
```

Example 2
```bash
[hdfs@master ~]$ hadoop ossguard init -read 100 -write 100 oss://ossguard-test
19/04/02 13:56:30 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 13:55:37 CST 2019
19/04/02 13:56:30 INFO oss.AliyunOSSFileSystem: Using metadata store org.apache.hadoop.fs.aliyun.oss.guard.OTSMetadataStore, authoritative = true
19/04/02 13:56:30 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 13:55:37 CST 2019
19/04/02 13:56:30 INFO util.ExitUtil: Exiting with status 0
```

### Import a bucket: `ossguard import`
```bash
hadoop ossguard import oss://BUCKET
```

Pre-populates a metadata store according to the current contents of an OSS
bucket. The configurations are taken from the `core-site.xml` configuration.

Example

```bash
[hdfs@master ~]$ hadoop ossguard import oss://ossguard-test
19/04/02 14:17:52 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 14:17:41 CST 2019
19/04/02 14:17:52 INFO oss.AliyunOSSFileSystem: Using metadata store org.apache.hadoop.fs.aliyun.oss.guard.OTSMetadataStore, authoritative = true
Imported 6 dirs, 5 files into Metadata Store
19/04/02 14:17:52 INFO util.ExitUtil: Exiting with status 0
```

### Audit a table: `ossguard diff`
```bash
hadoop ossguard diff oss://BUCKET
```

Lists discrepancies between a metadata store and bucket. Note that depending on
how OSSGuard is used, certain discrepancies are to be expected.

Example 1(no diff)

```bash
[hdfs@master ~]$ hadoop ossguard diff oss://ossguard-test
19/04/02 14:19:57 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 14:17:41 CST 2019
19/04/02 14:19:57 INFO oss.AliyunOSSFileSystem: Using metadata store org.apache.hadoop.cloud.core.metadata.NullMetadataStore, authoritative = false
19/04/02 14:19:57 INFO util.ExitUtil: Exiting with status 0
```

Example 2(delete one file from metadata store and OSS)
```bash
[hdfs@master ~]$ hadoop ossguard diff oss://ossguard-test
19/04/02 14:21:07 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 14:17:41 CST 2019
19/04/02 14:21:07 INFO oss.AliyunOSSFileSystem: Using metadata store org.apache.hadoop.cloud.core.metadata.NullMetadataStore, authoritative = false
OSS	F	0	oss://ossguard-test/dir1/file2
MS	D	0	oss://ossguard-test/dir3/subdir1
19/04/02 14:21:07 INFO util.ExitUtil: Exiting with status 0
```

### Delete a table: `ossguard destroy`
Deletes a metadata store. With OTS as the store, this means
the specific OTS table use to store the metadata.

```bash
hadoop ossguard destroy oss://BUCKET
```
This *does not* delete the bucket, only the OSSGuard table which it is bound
to.

Example
```bash
[hdfs@master ~]$ hadoop ossguard destroy oss://ossguard-test
19/04/02 14:22:55 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 14:17:41 CST 2019
19/04/02 14:22:55 INFO oss.AliyunOSSFileSystem: Using metadata store org.apache.hadoop.fs.aliyun.oss.guard.OTSMetadataStore, authoritative = true
19/04/02 14:22:55 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 14:17:41 CST 2019
19/04/02 14:22:56 INFO util.ExitUtil: Exiting with status 0
```
### Clean up a table, `ossguard prune`
Delete all file entries in the MetadataStore table whose object "modification
time" is older than the specified age.

```bash
hadoop ossguard prune [-days DAYS] [-hours HOURS] [-minutes MINUTES]
    [-seconds SECONDS] oss://BUCKET/path/to/prune
```

A time value of hours, minutes and/or seconds must be supplied.

1. This does not delete the entries in the bucket itself.
1. The modification time is effectively the last modification time of the
objects in the OSS Bucket.

Example
```bash
hadoop ossguard prune -days 7 oss://ossguard-test
```

### Tune the capacity unit of the OTS Table, `ossguard set-capacity`
Alter the read and/or write capacity of a ossguard table.
```bash
hadoop ossguard set-capacity [--read UNIT] [--write UNIT] oss://BUCKET
```

Example
```bash
[hdfs@master ~]$ hadoop ossguard set-capacity -read 100 -write 100 oss://ossguard-test
19/04/02 14:24:21 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 14:23:17 CST 2019
19/04/02 14:24:21 INFO oss.AliyunOSSFileSystem: Using metadata store org.apache.hadoop.fs.aliyun.oss.guard.OTSMetadataStore, authoritative = true
19/04/02 14:24:21 INFO guard.OTSMetadataStore: table: hadoop_oss_test, version: 1, created time: Tue Apr 02 14:23:17 CST 2019
Read capacity set to 100
Write capacity set to 100
19/04/02 14:24:21 INFO guard.OTSMetadataStore: Current table capacity is read: 0, write: 0
19/04/02 14:24:21 INFO guard.OTSMetadataStore: Changing capacity of table to read: 100, write: 100
19/04/02 14:24:22 INFO util.ExitUtil: Exiting with status 0
```