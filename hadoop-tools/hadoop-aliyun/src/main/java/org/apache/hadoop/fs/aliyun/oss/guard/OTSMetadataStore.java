/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.guard;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.ListTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.ReservedThroughputDetails;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.UpdateTableRequest;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.cloud.core.metadata.DescendantsIterator;
import org.apache.hadoop.cloud.core.metadata.DirListingMetadata;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.cloud.core.metadata.Tristate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.cloud.core.metadata.MetadataStoreCapabilities.PERSISTS_AUTHORITATIVE_BIT;
import static org.apache.hadoop.cloud.core.metadata.MetadataUtils.pathToParentKey;

/**
 * Aliyun OTS(Table Store) implementation of {@link MetadataStore}.
 * The DynamoDB partition key is the parent, and the range key is the child.
 *
 * To allow multiple buckets to share the same OTS table, the bucket
 * name is treated as the root directory.
 *
 * For example, assume the metadata store contains metadata representing this
 * file system structure:
 *
 * <pre>
 * oss://bucket/dir1
 * |-- dir2
 * |   |-- file1
 * |   `-- file2
 * `-- dir3
 *     |-- dir4
 *     |   `-- file3
 *     |-- dir5
 *     |   `-- file4
 *     `-- dir6
 * </pre>
 *
 * This is persisted to a single OTS table as:
 *
 * <pre>
 * =========================================================================
 * | parent                 | child | is_dir | mod_time | len |     ...    |
 * =========================================================================
 * | /bucket                | dir1  | true   |          |     |            |
 * | /bucket/dir1           | dir2  | true   |          |     |            |
 * | /bucket/dir1           | dir3  | true   |          |     |            |
 * | /bucket/dir1/dir2      | file1 |        |   100    | 111 |            |
 * | /bucket/dir1/dir2      | file2 |        |   200    | 222 |            |
 * | /bucket/dir1/dir3      | dir4  | true   |          |     |            |
 * | /bucket/dir1/dir3      | dir5  | true   |          |     |            |
 * | /bucket/dir1/dir3/dir4 | file3 |        |   300    | 333 |            |
 * | /bucket/dir1/dir3/dir5 | file4 |        |   400    | 444 |            |
 * | /bucket/dir1/dir3      | dir6  | true   |          |     |            |
 * =========================================================================
 * </pre>
 *
 * This choice of schema is efficient for read access patterns.
 * {@link #get(Path)} can be served from a single item lookup.
 * {@link #listChildren(Path)} can be served from a query against all rows
 * matching the parent (the partition key) and the returned list is guaranteed
 * to be sorted by child (the range key).  Tracking whether or not a path is a
 * directory helps prevent unnecessary queries during traversal of an entire
 * sub-tree.
 *
 * Some mutating operations, notably {@link #deleteSubtree(Path)} and
 * {@link #move(Collection, Collection)}, are less efficient with this schema.
 * They require mutating multiple items in the OTS table.
 */
public class OTSMetadataStore extends AbstractMetadataStore {
  public static final Logger LOG = LoggerFactory.getLogger(
      OTSMetadataStore.class);

  @VisibleForTesting
  static final String PARENT = "parent";
  @VisibleForTesting
  static final String CHILD = "child";
  @VisibleForTesting
  static final String IS_DIR = "is_dir";
  @VisibleForTesting
  static final String MOD_TIME = "mod_time";
  @VisibleForTesting
  static final String FILE_LENGTH = "file_length";
  @VisibleForTesting
  static final String BLOCK_SIZE = "block_size";
  @VisibleForTesting
  static final String IS_AUTHORITATIVE = "is_authoritative";
  @VisibleForTesting
  static final String LAST_UPDATED = "last_updated";

  /** parent/child name to use in the version marker. */
  @VisibleForTesting
  static final String VERSION_MARKER = "../VERSION";

  /** Table version field {@value} in version marker item. */
  @VisibleForTesting
  static final String TABLE_VERSION = "table_version";

  /** Table creation timestampfield {@value} in version marker item. */
  @VisibleForTesting
  static final String TABLE_CREATED = "table_created";

  /** Current version number. */
  @VisibleForTesting
  static final int VERSION = 1;

  private SyncClient otsClient;
  private String table;
  private int batchWriteLimit;
  private RetryPolicy dataAccessRetryPolicy;

  @Override
  public void initialize(Configuration conf) throws IOException {
    if (this.conf == null) {
      this.conf = conf;
    }

    if (username == null) {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    }

    if (credentials == null) {
      credentials = AliyunOSSUtils.getCredentialsProvider(
          null, conf).getCredentials();
    }

    ClientConfiguration configuration = new ClientConfiguration();
    String endPoint = conf.getTrimmed(Constants.OTS_ENDPOINT_KEY, "");
    if (StringUtils.isEmpty(endPoint)) {
      throw new IllegalArgumentException("Aliyun OTS endpoint should not be " +
          "null or empty. Please set proper endpoint with 'fs.ots.endpoint'.");
    }

    String instance = conf.getTrimmed(Constants.OTS_INSTANCE_KEY, "");
    if (StringUtils.isEmpty(instance)) {
      throw new IllegalArgumentException("Aliyun OTS instance should not be " +
          "null or empty. Please set proper instance with 'fs.ots.instance'.");
    }

    // We could not use bucket name as ots table name,
    // because OTS and OSS have different naming rules.
    table = conf.getTrimmed(Constants.OTS_TABLE_KEY, "");
    if (StringUtils.isEmpty(table)) {
      throw new IllegalArgumentException("Aliyun OTS table should not be " +
          "null or empty. Please set proper table with 'fs.ots.table'.");
    }

    int maxAttempts = conf.getInt(Constants.OTS_TABLE_OPS_RETRIES_KEY,
        Constants.OTS_TABLE_OPS_RETRIES_DEFAULT);

    dataAccessRetryPolicy = RetryPolicies.exponentialBackoffRetry(maxAttempts,
        Constants.OTS_TABLE_OPS_RETRIES_DELAY_MS, TimeUnit.MILLISECONDS);

    batchWriteLimit = conf.getInt(
        Constants.OTS_TABLE_BATCH_WRITE_REQUEST_LIMIT_KEY,
        Constants.OTS_TABLE_BATCH_WRITE_REQUEST_LIMIT_DEFAULT);

    if (StringUtils.isEmpty(credentials.getSecurityToken())) {
      otsClient = new SyncClient(endPoint, credentials.getAccessKeyId(),
          credentials.getSecretAccessKey(), instance, configuration);
    } else {
      otsClient = new SyncClient(endPoint, credentials.getAccessKeyId(),
          credentials.getSecretAccessKey(), instance, configuration,
          credentials.getSecurityToken());
    }

    initTable(conf);
  }

  private void initTable(Configuration conf) throws IOException {
    ListTableResponse response = otsClient.listTable();
    if (!response.getTableNames().contains(table)) {
      boolean createTable = conf.getBoolean(
          Constants.OTS_TABLE_AUTO_CREATE_KEY,
          Constants.OTS_TABLE_AUTO_CREATE_DEFAULT);

      if (!createTable) {
        LOG.debug("{} is false, skip creating table",
            Constants.OTS_TABLE_AUTO_CREATE_KEY);
        throw new IOException("OTS table " + table + " does not exists");
      }

      TableMeta tableMeta = new TableMeta(table);
      tableMeta.addPrimaryKeyColumn(PARENT, PrimaryKeyType.STRING);
      tableMeta.addPrimaryKeyColumn(CHILD, PrimaryKeyType.STRING);

      TableOptions options = new TableOptions(-1, 1);
      int readCap = AliyunOSSUtils.intOption(conf,
          Constants.OTS_TABLE_CAPACITY_READ_KEY,
          Constants.OTS_TABLE_CAPACITY_READ_DEFAULT, 0);

      int writeCap = AliyunOSSUtils.intOption(conf,
          Constants.OTS_TABLE_CAPACITY_WRITE_KEY,
          Constants.OTS_TABLE_CAPACITY_WRITE_DEFAULT, 0);

      try {
        if (readCap > 0 || writeCap > 0) {
          ReservedThroughput throughput = new ReservedThroughput(
              readCap, writeCap);
          otsClient.createTable(
              new CreateTableRequest(tableMeta, options, throughput));
        } else {
          otsClient.createTable(
              new CreateTableRequest(tableMeta, options));
        }
      } catch (TableStoreException tse) {
        if (!tse.getErrorCode().equals(ErrorCode.OBJECT_ALREADY_EXIST)) {
          throw new IOException(tse);
        }
      }

      putVersionMarker();
    } else {
      verifyVersionMarker();
    }
  }

  private void putVersionMarker() {
    RowPutChange rowPutChange = new RowPutChange(table, versionPrimaryKey());
    rowPutChange.addColumn(TABLE_VERSION, ColumnValue.fromLong(VERSION));
    rowPutChange.addColumn(TABLE_CREATED,
        ColumnValue.fromLong(System.currentTimeMillis()));
    otsClient.putRow(new PutRowRequest(rowPutChange));
  }

  private void verifyVersionMarker() throws IOException {
    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(table,
        versionPrimaryKey());
    criteria.setMaxVersions(1);
    Row row = otsClient.getRow(new GetRowRequest(criteria)).getRow();
    int retryCount = 0;
    while (row == null) {
      // Get the version mark item in the existing OTS table.
      // As the version marker item may be created by another concurrent thread
      // or process, we retry a limited times before we fail to get it.
      try {
        retryBackoff(retryCount++);
      } catch (IOException e) {
        LOG.warn("Table {} contains no version marker", table);
        throw new IOException("Table " + table + " contains no version marker");
      }
      row = otsClient.getRow(new GetRowRequest(criteria)).getRow();
    }

    long version = row.getLatestColumn(TABLE_VERSION).getValue().asLong();
    long created = row.getLatestColumn(TABLE_CREATED).getValue().asLong();

    String date = (new Date(created)).toString();
    if (version != VERSION) {
      throw new IOException("Failed to verify table " +
          table + "'s version, current: " +
          version + ", expect: " +
          VERSION + ", created time: " + date);
    }

    LOG.info("table: {}, version: {}, created time: {}", table, version, date);
  }

  @VisibleForTesting
  boolean tableExists(String table) throws Exception {
    return otsClient.listTable().getTableNames().contains(table);
  }

  @VisibleForTesting
  void deleteTable(String table) throws Exception {
    otsClient.deleteTable(new DeleteTableRequest(table));
  }

  @Override
  public void delete(Path path) throws IOException {
    innerDelete(path);
  }

  @Override
  public void forgetMetadata(Path path) throws IOException {
    innerDelete(path);
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
    LOG.debug("Deleting subtree from table {} : {}", table, path);
    final PathMetadata meta = get(path);
    if (meta == null || meta.isDeleted()) {
      LOG.debug("Subtree path {} does not exist; this will be a no-op", path);
      return;
    }

    for (DescendantsIterator desc = new DescendantsIterator(this, meta);
        desc.hasNext();) {
      innerDelete(desc.next().getPath());
    }
  }

  private void innerDelete(Path path) {
    validateOSSPath(path);
    if (path.isRoot()) {
      LOG.debug("Skip deleting root directory as it does not exist in table");
      return;
    }

    RowDeleteChange deleteChange = new RowDeleteChange(
        table, primaryKey(path));
    otsClient.deleteRow(new DeleteRowRequest(deleteChange));
  }

  @Override
  public OTSPathMetadata get(Path path) throws IOException {
    return get(path, false);
  }

  @Override
  public OTSPathMetadata get(Path path, boolean wantEmptyDirectoryFlag)
      throws IOException {
    validateOSSPath(path);
    LOG.debug("Get from table {} : {}", table, path);
    final OTSPathMetadata pm;
    if (path.isRoot()) {
      pm = new OTSPathMetadata(new FileStatus(0, true, 1, 0, 0, 0, null,
          username, username, path));
    } else {
      pm = rowToPathMetadata(getRow(path));
      LOG.debug("Get from table {} returning for {}: {}", table, path, pm);
    }

    if (wantEmptyDirectoryFlag && pm != null) {
      final FileStatus status = pm.getFileStatus();
      if (status.isDirectory()) {
        DirListingMetadata dm = listChildren(path, 1);
        boolean hasChildren = !dm.getListing().isEmpty();
        if (pm.isAuthoritativeDir()) {
          pm.setIsEmptyDirectory(hasChildren ? Tristate.FALSE : Tristate.TRUE);
        } else {
          pm.setIsEmptyDirectory(
              hasChildren ? Tristate.FALSE : Tristate.UNKNOWN);
        }
      }
    }
    return pm;
  }

  private DirListingMetadata listChildren(Path path, int limit)
      throws IOException {
    RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(table);
    criteria.setInclusiveStartPrimaryKey(parentPrimaryKey(path, true));
    criteria.setExclusiveEndPrimaryKey(parentPrimaryKey(path, false));
    criteria.setMaxVersions(1);
    if (limit > 0) {
      criteria.setLimit(1);
    }

    GetRangeRequest request = new GetRangeRequest(criteria);
    final List<PathMetadata> metas = new ArrayList<>();
    while (true) {
      GetRangeResponse response = otsClient.getRange(request);
      for (Row row : response.getRows()) {
        metas.add(rowToPathMetadata(row));
      }

      if (response.getNextStartPrimaryKey() != null) {
        criteria.setInclusiveStartPrimaryKey(response.getNextStartPrimaryKey());
      } else {
        break;
      }
    }

    OTSPathMetadata pm = get(path);
    boolean isAuthoritative = false;
    if (pm != null) {
      isAuthoritative = pm.isAuthoritativeDir();
    }

    LOG.trace("Listing table {} for {} returning {}", table, path, metas);
    return (metas.isEmpty() && pm == null)
        ? null
        : new DirListingMetadata(path, metas, isAuthoritative,
        pm.getLastUpdated());
  }

  @Override
  public DirListingMetadata listChildren(Path path) throws IOException {
    return listChildren(path, -1);
  }

  @Override
  public void move(Collection<Path> pathsToDelete,
      Collection<PathMetadata> pathsToCreate) throws IOException {
    if (pathsToDelete == null && pathsToCreate == null) {
      return;
    }

    LOG.debug("Moving paths of table {}: {} paths to delete and {}"
        + " paths to create", table,
        pathsToDelete == null ? 0 : pathsToDelete.size(),
        pathsToCreate == null ? 0 : pathsToCreate.size());
    LOG.trace("move: pathsToDelete = {}, pathsToCreate = {}", pathsToDelete,
        pathsToCreate);

    // In OTSMetadataStore implementation, we assume that if a path
    // exists, all its ancestors will also exist in the table.
    // Following code is to maintain this invariant by putting all ancestor
    // directories of the paths to create.
    // ancestor paths that are not explicitly added to paths to create

    Collection<OTSPathMetadata> newItems = new ArrayList<>();
    if (pathsToCreate != null) {
      newItems.addAll(completeAncestry(pathMetaToOTSPathMeta(pathsToCreate)));
    }

    List<PrimaryKey> keysToDelete = new ArrayList<>();
    if (pathsToDelete != null) {
      for (Path path : pathsToDelete) {
        keysToDelete.add(primaryKey(path));
      }
    }

    processBatchWriteRequest(keysToDelete, pathMetadataToRowChange(newItems));
  }

  @Override
  public void put(PathMetadata meta) throws IOException {
    // For a deeply nested path, this method will automatically create the full
    // ancestry and save respective item in OTS table.
    // So after put operation, we maintain the invariant that if a path exists,
    // all its ancestors will also exist in the table.
    // For performance purpose, we generate the full paths to put and use batch
    // write row request to save the rows.
    LOG.debug("Saving to table {}: {}", table, meta);
    Collection<PathMetadata> wrapper = new ArrayList<>(1);
    wrapper.add(meta);
    put(wrapper);
  }

  @Override
  public void put(Collection<PathMetadata> metas) throws IOException {
    RowPutChange[] rows = pathMetadataToRowChange(
        completeAncestry(pathMetaToOTSPathMeta(metas)));
    LOG.debug("Saving batch of {} items to table {}", rows.length, table);
    processBatchWriteRequest(null, rows);
  }

  @Override
  public void put(DirListingMetadata meta) throws IOException {
    LOG.debug("Saving to table {}: {}", table, meta);
    Path path = meta.getPath();
    OTSPathMetadata pm =
        new OTSPathMetadata(makeDirStatus(path, username, 0), meta.isEmpty(),
            false, meta.isAuthoritative(), meta.getLastUpdated());

    // First add any missing ancestors...
    final Collection<OTSPathMetadata> metasToPut = fullPathsToPut(pm);

    // next add all children of the directory
    metasToPut.addAll(pathMetaToOTSPathMeta(meta.getListing()));

    processBatchWriteRequest(null, pathMetadataToRowChange(metasToPut));
  }

  @Override
  public void destroy() throws IOException {
    DeleteTableRequest request = new DeleteTableRequest(table);
    try {
      otsClient.deleteTable(request);
    } catch (TableStoreException | ClientException e) {
      LOG.warn("Failed to detory ots table {}, exception {}", table, e);
      throw e;
    }
  }

  @Override
  public void prune(long modTime)
      throws IOException, UnsupportedOperationException {
    prune(modTime, "/");
  }

  @Override
  public void prune(long modTime, String keyPrefix)
      throws IOException, UnsupportedOperationException {
    RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(table);
    criteria.setInclusiveStartPrimaryKey(scanPrimaryKey(true));
    criteria.setExclusiveEndPrimaryKey(scanPrimaryKey(false));
    criteria.setMaxVersions(1);

    SingleColumnValueFilter parentFilter = new SingleColumnValueFilter(PARENT,
        SingleColumnValueFilter.CompareOperator.GREATER_EQUAL,
        ColumnValue.fromString(keyPrefix));

    SingleColumnValueFilter modTimeFilter = new SingleColumnValueFilter(
        MOD_TIME,
        SingleColumnValueFilter.CompareOperator.LESS_THAN,
        ColumnValue.fromLong(modTime));

    CompositeColumnValueFilter filter = new CompositeColumnValueFilter(
        CompositeColumnValueFilter.LogicOperator.AND);
    filter.addFilter(parentFilter);
    filter.addFilter(modTimeFilter);

    criteria.setFilter(filter);
    GetRangeRequest request = new GetRangeRequest(criteria);
    List<PrimaryKey> keysToDelete = new ArrayList<>();
    Set<Path> parentPathSet = new HashSet<>();
    while (true) {
      GetRangeResponse response = otsClient.getRange(request);
      for (Row row : response.getRows()) {
        keysToDelete.add(row.getPrimaryKey());
        FileStatus status = rowToPathMetadata(row).getFileStatus();
        Path parent = status.getPath().getParent();
        if (parent != null) {
          parentPathSet.add(parent);
        }
      }

      if (response.getNextStartPrimaryKey() != null) {
        criteria.setInclusiveStartPrimaryKey(response.getNextStartPrimaryKey());
      } else {
        break;
      }
    }

    List<RowChange> rowChanges = new ArrayList<>();
    for (Path pathToUpdate : parentPathSet) {
      if (pathToUpdate.isRoot()) {
        // Because we do not persist root in OTS table
        continue;
      }

      PrimaryKey pk = primaryKey(pathToUpdate);
      if (keysToDelete.contains(pk)) {
        continue;
      }

      RowUpdateChange change = new RowUpdateChange(table, pk);
      change.put(IS_AUTHORITATIVE, ColumnValue.fromBoolean(false));
      rowChanges.add(change);
    }
    processBatchWriteRequest(keysToDelete,
        rowChanges.toArray(new RowChange[0]));
  }

  @Override
  public Map<String, String> getDiagnostics() throws IOException {
    Map<String, String> diagnostics = new HashMap<>();
    diagnostics.put(PERSISTS_AUTHORITATIVE_BIT,
        conf.get(Constants.METADATASTORE_PERSISTS_AUTHORITATIVE_BIT));

    DescribeTableResponse response = otsClient.describeTable(
        new DescribeTableRequest(table));
    ReservedThroughputDetails detail = response.getReservedThroughputDetails();
    diagnostics.put(Constants.OTS_TABLE_CAPACITY_READ_KEY,
        detail.getCapacityUnit().getReadCapacityUnit() + "");
    diagnostics.put(Constants.OTS_TABLE_CAPACITY_WRITE_KEY,
        detail.getCapacityUnit().getWriteCapacityUnit() + "");
    return diagnostics;
  }

  @Override
  public void updateParameters(Map<String, String> parameters)
      throws IOException {
    DescribeTableResponse response = otsClient.describeTable(
        new DescribeTableRequest(table));

    ReservedThroughputDetails detail = response.getReservedThroughputDetails();
    CapacityUnit capacityUnit = detail.getCapacityUnit();

    int currentRead = capacityUnit.getReadCapacityUnit();
    int currentWrite = capacityUnit.getWriteCapacityUnit();
    int newRead = getIntParam(parameters,
        Constants.OTS_TABLE_CAPACITY_READ_KEY, currentRead);
    int newWrite = getIntParam(parameters,
        Constants.OTS_TABLE_CAPACITY_WRITE_KEY, currentWrite);


    if (newRead != currentRead || newWrite != currentWrite) {
      LOG.info("Current table capacity is read: {}, write: {}",
          currentRead, currentWrite);
      LOG.info("Changing capacity of table to read: {}, write: {}",
          newRead, newWrite);
      UpdateTableRequest request = new UpdateTableRequest(table);
      request.setReservedThroughputForUpdate(
          new ReservedThroughput(newRead, newWrite));

      otsClient.updateTable(request);
    } else {
      LOG.info("Table capacity unchanged at read: {}, write: {}",
          newRead, newWrite);
    }
  }

  @Override
  public void close() throws IOException {
    otsClient.shutdown();
  }

  private Row getRow(Path path) throws IOException {
    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(
        table, primaryKey(path));
    criteria.setMaxVersions(1);
    return otsClient.getRow(new GetRowRequest(criteria)).getRow();
  }

  private int getIntParam(Map<String, String> parameters,
      String key,
      int defVal) {
    String k = parameters.get(key);
    if (k != null) {
      return Integer.parseInt(k);
    } else {
      return defVal;
    }
  }

  private PrimaryKey scanPrimaryKey(boolean start) {
    PrimaryKeyColumn parent, child;
    if (start) {
      parent = new PrimaryKeyColumn(PARENT, PrimaryKeyValue.INF_MIN);
      child = new PrimaryKeyColumn(CHILD, PrimaryKeyValue.INF_MIN);
    } else {
      parent = new PrimaryKeyColumn(PARENT, PrimaryKeyValue.INF_MAX);
      child = new PrimaryKeyColumn(CHILD, PrimaryKeyValue.INF_MAX);
    }

    return new PrimaryKey(Arrays.asList(parent, child));
  }

  private PrimaryKey versionPrimaryKey() {
    PrimaryKeyColumn parent = new PrimaryKeyColumn(PARENT,
        PrimaryKeyValue.fromString(VERSION_MARKER));
    PrimaryKeyColumn child = new PrimaryKeyColumn(CHILD,
        PrimaryKeyValue.fromString(VERSION_MARKER));
    return new PrimaryKey(Arrays.asList(parent, child));
  }

  private PrimaryKey primaryKey(Path path) {
    PrimaryKeyColumn parent = new PrimaryKeyColumn(PARENT,
        PrimaryKeyValue.fromString(pathToParentKey(path.getParent())));
    PrimaryKeyColumn child = new PrimaryKeyColumn(CHILD,
        PrimaryKeyValue.fromString(path.getName()));
    return new PrimaryKey(Arrays.asList(parent, child));
  }

  private PrimaryKey parentPrimaryKey(Path path, boolean start) {
    PrimaryKeyColumn parent = new PrimaryKeyColumn(PARENT,
        PrimaryKeyValue.fromString(pathToParentKey(path)));
    PrimaryKeyColumn child;
    if (start) {
      child = new PrimaryKeyColumn(CHILD, PrimaryKeyValue.INF_MIN);
    } else {
      child = new PrimaryKeyColumn(CHILD, PrimaryKeyValue.INF_MAX);
    }

    return new PrimaryKey(Arrays.asList(parent, child));
  }

  private OTSPathMetadata rowToPathMetadata(Row row) {
    if (row == null) {
      return null;
    }

    PrimaryKey pk = row.getPrimaryKey();
    String parentStr = pk.getPrimaryKeyColumn(PARENT).getValue().asString();
    String childStr = pk.getPrimaryKeyColumn(CHILD).getValue().asString();

    Preconditions.checkNotNull(parentStr, "No parent entry in row %s", row);
    Preconditions.checkNotNull(childStr, "No child entry in row %s", row);

    boolean isDir = false;
    boolean isAuthoritativeDir = false;
    long len = 0, modTime = 0, blockSize = 0, lastUpdated = 0;
    Column[] columns = row.getColumns();
    for (Column column : columns) {
      String key = column.getName();
      ColumnValue value = column.getValue();
      if (key.equals(IS_DIR)) {
        isDir = value.asBoolean();
      } else if (key.equals(IS_AUTHORITATIVE)) {
        isAuthoritativeDir = value.asBoolean();
      } else if (key.equals(FILE_LENGTH)) {
        len = value.asLong();
      } else if (key.equals(MOD_TIME)) {
        modTime = value.asLong();
      } else if (key.equals(BLOCK_SIZE)) {
        blockSize = value.asLong();
      } else if (key.equals(LAST_UPDATED)) {
        lastUpdated = value.asLong();
      }
    }

    Path rawPath = new Path(parentStr, childStr);
    if (!rawPath.isAbsoluteAndSchemeAuthorityNull()) {
      return null;
    }
    Path parent = new Path(Constants.FS_OSS + ":/" + parentStr + "/");
    Path path = new Path(parent, childStr);

    final FileStatus fileStatus;
    if (isDir) {
      fileStatus = makeDirStatus(path, username, modTime);
    } else {
      fileStatus = new FileStatus(len, false, 1, blockSize, modTime, 0, null,
          username, username, path);
    }

    return new OTSPathMetadata(fileStatus, Tristate.UNKNOWN, false,
        isAuthoritativeDir, lastUpdated);
  }

  private RowPutChange[] pathMetadataToRowChange(
      Collection<OTSPathMetadata> pms) {
    RowPutChange[] rowPutChanges = new RowPutChange[pms.size()];
    int index = 0;
    for (OTSPathMetadata pm : pms) {
      rowPutChanges[index++] = pathMetadataToRowChange(pm);
    }
    return rowPutChanges;
  }

  private RowPutChange pathMetadataToRowChange(OTSPathMetadata pm) {
    Preconditions.checkNotNull(pm);
    final FileStatus status = pm.getFileStatus();
    RowPutChange rowPutChange = new RowPutChange(
        table, primaryKey(status.getPath()));

    if (status.isDirectory()) {
      rowPutChange.addColumn(IS_DIR, ColumnValue.fromBoolean(true));
      rowPutChange.addColumn(IS_AUTHORITATIVE,
          ColumnValue.fromBoolean(pm.isAuthoritativeDir()));
    } else {
      rowPutChange.addColumn(FILE_LENGTH,
          ColumnValue.fromLong(status.getLen()));

      rowPutChange.addColumn(BLOCK_SIZE,
          ColumnValue.fromLong(status.getBlockSize()));
    }

    rowPutChange.addColumn(MOD_TIME,
        ColumnValue.fromLong(status.getModificationTime()));

    rowPutChange.addColumn(LAST_UPDATED,
        ColumnValue.fromLong(System.currentTimeMillis()));

    return rowPutChange;
  }

  /** Create a directory FileStatus using current system time as mod time. */
  static FileStatus makeDirStatus(Path f, String owner, long modTime) {
    if (modTime == 0) {
      modTime = System.currentTimeMillis();
    }
    return new FileStatus(0, true, 1, 0, modTime, 0,
        null, owner, owner, f);
  }

  /**
   * Helper method to get full path of ancestors that are nonexistent in table.
   */
  Collection<OTSPathMetadata> fullPathsToPut(OTSPathMetadata pm)
      throws IOException {
    Preconditions.checkNotNull(pm);
    Preconditions.checkNotNull(pm.getFileStatus());
    Preconditions.checkNotNull(pm.getFileStatus().getPath());

    final Collection<OTSPathMetadata> pmsToPut = new ArrayList<>();
    FileStatus status = pm.getFileStatus();
    // root path is not persisted
    if (!status.getPath().isRoot()) {
      pmsToPut.add(pm);
    }

    // put all its ancestors if not present; as an optimization we return at its
    // first existent ancestor
    Path path = status.getPath().getParent();
    while (path != null && !path.isRoot()) {
      Row row = getRow(path);
      if (row != null) {
        break;
      }

      // we mark parent as un-authoritative, consider below case:
      // oss://bucket/dir1
      //   |-- dir2
      //   |   |-- file1
      //   |   `-- file2
      // When we persist oss://bucket/dir1/dir2/file1 to OTS table,
      // oss://bucket/dir1/dir2/file2 has not been persisted to OTS table,
      // SO, oss://bucket/dir1/dir2 should be un-authoritative
      pmsToPut.add(new OTSPathMetadata(makeDirStatus(path, username, 0),
          Tristate.FALSE, false, false, pm.getLastUpdated()));
      path = path.getParent();
    }
    return pmsToPut;
  }

  static List<OTSPathMetadata> pathMetaToOTSPathMeta(
      Collection<PathMetadata> pms) {
    List<OTSPathMetadata> otsPms = new ArrayList<>();
    for (PathMetadata pm : pms) {
      otsPms.add(new OTSPathMetadata(pm));
    }
    return otsPms;
  }

  /**
   * Helper method to issue a batch write request to OTS.
   *
   * As well as retrying on the operation invocation, incomplete
   * batches are retried until all have been deleted.
   * @param keysToDelete primary keys to be deleted; can be null
   * @param rowsToChange new rows to be put or rows to be updated; can be null
   * @return the number of iterations needed to complete the call.
   */
  private int processBatchWriteRequest(List<PrimaryKey> keysToDelete,
      RowChange[] rowsToChange) throws IOException {
    final int totalToDelete = (keysToDelete == null ? 0 : keysToDelete.size());
    final int totalToPut = (rowsToChange == null ? 0 : rowsToChange.length);

    int count = 0;
    int batches = 0;
    while (count < totalToDelete + totalToPut) {
      BatchWriteRowRequest request = new BatchWriteRowRequest();
      int numToDelete = 0;
      if (keysToDelete != null && count < totalToDelete) {
        numToDelete = Math.min(batchWriteLimit, totalToDelete - count);
        for (int i = count; i < count + numToDelete; ++i) {
          request.addRowChange(
              new RowDeleteChange(table, keysToDelete.get(i)));
        }
        count += numToDelete;
      }

      if (numToDelete < batchWriteLimit && rowsToChange != null
          && count < totalToDelete + totalToPut) {
        final int numToPut = Math.min(batchWriteLimit - numToDelete,
            totalToDelete + totalToPut - count);
        int index = count - totalToDelete;
        for (int i = index; i < index + numToPut; ++i) {
          request.addRowChange(rowsToChange[i]);
        }
        count += numToPut;
      }

      // if there's a retry and another process updates things then it's not
      // quite idempotent, but this was the case anyway
      batches++;

      BatchWriteRowResponse response = otsClient.batchWriteRow(request);
      int retryCount = 0;
      while (response.getFailedRows().size() > 0) {
        BatchWriteRowRequest retryRequest = request.createRequestForRetry(response.getFailedRows());
        retryBackoff(retryCount++);
        response = otsClient.batchWriteRow(retryRequest);
        request = retryRequest;
      }
    }

    return batches;
  }

  private void retryBackoff(int retryCount) throws IOException {
    try {
      RetryPolicy.RetryAction action = dataAccessRetryPolicy.shouldRetry(null,
          retryCount, 0, true);
      if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
        throw new IOException(
            String.format("Max retries exceeded (%d) for OTS", retryCount));
      } else {
        LOG.debug("Sleeping {} msec before next retry", action.delayMillis);
        Thread.sleep(action.delayMillis);
      }
    } catch (Exception e) {
      throw new IOException("Unexpected exception", e);
    }
  }

  /**
   * build the list of all parent entries.
   * @param pathsToCreate paths to create
   * @return the full ancestry paths
   */
  Collection<OTSPathMetadata> completeAncestry(
      Collection<OTSPathMetadata> pathsToCreate) {
    // Key on path to allow fast lookup
    Map<Path, OTSPathMetadata> ancestry = new HashMap<>();
    for (OTSPathMetadata meta : pathsToCreate) {
      Preconditions.checkArgument(meta != null);

      Path path = meta.getFileStatus().getPath();
      if (path.isRoot()) {
        continue;
      }

      ancestry.put(path, new OTSPathMetadata(meta));
      Path parent = path.getParent();
      while (!parent.isRoot() && !ancestry.containsKey(parent)) {
        LOG.debug("auto-create ancestor path {} for child path {}",
            parent, path);
        final FileStatus status = makeDirStatus(parent, username, 0);
        ancestry.put(parent,
            new OTSPathMetadata(status, Tristate.FALSE, false));
        parent = parent.getParent();
      }
    }

    return ancestry.values();
  }
}
