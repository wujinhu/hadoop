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

package org.apache.hadoop.fs.aliyun.oss;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.NullMetadataStore;
import org.apache.hadoop.cloud.core.metadata.Tristate;
import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.aliyun.oss.guard.OSSGuard;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;

import com.aliyun.oss.model.ObjectMetadata;

import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.createFileStatus;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.intOption;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.keyToPath;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.longOption;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.objectRepresentsDirectory;
import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * Implementation of {@link FileSystem} for <a href="https://oss.aliyun.com">
 * Aliyun OSS</a>, used to access OSS blob system in a filesystem style.
 */
public class AliyunOSSFileSystem extends FileSystem {
  private static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSFileSystem.class);
  private URI uri;
  private String bucket;
  private String username;
  private Path workingDir;
  private int blockOutputActiveBlocks;
  private AliyunOSSFileSystemStore store;
  private int maxKeys;
  private int maxReadAheadPartNumber;
  private int maxConcurrentCopyTasksPerDir;
  private ListeningExecutorService boundedThreadPool;
  private ListeningExecutorService boundedCopyThreadPool;

  private MetadataStore metadataStore;
  private boolean authoritativeMeta;
  private OSSGuard ossGuard;

  private static final PathFilter DEFAULT_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      return true;
    }
  };

  @Override
  public FSDataOutputStream append(Path path, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Append is not supported!");
  }

  @Override
  public void close() throws IOException {
    try {
      store.close();
      boundedThreadPool.shutdown();
      boundedCopyThreadPool.shutdown();
    } finally {
      super.close();
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    String key = pathToKey(path);
    FileStatus status = null;

    try {
      // get the status or throw a FNFE
      status = getFileStatus(path);

      // if the thread reaches here, there is something at the path
      if (status.isDirectory()) {
        // path references a directory
        throw new FileAlreadyExistsException(path + " is a directory");
      }
      if (!overwrite) {
        // path references a file and overwrite is disabled
        throw new FileAlreadyExistsException(path + " already exists");
      }
      LOG.debug("Overwriting file {}", path);
    } catch (FileNotFoundException e) {
      // this means the file is not found
    }

    long uploadPartSize = AliyunOSSUtils.getMultipartSizeProperty(getConf(),
        MULTIPART_UPLOAD_PART_SIZE_KEY, MULTIPART_UPLOAD_PART_SIZE_DEFAULT);

    return new FSDataOutputStream(
        new AliyunOSSBlockOutputStream(getConf(),
            store,
            key,
            uploadPartSize,
            ossGuard,
            new SemaphoredDelegatingExecutor(boundedThreadPool,
                blockOutputActiveBlocks, true)), statistics);
  }

  /**
   * {@inheritDoc}
   * @throws FileNotFoundException if the parent directory is not present -or
   * is not a directory.
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      // expect this to raise an exception if there is no parent
      if (!getFileStatus(parent).isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }
    return create(path, permission, flags.contains(CreateFlag.OVERWRITE),
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      return innerDelete(getFileStatus(path, true), recursive);
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist", path);
      return false;
    }
  }

  /**
   * Delete an object. See {@link #delete(Path, boolean)}.
   *
   * @param status fileStatus object
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   */
  private boolean innerDelete(OSSFileStatus status, boolean recursive)
      throws IOException {
    Path f = status.getPath();

    String key = pathToKey(f);
    if (status.isDirectory()) {
      Preconditions.checkArgument(
          status.isEmptyDirectory() != Tristate.UNKNOWN,
          "File status must have directory emptiness computed");

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if (key.equals("/")) {
        return rejectRootDirectoryDelete(
            status.isEmptyDirectory() == Tristate.TRUE, recursive);
      }

      if (!recursive && status.isEmptyDirectory() == Tristate.FALSE) {
        throw new IOException("Cannot remove directory " + f +
            ": It is not empty!");
      }
      if (status.isEmptyDirectory() == Tristate.TRUE) {
        // Delete empty directory without '-r'
        key = AliyunOSSUtils.maybeAddTrailingSlash(key);
        store.deleteObject(key);
        metadataStore.delete(f);
      } else {
        store.deleteDirs(key);
        metadataStore.deleteSubtree(f);
      }
    } else {
      store.deleteObject(key);
      metadataStore.delete(f);
    }

    createFakeDirectoryIfNecessary(f);
    return true;
  }

  /**
   * Implements the specific logic to reject root directory deletion.
   * The caller must return the result of this call, rather than
   * attempt to continue with the delete operation: deleting root
   * directories is never allowed. This method simply implements
   * the policy of when to return an exit code versus raise an exception.
   * @param isEmptyDir empty directory or not
   * @param recursive recursive flag from command
   * @return a return code for the operation
   * @throws PathIOException if the operation was explicitly rejected.
   */
  private boolean rejectRootDirectoryDelete(boolean isEmptyDir,
      boolean recursive) throws IOException {
    LOG.info("oss delete the {} root directory of {}", bucket, recursive);
    if (isEmptyDir) {
      return true;
    }
    if (recursive) {
      return false;
    } else {
      // reject
      throw new PathIOException(bucket, "Cannot delete root path");
    }
  }

  private void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    if (StringUtils.isNotEmpty(key) && !exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      mkdir(pathToKey(f.getParent()));
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return getFileStatus(path, false);
  }

  public OSSFileStatus getFileStatus(Path path, boolean needEmptyDirectoryFlag)
      throws IOException {
    Path qualifiedPath = path.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);

    OSSFileStatus status;
    PathMetadata pm = metadataStore.get(qualifiedPath, needEmptyDirectoryFlag);
    if (pm != null && authoritativeMeta && !pm.isDeleted()) {
      if (pm.getFileStatus().isDirectory()) {
        if (pm.isEmptyDirectory() != Tristate.UNKNOWN
            || !needEmptyDirectoryFlag) {
          return OSSFileStatus.fromFileStatus(pm.getFileStatus(),
              pm.isEmptyDirectory());
        } else {
          LOG.debug(
              "MetadataStore doesn't know if dir {} is empty, using OSS.", qualifiedPath);
        }
      } else {
        // Either this is not a directory, or we don't care if it is empty
        return OSSFileStatus.fromFileStatus(pm.getFileStatus(),
            pm.isEmptyDirectory());
      }
    }

    Tristate isEmptyDirectory;
    // Root always exists
    if (key.length() == 0) {
      if (needEmptyDirectoryFlag) {
        isEmptyDirectory = store.isEmptyDirectory(path, key);
      } else {
        isEmptyDirectory = Tristate.UNKNOWN;
      }
      return createFileStatus(qualifiedPath, true,0, 0, 0,
          username, isEmptyDirectory);
    }

    // there was no entry in OSSGuard
    // retrieve the data and update the metadata store in the process.
    ObjectMetadata meta = store.getObjectMetadata(key);
    // If key not found and key does not end with "/"
    if (meta == null && !key.endsWith("/")) {
      // In case of 'dir + "/"'
      key += "/";
      meta = store.getObjectMetadata(key);
    }

    if (meta == null) {
      isEmptyDirectory = store.isEmptyDirectory(path, key);
      status = createFileStatus(qualifiedPath, true, 0, 0, 0, username,
          isEmptyDirectory);
    } else if (objectRepresentsDirectory(key, meta.getContentLength())) {
      if (needEmptyDirectoryFlag) {
        isEmptyDirectory = store.isEmptyDirectory(path, key);
      } else {
        isEmptyDirectory = Tristate.UNKNOWN;
      }
      status = createFileStatus(qualifiedPath, true, 0,
          meta.getLastModified().getTime(), 0, username, isEmptyDirectory);
    } else {
      isEmptyDirectory = Tristate.UNKNOWN;
      status = createFileStatus(qualifiedPath, false, meta.getContentLength(),
          meta.getLastModified().getTime(), getDefaultBlockSize(path),
          username, isEmptyDirectory);
    }

    metadataStore.put(new PathMetadata(status, status.isEmptyDirectory()));
    return status;
  }

  @Override
  public String getScheme() {
    return "oss";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public int getDefaultPort() {
    return Constants.OSS_DEFAULT_PORT;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Deprecated
  public long getDefaultBlockSize() {
    return getConf().getLong(FS_OSS_BLOCK_SIZE_KEY, FS_OSS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  /**
   * Initialize new FileSystem.
   *
   * @param name the uri of the file system, including host, port, etc.
   * @param conf configuration of the file system
   * @throws IOException IO problems
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);

    bucket = name.getHost();
    uri = java.net.URI.create(name.getScheme() + "://" + name.getAuthority());
    // Username is the current user at the time the FS was instantiated.
    username = UserGroupInformation.getCurrentUser().getShortUserName();
    workingDir = new Path("/user", username).makeQualified(uri, null);
    long keepAliveTime = longOption(conf,
        KEEPALIVE_TIME_KEY, KEEPALIVE_TIME_DEFAULT, 0);
    blockOutputActiveBlocks = intOption(conf,
        UPLOAD_ACTIVE_BLOCKS_KEY, UPLOAD_ACTIVE_BLOCKS_DEFAULT, 1);

    store = new AliyunOSSFileSystemStore();
    store.initialize(name, conf, username, statistics);
    maxKeys = conf.getInt(MAX_PAGING_KEYS_KEY, MAX_PAGING_KEYS_DEFAULT);

    int threadNum = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MULTIPART_DOWNLOAD_THREAD_NUMBER_KEY,
        Constants.MULTIPART_DOWNLOAD_THREAD_NUMBER_DEFAULT);

    int totalTasks = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MAX_TOTAL_TASKS_KEY, Constants.MAX_TOTAL_TASKS_DEFAULT);

    maxReadAheadPartNumber = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MULTIPART_DOWNLOAD_AHEAD_PART_MAX_NUM_KEY,
        Constants.MULTIPART_DOWNLOAD_AHEAD_PART_MAX_NUM_DEFAULT);

    this.boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
        threadNum, totalTasks, keepAliveTime, TimeUnit.SECONDS,
        "oss-transfer-shared");

    maxConcurrentCopyTasksPerDir = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MAX_CONCURRENT_COPY_TASKS_PER_DIR_KEY,
        Constants.MAX_CONCURRENT_COPY_TASKS_PER_DIR_DEFAULT);

    int maxCopyThreads = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MAX_COPY_THREADS_NUM_KEY,
        Constants.MAX_COPY_THREADS_DEFAULT);

    int maxCopyTasks = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MAX_COPY_TASKS_KEY,
        Constants.MAX_COPY_TASKS_DEFAULT);

    this.boundedCopyThreadPool = BlockingThreadPoolExecutorService.newInstance(
        maxCopyThreads, maxCopyTasks, 60L,
        TimeUnit.SECONDS, "oss-copy-unbounded");

    setConf(conf);

    this.metadataStore = OSSGuard.getMetadataStore(this);
    this.authoritativeMeta = conf.getBoolean(METADATASTORE_AUTHORITATIVE,
        METADATASTORE_AUTHORITATIVE_DEFAULT);
    if (this.metadataStore instanceof NullMetadataStore) {
      this.authoritativeMeta = false;
      LOG.debug("Using metadata store {}, set {} to false",
          metadataStore, METADATASTORE_AUTHORITATIVE);
    }
    LOG.info("Using metadata store {}, authoritative = {}",
        metadataStore.getClass().getName(), authoritativeMeta);
    this.ossGuard = newOSSGuard();
  }

  private String pathToKey(Path path) {
    return AliyunOSSUtils.pathToKey(path, this);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    String key = pathToKey(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("List status for path: " + path);
    }

    final List<FileStatus> result = new ArrayList<FileStatus>();
    final FileStatus fileStatus = getFileStatus(path);

    if (fileStatus.isDirectory()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("listStatus: doing listObjects for directory " + key);
      }

      Path qualified = makeQualified(path);
      RemoteIterator<LocatedFileStatus> children = innerList(qualified,
          fileStatus, DEFAULT_FILTER,
          new FileStatusAcceptor.AcceptAllButSelf(qualified), false);
      while (children.hasNext()) {
        LocatedFileStatus child = children.next();
        result.add(child);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding: rd (not a dir): " + path);
      }
      result.add(fileStatus);
    }

    return result.toArray(new FileStatus[result.size()]);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(
      final Path f, final boolean recursive) throws IOException {
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    final FileStatus status = getFileStatus(qualifiedPath);
    PathFilter filter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return status.isFile() || !path.equals(f);
      }
    };
    FileStatusAcceptor acceptor =
        new FileStatusAcceptor.AcceptFilesOnly(qualifiedPath);
    return innerList(f, status, filter, acceptor, recursive);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws IOException {
    return listLocatedStatus(f, DEFAULT_FILTER);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter) throws IOException {
    return listLocatedStatus(f, filter, false);
  }

  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter, boolean recursive) throws IOException {
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    final FileStatus status = getFileStatus(qualifiedPath);
    FileStatusAcceptor acceptor =
        new FileStatusAcceptor.AcceptAllButSelf(qualifiedPath);
    return innerList(f, status, filter, acceptor, recursive);
  }

  private RemoteIterator<LocatedFileStatus> innerList(final Path f,
      final FileStatus status,
      final PathFilter filter,
      final FileStatusAcceptor acceptor,
      final boolean recursive) throws IOException {
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);

    if (status.isFile()) {
      LOG.debug("{} is a File", qualifiedPath);
      final BlockLocation[] locations = getFileBlockLocations(status,
          0, status.getLen());
      return store.singleStatusRemoteIterator(filter.accept(f) ? status : null,
          locations);
    } else {
      return store.createLocatedFileStatusIterator(authoritativeMeta,
          ossGuard, key, maxKeys, this, filter,
          acceptor, recursive ? null : "/");
    }
  }

  /**
   * Used to create an empty file that represents an empty directory.
   *
   * @param key directory path
   * @return true if directory is successfully created
   * @throws IOException
   */
  private boolean mkdir(final String key) throws IOException {
    String dirName = key;
    if (StringUtils.isNotEmpty(key)) {
      if (!key.endsWith("/")) {
        dirName += "/";
      }
      store.storeEmptyFile(dirName);
      ossGuard.putPath(dirName, 0, true, false);
    }
    return true;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission)
      throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(path);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + path);
      }
    } catch (FileNotFoundException e) {
      validatePath(path);
      String key = pathToKey(path);
      return mkdir(key);
    }
  }

  /**
   * Check whether the path is a valid path.
   *
   * @param path the path to be checked.
   * @throws IOException
   */
  private void validatePath(Path path) throws IOException {
    Path fPart = path.getParent();
    do {
      try {
        FileStatus fileStatus = getFileStatus(fPart);
        if (fileStatus.isDirectory()) {
          // If path exists and a directory, exit
          break;
        } else {
          throw new FileAlreadyExistsException(String.format(
              "Can't make directory for path '%s', it is a file.", fPart));
        }
      } catch (FileNotFoundException fnfe) {
      }
      fPart = fPart.getParent();
    } while (fPart != null);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    final FileStatus fileStatus = getFileStatus(path);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + path +
          " because it is a directory");
    }

    return new FSDataInputStream(new AliyunOSSInputStream(getConf(),
        new SemaphoredDelegatingExecutor(
            boundedThreadPool, maxReadAheadPartNumber, true),
        maxReadAheadPartNumber, store, pathToKey(path), fileStatus.getLen(),
        statistics));
  }

  @Override
  public boolean rename(Path srcPath, Path dstPath) throws IOException {
    if (srcPath.isRoot()) {
      // Cannot rename root of file system
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot rename the root of a filesystem");
      }
      return false;
    }
    Path parent = dstPath.getParent();
    while (parent != null && !srcPath.equals(parent)) {
      parent = parent.getParent();
    }
    if (parent != null) {
      return false;
    }
    FileStatus srcStatus = getFileStatus(srcPath);
    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dstPath);
    } catch (FileNotFoundException fnde) {
      dstStatus = null;
    }
    if (dstStatus == null) {
      // If dst doesn't exist, check whether dst dir exists or not
      dstStatus = getFileStatus(dstPath.getParent());
      if (!dstStatus.isDirectory()) {
        throw new IOException(String.format(
            "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
            dstPath.getParent()));
      }
    } else {
      if (srcStatus.getPath().equals(dstStatus.getPath())) {
        return !srcStatus.isDirectory();
      } else if (dstStatus.isDirectory()) {
        // If dst is a directory
        dstPath = new Path(dstPath, srcPath.getName());
        FileStatus[] statuses;
        try {
          statuses = listStatus(dstPath);
        } catch (FileNotFoundException fnde) {
          statuses = null;
        }
        if (statuses != null && statuses.length > 0) {
          // If dst exists and not a directory / not empty
          throw new FileAlreadyExistsException(String.format(
              "Failed to rename %s to %s, file already exists or not empty!",
              srcPath, dstPath));
        }
      } else {
        // If dst is not a directory
        throw new FileAlreadyExistsException(String.format(
            "Failed to rename %s to %s, file already exists!", srcPath,
            dstPath));
      }
    }

    // If we have a MetadataStore, track deletions/creations.
    Collection<Path> srcPaths = null;
    List<PathMetadata> dstMetas = null;
    if (hasMetadataStore()) {
      srcPaths = new ArrayList<>();
      dstMetas = new ArrayList<>();
    }

    boolean succeed;
    if (srcStatus.isDirectory()) {
      succeed = copyDirectory(srcPath, dstPath, srcPaths, dstMetas);
    } else {
      succeed = copyFile(srcPath, srcStatus.getLen(), dstPath,
          srcPaths, dstMetas);
    }

    if (succeed) {
      metadataStore.move(srcPaths, dstMetas);
    }
    return succeed && innerDelete(
        OSSFileStatus.fromFileStatus(srcStatus, Tristate.FALSE), true);
  }

  /**
   * Copy file from source path to destination path.
   * (the caller should make sure srcPath is a file and dstPath is valid)
   *
   * @param srcPath source path.
   * @param srcLen source path length if it is a file.
   * @param dstPath destination path.
   */
  private boolean copyFile(Path srcPath, long srcLen, Path dstPath,
      Collection<Path> srcPaths, List<PathMetadata> dstMetas) {
    String srcKey = pathToKey(srcPath);
    String dstKey = pathToKey(dstPath);
    ossGuard.addMoveFile(srcPaths, dstMetas, srcPath, dstPath, srcLen);
    return store.copyFile(srcKey, srcLen, dstKey);
  }

  /**
   * Copy a directory from source path to destination path.
   * (the caller should make sure srcPath is a directory, and dstPath is valid)
   *
   * @param srcPath source path.
   * @param dstPath destination path.
   * @return true if directory is successfully copied.
   */
  private boolean copyDirectory(Path srcPath, Path dstPath,
      Collection<Path> srcPaths,
      List<PathMetadata> dstMetas) throws IOException {
    String srcKey = AliyunOSSUtils
        .maybeAddTrailingSlash(pathToKey(srcPath));
    String dstKey = AliyunOSSUtils
        .maybeAddTrailingSlash(pathToKey(dstPath));

    if (dstKey.startsWith(srcKey)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot rename a directory to a subdirectory of self");
      }
      return false;
    }

    mkdir(dstKey);
    List<CopyFileEntry> copyFiles = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> children = listLocatedStatus(srcPath,
        DEFAULT_FILTER, true);
    while (children.hasNext()) {
      LocatedFileStatus child = children.next();
      if (srcPaths != null) {
        srcPaths.add(makeQualified(child.getPath()));
      }
      String src = pathToKey(child.getPath());
      if (child.isDirectory()) {
        src = AliyunOSSUtils.maybeAddTrailingSlash(src);
      }

      String newKey = dstKey.concat(src.substring(srcKey.length()));
      OSSFileStatus fileStatus = createFileStatus(
          makeQualified(keyToPath(newKey)), child.isDirectory(),
          child.getLen(), child.getModificationTime(),
          child.getBlockSize(), username, Tristate.UNKNOWN);

      if (dstMetas != null) {
        dstMetas.add(new PathMetadata(fileStatus));
      }

      copyFiles.add(
          new CopyFileEntry(src, newKey, child.getLen(), child.isDirectory()));
    }

    AliyunOSSCopyFileContext copyFileContext = new AliyunOSSCopyFileContext();
    ExecutorService executorService = MoreExecutors.listeningDecorator(
        new SemaphoredDelegatingExecutor(boundedCopyThreadPool,
            maxConcurrentCopyTasksPerDir, true));
    int copiesToFinish = 0;
    for (CopyFileEntry copyFile : copyFiles) {
      //copy operation just copies metadata, oss will support shallow copy
      executorService.execute(new AliyunOSSCopyFileTask(
          store, copyFile.getSrcKey(),
          copyFile.getSize(),
          copyFile.getDstKey(), copyFileContext));
      copiesToFinish++;
      // No need to call lock() here.
      // It's ok to copy one more file if the rename operation failed
      // Reduce the call of lock() can also improve our performance
      if (copyFileContext.isCopyFailure()) {
        //some error occurs, break
        break;
      }
    }

    //wait operations in progress to finish
    copyFileContext.lock();
    try {
      copyFileContext.awaitAllFinish(copiesToFinish);
    } catch (InterruptedException e) {
      LOG.warn("interrupted when wait copies to finish");
    } finally {
      copyFileContext.unlock();
    }
    return !copyFileContext.isCopyFailure();
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    this.workingDir = dir;
  }

  public AliyunOSSFileSystemStore getStore() {
    return store;
  }

  public OSSGuard newOSSGuard() {
    return new OSSGuard(this, username);
  }

  public String getUsername() {
    return username;
  }

  /**
   * Does this Filesystem have a metadata store?
   * @return true iff the FS has been instantiated with a metadata store
   */
  public boolean hasMetadataStore() {
    return !OSSGuard.isNullMetadataStore(metadataStore);
  }

  public MetadataStore getMetadataStore() {
    return metadataStore;
  }
}
