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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.cloud.core.metadata.DirListingMetadata;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.cloud.core.metadata.Tristate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.cloud.core.metadata.MetadataStoreCapabilities.PERSISTS_AUTHORITATIVE_BIT;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FS_OSS;
import static org.apache.hadoop.fs.aliyun.oss.Constants.METADATASTORE_PERSISTS_AUTHORITATIVE_BIT;

/**
 * HDFS implementation of {@link MetadataStore}.
 * Implementation details:
 * 1. We will create a directory if OSS has a `directory`;
 * 2. We will add an extended attribute(user.authoritative) to created
 * directory to indicate whether it is authoritative
 * 3. We will create an empty file if OSS has an object.
 * 4. We store length of created empty to its extended attribute
 *  attribute name is 'user.file-length'
 *
 * NOTICE:
 * However, this implementation may not work well if one directory contains
 * a lot of files because name node may under a lot of pressure.
 */
public class HadoopMetadataStore extends AbstractMetadataStore {
  public static final Logger LOG = LoggerFactory.getLogger(
      HadoopMetadataStore.class);

  private static final String XATTR_FILE_LENGTH = "user.file-length";
  private static final String XATTR_AUTHORITATIVE = "user.authoritative";

  // Backend filesystem used to persist metadata
  private FileSystem metaFileSystem;

  // Base metadata path
  private String metadataPath;

  private ListeningExecutorService executorService;

  @Override
  public void initialize(Configuration conf) throws IOException {
    if (this.conf == null) {
      this.conf = conf;
    }

    if (username == null) {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    }

    metadataPath = conf.getTrimmed(Constants.OSS_METASTORE_METADATA_PATH);
    Preconditions.checkArgument(!metadataPath.isEmpty());

    if (!metadataPath.endsWith("/")) {
      metadataPath += "/";
    }

    Path mPath = new Path(metadataPath);
    metaFileSystem = FileSystem.get(mPath.toUri(), conf);

    int maxMetaOpsThreads = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MAX_METADATASTORE_OPS_THREADS_NUM_KEY,
        Constants.MAX_METADATASTORE_OPS_THREADS_DEFAULT);

    int maxMetaOpsTasks = AliyunOSSUtils.intPositiveOption(conf,
        Constants.MAX_METADATASTORE_OPS_TASKS_KEY,
        Constants.MAX_METADATASTORE_OPS_TASKS_DEFAULT);

    int activeMetaOps = AliyunOSSUtils.intPositiveOption(conf,
        Constants.METADATASTORE_ACTIVE_OPS_KEY,
        Constants.METADATASTORE_ACTIVE_OPS_DEFAULT);

    ListeningExecutorService threadPool =
        BlockingThreadPoolExecutorService.newInstance(maxMetaOpsThreads,
            maxMetaOpsTasks, 60L, TimeUnit.SECONDS, "hadoop-metadata-store");
    executorService = MoreExecutors.listeningDecorator(
        new SemaphoredDelegatingExecutor(threadPool, activeMetaOps, true));
  }

  @Override
  public void delete(Path path) throws IOException {
    innerDelete(pathTranslation(path, false), false);
  }

  @Override
  public void forgetMetadata(Path path) throws IOException {
    innerDelete(pathTranslation(path, false), false);
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
    innerDelete(pathTranslation(path, false), true);
  }

  private void innerDelete(Path mPath, boolean recursive) throws IOException {
    try {
      FileStatus status = metaFileSystem.getFileStatus(mPath);
      if (status.isDirectory()) {
        if (!recursive && metaFileSystem.listStatus(mPath).length > 0) {
          throw new IOException("Cannot remove directory " + mPath +
              ": It is not empty!");
        }
        metaFileSystem.delete(mPath, true);
      } else {
        metaFileSystem.delete(mPath, false);
        metaFileSystem.removeXAttr(mPath, XATTR_FILE_LENGTH);
      }
    } catch (FileNotFoundException e) {
      // Ignore this exception.
      LOG.debug("{}", e);
    }
  }

  private FileStatus transform(FileStatus status) throws IOException {
    Path dPath = pathTranslation(status.getPath(), true);
    long fileLength = 0;
    if (status.isFile()) {
      fileLength = Long.parseLong(new String(
          metaFileSystem.getXAttr(status.getPath(), XATTR_FILE_LENGTH)));
    }

    return new FileStatus(fileLength, status.isDirectory(),
        1, conf.getLong(Constants.FS_OSS_BLOCK_SIZE_KEY,
            Constants.FS_OSS_BLOCK_SIZE_DEFAULT),
        status.getModificationTime(), 0, null,
        username, username, dPath);
  }

  @Override
  public PathMetadata get(Path path) throws IOException {
    return get(path, false);
  }

  @Override
  public PathMetadata get(Path path, boolean wantEmptyDirectoryFlag)
      throws IOException {
    validateOSSPath(path);
    Path mPath = pathTranslation(path, false);
    try {
      final PathMetadata meta;
      if (mPath.isRoot()) {
        meta = new PathMetadata(
            new FileStatus(0, true, 1, 0, 0, 0, null,
                username, username, path));
      } else {
        FileStatus status = metaFileSystem.getFileStatus(mPath);
        meta = new PathMetadata(transform(status));
      }

      if (wantEmptyDirectoryFlag) {
        if (meta.getFileStatus().isDirectory()) {
          if (metaFileSystem.listStatus(mPath).length > 0) {
            meta.setIsEmptyDirectory(Tristate.FALSE);
          } else {
            meta.setIsEmptyDirectory(Tristate.UNKNOWN);
          }
        }
      }

      return meta;
    } catch (FileNotFoundException e) {
      LOG.debug("{}", e);
      return null;
    }
  }

  @Override
  public DirListingMetadata listChildren(Path path) throws IOException {
    validateOSSPath(path);
    Path mPath = pathTranslation(path, false);
    try {
      final List<PathMetadata> metas;
      List<ListenableFuture<PathMetadata>> futures = new ArrayList<>();
      FileStatus[] statuses = metaFileSystem.listStatus(mPath);
      for (final FileStatus status : statuses) {
        ListenableFuture<PathMetadata> future = executorService.submit(
          new Callable<PathMetadata>() {
            @Override
            public PathMetadata call() throws Exception {
              return new PathMetadata(transform(status));
            }
          });
        futures.add(future);
      }

      try {
        metas = new ArrayList<>(Futures.allAsList(futures).get());
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException(e);
      }
      DirListingMetadata dirMeta = new DirListingMetadata(path, metas, false);
      try {
        dirMeta.setAuthoritative(Boolean.parseBoolean(
            new String(metaFileSystem.getXAttr(mPath, XATTR_AUTHORITATIVE))));
      } catch (IOException e) {
        // pass
      }

      return dirMeta;
    } catch (FileNotFoundException e) {
      LOG.debug("{}", e);
      return null;
    }
  }

  @Override
  public void move(Collection<Path> pathsToDelete,
      Collection<PathMetadata> pathsToCreate) throws IOException {
    Preconditions.checkNotNull(pathsToDelete);
    Preconditions.checkNotNull(pathsToCreate);
    Preconditions.checkArgument(pathsToDelete.size() == pathsToCreate.size(),
        "Sizes of pathsToDelete and pathsToCreate are not equal!");

    Iterator<Path> deleteIterator = pathsToDelete.iterator();
    Iterator<PathMetadata> createIterator = pathsToCreate.iterator();
    while (deleteIterator.hasNext() && createIterator.hasNext()) {
      Path mSrcPath = pathTranslation(deleteIterator.next(), false);
      FileStatus status = createIterator.next().getFileStatus();
      Path mDstPath = pathTranslation(status.getPath(), false);
      Path dstParent = mDstPath.getParent();
      if (!metaFileSystem.exists(mSrcPath)) {
        continue;
      }

      if (!metaFileSystem.exists(dstParent)) {
        metaFileSystem.mkdirs(dstParent);
      }

      metaFileSystem.rename(mSrcPath, mDstPath);
    }
  }

  @Override
  public void put(PathMetadata meta) throws IOException {
    FileStatus status = meta.getFileStatus();
    Path qualifiedPath = pathTranslation(status.getPath(), false);

    if (status.isDirectory()) {
      metaFileSystem.mkdirs(qualifiedPath);
      metaFileSystem.setXAttr(qualifiedPath, XATTR_AUTHORITATIVE,
          "false".getBytes());
    } else {
      metaFileSystem.createNewFile(qualifiedPath);
      metaFileSystem.setXAttr(qualifiedPath, XATTR_FILE_LENGTH,
          String.valueOf(status.getLen()).getBytes());
    }

    metaFileSystem.setTimes(qualifiedPath,
        status.getModificationTime(),
        status.getAccessTime());
  }

  @Override
  public void put(Collection<PathMetadata> metas) throws IOException {
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    for (final PathMetadata pathMetadata : metas) {
      ListenableFuture<Boolean> future = executorService.submit(
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            put(pathMetadata);
            return Boolean.TRUE;
          }
        });
      futures.add(future);
    }

    try {
      Futures.allAsList(futures).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(DirListingMetadata meta) throws IOException {
    Path metaPath = pathTranslation(meta.getPath(), false);
    metaFileSystem.mkdirs(metaPath);
    
    put(meta.getListing());

    metaFileSystem.setXAttr(metaPath, XATTR_AUTHORITATIVE,
        String.valueOf(meta.isAuthoritative()).getBytes());
  }

  @Override
  public void destroy() throws IOException {
    Path metaPath = makeQualified(new Path(metadataPath));
    metaFileSystem.delete(metaPath, true);
  }

  @Override
  public void prune(long modTime)
      throws IOException, UnsupportedOperationException {
    prune(modTime, "/");
  }

  @Override
  public void prune(long modTime, String keyPrefix)
      throws IOException, UnsupportedOperationException {
    Path metaPath = pathTranslation(
        AliyunOSSUtils.keyToPath(keyPrefix), false);
    class PruneListIterator implements RemoteIterator<FileStatus> {
      private Stack<FileStatus> children = new Stack<>();
      private Set<Path> visited = new HashSet<>();
      private FileStatus currentFileStatus;
      private Map<Path, Integer> directChildren = new HashMap<>();

      private PruneListIterator(Path path) throws IOException {
        for (FileStatus status : metaFileSystem.listStatus(path)) {
          children.push(status);
        }

        directChildren.put(path, children.size());
        currentFileStatus = children.peek();
        visited.add(path);
      }

      public int getDirectChildren(Path path) {
        return directChildren.get(path);
      }

      @Override
      public boolean hasNext() throws IOException {
        return !children.isEmpty();
      }

      @Override
      public FileStatus next() throws IOException {
        FileStatus top = children.peek();
        while (!visited.contains(top.getPath())) {
          if (top.isDirectory()) {
            FileStatus[] statuses =
                metaFileSystem.listStatus(top.getPath());
            visited.add(top.getPath());
            for (FileStatus child : statuses) {
              children.push(child);
            }
            directChildren.put(top.getPath(), statuses.length);
            top = children.peek();
          } else {
            break;
          }
        }

        currentFileStatus = children.pop();
        visited.remove(currentFileStatus.getPath());
        return currentFileStatus;
      }
    }

    PruneListIterator itr = new PruneListIterator(metaPath);
    while (itr.hasNext()) {
      FileStatus status = itr.next();
      Path path = status.getPath();
      if (status.isDirectory()) {
        int size = metaFileSystem.listStatus(path).length;
        boolean changed = size != itr.getDirectChildren(path);
        if (changed) {
          if (size == 0) {
            metaFileSystem.delete(path, false);
          } else {
            metaFileSystem.setXAttr(path, XATTR_AUTHORITATIVE,
                "false".getBytes());
          }

          if (!path.isRoot()) {
            metaFileSystem.setXAttr(path.getParent(), XATTR_AUTHORITATIVE,
                "false".getBytes());
          }
        }
      } else if (status.getModificationTime() < modTime) {
        innerDelete(path, false);
      }
    }
  }

  @Override
  public Map<String, String> getDiagnostics() throws IOException {
    Map<String, String> diagnostics = new HashMap<>();
    diagnostics.put("meta.path", metaFileSystem.getUri().toString());
    diagnostics.put(PERSISTS_AUTHORITATIVE_BIT,
        conf.get(METADATASTORE_PERSISTS_AUTHORITATIVE_BIT));
    return diagnostics;
  }

  @Override
  public void updateParameters(Map<String, String> parameters)
      throws IOException {
    //pass, nothing to do.
  }

  @Override
  public void close() throws IOException {
    //pass, nothing to do.
  }

  /**
   * Translate between OSS paths and HDFS paths.
   * @param path
   * @param isHdfsPath
   * @return Translated path.
   */
  private Path pathTranslation(Path path, boolean isHdfsPath) {
    String rawPath = path.toUri().getPath();
    if (isHdfsPath) {
      String subPath = rawPath.substring(metadataPath.length());
      if (subPath.isEmpty()) {
        subPath = "/";
      }

      if (!subPath.contains("/")) {
        subPath += "/";
      }
      return new Path(FS_OSS + "://" + subPath);
    } else {
      String host = path.toUri().getHost();
      if (host == null) {
        return metaFileSystem.makeQualified(new Path(metadataPath + rawPath));
      } else {
        return metaFileSystem.makeQualified(
            new Path(metadataPath + host + "/" + rawPath));
      }
    }
  }

  private Path makeQualified(Path path) {
    return metaFileSystem.makeQualified(new Path(path.toUri().getPath()));
  }
}
