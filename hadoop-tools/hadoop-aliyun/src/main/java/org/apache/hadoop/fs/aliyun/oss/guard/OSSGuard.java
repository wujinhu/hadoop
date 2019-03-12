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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.cloud.core.metadata.DirListingMetadata;
import org.apache.hadoop.cloud.core.metadata.LocalMetadataStore;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.NullMetadataStore;
import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.cloud.core.metadata.Tristate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.OSSFileStatus;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.createFileStatus;
import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.keyToPath;
import static org.apache.hadoop.fs.aliyun.oss.Constants.OSS_METADATA_STORE_IMPL;

public class OSSGuard {
  private static final Logger LOG = LoggerFactory.getLogger(OSSGuard.class);

  /**
   * For testing only.
   */
  @VisibleForTesting
  private static MetadataStore localMetadataStore = new LocalMetadataStore();
  @VisibleForTesting
  private static boolean localMetadataStoreInitialized = false;

  private AliyunOSSFileSystem fs;
  private String username;
  private MetadataStore metadataStore;

  public OSSGuard(AliyunOSSFileSystem fs, String username) {
    this.fs = fs;
    this.username = username;
    this.metadataStore = fs.getMetadataStore();
  }

  public MetadataStore getMetadataStore() {
    return metadataStore;
  }

  public static MetadataStore getMetadataStore(FileSystem fs)
      throws IOException {
    return getMetadataStore(fs, null);
  }

  public static MetadataStore getMetadataStore(Configuration conf)
      throws IOException {
    return getMetadataStore(null, conf);
  }

  public static MetadataStore getMetadataStore(
      FileSystem fs, Configuration configuration) throws IOException {
    Preconditions.checkState(!(fs != null && configuration != null));
    Preconditions.checkState(!(fs == null && configuration == null));
    Configuration conf = fs == null ? configuration : fs.getConf();
    Preconditions.checkNotNull(conf);
    MetadataStore msInstance;
    try {
      Class<? extends MetadataStore> msClass = getMetadataStoreClass(conf);
      if (msClass.equals(LocalMetadataStore.class)) {
        // For testing only.
        msInstance = localMetadataStore;
        if (!localMetadataStoreInitialized) {
          if (fs != null) {
            msInstance.initialize(fs);
          } else {
            msInstance.initialize(conf);
          }
          localMetadataStoreInitialized = true;
        }
      } else {
        msInstance = ReflectionUtils.newInstance(msClass, conf);
        if (fs != null) {
          msInstance.initialize(fs);
        } else {
          msInstance.initialize(conf);
        }
      }
      return msInstance;
    } catch (FileNotFoundException e) {
      throw e;
    } catch (RuntimeException | IOException e) {
      String message = "Failed to instantiate metadata store " +
          conf.get(OSS_METADATA_STORE_IMPL) +
          " defined in " + OSS_METADATA_STORE_IMPL + " : " + e;
      if (e instanceof IOException) {
        throw e;
      } else {
        throw new IOException(message, e);
      }
    }
  }

  private static Class<? extends MetadataStore> getMetadataStoreClass(
      Configuration conf) {

    if (conf == null) {
      return NullMetadataStore.class;
    }

    return conf.getClass(
        OSS_METADATA_STORE_IMPL,
        NullMetadataStore.class,
        MetadataStore.class);
  }

  /**
   * Although NullMetadataStore does nothing, callers may wish to avoid work
   * (fast path) when the NullMetadataStore is in use.
   * @param ms The MetadataStore to test
   * @return true iff the MetadataStore is the null, or no-op, implementation.
   */
  public static boolean isNullMetadataStore(MetadataStore ms) {
    return (ms instanceof NullMetadataStore);
  }

  public void addAncestors(Path qualifiedPath) throws IOException {
    Collection<PathMetadata> newDirs = new ArrayList<>();
    Path parent = qualifiedPath.getParent();
    while (!parent.isRoot()) {
      PathMetadata directory = metadataStore.get(parent);
      if (directory == null || directory.isDeleted()) {
        OSSFileStatus status = createFileStatus(parent, true, 0,
            System.currentTimeMillis(), 0, username, Tristate.FALSE);
        PathMetadata meta = new PathMetadata(status,
            status.isEmptyDirectory(), false);
        newDirs.add(meta);
      } else {
        break;
      }
      parent = parent.getParent();
    }
    metadataStore.put(newDirs);
  }

  public void dirListingUnion(Path path, Collection<FileStatus> backingStatuses,
      DirListingMetadata dirMeta, boolean isAuthoritative) throws IOException {
    if (isNullMetadataStore(metadataStore)) {
      return;
    }

    if (dirMeta == null) {
      dirMeta = new DirListingMetadata(path, DirListingMetadata.EMPTY_DIR,
          false);
    }

    boolean changed = false;
    for (FileStatus s : backingStatuses) {
      boolean updated = dirMeta.put(s);
      changed = changed || updated;
    }

    changed = changed || (!dirMeta.isAuthoritative() && isAuthoritative);

    if (changed && isAuthoritative) {
      dirMeta.setAuthoritative(true); // This is the full directory contents
      metadataStore.put(dirMeta);
    }
  }

  public boolean putPath(String key, long length, boolean isDir)
      throws IOException {
    return putPath(key, length, isDir, false);
  }

  public boolean putPath(String key, long length, boolean isDir,
      boolean authoritative) throws IOException {
    Path qualifiedPath
        = keyToPath(key).makeQualified(fs.getUri(), fs.getWorkingDirectory());
    addAncestors(qualifiedPath);

    PathMetadata pm = new PathMetadata(new OSSFileStatus(
        length, isDir, 1, fs.getDefaultBlockSize(qualifiedPath),
        System.currentTimeMillis(), qualifiedPath, username));

    if (authoritative) {
      metadataStore.put(
          new DirListingMetadata(qualifiedPath, new ArrayList<PathMetadata>(),
              true, System.currentTimeMillis()));
    } else {
      metadataStore.put(pm);
    }
    return true;
  }

  public void addMoveFile(Collection<Path> srcPaths,
      Collection<PathMetadata> dstMetas,
      Path srcPath, Path dstPath, long size) {
    if (isNullMetadataStore(metadataStore)) {
      return;
    }

    FileStatus dstStatus = createFileStatus(fs.makeQualified(dstPath), false,
        size, System.currentTimeMillis(), fs.getDefaultBlockSize(dstPath),
        username, Tristate.UNKNOWN);
    srcPaths.add(fs.makeQualified(srcPath));
    dstMetas.add(new PathMetadata(dstStatus));
  }
}
