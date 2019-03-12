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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.cloud.core.metadata.Tristate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * This class is used by listStatus for oss files.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OSSFileStatus extends FileStatus {
  private Tristate emptyDirectory;

  public OSSFileStatus(long length, boolean isdir, int blockReplication,
      long blocksize, long modTime, Path path, String user) {
    super(length, isdir, blockReplication, blocksize, modTime, path);
    setOwner(user);
    setGroup(user);
  }

  /**
   * Convenience constructor for creating from a vanilla FileStatus plus
   * an isEmptyDirectory flag.
   * @param source FileStatus to convert to OSSFileStatus
   * @param isEmptyDirectory TRUE/FALSE if known to be / not be an empty
   *     directory, UNKNOWN if that information was not computed.
   * @return a new OSSFileStatus
   */
  public static OSSFileStatus fromFileStatus(FileStatus source,
      Tristate isEmptyDirectory) {
    OSSFileStatus ossFileStatus = new OSSFileStatus(source.getLen(),
        source.isDirectory(), source.getReplication(),
        source.getBlockSize(), source.getModificationTime(),
        source.getPath(), source.getOwner());
    if (source.isDirectory()) {
      ossFileStatus.emptyDirectory = isEmptyDirectory;
    }
    return ossFileStatus;
  }

  public Tristate isEmptyDirectory() {
    return emptyDirectory;
  }
}
