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

import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.cloud.core.metadata.Tristate;
import org.apache.hadoop.fs.FileStatus;

/**
 * {@code OTSPathMetadata} wraps {@link PathMetadata} and adds the
 * isAuthoritativeDir flag to provide support for authoritative directory
 * listings in {@link OTSMetadataStore}.
 */
public class OTSPathMetadata extends PathMetadata {
  private boolean isAuthoritativeDir;

  public OTSPathMetadata(PathMetadata pmd) {
    super(pmd.getFileStatus(), pmd.isEmptyDirectory(), pmd.isDeleted());
    this.isAuthoritativeDir = false;
    this.setLastUpdated(pmd.getLastUpdated());
  }

  public OTSPathMetadata(FileStatus fileStatus) {
    super(fileStatus);
    this.isAuthoritativeDir = false;
  }

  public OTSPathMetadata(FileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted) {
    super(fileStatus, isEmptyDir, isDeleted);
    this.isAuthoritativeDir = false;
  }

  public OTSPathMetadata(FileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, boolean isAuthoritativeDir, long lastUpdated) {
    super(fileStatus, isEmptyDir, isDeleted);
    this.isAuthoritativeDir = isAuthoritativeDir;
    this.setLastUpdated(lastUpdated);
  }

  public boolean isAuthoritativeDir() {
    return isAuthoritativeDir;
  }

  public void setAuthoritativeDir(boolean authoritativeDir) {
    isAuthoritativeDir = authoritativeDir;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode();
  }

  @Override public String toString() {
    return "OTSPathMetadata{" +
        "isAuthoritativeDir=" + isAuthoritativeDir +
        ", lastUpdated=" + this.getLastUpdated() +
        ", PathMetadata=" + super.toString() +
        '}';
  }
}
