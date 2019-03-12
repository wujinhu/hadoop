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

/**
 * Copy file entry class for rename.
 */
public class CopyFileEntry {
  private String srcKey;
  private String dstKey;
  private long size;
  private boolean isDir;

  public CopyFileEntry(
      String srcKey, String dstKey, long size, boolean isDir) {
    this.srcKey = srcKey;
    this.dstKey = dstKey;
    this.size = size;
    this.isDir = isDir;
  }

  public String getSrcKey() {
    return srcKey;
  }

  public String getDstKey() {
    return dstKey;
  }

  public long getSize() {
    return size;
  }

  public boolean isDir() {
    return isDir;
  }
}
