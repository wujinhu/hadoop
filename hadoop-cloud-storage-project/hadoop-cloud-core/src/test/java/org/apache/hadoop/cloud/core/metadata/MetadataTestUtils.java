/*
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

package org.apache.hadoop.cloud.core.metadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for metadata tests.
 */
public class MetadataTestUtils {

  /**
   * Verify the core size, block size and timestamp values of a file.
   * @param status status entry to check
   * @param size file size
   * @param blockSize block size
   * @param modTime modified time
   */
  public static void verifyFileStatus(FileStatus status, long size,
      long blockSize, long modTime) {
    verifyFileStatus(status, size, 0, modTime, 0, blockSize, null, null, null);
  }

  /**
   * Verify the status entry of a file matches that expected.
   * @param status status entry to check
   * @param size file size
   * @param replication replication factor (may be 0)
   * @param modTime modified time
   * @param accessTime access time (may be 0)
   * @param blockSize block size
   * @param owner owner (may be null)
   * @param group user group (may be null)
   * @param permission permission (may be null)
   */
  public static void verifyFileStatus(FileStatus status,
      long size,
      int replication,
      long modTime,
      long accessTime,
      long blockSize,
      String owner,
      String group,
      FsPermission permission) {
    String details = status.toString();
    assertFalse("Not a dir: " + details, status.isDirectory());
    assertEquals("Mod time: " + details,
        modTime, status.getModificationTime());
    assertEquals("File size: " + details, size, status.getLen());
    assertEquals("Block size: " + details, blockSize, status.getBlockSize());
    if (replication > 0) {
      assertEquals("Replication value: " + details, replication,
          status.getReplication());
    }
    if (accessTime != 0) {
      assertEquals("Access time: " + details, accessTime,
          status.getAccessTime());
    }
    if (owner != null) {
      assertEquals("Owner: " + details, owner, status.getOwner());
    }
    if (group != null) {
      assertEquals("Group: " + details, group, status.getGroup());
    }
    if (permission != null) {
      assertEquals("Permission: " + details, permission,
          status.getPermission());
    }
  }

  /**
   * Verify the status entry of a directory matches that expected.
   * @param status status entry to check
   * @param replication replication factor
   * @param modTime modified time
   * @param accessTime access time
   * @param owner owner
   * @param group user group
   * @param permission permission.
   */
  public static void verifyDirStatus(FileStatus status,
      int replication,
      long modTime,
      long accessTime,
      String owner,
      String group,
      FsPermission permission) {
    String details = status.toString();
    assertTrue("Is a dir: " + details, status.isDirectory());
    assertEquals("zero length: " + details, 0, status.getLen());

    assertEquals("Mod time: " + details,
        modTime, status.getModificationTime());
    assertEquals("Replication value: " + details, replication,
        status.getReplication());
    assertEquals("Access time: " + details,
        accessTime, status.getAccessTime());
    assertEquals("Owner: " + details, owner, status.getOwner());
    assertEquals("Group: " + details, group, status.getGroup());
    assertEquals("Permission: " + details, permission, status.getPermission());
  }

  public static boolean metadataStorePersistsAuthoritativeBit(MetadataStore ms)
      throws IOException {
    Map<String, String> diags = ms.getDiagnostics();
    String persists =
        diags.get(MetadataStoreCapabilities.PERSISTS_AUTHORITATIVE_BIT);
    if (persists == null) {
      return false;
    }
    return Boolean.valueOf(persists);
  }
}
