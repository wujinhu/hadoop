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

package org.apache.hadoop.fs.aliyun.oss.guard;

import org.apache.hadoop.cloud.core.metadata.AbstractMSContract;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.MetadataStoreTestBase;
import org.apache.hadoop.cloud.core.metadata.NullMetadataStore;
import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * Test that {@link HadoopMetadataStore} implements {@link MetadataStore}.
 * In this integration test, we create a mini dfs cluster to initialize
 * {@link HadoopMetadataStore} instance.
 */
public class TestHadoopMetadataStore extends MetadataStoreTestBase {

  private AliyunOSSFileSystem fileSystem;

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(Constants.FS_OSS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    assumeThatHadoopMetadataStoreImpl(conf);

    fileSystem = AliyunOSSTestUtils.createTestFileSystem(conf);

    super.setUp();
  }

  private static void assumeThatHadoopMetadataStoreImpl(Configuration conf) {
    Assume.assumeTrue(
        "Test only applies when HadoopMetadataStore is used for OSSGuard",
        conf.get(Constants.OSS_METADATA_STORE_IMPL)
            .equals(HadoopMetadataStore.class.getName()));
  }

  @Override
  public AbstractMSContract createContract() throws IOException {
    return new HadoopMSContract();
  }

  @Override
  public AbstractMSContract createContract(Configuration conf)
      throws IOException {
    return new HadoopMSContract(conf);
  }

  private class HadoopMSContract extends AbstractMSContract {
    public HadoopMSContract() {
      this(new Configuration());
    }

    public HadoopMSContract(Configuration conf) {
    }

    @Override
    public FileSystem getFileSystem() throws IOException {
      return fileSystem;
    }

    @Override
    public MetadataStore getMetadataStore() throws IOException {
      return fileSystem.getMetadataStore();
    }
  }

  @Test
  public void testGet() throws Exception {
    final String filePath = "/a1/b1/c1/some_file";
    final String dirPath = "/a1/b1/c1/d1";
    ms.put(new PathMetadata(makeFileStatus(filePath, 100)));
    ms.put(new PathMetadata(makeDirStatus(dirPath)));
    PathMetadata meta = ms.get(strToPath(filePath));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get found file", meta);
      verifyFileStatus(meta.getFileStatus(), 100);
    }

    if (!(ms instanceof NullMetadataStore)) {
      ms.delete(strToPath(filePath));
      assertNull(ms.get(strToPath(filePath)));
    }

    meta = ms.get(strToPath(dirPath));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get found file (dir)", meta);
      assertTrue("Found dir", meta.getFileStatus().isDirectory());
    }

    meta = ms.get(strToPath("/bollocks"));
    assertNull("Don't get non-existent file", meta);
  }

  @Test
  @Ignore
  public void testPutRetainsIsDeletedInParentListing() throws Exception {
    // HadoopMetadataStore do not support tombstones
  }
}
