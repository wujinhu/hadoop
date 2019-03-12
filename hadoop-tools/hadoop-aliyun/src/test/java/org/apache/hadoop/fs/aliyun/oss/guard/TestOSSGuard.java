/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.guard;

import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.NullMetadataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link OSSGuard} utility class.
 */
public class TestOSSGuard extends Assert {

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }

  @Test
  public void testGetMetadataStore() throws Exception {
    conf.set(Constants.OSS_METADATA_STORE_IMPL,
        NullMetadataStore.class.getName());
    MetadataStore ms = OSSGuard.getMetadataStore(conf);
    assertTrue(ms instanceof NullMetadataStore);
    ms.destroy();

    conf.set(Constants.OSS_METADATA_STORE_IMPL,
        HadoopMetadataStore.class.getName());
    ms = OSSGuard.getMetadataStore(conf);
    assertTrue(ms instanceof HadoopMetadataStore);
    ms.destroy();

    conf.set(Constants.OSS_METADATA_STORE_IMPL,
        OTSMetadataStore.class.getName());
    ms = OSSGuard.getMetadataStore(conf);
    assertTrue(ms instanceof OTSMetadataStore);
    ms.destroy();
  }
}
