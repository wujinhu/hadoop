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

import org.apache.hadoop.cloud.core.metadata.LocalMetadataStore;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestOSSGuardToolLocal extends AbstractOSSGuardTestBase {
  @Before
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(
        "Test only applies when a local store is used for OSSGuard;"
        + "Store is " + (ms == null ? "none" : ms.toString()),
        ms instanceof LocalMetadataStore);
  }

  @Test
  public void testInit() throws Exception {
    // LocalMetadataStore should ignore read and write flags
    OSSGuardTool.run(fs.getConf(), OSSGuardTool.Init.NAME,
        OSS_THIS_BUCKET_DOES_NOT_EXIST);
    OSSGuardTool.run(fs.getConf(),
        OSSGuardTool.Init.NAME, "-read", "1", "-write", "1",
        OSS_THIS_BUCKET_DOES_NOT_EXIST);
  }
}
