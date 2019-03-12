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

import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link OSSGuard} related CLI commands against OTS.
 */
public class TestOSSGuardToolOTS extends AbstractOSSGuardTestBase {
  @Before
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(
        "Test only applies when a OTS store is used for OSSGuard;"
        + "Store is " + (ms == null ? "none" : ms.toString()),
        ms instanceof OTSMetadataStore);
  }

  @Test
  public void testSetCapacity() throws Exception {
    // limitations by OTS, we can only change once in 2 minutes.
    Thread.sleep(120000);

    String readCap = "5";
    String writeCap = "6";

    OSSGuardTool.SetCapacity cmd = new OSSGuardTool.SetCapacity(fs.getConf());
    cmd.setFileSystem(fs);
    cmd.setStore(ms);

    exec(cmd, "set-capacity",
        "-read", readCap, "-write", writeCap, OSS_THIS_BUCKET_DOES_NOT_EXIST);

    assertEquals(readCap,
        ms.getDiagnostics().get(Constants.OTS_TABLE_CAPACITY_READ_KEY));
    assertEquals(writeCap,
        ms.getDiagnostics().get(Constants.OTS_TABLE_CAPACITY_WRITE_KEY));
  }

  @Test
  public void testDestroy() throws Exception {
    OTSMetadataStore otsMS = (OTSMetadataStore)ms;
    assertTrue(otsMS.tableExists(table));

    OSSGuardTool.Destroy cmd = new OSSGuardTool.Destroy(fs.getConf());
    cmd.setFileSystem(fs);
    cmd.setStore(ms);

    exec(cmd, "destroy", OSS_THIS_BUCKET_DOES_NOT_EXIST);
    assertFalse(otsMS.tableExists(table));

    OSSGuardTool.run(fs.getConf(), OSSGuardTool.Init.NAME,
        OSS_THIS_BUCKET_DOES_NOT_EXIST);
  }
}
