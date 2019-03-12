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

import org.apache.hadoop.cloud.core.metadata.NullMetadataStore;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.aliyun.oss.guard.OSSGuardTool.E_USAGE;
import static org.apache.hadoop.fs.aliyun.oss.guard.OSSGuardTool.INVALID_ARGUMENT;

/**
 * Test the {@link OSSGuardTool} CLI entry point.
 */
public class TestOSSGuardCLI extends AbstractOSSGuardTestBase {
  @Before
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(
        "Test only applies when a local/ots store is used for OSSGuard;"
        + "Store is " + (ms == null ? "none" : ms.toString()),
            !(ms instanceof NullMetadataStore));
  }

  @Test
  public void testNoCommand() throws Throwable {
    runToFailure(E_USAGE);
  }

  @Test
  public void testUnknownCommand() throws Throwable {
    runToFailure(E_USAGE, "unknown");
  }

  @Test
  public void testPruneNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, OSSGuardTool.Prune.NAME);
  }

  @Test
  public void testDiffNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, OSSGuardTool.Diff.NAME);
  }

  @Test
  public void testImportNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, OSSGuardTool.Import.NAME);
  }

  @Test
  public void testDestroyNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, OSSGuardTool.Destroy.NAME);
  }

  @Test
  public void testInitBucketAndRegion() throws Throwable {
    runToFailure(INVALID_ARGUMENT, OSSGuardTool.Init.NAME,
        "-table", "ots");
  }
}
