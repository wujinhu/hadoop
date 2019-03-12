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

import com.google.common.base.Preconditions;
import org.apache.hadoop.cloud.core.metadata.DirListingMetadata;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.NullMetadataStore;
import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.fs.aliyun.oss.contract.AliyunOSSContract;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for {@link OSSGuard} tests.
 */
public abstract class AbstractOSSGuardTestBase
    extends AbstractFSContractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractOSSGuardTestBase.class);

  protected static final String OSS_THIS_BUCKET_DOES_NOT_EXIST =
      "oss://this-test-bucket-does-not-exist-00000000000";
  private static final int PRUNE_MAX_AGE_SECS = 3;

  protected MetadataStore ms;
  protected AliyunOSSFileSystem fs;
  private AliyunOSSFileSystem rawFs;
  protected String table = "hadoop_oss_test_" + System.currentTimeMillis();

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf.set(Constants.OTS_TABLE_KEY, table);
    return new AliyunOSSContract(conf);
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    fs = (AliyunOSSFileSystem)getFileSystem();
    ms = fs.getMetadataStore();
    Configuration conf = new Configuration(fs.getConf());
    conf.set(Constants.OSS_METADATA_STORE_IMPL,
        NullMetadataStore.class.getName());
    rawFs = (AliyunOSSFileSystem)FileSystem.newInstance(fs.getUri(), conf);
    assertFalse(rawFs.hasMetadataStore());
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
    getFileSystem().close();
    ms.destroy();
    IOUtils.closeStream(rawFs);
  }

  /**
   * Run a {@link OSSGuardTool} command from a varargs list.
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(String... args) throws Exception {
    Configuration conf = new Configuration(false);
    return OSSGuardTool.run(conf, args);
  }

  /**
   * Run a {@link OSSGuardTool} command from a varargs list, catch any raised
   * ExitException and verify the status code matches that expected.
   * @param status expected status code of an exception
   * @param args argument list
   * @throws Exception any exception
   */
  protected void runToFailure(int status, String... args) throws Exception {
    ExitUtil.ExitException ex =
        LambdaTestUtils.intercept(ExitUtil.ExitException.class,
            () -> run(args));
    if (ex.status != status) {
      throw ex;
    }
  }

  /**
   * Run a {@link OSSGuardTool} command from a varargs list, catch any raised
   * ExitException and verify the exception matches that expected.
   * @param clazz expected exception
   * @param args argument list
   * @throws Exception any exception
   */
  protected <E extends Throwable> void runToFailure(
      Class<E> clazz, String... args) throws Exception {
    Exception ex = LambdaTestUtils.intercept(Exception.class, () -> run(args));
    if (!clazz.isAssignableFrom(ex.getClass())) {
      throw ex;
    }
  }

  /**
   * Execute a command, returning the buffer if the command actually completes.
   * If an exception is raised the output is logged instead.
   * @param cmd command
   * @param args argument list
   * @throws Exception on any failure
   */
  protected String exec(OSSGuardTool cmd, String... args) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try {
      exec(0, "", cmd, buf, args);
      return buf.toString();
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Command {} failed: \n{}", cmd, buf);
      throw e;
    }
  }

  /**
   * Execute a command, saving the output into the buffer.
   * @param expectedResult expected result of the command.
   * @param errorText error text to include in the assertion.
   * @param cmd command
   * @param buf buffer to use for tool output (not SLF4J output)
   * @param args argument list
   * @throws Exception on any failure
   */
  public static void exec(final int expectedResult,
      final String errorText,
      final OSSGuardTool cmd,
      final ByteArrayOutputStream buf,
      final String... args) throws Exception {
    LOG.info("exec {}", (Object) args);
    int r;
    try (PrintStream out = new PrintStream(buf)) {
      r = cmd.run(args, out);
      out.flush();
    }
    if (expectedResult != r) {
      String message = errorText.isEmpty() ? "" : (errorText + ": ")
          + "Command " + cmd + " failed\n" + buf;
      assertEquals(message, expectedResult, r);
    }
  }

  /**
   * Create directory either on OSS or in metadata store.
   * @param path the directory path.
   * @param onOSS set to true to create directory on OSS.
   * @param onMetadataStore set to true to create the directory in
   *                        metadata store.
   * @throws IOException IO problem
   */
  protected void mkdirs(Path path, boolean onOSS, boolean onMetadataStore)
      throws IOException {
    Preconditions.checkArgument(onOSS || onMetadataStore);
    if (onOSS) {
      rawFs.mkdirs(path);
    }

    if (onMetadataStore) {
      OSSGuard ossGuard = new OSSGuard(fs, fs.getUsername());
      ossGuard.putPath(AliyunOSSUtils.pathToKey(path, fs), 0, true, true);
    }
  }

  /**
   * Create file either on OSS or in metadata store.
   * @param path the file path.
   * @param onOSS set to true to create the file on OSS.
   * @param onMetadataStore set to true to create the file in
   *                        metadata store.
   * @throws IOException IO problem
   */
  protected void createFile(Path path, boolean onOSS, boolean onMetadataStore)
      throws IOException {
    Preconditions.checkArgument(onOSS || onMetadataStore);
    if (onOSS) {
      ContractTestUtils.touch(rawFs, path);
    }

    if (onMetadataStore) {
      OSSGuard ossGuard = new OSSGuard(fs, fs.getUsername());
      ossGuard.putPath(AliyunOSSUtils.pathToKey(path, fs), 0, false);
    }
  }

  private void assertMetastoreListingCount(Path parent,
      String message, int expected) throws IOException {
    Collection<PathMetadata> listing = ms.listChildren(parent).getListing();
    assertEquals(message +" [" + StringUtils.join(", ", listing) + "]",
        expected, listing.size());
  }

  @Test
  public void testImport() throws Exception {
    Path parent = path("/test-import");
    fs.mkdirs(parent);
    Path dir = new Path(parent, "a");
    fs.mkdirs(dir);
    Path emptyDir = new Path(parent, "emptyDir");
    fs.mkdirs(emptyDir);
    for (int i = 0; i < 10; i++) {
      String child = String.format("file-%d", i);
      try (FSDataOutputStream out = fs.create(new Path(dir, child))) {
        out.write(1);
      }
    }

    OSSGuardTool.Import cmd = new OSSGuardTool.Import(fs.getConf());
    cmd.setStore(ms);
    cmd.setFileSystem(fs);

    exec(cmd, "import", parent.toString());

    DirListingMetadata children = ms.listChildren(dir);
    assertEquals("Unexpected number of paths imported", 10, children
        .getListing().size());
    assertEquals("Expected 2 items: empty directory and a parent directory", 2,
        ms.listChildren(parent).getListing().size());
  }

  @Test
  public void testInitNegativeRead() throws Exception {
    runToFailure(OSSGuardTool.INVALID_ARGUMENT,
        OSSGuardTool.Init.NAME, "-read", "-1", "-write", "1",
        OSS_THIS_BUCKET_DOES_NOT_EXIST);
  }

  @Test
  public void testImportNoFilesystem() throws Exception {
    runToFailure(IOException.class, OSSGuardTool.Import.NAME,
        OSS_THIS_BUCKET_DOES_NOT_EXIST);
  }

  @Test
  public void testDiff() throws Exception {
    Set<Path> filesOnOSS = new HashSet<>();
    Set<Path> filesOnMS = new HashSet<>();

    Path testPath = path("test-diff");
    // clean up through the store and behind it.
    fs.delete(testPath, true);
    rawFs.delete(testPath, true);
    mkdirs(testPath, true, true);

    Path msOnlyPath = new Path(testPath, "ms_only");
    mkdirs(msOnlyPath, false, true);
    filesOnMS.add(msOnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(msOnlyPath, String.format("file-%d", i));
      createFile(file, false, true);
      filesOnMS.add(file);
    }

    Path ossOnlyPath = new Path(testPath, "oss_only");
    mkdirs(ossOnlyPath, true, false);
    filesOnOSS.add(ossOnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(ossOnlyPath, String.format("file-%d", i));
      createFile(file, true, false);
      filesOnOSS.add(file);
    }

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    OSSGuardTool.Diff cmd = new OSSGuardTool.Diff(fs.getConf());
    cmd.setStore(ms);
    exec(0, "", cmd, buf, "diff", testPath.toString());

    Set<Path> actualOnOSS = new HashSet<>();
    Set<Path> actualOnMS = new HashSet<>();
    boolean duplicates = false;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(buf.toByteArray())))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] fields = line.split("\\s");
        assertEquals("[" + line + "] does not have enough fields",
                4, fields.length);
        String where = fields[0];
        Path path = new Path(fields[3]);
        if (OSSGuardTool.Diff.OSS_PREFIX.equals(where)) {
          duplicates = duplicates || actualOnOSS.contains(path);
          actualOnOSS.add(path);
        } else if (OSSGuardTool.Diff.MS_PREFIX.equals(where)) {
          duplicates = duplicates || actualOnMS.contains(path);
          actualOnMS.add(path);
        } else {
          fail("Unknown prefix: " + where);
        }
      }
    }
    String actualOut = buf.toString();
    assertEquals("Mismatched metadata store outputs: " + actualOut,
        filesOnMS, actualOnMS);
    assertEquals("Mismatched OSS outputs: " + actualOut,
        filesOnOSS, actualOnOSS);
    assertFalse("Diff contained duplicates", duplicates);
  }

  @Test
  /**
   * Attempt to test prune() with sleep() without having flaky tests
   * when things run slowly. Test is basically:
   * 1. Set max path age to X seconds
   * 2. Create some files (which writes entries to MetadataStore)
   * 3. Sleep X+3 seconds (all files from above are now "stale")
   * 4. Create some other files (these are "fresh").
   * 5. Run prune on MetadataStore.
   * 6. Assert that only files that were created before the sleep() were pruned.
   *
   * Problem is: #6 can fail if X seconds elapse between steps 4 and 5, since
   * the newer files also become stale and get pruned.  This is easy to
   * reproduce by running all integration tests in parallel with a ton of
   * threads, or anything else that slows down execution a lot.
   *
   * Solution: Keep track of time elapsed between #4 and #5, and if it
   * exceeds X, just print a warn() message instead of failing.
   *
   * @param cmdConf configuration for command
   * @param parent path
   * @param args command args
   * @throws Exception
   */
  public void testPrune() throws Exception {
    Path parent = path("testPruneCommandCLI");
    Path keepParent = path("prune-cli-keep");
    StopWatch timer = new StopWatch();
    try {
      OSSGuardTool.Prune cmd = new OSSGuardTool.Prune(fs.getConf());
      cmd.setStore(ms);
      cmd.setFileSystem(fs);

      getFileSystem().mkdirs(parent);
      getFileSystem().mkdirs(keepParent);
      createFile(new Path(parent, "stale"), true, true);
      createFile(new Path(keepParent, "stale-to-keep"), true, true);

      Thread.sleep(TimeUnit.SECONDS.toMillis(PRUNE_MAX_AGE_SECS + 3));

      timer.start();
      createFile(new Path(parent, "fresh"), true, true);

      assertMetastoreListingCount(parent, "Children count before pruning", 2);
      exec(cmd, "prune", "-seconds", String.valueOf(PRUNE_MAX_AGE_SECS),
          fs.makeQualified(parent).toString());
      long msecElapsed = timer.now(TimeUnit.MILLISECONDS);
      if (msecElapsed >= PRUNE_MAX_AGE_SECS * 1000) {
        LOG.warn("Skipping an assertion: Test running too slowly ({} msec)",
            msecElapsed);
      } else {
        assertMetastoreListingCount(parent, "Pruned children count remaining",
            1);
      }
      assertMetastoreListingCount(keepParent,
          "This child should have been kept (prefix restriction).", 1);
    } finally {
      fs.delete(parent, true);
      ms.prune(Long.MAX_VALUE);
    }
  }
}
