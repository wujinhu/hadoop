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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * Tests concurrent operations on {@link OSSGuard}.
 */
public class TestOSSGuardConcurrentOps {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOSSGuardConcurrentOps.class);

  private AliyunOSSFileSystem fs;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }

  @Test(timeout = 5 * 60 * 1000L)
  public void testConcurrentInitMS() throws Exception {
    final Configuration conf = fs.getConf();
    Assume.assumeTrue("Test only applies when OTS is used for OSSGuard",
        conf.get(Constants.OSS_METADATA_STORE_IMPL).equals(
            OTSMetadataStore.class.getName()));
    String tableName = "testConcurrentOTSTableCreations";
    conf.setBoolean(Constants.OTS_TABLE_AUTO_CREATE_KEY, true);
    conf.set(Constants.OTS_TABLE_KEY, tableName);

    int concurrentOps = 16;
    int iterations = 4;

    OTSMetadataStore ms = (OTSMetadataStore)fs.getMetadataStore();
    if (ms.tableExists(tableName)) {
      fail("Table " + tableName + " already exists!");
    }

    for (int i = 0; i < iterations; ++i) {
      ExecutorService executor = Executors.newFixedThreadPool(
          concurrentOps, new ThreadFactory() {
            private AtomicInteger count = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
              return new Thread(r,
                  "testConcurrentTableCreations" + count.getAndIncrement());
            }
          });
      ((ThreadPoolExecutor)executor).prestartAllCoreThreads();

      Future<Exception>[] futures = new Future[concurrentOps];
      for (int j = 0; j < concurrentOps; ++j) {
        futures[j] = executor.submit(new Callable<Exception>() {
          @Override
          public Exception call() throws Exception {
            Exception result = null;
            try (OTSMetadataStore ms = new OTSMetadataStore()) {
              ms.initialize(fs);
            } catch (Exception e) {
              result = e;
            }
            return result;
          }
        });
      }

      List<Exception> exceptions = new ArrayList<>(concurrentOps);
      for (int f = 0; f < concurrentOps; f++) {
        Exception outcome = futures[f].get();
        if (outcome != null) {
          exceptions.add(outcome);
        }
      }

      ms.deleteTable(tableName);

      int exceptionsThrown = exceptions.size();
      if (exceptionsThrown > 0) {
        // at least one exception was thrown. Fail the test & nest the first
        // exception caught
        throw new AssertionError(exceptionsThrown + "/" + concurrentOps +
            " threads threw exceptions while initializing on iteration " + i,
            exceptions.get(0));
      }
    }
  }
}
