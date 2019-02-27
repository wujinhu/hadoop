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

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used by metadata store.
 */
public class Constants {
  /**
   * Time to live in milliseconds in LocalMetadataStore.
   * If zero, time-based expiration is disabled.
   */
  @InterfaceStability.Unstable
  public static final String METASTORE_LOCAL_ENTRY_TTL_MS =
      "fs.metadatastore.local.ttl";
  public static final int DEFAULT_METASTORE_LOCAL_ENTRY_TTL_MS
      = 10 * 1000;

  /**
   * Maximum number of records in LocalMetadataStore.
   */
  @InterfaceStability.Unstable
  public static final String METASTORE_LOCAL_MAX_RECORDS =
      "fs.metadatastore.local.max_records";
  public static final int DEFAULT_METASTORE_LOCAL_MAX_RECORDS = 256;

  /**
   * The default "Null" metadata store: {@value}.
   */
  @InterfaceStability.Unstable
  public static final String METASTORE_NULL
      = "org.apache.hadoop.cloud.core.metadata.NullMetadataStore";

  /**
   * Use Local memory for the metadata: {@value}.
   * This is not coherent across processes and must be used for testing only.
   */
  @InterfaceStability.Unstable
  public static final String METASTORE_LOCAL
      = "org.apache.hadoop.cloud.core.metadata.LocalMetadataStore";
}
