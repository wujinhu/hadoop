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

package org.apache.hadoop.fs.aliyun.oss.guard;

import com.aliyun.oss.common.auth.Credentials;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Abstract metadata store class.
 */
public abstract class AbstractMetadataStore implements MetadataStore {
  protected Configuration conf;
  protected String username;
  protected Credentials credentials;
  protected String bucket;

  @Override
  public void initialize(FileSystem fs) throws IOException {
    credentials = ((AliyunOSSFileSystem)fs).getStore()
        .getCredentialsProvider().getCredentials();
    username = ((AliyunOSSFileSystem)fs).getUsername();
    conf = fs.getConf();
    bucket = fs.getUri().getHost();
    initialize(conf);
  }

  protected void validateOSSPath(Path path) {
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(path.isAbsolute(),
        "Path %s is not absolute", path);

    URI uri = path.toUri();

    Preconditions.checkArgument(!StringUtils.isEmpty(uri.getHost()),
        "Path %s is missing bucket.", path);
    Preconditions.checkNotNull(uri.getScheme(),
        "Path %s missing scheme", path);
  }
}
