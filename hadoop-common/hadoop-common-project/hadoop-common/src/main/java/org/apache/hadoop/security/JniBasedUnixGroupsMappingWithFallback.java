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

package org.apache.hadoop.security;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

public class JniBasedUnixGroupsMappingWithFallback implements
    @Tainted
    GroupMappingServiceProvider {

  private static final @Tainted Log LOG = LogFactory
      .getLog(JniBasedUnixGroupsMappingWithFallback.class);
  
  private @Tainted GroupMappingServiceProvider impl;

  public @Tainted JniBasedUnixGroupsMappingWithFallback() {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      this.impl = new @Tainted JniBasedUnixGroupsMapping();
    } else {
      LOG.debug("Falling back to shell based");
      this.impl = new @Tainted ShellBasedUnixGroupsMapping();
    }
    if (LOG.isDebugEnabled()){
      LOG.debug("Group mapping impl=" + impl.getClass().getName());
    }
  }

  @Override
  public @Tainted List<@Untainted String> getGroups(@Tainted JniBasedUnixGroupsMappingWithFallback this, @Untainted String user) throws IOException {
    return impl.getGroups(user);
  }

  @Override
  public void cacheGroupsRefresh(@Tainted JniBasedUnixGroupsMappingWithFallback this) throws IOException {
    impl.cacheGroupsRefresh();
  }

  @Override
  public void cacheGroupsAdd(@Tainted JniBasedUnixGroupsMappingWithFallback this, @Tainted List<@Untainted String> groups) throws IOException {
    impl.cacheGroupsAdd(groups);
  }

}
