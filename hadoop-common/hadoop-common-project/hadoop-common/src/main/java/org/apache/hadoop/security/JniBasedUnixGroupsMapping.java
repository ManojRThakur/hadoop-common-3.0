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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A JNI-based implementation of {@link GroupMappingServiceProvider} 
 * that invokes libC calls to get the group
 * memberships of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class JniBasedUnixGroupsMapping implements @Tainted GroupMappingServiceProvider {
  
  private static final @Tainted Log LOG = 
    LogFactory.getLog(JniBasedUnixGroupsMapping.class);

  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      throw new @Tainted RuntimeException("Bailing out since native library couldn't " +
        "be loaded");
    }
    anchorNative();
    LOG.debug("Using JniBasedUnixGroupsMapping for Group resolution");
  }

  /**
   * Set up our JNI resources.
   *
   * @throws                 RuntimeException if setup fails.
   */
  native static void anchorNative();

  /**
   * Get the set of groups associated with a user.
   *
   * @param username           The user name
   *
   * @return                   The set of groups associated with a user.
   */
  native static @Untainted String @Tainted [] getGroupsForUser(@Untainted String username);

  /**
   * Log an error message about a group.  Used from JNI.
   */
  static private void logError(@Tainted int groupId, @Tainted String error) {
    LOG.error("error looking up the name of group " + groupId + ": " + error);
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  @Override
  public @Tainted List<@Untainted String> getGroups(@Tainted JniBasedUnixGroupsMapping this, @Untainted String user) throws IOException {
    @Untainted
    String @Tainted [] groups = new @Untainted String @Tainted [0];
    try {
      groups = getGroupsForUser(user);
    } catch (@Tainted Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error getting groups for " + user, e);
      } else {
        LOG.info("Error getting groups for " + user + ": " + e.getMessage());
      }
    }
    return (@Tainted List<@Untainted String>) Arrays.asList(groups);
  }

  @Override
  public void cacheGroupsRefresh(@Tainted JniBasedUnixGroupsMapping this) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  @Override
  public void cacheGroupsAdd(@Tainted JniBasedUnixGroupsMapping this, @Tainted List<@Untainted String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }
}