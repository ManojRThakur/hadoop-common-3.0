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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A user-to-groups mapping service.
 * 
 * {@link Groups} allows for server to get the various group memberships
 * of a given user via the {@link #getGroups(String)} call, thus ensuring 
 * a consistent user-to-groups mapping and protects against vagaries of 
 * different mappings on servers and clients in a Hadoop cluster. 
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Groups {
  private static final @Tainted Log LOG = LogFactory.getLog(Groups.class);
  
  private final @Tainted GroupMappingServiceProvider impl;
  
  private final @Tainted Map<@Untainted String, @Untainted CachedGroups> userToGroupsMap = new @Tainted ConcurrentHashMap<@Untainted String, @Untainted CachedGroups>();
  private final @Tainted long cacheTimeout;

  public @Tainted Groups(@Tainted Configuration conf) {
    impl = 
      ReflectionUtils.newInstance(
          conf.getClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, 
                        ShellBasedUnixGroupsMapping.class, 
                        GroupMappingServiceProvider.class), 
          conf);
    
    cacheTimeout = 
      conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 5*60) * 1000;
    
    if(LOG.isDebugEnabled())
      LOG.debug("Group mapping impl=" + impl.getClass().getName() + 
          "; cacheTimeout=" + cacheTimeout);
  }
  
  /**
   * Get the group memberships of a given user.
   * @param user User's name
   * @return the group memberships of the user
   * @throws IOException
   */
  public @Tainted List<@Untainted String> getGroups(@Untainted Groups this, @Untainted String user) throws IOException {
    // Return cached value if available
    @Untainted CachedGroups groups = userToGroupsMap.get(user);
    @Tainted
    long now = Time.now();
    // if cache has a value and it hasn't expired
    if (groups != null && (groups.getTimestamp() + cacheTimeout > now)) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Returning cached groups for '" + user + "'");
      }
      return groups.getGroups();
    }
    
    // Create and cache user's groups
    groups = new @Untainted CachedGroups(impl.getGroups(user));
    if (groups.getGroups().isEmpty()) {
      throw new @Tainted IOException("No groups found for user " + user);
    }
    userToGroupsMap.put(user, groups);
    if(LOG.isDebugEnabled()) {
      LOG.debug("Returning fetched groups for '" + user + "'");
    }
    return groups.getGroups();
  }
  
  /**
   * Refresh all user-to-groups mappings.
   */
  public void refresh(@Tainted Groups this) {
    LOG.info("clearing userToGroupsMap cache");
    try {
      impl.cacheGroupsRefresh();
    } catch (@Tainted IOException e) {
      LOG.warn("Error refreshing groups cache", e);
    }
    userToGroupsMap.clear();
  }

  /**
   * Add groups to cache
   *
   * @param groups list of groups to add to cache
   */
  public void cacheGroupsAdd(@Tainted Groups this, @Tainted List<@Untainted String> groups) {
    try {
      impl.cacheGroupsAdd(groups);
    } catch (@Tainted IOException e) {
      LOG.warn("Error caching groups", e);
    }
  }

  /**
   * Class to hold the cached groups
   */
  private static class CachedGroups {
    final @Tainted long timestamp;
    final @Tainted List<@Untainted String> groups;
    
    /**
     * Create and initialize group cache
     */
    @Tainted
    CachedGroups(@Tainted List<@Untainted String> groups) {
      this.groups = groups;
      this.timestamp = Time.now();
    }

    /**
     * Returns time of last cache update
     *
     * @return time of last cache update
     */
    public @Tainted long getTimestamp(Groups.@Tainted CachedGroups this) {
      return timestamp;
    }

    /**
     * Get list of cached groups
     *
     * @return cached groups
     */
    public @Tainted List<@Untainted String> getGroups(Groups.@Tainted CachedGroups this) {
      return groups;
    }
  }

  private static @Tainted Groups GROUPS = null;
  
  /**
   * Get the groups being used to map user-to-groups.
   * @return the groups being used to map user-to-groups.
   */
  public static @Tainted Groups getUserToGroupsMappingService() {
    return getUserToGroupsMappingService(new @Tainted Configuration()); 
  }

  /**
   * Get the groups being used to map user-to-groups.
   * @param conf
   * @return the groups being used to map user-to-groups.
   */
  public static synchronized @Tainted Groups getUserToGroupsMappingService(
    @Tainted
    Configuration conf) {

    if(GROUPS == null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug(" Creating new Groups object");
      }
      GROUPS = new @Tainted Groups(conf);
    }
    return GROUPS;
  }
}
