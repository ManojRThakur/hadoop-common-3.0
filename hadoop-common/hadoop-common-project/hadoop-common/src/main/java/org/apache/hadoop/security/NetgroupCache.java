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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class that caches the netgroups and inverts group-to-user map
 * to user-to-group map, primarily intented for use with
 * netgroups (as returned by getent netgrgoup) which only returns
 * group to user mapping.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetgroupCache {

  private static final @Tainted Log LOG = LogFactory.getLog(NetgroupCache.class);

  private static @Tainted boolean netgroupToUsersMapUpdated = true;
  private static @Tainted Map<@Untainted String, @Tainted Set<@Untainted String>> netgroupToUsersMap =
    new @Tainted ConcurrentHashMap<@Untainted String, @Tainted Set<@Untainted String>>();

  private static @Tainted Map<@Untainted String, @Tainted Set<@Untainted String>> userToNetgroupsMap =
    new @Tainted ConcurrentHashMap<@Untainted String, @Tainted Set<@Untainted String>>();


  /**
   * Get netgroups for a given user
   *
   * @param user get groups for this user
   * @param groups put groups into this List
   */
  public static void getNetgroups(final @Untainted String user, @Tainted List<@Untainted String> groups) {
    if(netgroupToUsersMapUpdated) {
      netgroupToUsersMapUpdated = false; // at the beginning to avoid race
      //update userToNetgroupsMap
      for(@Untainted String netgroup : netgroupToUsersMap.keySet()) {
        for(@Untainted String netuser : netgroupToUsersMap.get(netgroup)) {
          // add to userToNetgroupsMap
          if(!userToNetgroupsMap.containsKey(netuser)) {
            userToNetgroupsMap.put(netuser, new @Tainted HashSet<@Untainted String>());
          }
          userToNetgroupsMap.get(netuser).add(netgroup);
        }
      }
    }
    if(userToNetgroupsMap.containsKey(user)) {
      for(@Tainted String netgroup : userToNetgroupsMap.get(user)) {
        groups.add(netgroup);
      }
    }
  }

  /**
   * Get the list of cached netgroups
   *
   * @return list of cached groups
   */
  public static @Tainted List<@Untainted String> getNetgroupNames() {
    return new @Tainted LinkedList<@Untainted String>(netgroupToUsersMap.keySet());
  }

  /**
   * Returns true if a given netgroup is cached
   *
   * @param group check if this group is cached
   * @return true if group is cached, false otherwise
   */
  public static @Tainted boolean isCached(@Untainted String group) {
    return netgroupToUsersMap.containsKey(group);
  }

  /**
   * Clear the cache
   */
  public static void clear() {
    netgroupToUsersMap.clear();
  }

  /**
   * Add group to cache
   *
   * @param group name of the group to add to cache
   * @param users list of users for a given group
   */
  public static void add(@Untainted String group, @Tainted List<@Untainted String> users) {
    if(!isCached(group)) {
      netgroupToUsersMap.put(group, new @Tainted HashSet<@Untainted String>());
      for(@Untainted String user: users) {
        netgroupToUsersMap.get(group).add(user);
      }
    }
    netgroupToUsersMapUpdated = true; // at the end to avoid race
  }
}
