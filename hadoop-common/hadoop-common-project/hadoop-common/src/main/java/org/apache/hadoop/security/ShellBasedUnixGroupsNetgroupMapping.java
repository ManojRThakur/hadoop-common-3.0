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
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;

import org.apache.hadoop.security.NetgroupCache;

/**
 * A simple shell-based implementation of {@link GroupMappingServiceProvider} 
 * that exec's the <code>groups</code> shell command to fetch the group
 * memberships of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ShellBasedUnixGroupsNetgroupMapping extends @Tainted ShellBasedUnixGroupsMapping {
  
  private static final @Tainted Log LOG =
    LogFactory.getLog(ShellBasedUnixGroupsNetgroupMapping.class);

  /**
   * Get unix groups (parent) and netgroups for given user
   *
   * @param user get groups and netgroups for this user
   * @return groups and netgroups for user
   */
  @Override
  public @Tainted List<@Untainted String> getGroups(@Tainted ShellBasedUnixGroupsNetgroupMapping this, @Untainted String user) throws IOException {
    // parent get unix groups
    @Tainted List<@Untainted String> groups = new @Tainted LinkedList<@Untainted String>(super.getGroups(user));
    NetgroupCache.getNetgroups(user, groups);
    return groups;
  }

  /**
   * Refresh the netgroup cache
   */
  @Override
  public void cacheGroupsRefresh(@Tainted ShellBasedUnixGroupsNetgroupMapping this) throws IOException {
    @Tainted
    List<@Untainted String> groups = NetgroupCache.getNetgroupNames();
    NetgroupCache.clear();
    cacheGroupsAdd(groups);
  }

  /**
   * Add a group to cache, only netgroups are cached
   *
   * @param groups list of group names to add to cache
   */
  @Override
  public void cacheGroupsAdd(@Tainted ShellBasedUnixGroupsNetgroupMapping this, @Tainted List<@Untainted String> groups) throws IOException {
    for(@Untainted String group: groups) {
      if(group.length() == 0) {
        // better safe than sorry (should never happen)
      } else if(group.charAt(0) == '@') {
        if(!NetgroupCache.isCached(group)) {
          NetgroupCache.add(group, getUsersForNetgroup(group));
        }
      } else {
        // unix group, not caching
      }
    }
  }

  /**
   * Gets users for a netgroup
   *
   * @param netgroup return users for this netgroup
   * @return list of users for a given netgroup
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  protected @Tainted List<@Untainted String> getUsersForNetgroup(@Tainted ShellBasedUnixGroupsNetgroupMapping this, @Untainted String netgroup)
    throws IOException {

    @Tainted List<@Untainted String> users = new @Tainted LinkedList<@Untainted String>();

    // returns a string similar to this:
    // group               ( , user, ) ( domain, user1, host.com )
    @Untainted String usersRaw = execShellGetUserForNetgroup(netgroup);
    // get rid of spaces, makes splitting much easier
    usersRaw = (@Untainted String) usersRaw.replaceAll(" +", "");
    // remove netgroup name at the beginning of the string
    usersRaw = (@Untainted String) usersRaw.replaceFirst( netgroup.replaceFirst("@", "") + "[()]+", "");
    // split string into user infos
    @Untainted String @Tainted [] userInfos = (@Untainted String @Tainted []) usersRaw.split("[()]+");
    for(@Untainted String userInfo : userInfos) {
      // userInfo: xxx,user,yyy (xxx, yyy can be empty strings)
      // get rid of everything before first and after last comma
      @Untainted String user = (@Untainted String) userInfo.replaceFirst("[^,]*,", "");
      user = (@Untainted String) user.replaceFirst(",.*$", "");
      // voila! got username!
      users.add(user);
    }

    return users;
  }

  /**
   * Calls shell to get users for a netgroup by calling getent
   * netgroup, this is a low level function that just returns string
   * that 
   *
   * @param netgroup get users for this netgroup
   * @return string of users for a given netgroup in getent netgroups format
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  protected @Untainted String execShellGetUserForNetgroup(@Tainted ShellBasedUnixGroupsNetgroupMapping this, final @Untainted String netgroup)
      throws IOException {
    @Untainted String result = "";
    try {
      // shell command does not expect '@' at the begining of the group name
      result = Shell.execCommand( Shell.getUsersForNetgroupCommand( (@Untainted String) netgroup.substring(1)));
    } catch (@Tainted ExitCodeException e) {
      // if we didn't get the group - just return empty list;
      LOG.warn("error getting users for netgroup " + netgroup, e);
    }
    return result;
  }
}
