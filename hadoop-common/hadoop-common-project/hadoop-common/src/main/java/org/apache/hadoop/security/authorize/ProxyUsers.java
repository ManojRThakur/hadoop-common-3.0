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

package org.apache.hadoop.security.authorize;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public class ProxyUsers {

  private static final @Tainted String CONF_HOSTS = ".hosts";
  public static final @Tainted String CONF_GROUPS = ".groups";
  public static final @Tainted String CONF_HADOOP_PROXYUSER = "hadoop.proxyuser.";
  public static final @Tainted String CONF_HADOOP_PROXYUSER_RE = "hadoop\\.proxyuser\\.";
  private static @Tainted boolean init = false;
  // list of groups and hosts per proxyuser
  private static @Tainted Map<@Tainted String, @Tainted Collection<@Tainted String>> proxyGroups = 
    new @Tainted HashMap<@Tainted String, @Tainted Collection<@Tainted String>>();
  private static @Tainted Map<@Tainted String, @Tainted Collection<@Tainted String>> proxyHosts = 
    new @Tainted HashMap<@Tainted String, @Tainted Collection<@Tainted String>>();

  /**
   * reread the conf and get new values for "hadoop.proxyuser.*.groups/hosts"
   */
  public static void refreshSuperUserGroupsConfiguration() {
    //load server side configuration;
    refreshSuperUserGroupsConfiguration(new @Tainted Configuration());
  }

  /**
   * refresh configuration
   * @param conf
   */
  public static synchronized void refreshSuperUserGroupsConfiguration(@Tainted Configuration conf) {
    
    // remove alle existing stuff
    proxyGroups.clear();
    proxyHosts.clear();

    // get all the new keys for groups
    @Tainted
    String regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_GROUPS;
    @Tainted
    Map<@Tainted String, @Tainted String> allMatchKeys = conf.getValByRegex(regex);
    for(@Tainted Entry<@Tainted String, @Tainted String> entry : allMatchKeys.entrySet()) {
      proxyGroups.put(entry.getKey(), 
          StringUtils.getStringCollection(entry.getValue()));
    }

    // now hosts
    regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_HOSTS;
    allMatchKeys = conf.getValByRegex(regex);
    for(@Tainted Entry<@Tainted String, @Tainted String> entry : allMatchKeys.entrySet()) {
      proxyHosts.put(entry.getKey(),
          StringUtils.getStringCollection(entry.getValue()));
    }
    
    init = true;
  }

  /**
   * Returns configuration key for effective user groups allowed for a superuser
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser groups
   */
  public static @Tainted String getProxySuperuserGroupConfKey(@Tainted String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_GROUPS;
  }
  
  /**
   * Return configuration key for superuser ip addresses
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser ip-addresses
   */
  public static @Tainted String getProxySuperuserIpConfKey(@Tainted String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_HOSTS;
  }
  
  /**
   * Authorize the superuser which is doing doAs
   * 
   * @param user ugi of the effective or proxy user which contains a real user
   * @param remoteAddress the ip address of client
   * @param newConf configuration
   * @throws AuthorizationException
   */
  public static synchronized void authorize(@Tainted UserGroupInformation user, 
      @Tainted
      String remoteAddress, @Tainted Configuration newConf) throws AuthorizationException {

    if(!init) {
      refreshSuperUserGroupsConfiguration(); 
    }

    if (user.getRealUser() == null) {
      return;
    }
    @Tainted
    boolean groupAuthorized = false;
    @Tainted
    boolean ipAuthorized = false;
    @Tainted
    UserGroupInformation superUser = user.getRealUser();

    @Tainted
    Collection<@Tainted String> allowedUserGroups = proxyGroups.get(
        getProxySuperuserGroupConfKey(superUser.getShortUserName()));
    
    if (isWildcardList(allowedUserGroups)) {
      groupAuthorized = true;
    } else if (allowedUserGroups != null && !allowedUserGroups.isEmpty()) {
      for (@Tainted String group : user.getGroupNames()) {
        if (allowedUserGroups.contains(group)) {
          groupAuthorized = true;
          break;
        }
      }
    }

    if (!groupAuthorized) {
      throw new @Tainted AuthorizationException("User: " + superUser.getUserName()
          + " is not allowed to impersonate " + user.getUserName());
    }
    
    @Tainted
    Collection<@Tainted String> ipList = proxyHosts.get(
        getProxySuperuserIpConfKey(superUser.getShortUserName()));
   
    if (isWildcardList(ipList)) {
      ipAuthorized = true;
    } else if (ipList != null && !ipList.isEmpty()) {
      for (@Tainted String allowedHost : ipList) {
        @Tainted
        InetAddress hostAddr;
        try {
          hostAddr = InetAddress.getByName(allowedHost);
        } catch (@Tainted UnknownHostException e) {
          continue;
        }
        if (hostAddr.getHostAddress().equals(remoteAddress)) {
          // Authorization is successful
          ipAuthorized = true;
        }
      }
    }
    if(!ipAuthorized) {
      throw new @Tainted AuthorizationException("Unauthorized connection for super-user: "
          + superUser.getUserName() + " from IP " + remoteAddress);
    }
  }

  /**
   * Return true if the configuration specifies the special configuration value
   * "*", indicating that any group or host list is allowed to use this configuration.
   */
  private static @Tainted boolean isWildcardList(@Tainted Collection<@Tainted String> list) {
    return (list != null) &&
      (list.size() == 1) &&
      (list.contains("*"));
  }

}
