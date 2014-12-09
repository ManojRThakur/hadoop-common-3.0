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
package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Utilities for working with ZooKeeper.
 */
@InterfaceAudience.Private
public class ZKUtil {
  
  /**
   * Parse ACL permission string, partially borrowed from
   * ZooKeeperMain private method
   */
  private static @Tainted int getPermFromString(@Tainted String permString) {
    @Tainted
    int perm = 0;
    for (@Tainted int i = 0; i < permString.length(); i++) {
      @Tainted
      char c = permString.charAt(i); 
      switch (c) {
      case 'r':
        perm |= ZooDefs.Perms.READ;
        break;
      case 'w':
        perm |= ZooDefs.Perms.WRITE;
        break;
      case 'c':
        perm |= ZooDefs.Perms.CREATE;
        break;
      case 'd':
        perm |= ZooDefs.Perms.DELETE;
        break;
      case 'a':
        perm |= ZooDefs.Perms.ADMIN;
        break;
      default:
        throw new @Tainted BadAclFormatException(
            "Invalid permission '" + c + "' in permission string '" +
            permString + "'");
      }
    }
    return perm;
  }

  /**
   * Parse comma separated list of ACL entries to secure generated nodes, e.g.
   * <code>sasl:hdfs/host1@MY.DOMAIN:cdrwa,sasl:hdfs/host2@MY.DOMAIN:cdrwa</code>
   *
   * @return ACL list
   * @throws {@link BadAclFormatException} if an ACL is invalid
   */
  public static @Tainted List<@Tainted ACL> parseACLs(@Tainted String aclString) throws
      BadAclFormatException {
    @Tainted
    List<@Tainted ACL> acl = Lists.newArrayList();
    if (aclString == null) {
      return acl;
    }
    
    @Tainted
    List<@Tainted String> aclComps = Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
        .split(aclString));
    for (@Tainted String a : aclComps) {
      // from ZooKeeperMain private method
      @Tainted
      int firstColon = a.indexOf(':');
      @Tainted
      int lastColon = a.lastIndexOf(':');
      if (firstColon == -1 || lastColon == -1 || firstColon == lastColon) {
        throw new @Tainted BadAclFormatException(
            "ACL '" + a + "' not of expected form scheme:id:perm");
      }

      @Tainted
      ACL newAcl = new @Tainted ACL();
      newAcl.setId(new @Tainted Id(a.substring(0, firstColon), a.substring(
          firstColon + 1, lastColon)));
      newAcl.setPerms(getPermFromString(a.substring(lastColon + 1)));
      acl.add(newAcl);
    }
    
    return acl;
  }
  
  /**
   * Parse a comma-separated list of authentication mechanisms. Each
   * such mechanism should be of the form 'scheme:auth' -- the same
   * syntax used for the 'addAuth' command in the ZK CLI.
   * 
   * @param authString the comma-separated auth mechanisms
   * @return a list of parsed authentications
   * @throws {@link BadAuthFormatException} if the auth format is invalid
   */
  public static @Tainted List<@Tainted ZKAuthInfo> parseAuth(@Tainted String authString) throws
      BadAuthFormatException{
    @Tainted
    List<@Tainted ZKAuthInfo> ret = Lists.newArrayList();
    if (authString == null) {
      return ret;
    }
    
    @Tainted
    List<@Tainted String> authComps = Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
        .split(authString));
    
    for (@Tainted String comp : authComps) {
      @Tainted
      String parts @Tainted [] = comp.split(":", 2);
      if (parts.length != 2) {
        throw new @Tainted BadAuthFormatException(
            "Auth '" + comp + "' not of expected form scheme:auth");
      }
      ret.add(new @Tainted ZKAuthInfo(parts[0],
          parts[1].getBytes(Charsets.UTF_8)));
    }
    return ret;
  }
  
  /**
   * Because ZK ACLs and authentication information may be secret,
   * allow the configuration values to be indirected through a file
   * by specifying the configuration as "@/path/to/file". If this
   * syntax is used, this function will return the contents of the file
   * as a String.
   * 
   * @param valInConf the value from the Configuration 
   * @return either the same value, or the contents of the referenced
   * file if the configured value starts with "@"
   * @throws IOException if the file cannot be read
   */
  public static @Tainted String resolveConfIndirection(@Tainted String valInConf)
      throws IOException {
    if (valInConf == null) return null;
    if (!valInConf.startsWith("@")) {
      return valInConf;
    }
    @Tainted
    String path = valInConf.substring(1).trim();
    return Files.toString(new @Tainted File(path), Charsets.UTF_8).trim();
  }

  /**
   * An authentication token passed to ZooKeeper.addAuthInfo
   */
  @InterfaceAudience.Private
  public static class ZKAuthInfo {
    private final @Tainted String scheme;
    private final @Tainted byte @Tainted [] auth;
    
    public @Tainted ZKAuthInfo(@Tainted String scheme, @Tainted byte @Tainted [] auth) {
      super();
      this.scheme = scheme;
      this.auth = auth;
    }

    public @Tainted String getScheme(ZKUtil.@Tainted ZKAuthInfo this) {
      return scheme;
    }

    public @Tainted byte @Tainted [] getAuth(ZKUtil.@Tainted ZKAuthInfo this) {
      return auth;
    }
  }

  @InterfaceAudience.Private
  public static class BadAclFormatException extends
      @Tainted
      HadoopIllegalArgumentException {
    private static final @Tainted long serialVersionUID = 1L;

    public @Tainted BadAclFormatException(@Tainted String message) {
      super(message);
    }
  }

  @InterfaceAudience.Private
  public static class BadAuthFormatException extends
      @Tainted
      HadoopIllegalArgumentException {
    private static final @Tainted long serialVersionUID = 1L;

    public @Tainted BadAuthFormatException(@Tainted String message) {
      super(message);
    }
  }
}
