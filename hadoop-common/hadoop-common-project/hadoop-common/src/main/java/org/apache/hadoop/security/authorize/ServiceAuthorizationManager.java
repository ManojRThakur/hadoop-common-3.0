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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.net.InetAddress;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An authorization manager which handles service-level authorization
 * for incoming service requests.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ServiceAuthorizationManager {
  private static final @Tainted String HADOOP_POLICY_FILE = "hadoop-policy.xml";

  private @Tainted Map<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted AccessControlList> protocolToAcl =
    new @Tainted IdentityHashMap<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted AccessControlList>();
  
  /**
   * Configuration key for controlling service-level authorization for Hadoop.
   * 
   * @deprecated Use
   *             {@link CommonConfigurationKeys#HADOOP_SECURITY_AUTHORIZATION}
   *             instead.
   */
  @Deprecated
  public static final @Tainted String SERVICE_AUTHORIZATION_CONFIG = 
    "hadoop.security.authorization";
  
  public static final @Tainted Log AUDITLOG =
    LogFactory.getLog("SecurityLogger."+ServiceAuthorizationManager.class.getName());

  private static final @Tainted String AUTHZ_SUCCESSFUL_FOR = "Authorization successful for ";
  private static final @Tainted String AUTHZ_FAILED_FOR = "Authorization failed for ";

  
  /**
   * Authorize the user to access the protocol being used.
   * 
   * @param user user accessing the service 
   * @param protocol service being accessed
   * @param conf configuration to use
   * @param addr InetAddress of the client
   * @throws AuthorizationException on authorization failure
   */
  public void authorize(@Tainted ServiceAuthorizationManager this, @Tainted UserGroupInformation user, 
                               @Tainted
                               Class<@Tainted ? extends java.lang.@Tainted Object> protocol,
                               @Tainted
                               Configuration conf,
                               @Tainted
                               InetAddress addr
                               ) throws AuthorizationException {
    @Tainted
    AccessControlList acl = protocolToAcl.get(protocol);
    if (acl == null) {
      throw new @Tainted AuthorizationException("Protocol " + protocol + 
                                       " is not known.");
    }
    
    // get client principal key to verify (if available)
    @Tainted
    KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
    @Tainted
    String clientPrincipal = null; 
    if (krbInfo != null) {
      @Tainted
      String clientKey = krbInfo.clientPrincipal();
      if (clientKey != null && !clientKey.isEmpty()) {
        try {
          clientPrincipal = SecurityUtil.getServerPrincipal(
              conf.get(clientKey), addr);
        } catch (@Tainted IOException e) {
          throw (@Tainted AuthorizationException) new @Tainted AuthorizationException(
              "Can't figure out Kerberos principal name for connection from "
                  + addr + " for user=" + user + " protocol=" + protocol)
              .initCause(e);
        }
      }
    }
    if((clientPrincipal != null && !clientPrincipal.equals(user.getUserName())) || 
        !acl.isUserAllowed(user)) {
      AUDITLOG.warn(AUTHZ_FAILED_FOR + user + " for protocol=" + protocol
          + ", expected client Kerberos principal is " + clientPrincipal);
      throw new @Tainted AuthorizationException("User " + user + 
          " is not authorized for protocol " + protocol + 
          ", expected client Kerberos principal is " + clientPrincipal);
    }
    AUDITLOG.info(AUTHZ_SUCCESSFUL_FOR + user + " for protocol="+protocol);
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  public synchronized void refresh(@Tainted ServiceAuthorizationManager this, @Tainted Configuration conf,
                                          @Tainted
                                          PolicyProvider provider) {
    //ostrusted, properties are trusted
    // Get the system property 'hadoop.policy.file'
    @Untainted String policyFile = (@Untainted String) System.getProperty("hadoop.policy.file", HADOOP_POLICY_FILE);
    
    // Make a copy of the original config, and load the policy file
    @Tainted
    Configuration policyConf = new @Tainted Configuration(conf);
    policyConf.addResource(policyFile);
    
    final @Tainted Map<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted AccessControlList> newAcls =
      new @Tainted IdentityHashMap<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted AccessControlList>();

    // Parse the config file
    @Tainted
    Service @Tainted [] services = provider.getServices();
    if (services != null) {
      for (@Tainted Service service : services) {
        @Tainted
        AccessControlList acl = 
          new @Tainted AccessControlList(
              policyConf.get(service.getServiceKey(), 
                             AccessControlList.WILDCARD_ACL_VALUE)
              );
        newAcls.put(service.getProtocol(), acl);
      }
    }

    // Flip to the newly parsed permissions
    protocolToAcl = newAcls;
  }

  // Package-protected for use in tests.
  @Tainted
  Set<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>> getProtocolsWithAcls(@Tainted ServiceAuthorizationManager this) {
    return protocolToAcl.keySet();
  }
}
