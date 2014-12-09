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
package org.apache.hadoop.ha;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolClientSideTranslatorPB;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolClientSideTranslatorPB;
import org.apache.hadoop.net.NetUtils;

import com.google.common.collect.Maps;

/**
 * Represents a target of the client side HA administration commands.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class HAServiceTarget {

  private static final @Tainted String HOST_SUBST_KEY = "host";
  private static final @Tainted String PORT_SUBST_KEY = "port";
  private static final @Tainted String ADDRESS_SUBST_KEY = "address";

  /**
   * @return the IPC address of the target node.
   */
  public abstract @Untainted InetSocketAddress getAddress(@Tainted HAServiceTarget this);

  /**
   * @return the IPC address of the ZKFC on the target node
   */
  public abstract @Tainted InetSocketAddress getZKFCAddress(@Tainted HAServiceTarget this);

  /**
   * @return a Fencer implementation configured for this target node
   */
  public abstract @Tainted NodeFencer getFencer(@Tainted HAServiceTarget this);
  
  /**
   * @throws BadFencingConfigurationException if the fencing configuration
   * appears to be invalid. This is divorced from the above
   * {@link #getFencer()} method so that the configuration can be checked
   * during the pre-flight phase of failover.
   */
  public abstract void checkFencingConfigured(@Tainted HAServiceTarget this)
      throws BadFencingConfigurationException;
  
  /**
   * @return a proxy to connect to the target HA Service.
   */
  public @Tainted HAServiceProtocol getProxy(@Tainted HAServiceTarget this, @Tainted Configuration conf, @Tainted int timeoutMs)
      throws IOException {
    @Tainted
    Configuration confCopy = new @Tainted Configuration(conf);
    // Lower the timeout so we quickly fail to connect
    confCopy.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    @Tainted
    SocketFactory factory = NetUtils.getDefaultSocketFactory(confCopy);
    return new @Tainted HAServiceProtocolClientSideTranslatorPB(
        getAddress(),
        confCopy, factory, timeoutMs);
  }
  
  /**
   * @return a proxy to the ZKFC which is associated with this HA service.
   */
  public @Tainted ZKFCProtocol getZKFCProxy(@Tainted HAServiceTarget this, @Tainted Configuration conf, @Tainted int timeoutMs)
      throws IOException {
    @Tainted
    Configuration confCopy = new @Tainted Configuration(conf);
    // Lower the timeout so we quickly fail to connect
    confCopy.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    @Tainted
    SocketFactory factory = NetUtils.getDefaultSocketFactory(confCopy);
    return new @Tainted ZKFCProtocolClientSideTranslatorPB(
        getZKFCAddress(),
        confCopy, factory, timeoutMs);
  }
  
  public final @Tainted Map<@Tainted String, @Tainted String> getFencingParameters(@Tainted HAServiceTarget this) {
    @Tainted
    Map<@Tainted String, @Tainted String> ret = Maps.newHashMap();
    addFencingParameters(ret);
    return ret;
  }
  
  /**
   * Hook to allow subclasses to add any parameters they would like to
   * expose to fencing implementations/scripts. Fencing methods are free
   * to use this map as they see fit -- notably, the shell script
   * implementation takes each entry, prepends 'target_', substitutes
   * '_' for '.', and adds it to the environment of the script.
   *
   * Subclass implementations should be sure to delegate to the superclass
   * implementation as well as adding their own keys.
   *
   * @param ret map which can be mutated to pass parameters to the fencer
   */
  protected void addFencingParameters(@Tainted HAServiceTarget this, @Tainted Map<@Tainted String, @Tainted String> ret) {
    ret.put(ADDRESS_SUBST_KEY, String.valueOf(getAddress()));
    ret.put(HOST_SUBST_KEY, getAddress().getHostName());
    ret.put(PORT_SUBST_KEY, String.valueOf(getAddress().getPort()));
  }

  /**
   * @return true if auto failover should be considered enabled
   */
  public @Tainted boolean isAutoFailoverEnabled(@Tainted HAServiceTarget this) {
    return false;
  }
}
