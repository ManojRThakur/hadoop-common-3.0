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
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.ZKFCProtocolService;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.PolicyProvider;

import com.google.protobuf.BlockingService;

@InterfaceAudience.LimitedPrivate("HDFS")
@InterfaceStability.Evolving
public class ZKFCRpcServer implements @Tainted ZKFCProtocol {

  private static final @Tainted int HANDLER_COUNT = 3;
  private final @Tainted ZKFailoverController zkfc;
  private @Tainted Server server;

  @Tainted
  ZKFCRpcServer(@Tainted Configuration conf,
      @Tainted
      InetSocketAddress bindAddr,
      @Tainted
      ZKFailoverController zkfc,
      @Tainted
      PolicyProvider policy) throws IOException {
    this.zkfc = zkfc;
    
    RPC.setProtocolEngine(conf, ZKFCProtocolPB.class,
        ProtobufRpcEngine.class);
    @Tainted
    ZKFCProtocolServerSideTranslatorPB translator =
        new @Tainted ZKFCProtocolServerSideTranslatorPB(this);
    @Tainted
    BlockingService service = ZKFCProtocolService
        .newReflectiveBlockingService(translator);
    this.server = new RPC.@Tainted Builder(conf).setProtocol(ZKFCProtocolPB.class)
        .setInstance(service).setBindAddress(bindAddr.getHostName())
        .setPort(bindAddr.getPort()).setNumHandlers(HANDLER_COUNT)
        .setVerbose(false).build();
    
    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      server.refreshServiceAcl(conf, policy);
    }

  }
  
  void start(@Tainted ZKFCRpcServer this) {
    this.server.start();
  }

  public @Tainted InetSocketAddress getAddress(@Tainted ZKFCRpcServer this) {
    return server.getListenerAddress();
  }

  void stopAndJoin(@Tainted ZKFCRpcServer this) throws InterruptedException {
    this.server.stop();
    this.server.join();
  }
  
  @Override
  public void cedeActive(@Tainted ZKFCRpcServer this, @Tainted int millisToCede) throws IOException,
      AccessControlException {
    zkfc.checkRpcAdminAccess();
    zkfc.cedeActive(millisToCede);
  }

  @Override
  public void gracefulFailover(@Tainted ZKFCRpcServer this) throws IOException, AccessControlException {
    zkfc.checkRpcAdminAccess();
    zkfc.gracefulFailoverToYou();
  }

}
