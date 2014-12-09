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
package org.apache.hadoop.ha.protocolPB;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFCProtocol;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveRequestProto;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverRequestProto;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;


public class ZKFCProtocolClientSideTranslatorPB implements
  @Tainted
  ZKFCProtocol, @Tainted Closeable, @Tainted ProtocolTranslator {

  private final static @Tainted RpcController NULL_CONTROLLER = null;
  private final @Tainted ZKFCProtocolPB rpcProxy;

  public @Tainted ZKFCProtocolClientSideTranslatorPB(
      @Tainted
      InetSocketAddress addr, @Tainted Configuration conf,
      @Tainted
      SocketFactory socketFactory, @Tainted int timeout) throws IOException {
    RPC.setProtocolEngine(conf, ZKFCProtocolPB.class,
        ProtobufRpcEngine.class);
    rpcProxy = RPC.getProxy(ZKFCProtocolPB.class,
        RPC.getProtocolVersion(ZKFCProtocolPB.class), addr,
        UserGroupInformation.getCurrentUser(), conf, socketFactory, timeout);
  }

  @Override
  public void cedeActive(@Tainted ZKFCProtocolClientSideTranslatorPB this, @Tainted int millisToCede) throws IOException,
      AccessControlException {
    try {
      @Tainted
      CedeActiveRequestProto req = CedeActiveRequestProto.newBuilder()
          .setMillisToCede(millisToCede)
          .build();
      rpcProxy.cedeActive(NULL_CONTROLLER, req);      
    } catch (@Tainted ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void gracefulFailover(@Tainted ZKFCProtocolClientSideTranslatorPB this) throws IOException, AccessControlException {
    try {
      rpcProxy.gracefulFailover(NULL_CONTROLLER,
          GracefulFailoverRequestProto.getDefaultInstance());
    } catch (@Tainted ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }


  @Override
  public void close(@Tainted ZKFCProtocolClientSideTranslatorPB this) {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public @Tainted Object getUnderlyingProxyObject(@Tainted ZKFCProtocolClientSideTranslatorPB this) {
    return rpcProxy;
  }
}
