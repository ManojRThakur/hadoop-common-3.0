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

package org.apache.hadoop.security.protocolPB;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RefreshUserMappingsProtocolClientSideTranslatorPB implements
    @Tainted
    ProtocolMetaInterface, @Tainted RefreshUserMappingsProtocol, @Tainted Closeable {

  /** RpcController is not used and hence is set to null */
  private final static @Tainted RpcController NULL_CONTROLLER = null;
  private final @Tainted RefreshUserMappingsProtocolPB rpcProxy;
  
  private final static @Tainted RefreshUserToGroupsMappingsRequestProto 
  VOID_REFRESH_USER_TO_GROUPS_MAPPING_REQUEST = 
      RefreshUserToGroupsMappingsRequestProto.newBuilder().build();

  private final static @Tainted RefreshSuperUserGroupsConfigurationRequestProto
  VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_REQUEST = 
      RefreshSuperUserGroupsConfigurationRequestProto.newBuilder().build();

  public @Tainted RefreshUserMappingsProtocolClientSideTranslatorPB(
      @Tainted
      RefreshUserMappingsProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close(@Tainted RefreshUserMappingsProtocolClientSideTranslatorPB this) throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void refreshUserToGroupsMappings(@Tainted RefreshUserMappingsProtocolClientSideTranslatorPB this) throws IOException {
    try {
      rpcProxy.refreshUserToGroupsMappings(NULL_CONTROLLER,
          VOID_REFRESH_USER_TO_GROUPS_MAPPING_REQUEST);
    } catch (@Tainted ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public void refreshSuperUserGroupsConfiguration(@Tainted RefreshUserMappingsProtocolClientSideTranslatorPB this) throws IOException {
    try {
      rpcProxy.refreshSuperUserGroupsConfiguration(NULL_CONTROLLER,
          VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_REQUEST);
    } catch (@Tainted ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public @Tainted boolean isMethodSupported(@Tainted RefreshUserMappingsProtocolClientSideTranslatorPB this, @Tainted String methodName) throws IOException {
    return RpcClientUtil
        .isMethodSupported(rpcProxy, RefreshUserMappingsProtocolPB.class,
            RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            RPC.getProtocolVersion(RefreshUserMappingsProtocolPB.class),
            methodName);
  }
}
