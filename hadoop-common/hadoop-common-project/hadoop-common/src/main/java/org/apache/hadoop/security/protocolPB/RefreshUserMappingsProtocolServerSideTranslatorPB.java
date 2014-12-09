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
import java.io.IOException;

import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RefreshUserMappingsProtocolServerSideTranslatorPB implements @Tainted RefreshUserMappingsProtocolPB {

  private final @Tainted RefreshUserMappingsProtocol impl;
  
  private final static @Tainted RefreshUserToGroupsMappingsResponseProto 
  VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE =
      RefreshUserToGroupsMappingsResponseProto.newBuilder().build();

  private final static @Tainted RefreshSuperUserGroupsConfigurationResponseProto
  VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE = 
      RefreshSuperUserGroupsConfigurationResponseProto.newBuilder()
      .build();

  public @Tainted RefreshUserMappingsProtocolServerSideTranslatorPB(@Tainted RefreshUserMappingsProtocol impl) {
    this.impl = impl;
  }
  
  @Override
  public @Tainted RefreshUserToGroupsMappingsResponseProto 
      refreshUserToGroupsMappings(@Tainted RefreshUserMappingsProtocolServerSideTranslatorPB this, @Tainted RpcController controller, 
      @Tainted
      RefreshUserToGroupsMappingsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshUserToGroupsMappings();
    } catch (@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
    return VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE;
  }

  @Override
  public @Tainted RefreshSuperUserGroupsConfigurationResponseProto 
      refreshSuperUserGroupsConfiguration(@Tainted RefreshUserMappingsProtocolServerSideTranslatorPB this, @Tainted RpcController controller,
      @Tainted
      RefreshSuperUserGroupsConfigurationRequestProto request)
      throws ServiceException {
    try {
      impl.refreshSuperUserGroupsConfiguration();
    } catch (@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
    return VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE;
  }
}
