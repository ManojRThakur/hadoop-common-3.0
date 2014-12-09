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

package org.apache.hadoop.tools.protocolPB;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GetUserMappingsProtocolServerSideTranslatorPB implements
    @Tainted
    GetUserMappingsProtocolPB {

  private final @Tainted GetUserMappingsProtocol impl;

  public @Tainted GetUserMappingsProtocolServerSideTranslatorPB(
      @Tainted
      GetUserMappingsProtocol impl) {
    this.impl = impl;
  }

  @Override
  public @Tainted GetGroupsForUserResponseProto getGroupsForUser(
      @Tainted GetUserMappingsProtocolServerSideTranslatorPB this, @Tainted
      RpcController controller, @Tainted GetGroupsForUserRequestProto request)
      throws ServiceException {
    @Tainted
    String @Tainted [] groups;
    try {
      groups = impl.getGroupsForUser(request.getUser());
    } catch (@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
    GetGroupsForUserResponseProto.@Tainted Builder builder = GetGroupsForUserResponseProto
        .newBuilder();
    for (@Tainted String g : groups) {
      builder.addGroups(g);
    }
    return builder.build();
  }
}
