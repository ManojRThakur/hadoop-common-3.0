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

import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RefreshAuthorizationPolicyProtocolServerSideTranslatorPB implements
    @Tainted
    RefreshAuthorizationPolicyProtocolPB {

  private final @Tainted RefreshAuthorizationPolicyProtocol impl;

  private final static @Tainted RefreshServiceAclResponseProto
  VOID_REFRESH_SERVICE_ACL_RESPONSE = RefreshServiceAclResponseProto
      .newBuilder().build();

  public @Tainted RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(
      @Tainted
      RefreshAuthorizationPolicyProtocol impl) {
    this.impl = impl;
  }

  @Override
  public @Tainted RefreshServiceAclResponseProto refreshServiceAcl(
      @Tainted RefreshAuthorizationPolicyProtocolServerSideTranslatorPB this, @Tainted
      RpcController controller, @Tainted RefreshServiceAclRequestProto request)
      throws ServiceException {
    try {
      impl.refreshServiceAcl();
    } catch (@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
    return VOID_REFRESH_SERVICE_ACL_RESPONSE;
  }
}
