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
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.*;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.ByteString;

public abstract class ProtoUtil {

  /**
   * Read a variable length integer in the same format that ProtoBufs encodes.
   * @param in the input stream to read from
   * @return the integer
   * @throws IOException if it is malformed or EOF.
   */
  public static @Tainted int readRawVarint32(@Tainted DataInput in) throws IOException {
    @Tainted
    byte tmp = in.readByte();
    if (tmp >= 0) {
      return tmp;
    }
    @Tainted
    int result = tmp & 0x7f;
    if ((tmp = in.readByte()) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = in.readByte()) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = in.readByte()) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = in.readByte()) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (@Tainted int i = 0; i < 5; i++) {
              if (in.readByte() >= 0) {
                return result;
              }
            }
            throw new @Tainted IOException("Malformed varint");
          }
        }
      }
    }
    return result;
  }

  
  /** 
   * This method creates the connection context  using exactly the same logic
   * as the old connection context as was done for writable where
   * the effective and real users are set based on the auth method.
   *
   */
  public static @Tainted IpcConnectionContextProto makeIpcConnectionContext(
      final @Tainted String protocol,
      final @Tainted UserGroupInformation ugi, final @Tainted AuthMethod authMethod) {
    IpcConnectionContextProto.@Tainted Builder result = IpcConnectionContextProto.newBuilder();
    if (protocol != null) {
      result.setProtocol(protocol);
    }
    UserInformationProto.@Tainted Builder ugiProto =  UserInformationProto.newBuilder();
    if (ugi != null) {
      /*
       * In the connection context we send only additional user info that
       * is not derived from the authentication done during connection setup.
       */
      if (authMethod == AuthMethod.KERBEROS) {
        // Real user was established as part of the connection.
        // Send effective user only.
        ugiProto.setEffectiveUser(ugi.getUserName());
      } else if (authMethod == AuthMethod.TOKEN) {
        // With token, the connection itself establishes 
        // both real and effective user. Hence send none in header.
      } else {  // Simple authentication
        // No user info is established as part of the connection.
        // Send both effective user and real user
        ugiProto.setEffectiveUser(ugi.getUserName());
        if (ugi.getRealUser() != null) {
          ugiProto.setRealUser(ugi.getRealUser().getUserName());
        }
      }
    }   
    result.setUserInfo(ugiProto);
    return result.build();
  }
  
  public static @Tainted UserGroupInformation getUgi(@Tainted IpcConnectionContextProto context) {
    if (context.hasUserInfo()) {
      @Tainted
      UserInformationProto userInfo = context.getUserInfo();
        return getUgi(userInfo);
    } else {
      return null;
    }
  }
  
  public static @Tainted UserGroupInformation getUgi(@Tainted UserInformationProto userInfo) {
    @Tainted
    UserGroupInformation ugi = null;
    @Tainted
    String effectiveUser = userInfo.hasEffectiveUser() ? userInfo
        .getEffectiveUser() : null;
    @Tainted
    String realUser = userInfo.hasRealUser() ? userInfo.getRealUser() : null;
    if (effectiveUser != null) {
      if (realUser != null) {
        @Tainted
        UserGroupInformation realUserUgi = UserGroupInformation
            .createRemoteUser(realUser);
        ugi = UserGroupInformation
            .createProxyUser(effectiveUser, realUserUgi);
      } else {
        ugi = org.apache.hadoop.security.UserGroupInformation
            .createRemoteUser(effectiveUser);
      }
    }
    return ugi;
  }
  
  static @Tainted RpcKindProto convert(RPC.@Tainted RpcKind kind) {
    switch (kind) {
    case RPC_BUILTIN: return RpcKindProto.RPC_BUILTIN;
    case RPC_WRITABLE: return RpcKindProto.RPC_WRITABLE;
    case RPC_PROTOCOL_BUFFER: return RpcKindProto.RPC_PROTOCOL_BUFFER;
    }
    return null;
  }
  
  
  public static RPC.@Tainted RpcKind convert( @Tainted RpcKindProto kind) {
    switch (kind) {
    case RPC_BUILTIN: return RPC.RpcKind.RPC_BUILTIN;
    case RPC_WRITABLE: return RPC.RpcKind.RPC_WRITABLE;
    case RPC_PROTOCOL_BUFFER: return RPC.RpcKind.RPC_PROTOCOL_BUFFER;
    }
    return null;
  }
 
  public static @Tainted RpcRequestHeaderProto makeRpcRequestHeader(RPC.@Tainted RpcKind rpcKind,
      RpcRequestHeaderProto.@Tainted OperationProto operation, @Tainted int callId,
      @Tainted
      int retryCount, @Tainted byte @Tainted [] uuid) {
    RpcRequestHeaderProto.@Tainted Builder result = RpcRequestHeaderProto.newBuilder();
    result.setRpcKind(convert(rpcKind)).setRpcOp(operation).setCallId(callId)
        .setRetryCount(retryCount).setClientId(ByteString.copyFrom(uuid));
    return result.build();
  }
}
