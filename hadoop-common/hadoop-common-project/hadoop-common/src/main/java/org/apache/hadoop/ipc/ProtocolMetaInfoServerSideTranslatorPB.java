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
package org.apache.hadoop.ipc;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolVersionProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class serves the requests for protocol versions and signatures by
 * looking them up in the server registry.
 */
public class ProtocolMetaInfoServerSideTranslatorPB implements
    @Tainted
    ProtocolMetaInfoPB {

  RPC.@Tainted Server server;
  
  public @Tainted ProtocolMetaInfoServerSideTranslatorPB(RPC.@Tainted Server server) {
    this.server = server;
  }
  
  @Override
  public @Tainted GetProtocolVersionsResponseProto getProtocolVersions(
      @Tainted ProtocolMetaInfoServerSideTranslatorPB this, @Tainted
      RpcController controller, @Tainted GetProtocolVersionsRequestProto request)
      throws ServiceException {
    @Tainted
    String protocol = request.getProtocol();
    GetProtocolVersionsResponseProto.@Tainted Builder builder = 
        GetProtocolVersionsResponseProto.newBuilder();
    for (RPC.@Tainted RpcKind r : RPC.RpcKind.values()) {
      @Tainted
      long @Tainted [] versions;
      try {
        versions = getProtocolVersionForRpcKind(r, protocol);
      } catch (@Tainted ClassNotFoundException e) {
        throw new @Tainted ServiceException(e);
      }
      ProtocolVersionProto.@Tainted Builder b = ProtocolVersionProto.newBuilder();
      if (versions != null) {
        b.setRpcKind(r.toString());
        for (@Tainted long v : versions) {
          b.addVersions(v);
        }
      }
      builder.addProtocolVersions(b.build());
    }
    return builder.build();
  }

  @Override
  public @Tainted GetProtocolSignatureResponseProto getProtocolSignature(
      @Tainted ProtocolMetaInfoServerSideTranslatorPB this, @Tainted
      RpcController controller, @Tainted GetProtocolSignatureRequestProto request)
      throws ServiceException {
    GetProtocolSignatureResponseProto.@Tainted Builder builder = GetProtocolSignatureResponseProto
        .newBuilder();
    @Tainted
    String protocol = request.getProtocol();
    @Tainted
    String rpcKind = request.getRpcKind();
    @Tainted
    long @Tainted [] versions;
    try {
      versions = getProtocolVersionForRpcKind(RPC.RpcKind.valueOf(rpcKind),
          protocol);
    } catch (@Tainted ClassNotFoundException e1) {
      throw new @Tainted ServiceException(e1);
    }
    if (versions == null) {
      return builder.build();
    }
    for (@Tainted long v : versions) {
      ProtocolSignatureProto.@Tainted Builder sigBuilder = ProtocolSignatureProto
          .newBuilder();
      sigBuilder.setVersion(v);
      try {
        @Tainted
        ProtocolSignature signature = ProtocolSignature.getProtocolSignature(
            protocol, v);
        for (@Tainted int m : signature.getMethods()) {
          sigBuilder.addMethods(m);
        }
      } catch (@Tainted ClassNotFoundException e) {
        throw new @Tainted ServiceException(e);
      }
      builder.addProtocolSignature(sigBuilder.build());
    }
    return builder.build();
  }
  
  private @Tainted long @Tainted [] getProtocolVersionForRpcKind(@Tainted ProtocolMetaInfoServerSideTranslatorPB this, RPC.@Tainted RpcKind rpcKind,
      @Tainted
      String protocol) throws ClassNotFoundException {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass = Class.forName(protocol);
    @Tainted
    String protocolName = RPC.getProtocolName(protocolClass);
    @Tainted
    VerProtocolImpl @Tainted [] vers = server.getSupportedProtocolVersions(rpcKind,
        protocolName);
    if (vers == null) {
      return null;
    }
    @Tainted
    long @Tainted [] versions = new @Tainted long @Tainted [vers.length];
    for (@Tainted int i=0; i<versions.length; i++) {
      versions[i] = vers[i].version;
    }
    return versions;
  }
}
