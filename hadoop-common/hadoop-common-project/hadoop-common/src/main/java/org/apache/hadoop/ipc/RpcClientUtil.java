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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto;
import org.apache.hadoop.net.NetUtils;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class maintains a cache of protocol versions and corresponding protocol
 * signatures, keyed by server address, protocol and rpc kind.
 * The cache is lazily populated. 
 */
public class RpcClientUtil {
  private static @Tainted RpcController NULL_CONTROLLER = null;
  private static final @Tainted int PRIME = 16777619;
  
  private static class ProtoSigCacheKey {
    private @Tainted InetSocketAddress serverAddress;
    private @Tainted String protocol;
    private @Tainted String rpcKind;
    
    @Tainted
    ProtoSigCacheKey(@Tainted InetSocketAddress addr, @Tainted String p, @Tainted String rk) {
      this.serverAddress = addr;
      this.protocol = p;
      this.rpcKind = rk;
    }
    
    @Override //Object
    public @Tainted int hashCode(RpcClientUtil.@Tainted ProtoSigCacheKey this) {
      @Tainted
      int result = 1;
      result = PRIME * result
          + ((serverAddress == null) ? 0 : serverAddress.hashCode());
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + ((rpcKind == null) ? 0 : rpcKind.hashCode());
      return result;
    }
    
    @Override //Object
    public @Tainted boolean equals(RpcClientUtil.@Tainted ProtoSigCacheKey this, @Tainted Object other) {
      if (other == this) {
        return true;
      }
      if (other instanceof @Tainted ProtoSigCacheKey) {
        @Tainted
        ProtoSigCacheKey otherKey = (@Tainted ProtoSigCacheKey) other;
        return (serverAddress.equals(otherKey.serverAddress) &&
            protocol.equals(otherKey.protocol) &&
            rpcKind.equals(otherKey.rpcKind));
      }
      return false;
    }
  }
  
  private static @Tainted ConcurrentHashMap<@Tainted ProtoSigCacheKey, @Tainted Map<@Tainted Long, @Tainted ProtocolSignature>> 
  signatureMap = new @Tainted ConcurrentHashMap<@Tainted ProtoSigCacheKey, @Tainted Map<@Tainted Long, @Tainted ProtocolSignature>>();

  private static void putVersionSignatureMap(@Tainted InetSocketAddress addr,
      @Tainted
      String protocol, @Tainted String rpcKind, @Tainted Map<@Tainted Long, @Tainted ProtocolSignature> map) {
    signatureMap.put(new @Tainted ProtoSigCacheKey(addr, protocol, rpcKind), map);
  }
  
  private static @Tainted Map<@Tainted Long, @Tainted ProtocolSignature> getVersionSignatureMap(
      @Tainted
      InetSocketAddress addr, @Tainted String protocol, @Tainted String rpcKind) {
    return signatureMap.get(new @Tainted ProtoSigCacheKey(addr, protocol, rpcKind));
  }

  /**
   * Returns whether the given method is supported or not.
   * The protocol signatures are fetched and cached. The connection id for the
   * proxy provided is re-used.
   * @param rpcProxy Proxy which provides an existing connection id.
   * @param protocol Protocol for which the method check is required.
   * @param rpcKind The RpcKind for which the method check is required.
   * @param version The version at the client.
   * @param methodName Name of the method.
   * @return true if the method is supported, false otherwise.
   * @throws IOException
   */
  public static @Tainted boolean isMethodSupported(@Tainted Object rpcProxy, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol,
      RPC.@Tainted RpcKind rpcKind, @Tainted long version, @Tainted String methodName) throws IOException {
    @Tainted
    InetSocketAddress serverAddress = RPC.getServerAddress(rpcProxy);
    @Tainted
    Map<@Tainted Long, @Tainted ProtocolSignature> versionMap = getVersionSignatureMap(
        serverAddress, protocol.getName(), rpcKind.toString());

    if (versionMap == null) {
      @Tainted
      Configuration conf = new @Tainted Configuration();
      RPC.setProtocolEngine(conf, ProtocolMetaInfoPB.class,
          ProtobufRpcEngine.class);
      @Tainted
      ProtocolMetaInfoPB protocolInfoProxy = getProtocolMetaInfoProxy(rpcProxy,
          conf);
      GetProtocolSignatureRequestProto.@Tainted Builder builder = 
          GetProtocolSignatureRequestProto.newBuilder();
      builder.setProtocol(protocol.getName());
      builder.setRpcKind(rpcKind.toString());
      @Tainted
      GetProtocolSignatureResponseProto resp;
      try {
        resp = protocolInfoProxy.getProtocolSignature(NULL_CONTROLLER,
            builder.build());
      } catch (@Tainted ServiceException se) {
        throw ProtobufHelper.getRemoteException(se);
      }
      versionMap = convertProtocolSignatureProtos(resp
          .getProtocolSignatureList());
      putVersionSignatureMap(serverAddress, protocol.getName(),
          rpcKind.toString(), versionMap);
    }
    // Assuming unique method names.
    @Tainted
    Method desiredMethod;
    @Tainted
    Method @Tainted [] allMethods = protocol.getMethods();
    desiredMethod = null;
    for (@Tainted Method m : allMethods) {
      if (m.getName().equals(methodName)) {
        desiredMethod = m;
        break;
      }
    }
    if (desiredMethod == null) {
      return false;
    }
    @Tainted
    int methodHash = ProtocolSignature.getFingerprint(desiredMethod);
    return methodExists(methodHash, version, versionMap);
  }
  
  private static @Tainted Map<@Tainted Long, @Tainted ProtocolSignature> 
  convertProtocolSignatureProtos(@Tainted List<@Tainted ProtocolSignatureProto> protoList) {
    @Tainted
    Map<@Tainted Long, @Tainted ProtocolSignature> map = new @Tainted TreeMap<@Tainted Long, @Tainted ProtocolSignature>();
    for (@Tainted ProtocolSignatureProto p : protoList) {
      @Tainted
      int @Tainted [] methods = new @Tainted int @Tainted [p.getMethodsList().size()];
      @Tainted
      int index=0;
      for (@Tainted int m : p.getMethodsList()) {
        methods[index++] = m;
      }
      map.put(p.getVersion(), new @Tainted ProtocolSignature(p.getVersion(), methods));
    }
    return map;
  }

  private static @Tainted boolean methodExists(@Tainted int methodHash, @Tainted long version,
      @Tainted
      Map<@Tainted Long, @Tainted ProtocolSignature> versionMap) {
    @Tainted
    ProtocolSignature sig = versionMap.get(version);
    if (sig != null) {
      for (@Tainted int m : sig.getMethods()) {
        if (m == methodHash) {
          return true;
        }
      }
    }
    return false;
  }
  
  // The proxy returned re-uses the underlying connection. This is a special 
  // mechanism for ProtocolMetaInfoPB.
  // Don't do this for any other protocol, it might cause a security hole.
  private static @Tainted ProtocolMetaInfoPB getProtocolMetaInfoProxy(@Tainted Object proxy,
      @Tainted
      Configuration conf) throws IOException {
    @Tainted
    RpcInvocationHandler inv = (@Tainted RpcInvocationHandler) Proxy
        .getInvocationHandler(proxy);
    return RPC
        .getProtocolEngine(ProtocolMetaInfoPB.class, conf)
        .getProtocolMetaInfoProxy(inv.getConnectionId(), conf,
            NetUtils.getDefaultSocketFactory(conf)).getProxy();
  }
}
