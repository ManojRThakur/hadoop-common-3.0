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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

/**
 * RPC Engine for for protobuf based RPCs.
 */
@InterfaceStability.Evolving
public class ProtobufRpcEngine implements @Tainted RpcEngine {
  public static final @Tainted Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);
  
  static { // Register the rpcRequest deserializer for WritableRpcEngine 
    org.apache.hadoop.ipc.Server.registerProtocolEngine(
        RPC.RpcKind.RPC_PROTOCOL_BUFFER, RpcRequestWrapper.class,
        new Server.@Tainted ProtoBufRpcInvoker());
  }

  private static final @Tainted ClientCache CLIENTS = new @Tainted ClientCache();

  public <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> getProxy(@Tainted ProtobufRpcEngine this, @Tainted Class<@Tainted T> protocol, @Tainted long clientVersion,
      @Tainted
      InetSocketAddress addr, @Tainted UserGroupInformation ticket, @Tainted Configuration conf,
      @Tainted
      SocketFactory factory, @Tainted int rpcTimeout) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
        rpcTimeout, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> getProxy(@Tainted ProtobufRpcEngine this, @Tainted Class<@Tainted T> protocol, @Tainted long clientVersion,
      @Tainted
      InetSocketAddress addr, @Tainted UserGroupInformation ticket, @Tainted Configuration conf,
      @Tainted
      SocketFactory factory, @Tainted int rpcTimeout, @Tainted RetryPolicy connectionRetryPolicy
      ) throws IOException {

    final @Tainted Invoker invoker = new @Tainted Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout, connectionRetryPolicy);
    return new @Tainted ProtocolProxy<@Tainted T>(protocol, (@Tainted T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new @Tainted Class @Tainted []{protocol}, invoker), false);
  }
  
  @Override
  public @Tainted ProtocolProxy<@Tainted ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      @Tainted ProtobufRpcEngine this, @Tainted
      ConnectionId connId, @Tainted Configuration conf, @Tainted SocketFactory factory)
      throws IOException {
    @Tainted
    Class<@Tainted ProtocolMetaInfoPB> protocol = ProtocolMetaInfoPB.class;
    return new @Tainted ProtocolProxy<@Tainted ProtocolMetaInfoPB>(protocol,
        (@Tainted ProtocolMetaInfoPB) Proxy.newProxyInstance(protocol.getClassLoader(),
            new @Tainted Class @Tainted [] { protocol }, new @Tainted Invoker(protocol, connId, conf,
                factory)), false);
  }

  private static class Invoker implements @Tainted RpcInvocationHandler {
    private final @Tainted Map<@Tainted String, @Tainted Message> returnTypes = 
        new @Tainted ConcurrentHashMap<@Tainted String, @Tainted Message>();
    private @Tainted boolean isClosed = false;
    private final Client.@Tainted ConnectionId remoteId;
    private final @Tainted Client client;
    private final @Tainted long clientProtocolVersion;
    private final @Tainted String protocolName;

    private @Tainted Invoker(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted InetSocketAddress addr,
        @Tainted
        UserGroupInformation ticket, @Tainted Configuration conf, @Tainted SocketFactory factory,
        @Tainted
        int rpcTimeout, @Tainted RetryPolicy connectionRetryPolicy) throws IOException {
      this(protocol, Client.ConnectionId.getConnectionId(
          addr, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf),
          conf, factory);
    }
    
    /**
     * This constructor takes a connectionId, instead of creating a new one.
     */
    private @Tainted Invoker(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol, Client.@Tainted ConnectionId connId,
        @Tainted
        Configuration conf, @Tainted SocketFactory factory) {
      this.remoteId = connId;
      this.client = CLIENTS.getClient(conf, factory, RpcResponseWrapper.class);
      this.protocolName = RPC.getProtocolName(protocol);
      this.clientProtocolVersion = RPC
          .getProtocolVersion(protocol);
    }

    private @Tainted RequestHeaderProto constructRpcRequestHeader(ProtobufRpcEngine.@Tainted Invoker this, @Tainted Method method) {
      RequestHeaderProto.@Tainted Builder builder = RequestHeaderProto
          .newBuilder();
      builder.setMethodName(method.getName());
     

      // For protobuf, {@code protocol} used when creating client side proxy is
      // the interface extending BlockingInterface, which has the annotations 
      // such as ProtocolName etc.
      //
      // Using Method.getDeclaringClass(), as in WritableEngine to get at
      // the protocol interface will return BlockingInterface, from where 
      // the annotation ProtocolName and Version cannot be
      // obtained.
      //
      // Hence we simply use the protocol class used to create the proxy.
      // For PB this may limit the use of mixins on client side.
      builder.setDeclaringClassProtocolName(protocolName);
      builder.setClientProtocolVersion(clientProtocolVersion);
      return builder.build();
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     * 
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are 
     * set as cause in ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are
     * set as cause in ServiceException</li>
     * </ol>
     * 
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
    @Override
    public @Tainted Object invoke(ProtobufRpcEngine.@Tainted Invoker this, @Tainted Object proxy, @Tainted Method method, @Tainted Object @Tainted [] args)
        throws ServiceException {
      @Tainted
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }
      
      if (args.length != 2) { // RpcController + Message
        throw new @Tainted ServiceException("Too many parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + args.length);
      }
      if (args[1] == null) {
        throw new @Tainted ServiceException("null param while calling Method: ["
            + method.getName() + "]");
      }

      @Tainted
      RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
      
      if (LOG.isTraceEnabled()) {
        LOG.trace(Thread.currentThread().getId() + ": Call -> " +
            remoteId + ": " + method.getName() +
            " {" + TextFormat.shortDebugString((@Tainted Message) args[1]) + "}");
      }


      @Tainted
      Message theRequest = (@Tainted Message) args[1];
      final @Tainted RpcResponseWrapper val;
      try {
        val = (@Tainted RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            new @Tainted RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId);

      } catch (@Tainted Throwable e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Exception <- " +
              remoteId + ": " + method.getName() +
                " {" + e + "}");
        }

        throw new @Tainted ServiceException(e);
      }

      if (LOG.isDebugEnabled()) {
        @Tainted
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
      }
      
      @Tainted
      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (@Tainted Exception e) {
        throw new @Tainted ServiceException(e);
      }
      @Tainted
      Message returnMessage;
      try {
        returnMessage = prototype.newBuilderForType()
            .mergeFrom(val.theResponseRead).build();

        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Response <- " +
              remoteId + ": " + method.getName() +
                " {" + TextFormat.shortDebugString(returnMessage) + "}");
        }

      } catch (@Tainted Throwable e) {
        throw new @Tainted ServiceException(e);
      }
      return returnMessage;
    }

    @Override
    public void close(ProtobufRpcEngine.@Tainted Invoker this) throws IOException {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    private @Tainted Message getReturnProtoType(ProtobufRpcEngine.@Tainted Invoker this, @Tainted Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }
      
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> returnType = method.getReturnType();
      @Tainted
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      @Tainted
      Message prototype = (@Tainted Message) newInstMethod.invoke(null, (@Tainted Object[]) null);
      returnTypes.put(method.getName(), prototype);
      return prototype;
    }

    @Override //RpcInvocationHandler
    public @Tainted ConnectionId getConnectionId(ProtobufRpcEngine.@Tainted Invoker this) {
      return remoteId;
    }
  }

  interface RpcWrapper extends @Tainted Writable {
    @Tainted
    int getLength(ProtobufRpcEngine.@Tainted RpcWrapper this);
  }
  /**
   * Wrapper for Protocol Buffer Requests
   * 
   * Note while this wrapper is writable, the request on the wire is in
   * Protobuf. Several methods on {@link org.apache.hadoop.ipc.Server and RPC} 
   * use type Writable as a wrapper to work across multiple RpcEngine kinds.
   */
  private static abstract class RpcMessageWithHeader<@Tainted T extends @Tainted GeneratedMessage>
    implements @Tainted RpcWrapper {
    @Tainted
    T requestHeader;
    @Tainted
    Message theRequest; // for clientSide, the request is here
    @Tainted
    byte @Tainted [] theRequestRead; // for server side, the request is here

    public @Tainted RpcMessageWithHeader() {
    }

    public @Tainted RpcMessageWithHeader(@Tainted T requestHeader, @Tainted Message theRequest) {
      this.requestHeader = requestHeader;
      this.theRequest = theRequest;
    }

    @Override
    public void write(ProtobufRpcEngine.@Tainted RpcMessageWithHeader<T> this, @Tainted DataOutput out) throws IOException {
      @Tainted
      OutputStream os = DataOutputOutputStream.constructOutputStream(out);
      
      ((@Tainted Message)requestHeader).writeDelimitedTo(os);
      theRequest.writeDelimitedTo(os);
    }

    @Override
    public void readFields(ProtobufRpcEngine.@Tainted RpcMessageWithHeader<T> this, @Tainted DataInput in) throws IOException {
      requestHeader = parseHeaderFrom(readVarintBytes(in));
      theRequestRead = readMessageRequest(in);
    }

    abstract @Tainted T parseHeaderFrom(ProtobufRpcEngine.@Tainted RpcMessageWithHeader<T> this, @Tainted byte @Tainted [] bytes) throws IOException;

    @Tainted
    byte @Tainted [] readMessageRequest(ProtobufRpcEngine.@Tainted RpcMessageWithHeader<T> this, @Tainted DataInput in) throws IOException {
      return readVarintBytes(in);
    }

    private static @Tainted byte @Tainted [] readVarintBytes(@Tainted DataInput in) throws IOException {
      final @Tainted int length = ProtoUtil.readRawVarint32(in);
      final @Tainted byte @Tainted [] bytes = new @Tainted byte @Tainted [length];
      in.readFully(bytes);
      return bytes;
    }

    public @Tainted T getMessageHeader(ProtobufRpcEngine.@Tainted RpcMessageWithHeader<T> this) {
      return requestHeader;
    }

    public @Tainted byte @Tainted [] getMessageBytes(ProtobufRpcEngine.@Tainted RpcMessageWithHeader<T> this) {
      return theRequestRead;
    }
    
    @Override
    public @Tainted int getLength(ProtobufRpcEngine.@Tainted RpcMessageWithHeader<T> this) {
      @Tainted
      int headerLen = requestHeader.getSerializedSize();
      @Tainted
      int reqLen;
      if (theRequest != null) {
        reqLen = theRequest.getSerializedSize();
      } else if (theRequestRead != null ) {
        reqLen = theRequestRead.length;
      } else {
        throw new @Tainted IllegalArgumentException(
            "getLength on uninitialized RpcWrapper");      
      }
      return CodedOutputStream.computeRawVarint32Size(headerLen) +  headerLen
          + CodedOutputStream.computeRawVarint32Size(reqLen) + reqLen;
    }
  }
  
  private static class RpcRequestWrapper
  extends @Tainted RpcMessageWithHeader<@Tainted RequestHeaderProto> {
    @SuppressWarnings("unused")
    public @Tainted RpcRequestWrapper() {}
    
    public @Tainted RpcRequestWrapper(
        @Tainted
        RequestHeaderProto requestHeader, @Tainted Message theRequest) {
      super(requestHeader, theRequest);
    }
    
    @Override
    @Tainted
    RequestHeaderProto parseHeaderFrom(ProtobufRpcEngine.@Tainted RpcRequestWrapper this, @Tainted byte @Tainted [] bytes) throws IOException {
      return RequestHeaderProto.parseFrom(bytes);
    }
    
    @Override
    public @Tainted String toString(ProtobufRpcEngine.@Tainted RpcRequestWrapper this) {
      return requestHeader.getDeclaringClassProtocolName() + "." +
          requestHeader.getMethodName();
    }
  }

  @InterfaceAudience.LimitedPrivate({"RPC"})
  public static class RpcRequestMessageWrapper
  extends @Tainted RpcMessageWithHeader<@Tainted RpcRequestHeaderProto> {
    public @Tainted RpcRequestMessageWrapper() {}
    
    public @Tainted RpcRequestMessageWrapper(
        @Tainted
        RpcRequestHeaderProto requestHeader, @Tainted Message theRequest) {
      super(requestHeader, theRequest);
    }
    
    @Override
    @Tainted
    RpcRequestHeaderProto parseHeaderFrom(ProtobufRpcEngine.@Tainted RpcRequestMessageWrapper this, @Tainted byte @Tainted [] bytes) throws IOException {
      return RpcRequestHeaderProto.parseFrom(bytes);
    }
  }

  @InterfaceAudience.LimitedPrivate({"RPC"})
  public static class RpcResponseMessageWrapper
  extends @Tainted RpcMessageWithHeader<@Tainted RpcResponseHeaderProto> {
    public @Tainted RpcResponseMessageWrapper() {}
    
    public @Tainted RpcResponseMessageWrapper(
        @Tainted
        RpcResponseHeaderProto responseHeader, @Tainted Message theRequest) {
      super(responseHeader, theRequest);
    }
    
    @Override
    @Tainted
    byte @Tainted [] readMessageRequest(ProtobufRpcEngine.@Tainted RpcResponseMessageWrapper this, @Tainted DataInput in) throws IOException {
      // error message contain no message body
      switch (requestHeader.getStatus()) {
        case ERROR:
        case FATAL:
          return null;
        default:
          return super.readMessageRequest(in);
      }
    }
    
    @Override
    @Tainted
    RpcResponseHeaderProto parseHeaderFrom(ProtobufRpcEngine.@Tainted RpcResponseMessageWrapper this, @Tainted byte @Tainted [] bytes) throws IOException {
      return RpcResponseHeaderProto.parseFrom(bytes);
    }
  }

  /**
   *  Wrapper for Protocol Buffer Responses
   * 
   * Note while this wrapper is writable, the request on the wire is in
   * Protobuf. Several methods on {@link org.apache.hadoop.ipc.Server and RPC} 
   * use type Writable as a wrapper to work across multiple RpcEngine kinds.
   */
  @InterfaceAudience.LimitedPrivate({"RPC"}) // temporarily exposed 
  public static class RpcResponseWrapper implements @Tainted RpcWrapper {
    @Tainted
    Message theResponse; // for senderSide, the response is here
    @Tainted
    byte @Tainted [] theResponseRead; // for receiver side, the response is here

    public @Tainted RpcResponseWrapper() {
    }

    public @Tainted RpcResponseWrapper(@Tainted Message message) {
      this.theResponse = message;
    }

    @Override
    public void write(ProtobufRpcEngine.@Tainted RpcResponseWrapper this, @Tainted DataOutput out) throws IOException {
      @Tainted
      OutputStream os = DataOutputOutputStream.constructOutputStream(out);
      theResponse.writeDelimitedTo(os);   
    }

    @Override
    public void readFields(ProtobufRpcEngine.@Tainted RpcResponseWrapper this, @Tainted DataInput in) throws IOException {
      @Tainted
      int length = ProtoUtil.readRawVarint32(in);
      theResponseRead = new @Tainted byte @Tainted [length];
      in.readFully(theResponseRead);
    }
    
    @Override
    public @Tainted int getLength(ProtobufRpcEngine.@Tainted RpcResponseWrapper this) {
      @Tainted
      int resLen;
      if (theResponse != null) {
        resLen = theResponse.getSerializedSize();
      } else if (theResponseRead != null ) {
        resLen = theResponseRead.length;
      } else {
        throw new @Tainted IllegalArgumentException(
            "getLength on uninitialized RpcWrapper");      
      }
      return CodedOutputStream.computeRawVarint32Size(resLen) + resLen;
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static @Tainted Client getClient(@Tainted Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        RpcResponseWrapper.class);
  }
  
 

  @Override
  public RPC.@Tainted Server getServer(@Tainted ProtobufRpcEngine this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted Object protocolImpl,
      @Tainted
      String bindAddress, @Tainted int port, @Tainted int numHandlers, @Tainted int numReaders,
      @Tainted
      int queueSizePerHandler, @Tainted boolean verbose, @Tainted Configuration conf,
      @Tainted
      SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager,
      @Tainted
      String portRangeConfig)
      throws IOException {
    return new @Tainted Server(protocol, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig);
  }
  
  public static class Server extends RPC.@Tainted Server {
    /**
     * Construct an RPC server.
     * 
     * @param protocolClass the class of protocol
     * @param protocolImpl the protocolImpl whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param portRangeConfig A config parameter that can be used to restrict
     * the range of ports used when port is 0 (an ephemeral port)
     */
    public @Tainted Server(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass, @Tainted Object protocolImpl,
        @Tainted
        Configuration conf, @Tainted String bindAddress, @Tainted int port, @Tainted int numHandlers,
        @Tainted
        int numReaders, @Tainted int queueSizePerHandler, @Tainted boolean verbose,
        @Tainted
        SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager, 
        @Tainted
        String portRangeConfig)
        throws IOException {
      super(bindAddress, port, null, numHandlers,
          numReaders, queueSizePerHandler, conf, classNameBase(protocolImpl
              .getClass().getName()), secretManager, portRangeConfig);
      this.verbose = verbose;  
      registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocolClass,
          protocolImpl);
    }
    
    /**
     * Protobuf invoker for {@link RpcInvoker}
     */
    static class ProtoBufRpcInvoker implements @Tainted RpcInvoker {
      private static @Tainted ProtoClassProtoImpl getProtocolImpl(RPC.@Tainted Server server,
          @Tainted
          String protoName, @Tainted long clientVersion) throws RpcServerException {
        @Tainted
        ProtoNameVer pv = new @Tainted ProtoNameVer(protoName, clientVersion);
        @Tainted
        ProtoClassProtoImpl impl = 
            server.getProtocolImplMap(RPC.RpcKind.RPC_PROTOCOL_BUFFER).get(pv);
        if (impl == null) { // no match for Protocol AND Version
          @Tainted
          VerProtocolImpl highest = 
              server.getHighestSupportedProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, 
                  protoName);
          if (highest == null) {
            throw new @Tainted RpcNoSuchProtocolException(
                "Unknown protocol: " + protoName);
          }
          // protocol supported but not the version that client wants
          throw new RPC.@Tainted VersionMismatch(protoName, clientVersion,
              highest.version);
        }
        return impl;
      }

      @Override 
      /**
       * This is a server side method, which is invoked over RPC. On success
       * the return response has protobuf response payload. On failure, the
       * exception name and the stack trace are return in the resposne.
       * See {@link HadoopRpcResponseProto}
       * 
       * In this method there three types of exceptions possible and they are
       * returned in response as follows.
       * <ol>
       * <li> Exceptions encountered in this method that are returned 
       * as {@link RpcServerException} </li>
       * <li> Exceptions thrown by the service is wrapped in ServiceException. 
       * In that this method returns in response the exception thrown by the 
       * service.</li>
       * <li> Other exceptions thrown by the service. They are returned as
       * it is.</li>
       * </ol>
       */
      public @Tainted Writable call(ProtobufRpcEngine.Server.@Tainted ProtoBufRpcInvoker this, RPC.@Tainted Server server, @Tainted String connectionProtocolName,
          @Tainted
          Writable writableRequest, @Tainted long receiveTime) throws Exception {
        @Tainted
        RpcRequestWrapper request = (@Tainted RpcRequestWrapper) writableRequest;
        @Tainted
        RequestHeaderProto rpcRequest = request.requestHeader;
        @Tainted
        String methodName = rpcRequest.getMethodName();
        
        
        /** 
         * RPCs for a particular interface (ie protocol) are done using a
         * IPC connection that is setup using rpcProxy.
         * The rpcProxy's has a declared protocol name that is 
         * sent form client to server at connection time. 
         * 
         * Each Rpc call also sends a protocol name 
         * (called declaringClassprotocolName). This name is usually the same
         * as the connection protocol name except in some cases. 
         * For example metaProtocols such ProtocolInfoProto which get info
         * about the protocol reuse the connection but need to indicate that
         * the actual protocol is different (i.e. the protocol is
         * ProtocolInfoProto) since they reuse the connection; in this case
         * the declaringClassProtocolName field is set to the ProtocolInfoProto.
         */

        @Tainted
        String declaringClassProtoName = 
            rpcRequest.getDeclaringClassProtocolName();
        @Tainted
        long clientVersion = rpcRequest.getClientProtocolVersion();
        if (server.verbose)
          LOG.info("Call: connectionProtocolName=" + connectionProtocolName + 
              ", method=" + methodName);
        
        @Tainted
        ProtoClassProtoImpl protocolImpl = getProtocolImpl(server, 
                              declaringClassProtoName, clientVersion);
        @Tainted
        BlockingService service = (@Tainted BlockingService) protocolImpl.protocolImpl;
        @Tainted
        MethodDescriptor methodDescriptor = service.getDescriptorForType()
            .findMethodByName(methodName);
        if (methodDescriptor == null) {
          @Tainted
          String msg = "Unknown method " + methodName + " called on " 
                                + connectionProtocolName + " protocol.";
          LOG.warn(msg);
          throw new @Tainted RpcNoSuchMethodException(msg);
        }
        @Tainted
        Message prototype = service.getRequestPrototype(methodDescriptor);
        @Tainted
        Message param = prototype.newBuilderForType()
            .mergeFrom(request.theRequestRead).build();
        
        @Tainted
        Message result;
        try {
          @Tainted
          long startTime = Time.now();
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          result = service.callBlockingMethod(methodDescriptor, null, param);
          @Tainted
          int processingTime = (@Tainted int) (Time.now() - startTime);
          @Tainted
          int qTime = (@Tainted int) (startTime - receiveTime);
          if (LOG.isDebugEnabled()) {
            LOG.info("Served: " + methodName + " queueTime= " + qTime +
                      " procesingTime= " + processingTime);
          }
          server.rpcMetrics.addRpcQueueTime(qTime);
          server.rpcMetrics.addRpcProcessingTime(processingTime);
          server.rpcDetailedMetrics.addProcessingTime(methodName,
              processingTime);
        } catch (@Tainted ServiceException e) {
          throw (@Tainted Exception) e.getCause();
        } catch (@Tainted Exception e) {
          throw e;
        }
        return new @Tainted RpcResponseWrapper(result);
      }
    }
  }
}
