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
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import java.net.InetSocketAddress;
import java.io.*;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;

/** An RpcEngine implementation for Writable data. */
@InterfaceStability.Evolving
public class WritableRpcEngine implements @Tainted RpcEngine {
  private static final @Tainted Log LOG = LogFactory.getLog(RPC.class);
  
  //writableRpcVersion should be updated if there is a change
  //in format of the rpc messages.
  
  // 2L - added declared class to Invocation
  public static final @Tainted long writableRpcVersion = 2L;
  
  /**
   * Whether or not this class has been initialized.
   */
  private static @Tainted boolean isInitialized = false;
  
  static { 
    ensureInitialized();
  }
  
  /**
   * Initialize this class if it isn't already.
   */
  public static synchronized void ensureInitialized() {
    if (!isInitialized) {
      initialize();
    }
  }
  
  /**
   * Register the rpcRequest deserializer for WritableRpcEngine
   */
  private static synchronized void initialize() {
    org.apache.hadoop.ipc.Server.registerProtocolEngine(RPC.RpcKind.RPC_WRITABLE,
        Invocation.class, new Server.@Tainted WritableRpcInvoker());
    isInitialized = true;
  }

  
  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements @Tainted Writable, @Tainted Configurable {
    private @Tainted String methodName;
    private @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] parameterClasses;
    private @Tainted Object @Tainted [] parameters;
    private @Tainted Configuration conf;
    private @Tainted long clientVersion;
    private @Tainted int clientMethodsHash;
    private @Tainted String declaringClassProtocolName;
    
    //This could be different from static writableRpcVersion when received
    //at server, if client is using a different version.
    private @Tainted long rpcVersion;

    @SuppressWarnings("unused") // called when deserializing an invocation
    public @Tainted Invocation() {}

    public @Tainted Invocation(@Tainted Method method, @Tainted Object @Tainted [] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
      rpcVersion = writableRpcVersion;
      if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
        //VersionedProtocol is exempted from version check.
        clientVersion = 0;
        clientMethodsHash = 0;
      } else {
        this.clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
        this.clientMethodsHash = ProtocolSignature.getFingerprint(method
            .getDeclaringClass().getMethods());
      }
      this.declaringClassProtocolName = 
          RPC.getProtocolName(method.getDeclaringClass());
    }

    /** The name of the method invoked. */
    public @Tainted String getMethodName(WritableRpcEngine.@Tainted Invocation this) { return methodName; }

    /** The parameter classes. */
    public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] getParameterClasses(WritableRpcEngine.@Tainted Invocation this) { return parameterClasses; }

    /** The parameter instances. */
    public @Tainted Object @Tainted [] getParameters(WritableRpcEngine.@Tainted Invocation this) { return parameters; }
    
    private @Tainted long getProtocolVersion(WritableRpcEngine.@Tainted Invocation this) {
      return clientVersion;
    }

    @SuppressWarnings("unused")
    private @Tainted int getClientMethodsHash(WritableRpcEngine.@Tainted Invocation this) {
      return clientMethodsHash;
    }
    
    /**
     * Returns the rpc version used by the client.
     * @return rpcVersion
     */
    public @Tainted long getRpcVersion(WritableRpcEngine.@Tainted Invocation this) {
      return rpcVersion;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void readFields(WritableRpcEngine.@Tainted Invocation this, @Tainted DataInput in) throws IOException {
      rpcVersion = in.readLong();
      declaringClassProtocolName = UTF8.readString(in);
      methodName = UTF8.readString(in);
      clientVersion = in.readLong();
      clientMethodsHash = in.readInt();
      parameters = new @Tainted Object @Tainted [in.readInt()];
      parameterClasses = new @Tainted Class @Tainted [parameters.length];
      @Tainted
      ObjectWritable objectWritable = new @Tainted ObjectWritable();
      for (@Tainted int i = 0; i < parameters.length; i++) {
        parameters[i] = 
            ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void write(WritableRpcEngine.@Tainted Invocation this, @Tainted DataOutput out) throws IOException {
      out.writeLong(rpcVersion);
      UTF8.writeString(out, declaringClassProtocolName);
      UTF8.writeString(out, methodName);
      out.writeLong(clientVersion);
      out.writeInt(clientMethodsHash);
      out.writeInt(parameterClasses.length);
      for (@Tainted int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf, true);
      }
    }

    @Override
    public @Tainted String toString(WritableRpcEngine.@Tainted Invocation this) {
      @Tainted
      StringBuilder buffer = new @Tainted StringBuilder();
      buffer.append(methodName);
      buffer.append("(");
      for (@Tainted int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      buffer.append(", rpc version="+rpcVersion);
      buffer.append(", client version="+clientVersion);
      buffer.append(", methodsFingerPrint="+clientMethodsHash);
      return buffer.toString();
    }

    @Override
    public void setConf(WritableRpcEngine.@Tainted Invocation this, @Tainted Configuration conf) {
      this.conf = conf;
    }

    @Override
    public @Tainted Configuration getConf(WritableRpcEngine.@Tainted Invocation this) {
      return this.conf;
    }

  }

  private static @Tainted ClientCache CLIENTS=new @Tainted ClientCache();
  
  private static class Invoker implements @Tainted RpcInvocationHandler {
    private Client.@Tainted ConnectionId remoteId;
    private @Tainted Client client;
    private @Tainted boolean isClosed = false;

    public @Tainted Invoker(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol,
                   @Tainted
                   InetSocketAddress address, @Tainted UserGroupInformation ticket,
                   @Tainted
                   Configuration conf, @Tainted SocketFactory factory,
                   @Tainted
                   int rpcTimeout) throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol,
          ticket, rpcTimeout, conf);
      this.client = CLIENTS.getClient(conf, factory);
    }

    @Override
    public @Tainted Object invoke(WritableRpcEngine.@Tainted Invoker this, @Tainted Object proxy, @Tainted Method method, @Tainted Object @Tainted [] args)
      throws Throwable {
      @Tainted
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }

      @Tainted
      ObjectWritable value = (@Tainted ObjectWritable)
        client.call(RPC.RpcKind.RPC_WRITABLE, new @Tainted Invocation(method, args), remoteId);
      if (LOG.isDebugEnabled()) {
        @Tainted
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }
    
    /* close the IPC client that's responsible for this invoker's RPCs */ 
    @Override
    synchronized public void close(WritableRpcEngine.@Tainted Invoker this) {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    @Override
    public @Tainted ConnectionId getConnectionId(WritableRpcEngine.@Tainted Invoker this) {
      return remoteId;
    }
  }
  
  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static @Tainted Client getClient(@Tainted Configuration conf) {
    return CLIENTS.getClient(conf);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  @Override
  @SuppressWarnings("unchecked")
  public <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> getProxy(@Tainted WritableRpcEngine this, @Tainted Class<@Tainted T> protocol, @Tainted long clientVersion,
                         @Tainted
                         InetSocketAddress addr, @Tainted UserGroupInformation ticket,
                         @Tainted
                         Configuration conf, @Tainted SocketFactory factory,
                         @Tainted
                         int rpcTimeout, @Tainted RetryPolicy connectionRetryPolicy)
    throws IOException {    

    if (connectionRetryPolicy != null) {
      throw new @Tainted UnsupportedOperationException(
          "Not supported: connectionRetryPolicy=" + connectionRetryPolicy);
    }

    @Tainted
    T proxy = (@Tainted T) Proxy.newProxyInstance(protocol.getClassLoader(),
        new @Tainted Class @Tainted [] { protocol }, new @Tainted Invoker(protocol, addr, ticket, conf,
            factory, rpcTimeout));
    return new @Tainted ProtocolProxy<@Tainted T>(protocol, proxy, true);
  }
  
  /* Construct a server for a protocol implementation instance listening on a
   * port and address. */
  @Override
  public RPC.@Tainted Server getServer(@Tainted WritableRpcEngine this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass,
                      @Tainted
                      Object protocolImpl, @Tainted String bindAddress, @Tainted int port,
                      @Tainted
                      int numHandlers, @Tainted int numReaders, @Tainted int queueSizePerHandler,
                      @Tainted
                      boolean verbose, @Tainted Configuration conf,
                      @Tainted
                      SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager,
                      @Tainted
                      String portRangeConfig) 
    throws IOException {
    return new @Tainted Server(protocolClass, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig);
  }


  /** An RPC Server. */
  public static class Server extends RPC.@Tainted Server {
    /** 
     * Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * 
     * @deprecated Use #Server(Class, Object, Configuration, String, int)    
     */
    @Deprecated
    public @Tainted Server(@Tainted Object instance, @Tainted Configuration conf, @Tainted String bindAddress,
        @Tainted
        int port) throws IOException {
      this(null, instance, conf,  bindAddress, port);
    }
    
    
    /** Construct an RPC server.
     * @param protocolClass class
     * @param protocolImpl the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     */
    public @Tainted Server(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass, @Tainted Object protocolImpl, 
        @Tainted
        Configuration conf, @Tainted String bindAddress, @Tainted int port) 
      throws IOException {
      this(protocolClass, protocolImpl, conf,  bindAddress, port, 1, -1, -1,
          false, null, null);
    }
    
    /** 
     * Construct an RPC server.
     * @param protocolImpl the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * 
     * @deprecated use Server#Server(Class, Object, 
     *      Configuration, String, int, int, int, int, boolean, SecretManager)
     */
    @Deprecated
    public @Tainted Server(@Tainted Object protocolImpl, @Tainted Configuration conf, @Tainted String bindAddress,
        @Tainted
        int port, @Tainted int numHandlers, @Tainted int numReaders, @Tainted int queueSizePerHandler,
        @Tainted
        boolean verbose, @Tainted SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager) 
            throws IOException {
       this(null, protocolImpl,  conf,  bindAddress,   port,
                   numHandlers,  numReaders,  queueSizePerHandler,  verbose, 
                   secretManager, null);
   
    }
    
    /** 
     * Construct an RPC server.
     * @param protocolClass - the protocol being registered
     *     can be null for compatibility with old usage (see below for details)
     * @param protocolImpl the protocol impl that will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public @Tainted Server(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass, @Tainted Object protocolImpl,
        @Tainted
        Configuration conf, @Tainted String bindAddress,  @Tainted int port,
        @Tainted
        int numHandlers, @Tainted int numReaders, @Tainted int queueSizePerHandler, 
        @Tainted
        boolean verbose, @Tainted SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager,
        @Tainted
        String portRangeConfig) 
        throws IOException {
      super(bindAddress, port, null, numHandlers, numReaders,
          queueSizePerHandler, conf,
          classNameBase(protocolImpl.getClass().getName()), secretManager,
          portRangeConfig);

      this.verbose = verbose;
      
      
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] protocols;
      if (protocolClass == null) { // derive protocol from impl
        /*
         * In order to remain compatible with the old usage where a single
         * target protocolImpl is suppled for all protocol interfaces, and
         * the protocolImpl is derived from the protocolClass(es) 
         * we register all interfaces extended by the protocolImpl
         */
        protocols = RPC.getProtocolInterfaces(protocolImpl.getClass());

      } else {
        if (!protocolClass.isAssignableFrom(protocolImpl.getClass())) {
          throw new @Tainted IOException("protocolClass "+ protocolClass +
              " is not implemented by protocolImpl which is of class " +
              protocolImpl.getClass());
        }
        // register protocol class and its super interfaces
        registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, protocolClass, protocolImpl);
        protocols = RPC.getProtocolInterfaces(protocolClass);
      }
      for (@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> p : protocols) {
        if (!p.equals(VersionedProtocol.class)) {
          registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, p, protocolImpl);
        }
      }

    }

    private static void log(@Tainted String value) {
      if (value!= null && value.length() > 55)
        value = value.substring(0, 55)+"...";
      LOG.info(value);
    }
    
    static class WritableRpcInvoker implements @Tainted RpcInvoker {

     @Override
      public @Tainted Writable call(WritableRpcEngine.Server.@Tainted WritableRpcInvoker this, org.apache.hadoop.ipc.RPC.@Tainted Server server,
          @Tainted
          String protocolName, @Tainted Writable rpcRequest, @Tainted long receivedTime)
          throws IOException, RPC.VersionMismatch {

        @Tainted
        Invocation call = (@Tainted Invocation)rpcRequest;
        if (server.verbose) log("Call: " + call);

        // Verify writable rpc version
        if (call.getRpcVersion() != writableRpcVersion) {
          // Client is using a different version of WritableRpc
          throw new @Tainted RpcServerException(
              "WritableRpc version mismatch, client side version="
                  + call.getRpcVersion() + ", server side version="
                  + writableRpcVersion);
        }

        @Tainted
        long clientVersion = call.getProtocolVersion();
        final @Tainted String protoName;
        @Tainted
        ProtoClassProtoImpl protocolImpl;
        if (call.declaringClassProtocolName.equals(VersionedProtocol.class.getName())) {
          // VersionProtocol methods are often used by client to figure out
          // which version of protocol to use.
          //
          // Versioned protocol methods should go the protocolName protocol
          // rather than the declaring class of the method since the
          // the declaring class is VersionedProtocol which is not 
          // registered directly.
          // Send the call to the highest  protocol version
          @Tainted
          VerProtocolImpl highest = server.getHighestSupportedProtocol(
              RPC.RpcKind.RPC_WRITABLE, protocolName);
          if (highest == null) {
            throw new @Tainted RpcServerException("Unknown protocol: " + protocolName);
          }
          protocolImpl = highest.protocolTarget;
        } else {
          protoName = call.declaringClassProtocolName;

          // Find the right impl for the protocol based on client version.
          @Tainted
          ProtoNameVer pv = 
              new @Tainted ProtoNameVer(call.declaringClassProtocolName, clientVersion);
          protocolImpl = 
              server.getProtocolImplMap(RPC.RpcKind.RPC_WRITABLE).get(pv);
          if (protocolImpl == null) { // no match for Protocol AND Version
             @Tainted
             VerProtocolImpl highest = 
                 server.getHighestSupportedProtocol(RPC.RpcKind.RPC_WRITABLE, 
                     protoName);
            if (highest == null) {
              throw new @Tainted RpcServerException("Unknown protocol: " + protoName);
            } else { // protocol supported but not the version that client wants
              throw new RPC.@Tainted VersionMismatch(protoName, clientVersion,
                highest.version);
            }
          }
        }
          

          // Invoke the protocol method
       try {
          @Tainted
          long startTime = Time.now();
          @Tainted
          Method method = 
              protocolImpl.protocolClass.getMethod(call.getMethodName(),
              call.getParameterClasses());
          method.setAccessible(true);
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          @Tainted
          Object value = 
              method.invoke(protocolImpl.protocolImpl, call.getParameters());
          @Tainted
          int processingTime = (@Tainted int) (Time.now() - startTime);
          @Tainted
          int qTime = (@Tainted int) (startTime-receivedTime);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Served: " + call.getMethodName() +
                      " queueTime= " + qTime +
                      " procesingTime= " + processingTime);
          }
          server.rpcMetrics.addRpcQueueTime(qTime);
          server.rpcMetrics.addRpcProcessingTime(processingTime);
          server.rpcDetailedMetrics.addProcessingTime(call.getMethodName(),
                                               processingTime);
          if (server.verbose) log("Return: "+value);

          return new @Tainted ObjectWritable(method.getReturnType(), value);

        } catch (@Tainted InvocationTargetException e) {
          @Tainted
          Throwable target = e.getTargetException();
          if (target instanceof @Tainted IOException) {
            throw (@Tainted IOException)target;
          } else {
            @Tainted
            IOException ioe = new @Tainted IOException(target.toString());
            ioe.setStackTrace(target.getStackTrace());
            throw ioe;
          }
        } catch (@Tainted Throwable e) {
          if (!(e instanceof @Tainted IOException)) {
            LOG.error("Unexpected throwable object ", e);
          }
          @Tainted
          IOException ioe = new @Tainted IOException(e.toString());
          ioe.setStackTrace(e.getStackTrace());
          throw ioe;
        }
      }
    }
  }

  @Override
  public @Tainted ProtocolProxy<@Tainted ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      @Tainted WritableRpcEngine this, @Tainted
      ConnectionId connId, @Tainted Configuration conf, @Tainted SocketFactory factory)
      throws IOException {
    throw new @Tainted UnsupportedOperationException("This proxy is not supported");
  }
}
