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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.io.*;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolInfoService;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import com.google.protobuf.BlockingService;

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
@InterfaceAudience.LimitedPrivate(value = { "Common", "HDFS", "MapReduce", "Yarn" })
@InterfaceStability.Evolving
public class RPC {
  final static @Tainted int RPC_SERVICE_CLASS_DEFAULT = 0;
  public enum RpcKind {

@Tainted  RPC_BUILTIN ((@Tainted short) 1),         // Used for built in calls by tests

@Tainted  RPC_WRITABLE ((@Tainted short) 2),        // Use WritableRpcEngine

@Tainted  RPC_PROTOCOL_BUFFER ((@Tainted short) 3); // Use ProtobufRpcEngine
    final static @Tainted short MAX_INDEX = RPC_PROTOCOL_BUFFER.value; // used for array size
    public final @Tainted short value; //TODO make it private

    @Tainted
    RpcKind(@Tainted short val) {
      this.value = val;
    } 
  }
  
  interface RpcInvoker {   
    /**
     * Process a client call on the server side
     * @param server the server within whose context this rpc call is made
     * @param protocol - the protocol name (the class of the client proxy
     *      used to make calls to the rpc server.
     * @param rpcRequest  - deserialized
     * @param receiveTime time at which the call received (for metrics)
     * @return the call's return
     * @throws IOException
     **/
    public @Tainted Writable call(RPC.@Tainted RpcInvoker this, @Tainted Server server, @Tainted String protocol,
        @Tainted
        Writable rpcRequest, @Tainted long receiveTime) throws Exception ;
  }
  
  static final @Tainted Log LOG = LogFactory.getLog(RPC.class);
  
  /**
   * Get all superInterfaces that extend VersionedProtocol
   * @param childInterfaces
   * @return the super interfaces that extend VersionedProtocol
   */
  static @Tainted Class<@Tainted ?> @Tainted [] getSuperInterfaces(Class<?>[] childInterfaces) {
    @Tainted
    List<@Tainted Class<@Tainted ?>> allInterfaces = new @Tainted ArrayList<@Tainted Class<@Tainted ?>>();

    for (@Tainted Class<@Tainted ?> childInterface : childInterfaces) {
      if (VersionedProtocol.class.isAssignableFrom(childInterface)) {
          allInterfaces.add(childInterface);
          allInterfaces.addAll(
              Arrays.asList(
                  getSuperInterfaces(childInterface.getInterfaces())));
      } else {
        LOG.warn("Interface " + childInterface +
              " ignored because it does not extend VersionedProtocol");
      }
    }
    return allInterfaces.toArray(new @Tainted Class @Tainted [allInterfaces.size()]);
  }
  
  /**
   * Get all interfaces that the given protocol implements or extends
   * which are assignable from VersionedProtocol.
   */
  static @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] getProtocolInterfaces(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol) {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] interfaces  = protocol.getInterfaces();
    return getSuperInterfaces(interfaces);
  }
  
  /**
   * Get the protocol name.
   *  If the protocol class has a ProtocolAnnotation, then get the protocol
   *  name from the annotation; otherwise the class name is the protocol name.
   */
  static public @Tainted String getProtocolName(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol) {
    if (protocol == null) {
      return null;
    }
    @Tainted
    ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
    return  (anno == null) ? protocol.getName() : anno.protocolName();
  }
  
  /**
   * Get the protocol version from protocol class.
   * If the protocol class has a ProtocolAnnotation, then get the protocol
   * name from the annotation; otherwise the class name is the protocol name.
   */
  static public @Tainted long getProtocolVersion(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol) {
    if (protocol == null) {
      throw new @Tainted IllegalArgumentException("Null protocol");
    }
    @Tainted
    long version;
    @Tainted
    ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
    if (anno != null) {
      version = anno.protocolVersion();
      if (version != -1)
        return version;
    }
    try {
      @Tainted
      Field versionField = protocol.getField("versionID");
      versionField.setAccessible(true);
      return versionField.getLong(protocol);
    } catch (@Tainted NoSuchFieldException ex) {
      throw new @Tainted RuntimeException(ex);
    } catch (@Tainted IllegalAccessException ex) {
      throw new @Tainted RuntimeException(ex);
    }
  }

  private @Tainted RPC() {}                                  // no public ctor

  // cache of RpcEngines by protocol
  private static final @Tainted Map<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted RpcEngine> PROTOCOL_ENGINES
    = new @Tainted HashMap<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted RpcEngine>();

  private static final @Tainted String ENGINE_PROP = "rpc.engine";

  /**
   * Set a protocol to use a non-default RpcEngine.
   * @param conf configuration to use
   * @param protocol the protocol interface
   * @param engine the RpcEngine impl
   */
  public static void setProtocolEngine(@Tainted Configuration conf,
                                @Tainted
                                Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> engine) {
    conf.setClass(ENGINE_PROP+"."+protocol.getName(), engine, RpcEngine.class);
  }

  // return the RpcEngine configured to handle a protocol
  static synchronized @Tainted RpcEngine getProtocolEngine(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol,
      @Tainted
      Configuration conf) {
    @Tainted
    RpcEngine engine = PROTOCOL_ENGINES.get(protocol);
    if (engine == null) {
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> impl = conf.getClass(ENGINE_PROP+"."+protocol.getName(),
                                    WritableRpcEngine.class);
      engine = (@Tainted RpcEngine)ReflectionUtils.newInstance(impl, conf);
      PROTOCOL_ENGINES.put(protocol, engine);
    }
    return engine;
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  public static class VersionMismatch extends @Tainted RpcServerException {
    private static final @Tainted long serialVersionUID = 0;

    private @Tainted String interfaceName;
    private @Tainted long clientVersion;
    private @Tainted long serverVersion;
    
    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public @Tainted VersionMismatch(@Tainted String interfaceName, @Tainted long clientVersion,
                           @Tainted
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }
    
    /**
     * Get the interface name
     * @return the java class name 
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public @Tainted String getInterfaceName(RPC.@Tainted VersionMismatch this) {
      return interfaceName;
    }
    
    /**
     * Get the client's preferred version
     */
    public @Tainted long getClientVersion(RPC.@Tainted VersionMismatch this) {
      return clientVersion;
    }
    
    /**
     * Get the server's agreed to version.
     */
    public @Tainted long getServerVersion(RPC.@Tainted VersionMismatch this) {
      return serverVersion;
    }
    /**
     * get the rpc status corresponding to this exception
     */
    public @Tainted RpcStatusProto getRpcStatusProto(RPC.@Tainted VersionMismatch this) {
      return RpcStatusProto.ERROR;
    }

    /**
     * get the detailed rpc status corresponding to this exception
     */
    public @Tainted RpcErrorCodeProto getRpcErrorCodeProto(RPC.@Tainted VersionMismatch this) {
      return RpcErrorCodeProto.ERROR_RPC_VERSION_MISMATCH;
    }
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T waitForProxy(
      @Tainted
      Class<@Tainted T> protocol,
      @Tainted
      long clientVersion,
      @Tainted
      InetSocketAddress addr,
      @Tainted
      Configuration conf
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> waitForProtocolProxy(@Tainted Class<@Tainted T> protocol,
                             @Tainted
                             long clientVersion,
                             @Tainted
                             InetSocketAddress addr,
                             @Tainted
                             Configuration conf) throws IOException {
    return waitForProtocolProxy(
        protocol, clientVersion, addr, conf, Long.MAX_VALUE);
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T waitForProxy(@Tainted Class<@Tainted T> protocol, @Tainted long clientVersion,
                             @Tainted
                             InetSocketAddress addr, @Tainted Configuration conf,
                             @Tainted
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, connTimeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> waitForProtocolProxy(@Tainted Class<@Tainted T> protocol,
                             @Tainted
                             long clientVersion,
                             @Tainted
                             InetSocketAddress addr, @Tainted Configuration conf,
                             @Tainted
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr, conf, 0, null, connTimeout);
  }
  
  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T waitForProxy(@Tainted Class<@Tainted T> protocol,
                             @Tainted
                             long clientVersion,
                             @Tainted
                             InetSocketAddress addr, @Tainted Configuration conf,
                             @Tainted
                             int rpcTimeout,
                             @Tainted
                             long timeout) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, rpcTimeout, null, timeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> waitForProtocolProxy(@Tainted Class<@Tainted T> protocol,
                               @Tainted
                               long clientVersion,
                               @Tainted
                               InetSocketAddress addr, @Tainted Configuration conf,
                               @Tainted
                               int rpcTimeout,
                               @Tainted
                               RetryPolicy connectionRetryPolicy,
                               @Tainted
                               long timeout) throws IOException { 
    @Tainted
    long startTime = Time.now();
    @Tainted
    IOException ioe;
    while (true) {
      try {
        return getProtocolProxy(protocol, clientVersion, addr, 
            UserGroupInformation.getCurrentUser(), conf, NetUtils
            .getDefaultSocketFactory(conf), rpcTimeout, connectionRetryPolicy);
      } catch(@Tainted ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
      } catch(@Tainted SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      } catch(@Tainted NoRouteToHostException nrthe) { // perhaps a VIP is failing over
        LOG.info("No route to host for server: " + addr);
        ioe = nrthe;
      }
      // check if timed out
      if (Time.now()-timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (@Tainted InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T getProxy(@Tainted Class<@Tainted T> protocol,
                                @Tainted
                                long clientVersion,
                                @Tainted
                                InetSocketAddress addr, @Tainted Configuration conf,
                                @Tainted
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> getProtocolProxy(@Tainted Class<@Tainted T> protocol,
                                @Tainted
                                long clientVersion,
                                @Tainted
                                InetSocketAddress addr, @Tainted Configuration conf,
                                @Tainted
                                SocketFactory factory) throws IOException {
    @Tainted
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return getProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T getProxy(@Tainted Class<@Tainted T> protocol,
                                @Tainted
                                long clientVersion,
                                @Tainted
                                InetSocketAddress addr,
                                @Tainted
                                UserGroupInformation ticket,
                                @Tainted
                                Configuration conf,
                                @Tainted
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, ticket, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param ticket user group information
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> getProtocolProxy(@Tainted Class<@Tainted T> protocol,
                                @Tainted
                                long clientVersion,
                                @Tainted
                                InetSocketAddress addr,
                                @Tainted
                                UserGroupInformation ticket,
                                @Tainted
                                Configuration conf,
                                @Tainted
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, ticket, conf, factory, 0, null);
  }
  
  /**
   * Construct a client-side proxy that implements the named protocol,
   * talking to a server at the named address.
   * @param <T>
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T getProxy(@Tainted Class<@Tainted T> protocol,
                                @Tainted
                                long clientVersion,
                                @Tainted
                                InetSocketAddress addr,
                                @Tainted
                                UserGroupInformation ticket,
                                @Tainted
                                Configuration conf,
                                @Tainted
                                SocketFactory factory,
                                @Tainted
                                int rpcTimeout) throws IOException {
    return getProtocolProxy(protocol, clientVersion, addr, ticket,
             conf, factory, rpcTimeout, null).getProxy();
  }
  
  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
   public static <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> getProtocolProxy(@Tainted Class<@Tainted T> protocol,
                                @Tainted
                                long clientVersion,
                                @Tainted
                                InetSocketAddress addr,
                                @Tainted
                                UserGroupInformation ticket,
                                @Tainted
                                Configuration conf,
                                @Tainted
                                SocketFactory factory,
                                @Tainted
                                int rpcTimeout,
                                @Tainted
                                RetryPolicy connectionRetryPolicy) throws IOException {    
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    return getProtocolEngine(protocol,conf).getProxy(protocol, clientVersion,
        addr, ticket, conf, factory, rpcTimeout, connectionRetryPolicy);
  }

   /**
    * Construct a client-side proxy object with the default SocketFactory
    * @param <T>
    * 
    * @param protocol
    * @param clientVersion
    * @param addr
    * @param conf
    * @return a proxy instance
    * @throws IOException
    */
   public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T getProxy(@Tainted Class<@Tainted T> protocol,
                                 @Tainted
                                 long clientVersion,
                                 @Tainted
                                 InetSocketAddress addr, @Tainted Configuration conf)
     throws IOException {

     return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
   }
  
  /**
   * Returns the server address for a given proxy.
   */
  public static @Tainted InetSocketAddress getServerAddress(@Tainted Object proxy) {
    return getConnectionIdForProxy(proxy).getAddress();
  }

  /**
   * Return the connection ID of the given object. If the provided object is in
   * fact a protocol translator, we'll get the connection ID of the underlying
   * proxy object.
   * 
   * @param proxy the proxy object to get the connection ID of.
   * @return the connection ID for the provided proxy object.
   */
  public static @Tainted ConnectionId getConnectionIdForProxy(@Tainted Object proxy) {
    if (proxy instanceof @Tainted ProtocolTranslator) {
      proxy = ((@Tainted ProtocolTranslator)proxy).getUnderlyingProxyObject();
    }
    @Tainted
    RpcInvocationHandler inv = (@Tainted RpcInvocationHandler) Proxy
        .getInvocationHandler(proxy);
    return inv.getConnectionId();
  }
   
  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a protocol proxy
   * @throws IOException
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted ProtocolProxy<@Tainted T> getProtocolProxy(@Tainted Class<@Tainted T> protocol,
                                @Tainted
                                long clientVersion,
                                @Tainted
                                InetSocketAddress addr, @Tainted Configuration conf)
    throws IOException {

    return getProtocolProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

  /**
   * Stop the proxy. Proxy must either implement {@link Closeable} or must have
   * associated {@link RpcInvocationHandler}.
   * 
   * @param proxy
   *          the RPC proxy object to be stopped
   * @throws HadoopIllegalArgumentException
   *           if the proxy does not implement {@link Closeable} interface or
   *           does not have closeable {@link InvocationHandler}
   */
  public static void stopProxy(@Tainted Object proxy) {
    if (proxy == null) {
      throw new @Tainted HadoopIllegalArgumentException(
          "Cannot close proxy since it is null");
    }
    try {
      if (proxy instanceof @Tainted Closeable) {
        ((@Tainted Closeable) proxy).close();
        return;
      } else {
        @Tainted
        InvocationHandler handler = Proxy.getInvocationHandler(proxy);
        if (handler instanceof @Tainted Closeable) {
          ((@Tainted Closeable) handler).close();
          return;
        }
      }
    } catch (@Tainted IOException e) {
      LOG.error("Closing proxy or invocation handler caused exception", e);
    } catch (@Tainted IllegalArgumentException e) {
      LOG.error("RPC.stopProxy called on non proxy.", e);
    }
    
    // If you see this error on a mock object in a unit test you're
    // developing, make sure to use MockitoUtil.mockProtocol() to
    // create your mock.
    throw new @Tainted HadoopIllegalArgumentException(
        "Cannot close proxy - is not Closeable or "
            + "does not provide closeable invocation handler "
            + proxy.getClass());
  }

  /**
   * Class to construct instances of RPC server with specific options.
   */
  public static class Builder {
    private @Tainted Class<@Tainted ?> protocol = null;
    private @Tainted Object instance = null;
    private @Tainted String bindAddress = "0.0.0.0";
    private @Tainted int port = 0;
    private @Tainted int numHandlers = 1;
    private @Tainted int numReaders = -1;
    private @Tainted int queueSizePerHandler = -1;
    private @Tainted boolean verbose = false;
    private final @Tainted Configuration conf;    
    private @Tainted SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager = null;
    private @Tainted String portRangeConfig = null;
    
    public @Tainted Builder(@Tainted Configuration conf) {
      this.conf = conf;
    }

    /** Mandatory field */
    public @Tainted Builder setProtocol(RPC.@Tainted Builder this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol) {
      this.protocol = protocol;
      return this;
    }
    
    /** Mandatory field */
    public @Tainted Builder setInstance(RPC.@Tainted Builder this, @Tainted Object instance) {
      this.instance = instance;
      return this;
    }
    
    /** Default: 0.0.0.0 */
    public @Tainted Builder setBindAddress(RPC.@Tainted Builder this, @Tainted String bindAddress) {
      this.bindAddress = bindAddress;
      return this;
    }
    
    /** Default: 0 */
    public @Tainted Builder setPort(RPC.@Tainted Builder this, @Tainted int port) {
      this.port = port;
      return this;
    }
    
    /** Default: 1 */
    public @Tainted Builder setNumHandlers(RPC.@Tainted Builder this, @Tainted int numHandlers) {
      this.numHandlers = numHandlers;
      return this;
    }
    
    /** Default: -1 */
    public @Tainted Builder setnumReaders(RPC.@Tainted Builder this, @Tainted int numReaders) {
      this.numReaders = numReaders;
      return this;
    }
    
    /** Default: -1 */
    public @Tainted Builder setQueueSizePerHandler(RPC.@Tainted Builder this, @Tainted int queueSizePerHandler) {
      this.queueSizePerHandler = queueSizePerHandler;
      return this;
    }
    
    /** Default: false */
    public @Tainted Builder setVerbose(RPC.@Tainted Builder this, @Tainted boolean verbose) {
      this.verbose = verbose;
      return this;
    }
    
    /** Default: null */
    public @Tainted Builder setSecretManager(
        RPC.@Tainted Builder this, @Tainted
        SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager) {
      this.secretManager = secretManager;
      return this;
    }
    
    /** Default: null */
    public @Tainted Builder setPortRangeConfig(RPC.@Tainted Builder this, @Tainted String portRangeConfig) {
      this.portRangeConfig = portRangeConfig;
      return this;
    }
    
    /**
     * Build the RPC Server. 
     * @throws IOException on error
     * @throws HadoopIllegalArgumentException when mandatory fields are not set
     */
    public @Tainted Server build(RPC.@Tainted Builder this) throws IOException, HadoopIllegalArgumentException {
      if (this.conf == null) {
        throw new @Tainted HadoopIllegalArgumentException("conf is not set");
      }
      if (this.protocol == null) {
        throw new @Tainted HadoopIllegalArgumentException("protocol is not set");
      }
      if (this.instance == null) {
        throw new @Tainted HadoopIllegalArgumentException("instance is not set");
      }
      
      return getProtocolEngine(this.protocol, this.conf).getServer(
          this.protocol, this.instance, this.bindAddress, this.port,
          this.numHandlers, this.numReaders, this.queueSizePerHandler,
          this.verbose, this.conf, this.secretManager, this.portRangeConfig);
    }
  }
  
  /** An RPC Server. */
  public abstract static class Server extends org.apache.hadoop.ipc.Server {
   @Tainted
   boolean verbose;
   static @Tainted String classNameBase(@Tainted String className) {
      @Tainted
      String @Tainted [] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }
   
   /**
    * Store a map of protocol and version to its implementation
    */
   /**
    *  The key in Map
    */
   static class ProtoNameVer {
     final @Tainted String protocol;
     final @Tainted long   version;
     @Tainted
     ProtoNameVer(@Tainted String protocol, @Tainted long ver) {
       this.protocol = protocol;
       this.version = ver;
     }
     @Override
     public @Tainted boolean equals(RPC.Server.@Tainted ProtoNameVer this, @Tainted Object o) {
       if (o == null) 
         return false;
       if (this == o) 
         return true;
       if (! (o instanceof @Tainted ProtoNameVer))
         return false;
       @Tainted
       ProtoNameVer pv = (@Tainted ProtoNameVer) o;
       return ((pv.protocol.equals(this.protocol)) && 
           (pv.version == this.version));     
     }
     @Override
     public @Tainted int hashCode(RPC.Server.@Tainted ProtoNameVer this) {
       return protocol.hashCode() * 37 + (@Tainted int) version;    
     }
   }
   
   /**
    * The value in map
    */
   static class ProtoClassProtoImpl {
     final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass;
     final @Tainted Object protocolImpl; 
     @Tainted
     ProtoClassProtoImpl(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass, @Tainted Object protocolImpl) {
       this.protocolClass = protocolClass;
       this.protocolImpl = protocolImpl;
     }
   }

   @Tainted
   ArrayList<@Tainted Map<@Tainted ProtoNameVer, @Tainted ProtoClassProtoImpl>> protocolImplMapArray = 
       new @Tainted ArrayList<@Tainted Map<@Tainted ProtoNameVer, @Tainted ProtoClassProtoImpl>>(RpcKind.MAX_INDEX);
   
   @Tainted
   Map<@Tainted ProtoNameVer, @Tainted ProtoClassProtoImpl> getProtocolImplMap(RPC.@Tainted Server this, RPC.@Tainted RpcKind rpcKind) {
     if (protocolImplMapArray.size() == 0) {// initialize for all rpc kinds
       for (@Tainted int i=0; i <= RpcKind.MAX_INDEX; ++i) {
         protocolImplMapArray.add(
             new @Tainted HashMap<@Tainted ProtoNameVer, @Tainted ProtoClassProtoImpl>(10));
       }
     }
     return protocolImplMapArray.get(rpcKind.ordinal());   
   }
   
   // Register  protocol and its impl for rpc calls
   void registerProtocolAndImpl(RPC.@Tainted Server this, @Tainted RpcKind rpcKind, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass, 
       @Tainted
       Object protocolImpl) {
     @Tainted
     String protocolName = RPC.getProtocolName(protocolClass);
     @Tainted
     long version;
     

     try {
       version = RPC.getProtocolVersion(protocolClass);
     } catch (@Tainted Exception ex) {
       LOG.warn("Protocol "  + protocolClass + 
            " NOT registered as cannot get protocol version ");
       return;
     }


     getProtocolImplMap(rpcKind).put(new @Tainted ProtoNameVer(protocolName, version),
         new @Tainted ProtoClassProtoImpl(protocolClass, protocolImpl)); 
     LOG.debug("RpcKind = " + rpcKind + " Protocol Name = " + protocolName +  " version=" + version +
         " ProtocolImpl=" + protocolImpl.getClass().getName() + 
         " protocolClass=" + protocolClass.getName());
   }
   
   static class VerProtocolImpl {
     final @Tainted long version;
     final @Tainted ProtoClassProtoImpl protocolTarget;
     @Tainted
     VerProtocolImpl(@Tainted long ver, @Tainted ProtoClassProtoImpl protocolTarget) {
       this.version = ver;
       this.protocolTarget = protocolTarget;
     }
   }
   
   @Tainted
   VerProtocolImpl @Tainted [] getSupportedProtocolVersions(RPC.@Tainted Server this, RPC.@Tainted RpcKind rpcKind,
       @Tainted
       String protocolName) {
     @Tainted
     VerProtocolImpl @Tainted [] resultk = 
         new  @Tainted VerProtocolImpl @Tainted [getProtocolImplMap(rpcKind).size()];
     @Tainted
     int i = 0;
     for (Map.@Tainted Entry<@Tainted ProtoNameVer, @Tainted ProtoClassProtoImpl> pv :
                                       getProtocolImplMap(rpcKind).entrySet()) {
       if (pv.getKey().protocol.equals(protocolName)) {
         resultk[i++] = 
             new @Tainted VerProtocolImpl(pv.getKey().version, pv.getValue());
       }
     }
     if (i == 0) {
       return null;
     }
     @Tainted
     VerProtocolImpl @Tainted [] result = new @Tainted VerProtocolImpl @Tainted [i];
     System.arraycopy(resultk, 0, result, 0, i);
     return result;
   }
   
   @Tainted
   VerProtocolImpl getHighestSupportedProtocol(RPC.@Tainted Server this, @Tainted RpcKind rpcKind, 
       @Tainted
       String protocolName) {    
     @Tainted
     Long highestVersion = 0L;
     @Tainted
     ProtoClassProtoImpl highest = null;
     if (LOG.isDebugEnabled()) {
       LOG.debug("Size of protoMap for " + rpcKind + " ="
           + getProtocolImplMap(rpcKind).size());
     }
     for (Map.@Tainted Entry<@Tainted ProtoNameVer, @Tainted ProtoClassProtoImpl> pv : 
           getProtocolImplMap(rpcKind).entrySet()) {
       if (pv.getKey().protocol.equals(protocolName)) {
         if ((highest == null) || (pv.getKey().version > highestVersion)) {
           highest = pv.getValue();
           highestVersion = pv.getKey().version;
         } 
       }
     }
     if (highest == null) {
       return null;
     }
     return new @Tainted VerProtocolImpl(highestVersion,  highest);   
   }
  
    protected @Tainted Server(@Tainted String bindAddress, @Tainted int port, 
                     @Tainted
                     Class<@Tainted ? extends @Tainted Writable> paramClass, @Tainted int handlerCount,
                     @Tainted
                     int numReaders, @Tainted int queueSizePerHandler,
                     @Tainted
                     Configuration conf, @Tainted String serverName, 
                     @Tainted
                     SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager,
                     @Tainted
                     String portRangeConfig) throws IOException {
      super(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler,
            conf, serverName, secretManager, portRangeConfig);
      initProtocolMetaInfo(conf);
    }
    
    private void initProtocolMetaInfo(RPC.@Tainted Server this, @Tainted Configuration conf) {
      RPC.setProtocolEngine(conf, ProtocolMetaInfoPB.class,
          ProtobufRpcEngine.class);
      @Tainted
      ProtocolMetaInfoServerSideTranslatorPB xlator = 
          new @Tainted ProtocolMetaInfoServerSideTranslatorPB(this);
      @Tainted
      BlockingService protocolInfoBlockingService = ProtocolInfoService
          .newReflectiveBlockingService(xlator);
      addProtocol(RpcKind.RPC_PROTOCOL_BUFFER, ProtocolMetaInfoPB.class,
          protocolInfoBlockingService);
    }
    
    /**
     * Add a protocol to the existing server.
     * @param protocolClass - the protocol class
     * @param protocolImpl - the impl of the protocol that will be called
     * @return the server (for convenience)
     */
    public @Tainted Server addProtocol(RPC.@Tainted Server this, @Tainted RpcKind rpcKind, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocolClass,
        @Tainted
        Object protocolImpl) {
      registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
      return this;
    }
    
    @Override
    public @Tainted Writable call(RPC.@Tainted Server this, RPC.@Tainted RpcKind rpcKind, @Tainted String protocol,
        @Tainted
        Writable rpcRequest, @Tainted long receiveTime) throws Exception {
      return getRpcInvoker(rpcKind).call(this, protocol, rpcRequest,
          receiveTime);
    }
  }
}
