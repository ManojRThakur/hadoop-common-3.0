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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import static org.apache.hadoop.ipc.RpcConstants.*;

import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
@InterfaceAudience.LimitedPrivate(value = { "Common", "HDFS", "MapReduce", "Yarn" })
@InterfaceStability.Evolving
public abstract class Server {
  private final @Tainted boolean authorize;
  private @Tainted List<@Tainted AuthMethod> enabledAuthMethods;
  private @Tainted RpcSaslProto negotiateResponse;
  private @Tainted ExceptionsHandler exceptionsHandler = new @Tainted ExceptionsHandler();
  
  public void addTerseExceptions(@Tainted Server this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted ... exceptionClass) {
    exceptionsHandler.addTerseExceptions(exceptionClass);
  }

  /**
   * ExceptionsHandler manages Exception groups for special handling
   * e.g., terse exception group for concise logging messages
   */
  static class ExceptionsHandler {
    private volatile @Tainted Set<@Tainted String> terseExceptions = new @Tainted HashSet<@Tainted String>();

    /**
     * Add exception class so server won't log its stack trace.
     * Modifying the terseException through this method is thread safe.
     *
     * @param exceptionClass exception classes 
     */
    void addTerseExceptions(Server.@Tainted ExceptionsHandler this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted ... exceptionClass) {

      // Make a copy of terseException for performing modification
      final @Tainted HashSet<@Tainted String> newSet = new @Tainted HashSet<@Tainted String>(terseExceptions);

      // Add all class names into the HashSet
      for (@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> name : exceptionClass) {
        newSet.add(name.toString());
      }
      // Replace terseException set
      terseExceptions = Collections.unmodifiableSet(newSet);
    }

    @Tainted
    boolean isTerse(Server.@Tainted ExceptionsHandler this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> t) {
      return terseExceptions.contains(t.toString());
    }
  }

  
  /**
   * If the user accidentally sends an HTTP GET to an IPC port, we detect this
   * and send back a nicer response.
   */
  private static final @Tainted ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap(
      "GET ".getBytes());
  
  /**
   * An HTTP response to send back if we detect an HTTP request to our IPC
   * port.
   */
  static final @Tainted String RECEIVED_HTTP_REQ_RESPONSE =
    "HTTP/1.1 404 Not Found\r\n" +
    "Content-type: text/plain\r\n\r\n" +
    "It looks like you are making an HTTP request to a Hadoop IPC port. " +
    "This is not the correct port for the web interface on this daemon.\r\n";

  /**
   * Initial and max size of response buffer
   */
  static @Tainted int INITIAL_RESP_BUF_SIZE = 10240;
  
  static class RpcKindMapValue {
    final @Tainted Class<@Tainted ? extends @Tainted Writable> rpcRequestWrapperClass;
    final @Tainted RpcInvoker rpcInvoker;
    @Tainted
    RpcKindMapValue (@Tainted Class<@Tainted ? extends @Tainted Writable> rpcRequestWrapperClass,
          @Tainted
          RpcInvoker rpcInvoker) {
      this.rpcInvoker = rpcInvoker;
      this.rpcRequestWrapperClass = rpcRequestWrapperClass;
    }   
  }
  static @Tainted Map<RPC.@Tainted RpcKind, @Tainted RpcKindMapValue> rpcKindMap = new
      @Tainted HashMap<RPC.@Tainted RpcKind, @Tainted RpcKindMapValue>(4);
  
  

  /**
   * Register a RPC kind and the class to deserialize the rpc request.
   * 
   * Called by static initializers of rpcKind Engines
   * @param rpcKind
   * @param rpcRequestWrapperClass - this class is used to deserialze the
   *  the rpc request.
   *  @param rpcInvoker - use to process the calls on SS.
   */
  
  public static void registerProtocolEngine(RPC.@Tainted RpcKind rpcKind, 
          @Tainted
          Class<@Tainted ? extends @Tainted Writable> rpcRequestWrapperClass,
          @Tainted
          RpcInvoker rpcInvoker) {
    @Tainted
    RpcKindMapValue  old = 
        rpcKindMap.put(rpcKind, new @Tainted RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker));
    if (old != null) {
      rpcKindMap.put(rpcKind, old);
      throw new @Tainted IllegalArgumentException("ReRegistration of rpcKind: " +
          rpcKind);      
    }
    LOG.debug("rpcKind=" + rpcKind + 
        ", rpcRequestWrapperClass=" + rpcRequestWrapperClass + 
        ", rpcInvoker=" + rpcInvoker);
  }
  
  public @Tainted Class<@Tainted ? extends @Tainted Writable> getRpcRequestWrapper(
      @Tainted Server this, @Tainted
      RpcKindProto rpcKind) {
    if (rpcRequestClass != null)
       return rpcRequestClass;
    @Tainted
    RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convert(rpcKind));
    return (val == null) ? null : val.rpcRequestWrapperClass; 
  }
  
  public static @Tainted RpcInvoker  getRpcInvoker(RPC.@Tainted RpcKind rpcKind) {
    @Tainted
    RpcKindMapValue val = rpcKindMap.get(rpcKind);
    return (val == null) ? null : val.rpcInvoker; 
  }
  

  public static final @Tainted Log LOG = LogFactory.getLog(Server.class);
  public static final @Tainted Log AUDITLOG = 
    LogFactory.getLog("SecurityLogger."+Server.class.getName());
  private static final @Tainted String AUTH_FAILED_FOR = "Auth failed for ";
  private static final @Tainted String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  
  private static final @Tainted ThreadLocal<@Tainted Server> SERVER = new @Tainted ThreadLocal<@Tainted Server>();

  private static final @Tainted Map<@Tainted String, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object>> PROTOCOL_CACHE = 
    new @Tainted ConcurrentHashMap<@Tainted String, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>();
  
  static @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getProtocolClass(@Tainted String protocolName, @Tainted Configuration conf) 
  throws ClassNotFoundException {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }
  
  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static @Tainted Server get() {
    return SERVER.get();
  }
 
  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final @Tainted ThreadLocal<@Tainted Call> CurCall = new @Tainted ThreadLocal<@Tainted Call>();
  
  /** Get the current call */
  @VisibleForTesting
  public static @Tainted ThreadLocal<@Tainted Call> getCurCall() {
    return CurCall;
  }
  
  /**
   * Returns the currently active RPC call's sequential ID number.  A negative
   * call ID indicates an invalid value, such as if there is no currently active
   * RPC call.
   * 
   * @return int sequential ID number of currently active RPC call
   */
  public static @Tainted int getCallId() {
    @Tainted
    Call call = CurCall.get();
    return call != null ? call.callId : RpcConstants.INVALID_CALL_ID;
  }
  
  /**
   * @return The current active RPC call's retry count. -1 indicates the retry
   *         cache is not supported in the client side.
   */
  public static @Tainted int getCallRetryCount() {
    @Tainted
    Call call = CurCall.get();
    return call != null ? call.retryCount : RpcConstants.INVALID_RETRY_COUNT;
  }

  /** Returns the remote side ip address when invoked inside an RPC 
   *  Returns null incase of an error.
   */
  public static @Tainted InetAddress getRemoteIp() {
    @Tainted
    Call call = CurCall.get();
    return (call != null && call.connection != null) ? call.connection
        .getHostInetAddress() : null;
  }
  
  /**
   * Returns the clientId from the current RPC request
   */
  public static @Tainted byte @Tainted [] getClientId() {
    @Tainted
    Call call = CurCall.get();
    return call != null ? call.clientId : RpcConstants.DUMMY_CLIENT_ID;
  }
  
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static @Tainted String getRemoteAddress() {
    @Tainted
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  /** Returns the RPC remote user when invoked inside an RPC.  Note this
   *  may be different than the current user if called within another doAs
   *  @return connection's UGI or null if not an RPC
   */
  public static @Tainted UserGroupInformation getRemoteUser() {
    @Tainted
    Call call = CurCall.get();
    return (call != null && call.connection != null) ? call.connection.user
        : null;
  }
 
  /** Return true if the invocation was through an RPC.
   */
  public static @Tainted boolean isRpcInvocation() {
    return CurCall.get() != null;
  }

  private @Tainted String bindAddress; 
  private @Tainted int port;                               // port we listen on
  private @Tainted int handlerCount;                       // number of handler threads
  private @Tainted int readThreads;                        // number of read threads
  private @Tainted Class<@Tainted ? extends @Tainted Writable> rpcRequestClass;   // class used for deserializing the rpc request
  private @Tainted int maxIdleTime;                        // the maximum idle time after 
                                                  // which a client may be disconnected
  private @Tainted int thresholdIdleConnections;           // the number of idle connections
                                                  // after which we will start
                                                  // cleaning up idle 
                                                  // connections
  @Tainted
  int maxConnectionsToNuke;                       // the max number of 
                                                  // connections to nuke
                                                  //during a cleanup
  
  protected @Tainted RpcMetrics rpcMetrics;
  protected @Tainted RpcDetailedMetrics rpcDetailedMetrics;
  
  private @Tainted Configuration conf;
  private @Tainted String portRangeConfig = null;
  private @Tainted SecretManager<@Tainted TokenIdentifier> secretManager;
  private @Tainted ServiceAuthorizationManager serviceAuthorizationManager = new @Tainted ServiceAuthorizationManager();

  private @Tainted int maxQueueSize;
  private final @Tainted int maxRespSize;
  private @Tainted int socketSendBufferSize;
  private final @Tainted int maxDataLength;
  private final @Tainted boolean tcpNoDelay; // if T then disable Nagle's Algorithm

  volatile private @Tainted boolean running = true;         // true while server runs
  private @Tainted BlockingQueue<@Tainted Call> callQueue; // queued calls

  private @Tainted List<@Tainted Connection> connectionList = 
    Collections.synchronizedList(new @Tainted LinkedList<@Tainted Connection>());
  //maintain a list
  //of client connections
  private @Tainted Listener listener = null;
  private @Tainted Responder responder = null;
  private @Tainted int numConnections = 0;
  private @Tainted Handler @Tainted [] handlers = null;

  /**
   * A convenience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(@Tainted ServerSocket socket, @Tainted InetSocketAddress address, 
                          @Tainted
                          int backlog) throws IOException {
    bind(socket, address, backlog, null, null);
  }

  public static void bind(@Tainted ServerSocket socket, @Tainted InetSocketAddress address, 
      @Tainted
      int backlog, @Tainted Configuration conf, @Tainted String rangeConf) throws IOException {
    try {
      @Tainted
      IntegerRanges range = null;
      if (rangeConf != null) {
        range = conf.getRange(rangeConf, "");
      }
      if (range == null || range.isEmpty() || (address.getPort() != 0)) {
        socket.bind(address, backlog);
      } else {
        for (@Tainted Integer port : range) {
          if (socket.isBound()) break;
          try {
            @Tainted
            InetSocketAddress temp = new @Tainted InetSocketAddress(address.getAddress(),
                port);
            socket.bind(temp, backlog);
          } catch(@Tainted BindException e) {
            //Ignored
          }
        }
        if (!socket.isBound()) {
          throw new @Tainted BindException("Could not find a free port in "+range);
        }
      }
    } catch (@Tainted SocketException e) {
      throw NetUtils.wrapException(null,
          0,
          address.getHostName(),
          address.getPort(), e);
    }
  }
  
  /**
   * Returns a handle to the rpcMetrics (required in tests)
   * @return rpc metrics
   */
  @VisibleForTesting
  public @Tainted RpcMetrics getRpcMetrics(@Tainted Server this) {
    return rpcMetrics;
  }

  @VisibleForTesting
  public @Tainted RpcDetailedMetrics getRpcDetailedMetrics(@Tainted Server this) {
    return rpcDetailedMetrics;
  }
  
  @VisibleForTesting
  @Tainted
  Iterable<@Tainted ? extends @Tainted Thread> getHandlers(@Tainted Server this) {
    return Arrays.asList(handlers);
  }

  @VisibleForTesting
  @Tainted
  List<@Tainted Connection> getConnections(@Tainted Server this) {
    return connectionList;
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server.
   */
  public void refreshServiceAcl(@Tainted Server this, @Tainted Configuration conf, @Tainted PolicyProvider provider) {
    serviceAuthorizationManager.refresh(conf, provider);
  }

  /**
   * Returns a handle to the serviceAuthorizationManager (required in tests)
   * @return instance of ServiceAuthorizationManager for this server
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public @Tainted ServiceAuthorizationManager getServiceAuthorizationManager(@Tainted Server this) {
    return serviceAuthorizationManager;
  }

  /** A call queued for handling. */
  public static class Call {
    private final @Tainted int callId;             // the client's call id
    private final @Tainted int retryCount;        // the retry count of the call
    private final @Tainted Writable rpcRequest;    // Serialized Rpc request from client
    private final @Tainted Connection connection;  // connection to client
    private @Tainted long timestamp;               // time received when response is null
                                          // time served when response is not null
    private @Tainted ByteBuffer rpcResponse;       // the response for this call
    private final RPC.@Tainted RpcKind rpcKind;
    private final @Tainted byte @Tainted [] clientId;

    public @Tainted Call(@Tainted int id, @Tainted int retryCount, @Tainted Writable param, 
        @Tainted
        Connection connection) {
      this(id, retryCount, param, connection, RPC.RpcKind.RPC_BUILTIN,
          RpcConstants.DUMMY_CLIENT_ID);
    }

    public @Tainted Call(@Tainted int id, @Tainted int retryCount, @Tainted Writable param, @Tainted Connection connection,
        RPC.@Tainted RpcKind kind, @Tainted byte @Tainted [] clientId) {
      this.callId = id;
      this.retryCount = retryCount;
      this.rpcRequest = param;
      this.connection = connection;
      this.timestamp = Time.now();
      this.rpcResponse = null;
      this.rpcKind = kind;
      this.clientId = clientId;
    }
    
    @Override
    public @Tainted String toString(Server.@Tainted Call this) {
      return rpcRequest + " from " + connection + " Call#" + callId + " Retry#"
          + retryCount;
    }

    public void setResponse(Server.@Tainted Call this, @Tainted ByteBuffer response) {
      this.rpcResponse = response;
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends @Tainted Thread {
    
    private @Tainted ServerSocketChannel acceptChannel = null; //the accept channel
    private @Tainted Selector selector = null; //the selector that we use for the server
    private @Tainted Reader @Tainted [] readers = null;
    private @Tainted int currentReader = 0;
    private @Tainted InetSocketAddress address; //the address we bind at
    private @Tainted Random rand = new @Tainted Random();
    private @Tainted long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private @Tainted long cleanupInterval = 10000; //the minimum interval between 
                                          //two cleanup runs
    private @Tainted int backlogLength = conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
    
    public @Tainted Listener() throws IOException {
      address = new @Tainted InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength, conf, portRangeConfig);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();
      readers = new @Tainted Reader @Tainted [readThreads];
      for (@Tainted int i = 0; i < readThreads; i++) {
        @Tainted
        Reader reader = new @Tainted Reader(
            "Socket Reader #" + (i + 1) + " for port " + port);
        readers[i] = reader;
        reader.start();
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }
    
    private class Reader extends @Tainted Thread {
      private volatile @Tainted boolean adding = false;
      private final @Tainted Selector readSelector;

      @Tainted
      Reader(@Tainted String name) throws IOException {
        super(name);

        this.readSelector = Selector.open();
      }
      
      @Override
      public void run(@Tainted Server.Listener.Reader this) {
        LOG.info("Starting " + getName());
        try {
          doRunLoop();
        } finally {
          try {
            readSelector.close();
          } catch (@Tainted IOException ioe) {
            LOG.error("Error closing read selector in " + this.getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop(@Tainted Server.Listener.Reader this) {
        while (running) {
          @Tainted
          SelectionKey key = null;
          try {
            readSelector.select();
            while (adding) {
              this.wait(1000);
            }              

            @Tainted
            Iterator<@Tainted SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);
                }
              }
              key = null;
            }
          } catch (@Tainted InterruptedException e) {
            if (running) {                      // unexpected -- log it
              LOG.info(getName() + " unexpectedly interrupted", e);
            }
          } catch (@Tainted IOException ex) {
            LOG.error("Error in Reader", ex);
          }
        }
      }

      /**
       * This gets reader into the state that waits for the new channel
       * to be registered with readSelector. If it was waiting in select()
       * the thread will be woken up, otherwise whenever select() is called
       * it will return even if there is nothing to read and wait
       * in while(adding) for finishAdd call
       */
      public void startAdd(@Tainted Server.Listener.Reader this) {
        adding = true;
        readSelector.wakeup();
      }
      
      public synchronized @Tainted SelectionKey registerChannel(@Tainted Server.Listener.Reader this, @Tainted SocketChannel channel)
                                                          throws IOException {
          return channel.register(readSelector, SelectionKey.OP_READ);
      }

      public synchronized void finishAdd(@Tainted Server.Listener.Reader this) {
        adding = false;
        this.notify();        
      }

      void shutdown(@Tainted Server.Listener.Reader this) {
        assert !running;
        readSelector.wakeup();
        try {
          join();
        } catch (@Tainted InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all 
     * connections will be looked at for the cleanup.
     */
    private void cleanupConnections(@Tainted Server.Listener this, @Tainted boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        @Tainted
        long currentTime = Time.now();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        @Tainted
        int start = 0;
        @Tainted
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          @Tainted
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        @Tainted
        int i = start;
        @Tainted
        int numNuked = 0;
        while (i <= end) {
          @Tainted
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (@Tainted Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = Time.now();
      }
    }

    @Override
    public void run(@Tainted Server.Listener this) {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        @Tainted
        SelectionKey key = null;
        try {
          getSelector().select();
          @Tainted
          Iterator<@Tainted SelectionKey> iter = getSelector().selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (@Tainted IOException e) {
            }
            key = null;
          }
        } catch (@Tainted OutOfMemoryError e) {
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give 
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          cleanupConnections(true);
          try { Thread.sleep(60000); } catch (@Tainted Exception ie) {}
        } catch (@Tainted Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (@Tainted IOException e) { }

        selector= null;
        acceptChannel= null;
        
        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    private void closeCurrentConnection(@Tainted Server.Listener this, @Tainted SelectionKey key, @Tainted Throwable e) {
      if (key != null) {
        @Tainted
        Connection c = (@Tainted Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
          closeConnection(c);
          c = null;
        }
      }
    }

    @Tainted
    InetSocketAddress getAddress(@Tainted Server.Listener this) {
      return (@Tainted InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }
    
    void doAccept(@Tainted Server.Listener this, @Tainted SelectionKey key) throws IOException,  OutOfMemoryError {
      @Tainted
      Connection c = null;
      @Tainted
      ServerSocketChannel server = (@Tainted ServerSocketChannel) key.channel();
      @Tainted
      SocketChannel channel;
      while ((channel = server.accept()) != null) {

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        
        @Tainted
        Reader reader = getReader();
        try {
          reader.startAdd();
          @Tainted
          SelectionKey readKey = reader.registerChannel(channel);
          c = new @Tainted Connection(readKey, channel, Time.now());
          readKey.attach(c);
          synchronized (connectionList) {
            connectionList.add(numConnections, c);
            numConnections++;
          }
          if (LOG.isDebugEnabled())
            LOG.debug("Server connection from " + c.toString() +
                "; # active connections: " + numConnections +
                "; # queued calls: " + callQueue.size());          
        } finally {
          reader.finishAdd(); 
        }
      }
    }

    void doRead(@Tainted Server.Listener this, @Tainted SelectionKey key) throws InterruptedException {
      @Tainted
      int count = 0;
      @Tainted
      Connection c = (@Tainted Connection)key.attachment();
      if (c == null) {
        return;  
      }
      c.setLastContact(Time.now());
      
      try {
        count = c.readAndProcess();
      } catch (@Tainted InterruptedException ieo) {
        LOG.info(getName() + ": readAndProcess caught InterruptedException", ieo);
        throw ieo;
      } catch (@Tainted Exception e) {
        // a WrappedRpcServerException is an exception that has been sent
        // to the client, so the stacktrace is unnecessary; any other
        // exceptions are unexpected internal server errors and thus the
        // stacktrace should be logged
        LOG.info(getName() + ": readAndProcess from client " +
            c.getHostAddress() + " threw exception [" + e + "]",
            (e instanceof @Tainted WrappedRpcServerException) ? null : e);
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + ": disconnecting client " + 
                    c + ". Number of active connections: "+
                    numConnections);
        closeConnection(c);
        c = null;
      }
      else {
        c.setLastContact(Time.now());
      }
    }   

    synchronized void doStop(@Tainted Server.Listener this) {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (@Tainted IOException e) {
          LOG.info(getName() + ":Exception in closing listener socket. " + e);
        }
      }
      for (@Tainted Reader r : readers) {
        r.shutdown();
      }
    }
    
    synchronized @Tainted Selector getSelector(@Tainted Server.Listener this) { return selector; }
    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    @Tainted
    Reader getReader(@Tainted Server.Listener this) {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends @Tainted Thread {
    private final @Tainted Selector writeSelector;
    private @Tainted int pending;         // connections waiting to register
    
    final static @Tainted int PURGE_INTERVAL = 900000; // 15mins

    @Tainted
    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run(@Tainted Server.Responder this) {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      try {
        doRunLoop();
      } finally {
        LOG.info("Stopping " + this.getName());
        try {
          writeSelector.close();
        } catch (@Tainted IOException ioe) {
          LOG.error("Couldn't close write selector in " + this.getName(), ioe);
        }
      }
    }
    
    private void doRunLoop(@Tainted Server.Responder this) {
      @Tainted
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          @Tainted
          Iterator<@Tainted SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            @Tainted
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (@Tainted IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          @Tainted
          long now = Time.now();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          if(LOG.isDebugEnabled()) {
            LOG.debug("Checking for old call responses.");
          }
          @Tainted
          ArrayList<@Tainted Call> calls;
          
          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new @Tainted ArrayList<@Tainted Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              @Tainted
              SelectionKey key = iter.next();
              @Tainted
              Call call = (@Tainted Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) { 
                calls.add(call);
              }
            }
          }
          
          for(@Tainted Call call : calls) {
            doPurge(call, now);
          }
        } catch (@Tainted OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (@Tainted Exception ie) {}
        } catch (@Tainted Exception e) {
          LOG.warn("Exception in Responder", e);
        }
      }
    }

    private void doAsyncWrite(@Tainted Server.Responder this, @Tainted SelectionKey key) throws IOException {
      @Tainted
      Call call = (@Tainted Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new @Tainted IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (@Tainted CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue 
    // for a long time.
    //
    private void doPurge(@Tainted Server.Responder this, @Tainted Call call, @Tainted long now) {
      @Tainted
      LinkedList<@Tainted Call> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        @Tainted
        Iterator<@Tainted Call> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private @Tainted boolean processResponse(@Tainted Server.Responder this, @Tainted LinkedList<@Tainted Call> responseQueue,
                                    @Tainted
                                    boolean inHandler) throws IOException {
      @Tainted
      boolean error = true;
      @Tainted
      boolean done = false;       // there is more data for this channel.
      @Tainted
      int numElements = 0;
      @Tainted
      Call call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          @Tainted
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to " + call);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          @Tainted
          int numBytes = channelWrite(channel, call.rpcResponse);
          if (numBytes < 0) {
            return true;
          }
          if (!call.rpcResponse.hasRemaining()) {
            //Clear out the response buffer so it can be collected
            call.rpcResponse = null;
            call.connection.decRpcCount();
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to " + call
                  + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then 
            // insert in Selector queue. 
            //
            call.connection.responseQueue.addFirst(call);
            
            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = Time.now();
              
              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call 
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (@Tainted ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to " + call
                  + " Wrote partial " + numBytes + " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(@Tainted Server.Responder this, @Tainted Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending(@Tainted Server.Responder this) {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending(@Tainted Server.Responder this) { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending(@Tainted Server.Responder this) throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  @InterfaceAudience.Private
  public static enum AuthProtocol {

@Tainted  NONE(0),

@Tainted  SASL(-33);
    
    public final @Tainted int callId;
    @Tainted
    AuthProtocol(@Tainted int callId) {
      this.callId = callId;
    }
    
    static @Tainted AuthProtocol valueOf(@Tainted int callId) {
      for (@Tainted AuthProtocol authType : AuthProtocol.values()) {
        if (authType.callId == callId) {
          return authType;
        }
      }
      return null;
    }
  };
  
  /**
   * Wrapper for RPC IOExceptions to be returned to the client.  Used to
   * let exceptions bubble up to top of processOneRpc where the correct
   * callId can be associated with the response.  Also used to prevent
   * unnecessary stack trace logging if it's not an internal server error. 
   */
  @SuppressWarnings("serial")
  private static class WrappedRpcServerException extends @Tainted RpcServerException {
    private final @Tainted RpcErrorCodeProto errCode;
    public @Tainted WrappedRpcServerException(@Tainted RpcErrorCodeProto errCode, @Tainted IOException ioe) {
      super(ioe.toString(), ioe);
      this.errCode = errCode;
    }
    public @Tainted WrappedRpcServerException(@Tainted RpcErrorCodeProto errCode, @Tainted String message) {
      this(errCode, new @Tainted RpcServerException(message));
    }
    @Override
    public @Tainted RpcErrorCodeProto getRpcErrorCodeProto(Server.@Tainted WrappedRpcServerException this) {
      return errCode;
    }
    @Override
    public @Tainted String toString(Server.@Tainted WrappedRpcServerException this) {
      return getCause().toString();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    private @Tainted boolean connectionHeaderRead = false; // connection  header is read?
    private @Tainted boolean connectionContextRead = false; //if connection context that
                                            //follows connection header is read

    private @Tainted SocketChannel channel;
    private @Tainted ByteBuffer data;
    private @Tainted ByteBuffer dataLengthBuffer;
    private @Tainted LinkedList<@Tainted Call> responseQueue;
    private volatile @Tainted int rpcCount = 0; // number of outstanding rpcs
    private @Tainted long lastContact;
    private @Tainted int dataLength;
    private @Tainted Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private @Tainted String hostAddress;
    private @Tainted int remotePort;
    private @Tainted InetAddress addr;
    
    @Tainted
    IpcConnectionContextProto connectionContext;
    @Tainted
    String protocolName;
    @Tainted
    SaslServer saslServer;
    private @Tainted AuthMethod authMethod;
    private @Tainted AuthProtocol authProtocol;
    private @Tainted boolean saslContextEstablished;
    private @Tainted ByteBuffer connectionHeaderBuf = null;
    private @Tainted ByteBuffer unwrappedData;
    private @Tainted ByteBuffer unwrappedDataLengthBuffer;
    private @Tainted int serviceClass;
    
    @Tainted
    UserGroupInformation user = null;
    public @Tainted UserGroupInformation attemptingUser = null; // user name before auth

    // Fake 'call' for failed authorization response
    private final @Tainted Call authFailedCall = new @Tainted Call(AUTHORIZATION_FAILED_CALL_ID,
        RpcConstants.INVALID_RETRY_COUNT, null, this);
    private @Tainted ByteArrayOutputStream authFailedResponse = new @Tainted ByteArrayOutputStream();
    
    private final @Tainted Call saslCall = new @Tainted Call(AuthProtocol.SASL.callId,
        RpcConstants.INVALID_RETRY_COUNT, null, this);
    private final @Tainted ByteArrayOutputStream saslResponse = new @Tainted ByteArrayOutputStream();
    
    private @Tainted boolean sentNegotiate = false;
    private @Tainted boolean useWrap = false;
    
    public @Tainted Connection(@Tainted SelectionKey key, @Tainted SocketChannel channel, 
                      @Tainted
                      long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.unwrappedData = null;
      this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new @Tainted LinkedList<@Tainted Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (@Tainted IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }   

    @Override
    public @Tainted String toString(@Tainted Server.Connection this) {
      return getHostAddress() + ":" + remotePort; 
    }
    
    public @Tainted String getHostAddress(@Tainted Server.Connection this) {
      return hostAddress;
    }

    public @Tainted InetAddress getHostInetAddress(@Tainted Server.Connection this) {
      return addr;
    }
    
    public void setLastContact(@Tainted Server.Connection this, @Tainted long lastContact) {
      this.lastContact = lastContact;
    }

    public @Tainted long getLastContact(@Tainted Server.Connection this) {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private @Tainted boolean isIdle(@Tainted Server.Connection this) {
      return rpcCount == 0;
    }
    
    /* Decrement the outstanding RPC count */
    private void decRpcCount(@Tainted Server.Connection this) {
      rpcCount--;
    }
    
    /* Increment the outstanding RPC count */
    private void incRpcCount(@Tainted Server.Connection this) {
      rpcCount++;
    }
    
    private @Tainted boolean timedOut(@Tainted Server.Connection this, @Tainted long currentTime) {
      if (isIdle() && currentTime -  lastContact > maxIdleTime)
        return true;
      return false;
    }
    
    private @Tainted UserGroupInformation getAuthorizedUgi(@Tainted Server.Connection this, @Tainted String authorizedId)
        throws InvalidToken, AccessControlException {
      if (authMethod == AuthMethod.TOKEN) {
        @Tainted
        TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        @Tainted
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new @Tainted AccessControlException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId);
      }
    }

    private void saslReadAndProcess(@Tainted Server.Connection this, @Tainted DataInputStream dis) throws
    WrappedRpcServerException, IOException, InterruptedException {
      final @Tainted RpcSaslProto saslMessage =
          decodeProtobufFromStream(RpcSaslProto.newBuilder(), dis);
      switch (saslMessage.getState()) {
        case WRAP: {
          if (!saslContextEstablished || !useWrap) {
            throw new @Tainted WrappedRpcServerException(
                RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                new @Tainted SaslException("Server is not wrapping data"));
          }
          // loops over decoded data and calls processOneRpc
          unwrapPacketAndProcessRpcs(saslMessage.getToken().toByteArray());
          break;
        }
        default:
          saslProcess(saslMessage);
      }
    }

    private @Tainted Throwable getCauseForInvalidToken(@Tainted Server.Connection this, @Tainted IOException e) {
      @Tainted
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof @Tainted RetriableException) {
          return (@Tainted RetriableException) cause;
        } else if (cause instanceof @Tainted StandbyException) {
          return (@Tainted StandbyException) cause;
        } else if (cause instanceof @Tainted InvalidToken) {
          // FIXME: hadoop method signatures are restricting the SASL
          // callbacks to only returning InvalidToken, but some services
          // need to throw other exceptions (ex. NN + StandyException),
          // so for now we'll tunnel the real exceptions via an
          // InvalidToken's cause which normally is not set 
          if (cause.getCause() != null) {
            cause = cause.getCause();
          }
          return cause;
        }
        cause = cause.getCause();
      }
      return e;
    }
    
    private void saslProcess(@Tainted Server.Connection this, @Tainted RpcSaslProto saslMessage)
        throws WrappedRpcServerException, IOException, InterruptedException {
      if (saslContextEstablished) {
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            new @Tainted SaslException("Negotiation is already complete"));
      }
      @Tainted
      RpcSaslProto saslResponse = null;
      try {
        try {
          saslResponse = processSaslMessage(saslMessage);
        } catch (@Tainted IOException e) {
          rpcMetrics.incrAuthenticationFailures();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + this.toString() + ":"
              + attemptingUser + " (" + e.getLocalizedMessage() + ")");
          throw (@Tainted IOException) getCauseForInvalidToken(e);
        }
        
        if (saslServer != null && saslServer.isComplete()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Negotiated QoP is "
                + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server successfully authenticated client: " + user);
          }
          rpcMetrics.incrAuthenticationSuccesses();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      } catch (@Tainted WrappedRpcServerException wrse) { // don't re-wrap
        throw wrse;
      } catch (@Tainted IOException ioe) {
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ioe);
      }
      // send back response if any, may throw IOException
      if (saslResponse != null) {
        doSaslReply(saslResponse);
      }
      // do NOT enable wrapping until the last auth response is sent
      if (saslContextEstablished) {
        @Tainted
        String qop = (@Tainted String) saslServer.getNegotiatedProperty(Sasl.QOP);
        // SASL wrapping is only used if the connection has a QOP, and
        // the value is not auth.  ex. auth-int & auth-priv
        useWrap = (qop != null && !"auth".equalsIgnoreCase(qop));        
      }
    }
    
    private @Tainted RpcSaslProto processSaslMessage(@Tainted Server.Connection this, @Tainted RpcSaslProto saslMessage)
        throws IOException, InterruptedException {
      @Tainted
      RpcSaslProto saslResponse = null;
      final @Tainted SaslState state = saslMessage.getState(); // required      
      switch (state) {
        case NEGOTIATE: {
          if (sentNegotiate) {
            throw new @Tainted AccessControlException(
                "Client already attempted negotiation");
          }
          saslResponse = buildSaslNegotiateResponse();
          // simple-only server negotiate response is success which client
          // interprets as switch to simple
          if (saslResponse.getState() == SaslState.SUCCESS) {
            switchToSimple();
          }
          break;
        }
        case INITIATE: {
          if (saslMessage.getAuthsCount() != 1) {
            throw new @Tainted SaslException("Client mechanism is malformed");
          }
          // verify the client requested an advertised authType
          @Tainted
          SaslAuth clientSaslAuth = saslMessage.getAuths(0);
          if (!negotiateResponse.getAuthsList().contains(clientSaslAuth)) {
            if (sentNegotiate) {
              throw new @Tainted AccessControlException(
                  clientSaslAuth.getMethod() + " authentication is not enabled."
                      + "  Available:" + enabledAuthMethods);
            }
            saslResponse = buildSaslNegotiateResponse();
            break;
          }
          authMethod = AuthMethod.valueOf(clientSaslAuth.getMethod());
          // abort SASL for SIMPLE auth, server has already ensured that
          // SIMPLE is a legit option above.  we will send no response
          if (authMethod == AuthMethod.SIMPLE) {
            switchToSimple();
            break;
          }
          // sasl server for tokens may already be instantiated
          if (saslServer == null || authMethod != AuthMethod.TOKEN) {
            saslServer = createSaslServer(authMethod);
          }
          // fallthru to process sasl token
        }
        case RESPONSE: {
          if (!saslMessage.hasToken()) {
            throw new @Tainted SaslException("Client did not send a token");
          }
          @Tainted
          byte @Tainted [] saslToken = saslMessage.getToken().toByteArray();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.length
                + " for processing by saslServer.evaluateResponse()");
          }
          saslToken = saslServer.evaluateResponse(saslToken);
          saslResponse = buildSaslResponse(
              saslServer.isComplete() ? SaslState.SUCCESS : SaslState.CHALLENGE,
              saslToken);
          break;
        }
        default:
          throw new @Tainted SaslException("Client sent unsupported state " + state);
      }
      return saslResponse;
    }

    private void switchToSimple(@Tainted Server.Connection this) {
      // disable SASL and blank out any SASL server
      authProtocol = AuthProtocol.NONE;
      saslServer = null;
    }
    
    private @Tainted RpcSaslProto buildSaslResponse(@Tainted Server.Connection this, @Tainted SaslState state, @Tainted byte @Tainted [] replyToken) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Will send " + state + " token of size "
            + ((replyToken != null) ? replyToken.length : null)
            + " from saslServer.");
      }
      RpcSaslProto.@Tainted Builder response = RpcSaslProto.newBuilder();
      response.setState(state);
      if (replyToken != null) {
        response.setToken(ByteString.copyFrom(replyToken));
      }
      return response.build();
    }
    
    private void doSaslReply(@Tainted Server.Connection this, @Tainted Message message) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending sasl message "+message);
      }
      setupResponse(saslResponse, saslCall,
          RpcStatusProto.SUCCESS, null,
          new @Tainted RpcResponseWrapper(message), null, null);
      responder.doRespond(saslCall);
    }
    
    private void doSaslReply(@Tainted Server.Connection this, @Tainted Exception ioe) throws IOException {
      setupResponse(authFailedResponse, authFailedCall,
          RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_UNAUTHORIZED,
          null, ioe.getClass().getName(), ioe.getLocalizedMessage());
      responder.doRespond(authFailedCall);
    }
    
    private void disposeSasl(@Tainted Server.Connection this) {
      if (saslServer != null) {
        try {
          saslServer.dispose();
        } catch (@Tainted SaslException ignored) {
        }
      }
    }

    private void checkDataLength(@Tainted Server.Connection this, @Tainted int dataLength) throws IOException {
      if (dataLength < 0) {
        @Tainted
        String error = "Unexpected data length " + dataLength +
                       "!! from " + getHostAddress();
        LOG.warn(error);
        throw new @Tainted IOException(error);
      } else if (dataLength > maxDataLength) {
        @Tainted
        String error = "Requested data length " + dataLength +
              " is longer than maximum configured RPC length " + 
            maxDataLength + ".  RPC came from " + getHostAddress();
        LOG.warn(error);
        throw new @Tainted IOException(error);
      }
    }

    public @Tainted int readAndProcess(@Tainted Server.Connection this)
        throws WrappedRpcServerException, IOException, InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */    
        @Tainted
        int count = -1;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);       
          if (count < 0 || dataLengthBuffer.remaining() > 0) 
            return count;
        }
        
        if (!connectionHeaderRead) {
          //Every connection is expected to send the header.
          if (connectionHeaderBuf == null) {
            connectionHeaderBuf = ByteBuffer.allocate(3);
          }
          count = channelRead(channel, connectionHeaderBuf);
          if (count < 0 || connectionHeaderBuf.remaining() > 0) {
            return count;
          }
          @Tainted
          int version = connectionHeaderBuf.get(0);
          // TODO we should add handler for service class later
          this.setServiceClass(connectionHeaderBuf.get(1));
          dataLengthBuffer.flip();
          
          // Check if it looks like the user is hitting an IPC port
          // with an HTTP GET - this is a common error, so we can
          // send back a simple string indicating as much.
          if (HTTP_GET_BYTES.equals(dataLengthBuffer)) {
            setupHttpRequestOnIpcPortResponse();
            return -1;
          }
          
          if (!RpcConstants.HEADER.equals(dataLengthBuffer)
              || version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " + 
                     hostAddress + ":" + remotePort +
                     " got version " + version + 
                     " expected version " + CURRENT_VERSION);
            setupBadVersionResponse(version);
            return -1;
          }
          
          // this may switch us into SIMPLE
          authProtocol = initializeAuthContext(connectionHeaderBuf.get(2));          
          
          dataLengthBuffer.clear();
          connectionHeaderBuf = null;
          connectionHeaderRead = true;
          continue;
        }
        
        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();
          checkDataLength(dataLength);
          data = ByteBuffer.allocate(dataLength);
        }
        
        count = channelRead(channel, data);
        
        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          @Tainted
          boolean isHeaderRead = connectionContextRead;
          processOneRpc(data.array());
          data = null;
          if (!isHeaderRead) {
            continue;
          }
        } 
        return count;
      }
    }

    private @Tainted AuthProtocol initializeAuthContext(@Tainted Server.Connection this, @Tainted int authType)
        throws IOException {
      @Tainted
      AuthProtocol authProtocol = AuthProtocol.valueOf(authType);
      if (authProtocol == null) {
        @Tainted
        IOException ioe = new @Tainted IpcException("Unknown auth protocol:" + authType);
        doSaslReply(ioe);
        throw ioe;        
      }
      @Tainted
      boolean isSimpleEnabled = enabledAuthMethods.contains(AuthMethod.SIMPLE);
      switch (authProtocol) {
        case NONE: {
          // don't reply if client is simple and server is insecure
          if (!isSimpleEnabled) {
            @Tainted
            IOException ioe = new @Tainted AccessControlException(
                "SIMPLE authentication is not enabled."
                    + "  Available:" + enabledAuthMethods);
            doSaslReply(ioe);
            throw ioe;
          }
          break;
        }
        default: {
          break;
        }
      }
      return authProtocol;
    }

    private @Tainted RpcSaslProto buildSaslNegotiateResponse(@Tainted Server.Connection this)
        throws IOException, InterruptedException {
      @Tainted
      RpcSaslProto negotiateMessage = negotiateResponse;
      // accelerate token negotiation by sending initial challenge
      // in the negotiation response
      if (enabledAuthMethods.contains(AuthMethod.TOKEN)) {
        saslServer = createSaslServer(AuthMethod.TOKEN);
        @Tainted
        byte @Tainted [] challenge = saslServer.evaluateResponse(new @Tainted byte @Tainted [0]);
        RpcSaslProto.@Tainted Builder negotiateBuilder =
            RpcSaslProto.newBuilder(negotiateResponse);
        negotiateBuilder.getAuthsBuilder(0)  // TOKEN is always first
            .setChallenge(ByteString.copyFrom(challenge));
        negotiateMessage = negotiateBuilder.build();
      }
      sentNegotiate = true;
      return negotiateMessage;
    }
    
    private @Tainted SaslServer createSaslServer(@Tainted Server.Connection this, @Tainted AuthMethod authMethod)
        throws IOException, InterruptedException {
      return new @Tainted SaslRpcServer(authMethod).create(this, secretManager);
    }
    
    /**
     * Try to set up the response to indicate that the client version
     * is incompatible with the server. This can contain special-case
     * code to speak enough of past IPC protocols to pass back
     * an exception to the caller.
     * @param clientVersion the version the caller is using 
     * @throws IOException
     */
    private void setupBadVersionResponse(@Tainted Server.Connection this, @Tainted int clientVersion) throws IOException {
      @Tainted
      String errMsg = "Server IPC version " + CURRENT_VERSION +
      " cannot communicate with client version " + clientVersion;
      @Tainted
      ByteArrayOutputStream buffer = new @Tainted ByteArrayOutputStream();
      
      if (clientVersion >= 9) {
        // Versions >>9  understand the normal response
        @Tainted
        Call fakeCall = new @Tainted Call(-1, RpcConstants.INVALID_RETRY_COUNT, null,
            this);
        setupResponse(buffer, fakeCall, 
            RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_VERSION_MISMATCH,
            null, VersionMismatch.class.getName(), errMsg);
        responder.doRespond(fakeCall);
      } else if (clientVersion >= 3) {
        @Tainted
        Call fakeCall = new @Tainted Call(-1, RpcConstants.INVALID_RETRY_COUNT, null,
            this);
        // Versions 3 to 8 use older response
        setupResponseOldVersionFatal(buffer, fakeCall,
            null, VersionMismatch.class.getName(), errMsg);

        responder.doRespond(fakeCall);
      } else if (clientVersion == 2) { // Hadoop 0.18.3
        @Tainted
        Call fakeCall = new @Tainted Call(0, RpcConstants.INVALID_RETRY_COUNT, null,
            this);
        @Tainted
        DataOutputStream out = new @Tainted DataOutputStream(buffer);
        out.writeInt(0); // call ID
        out.writeBoolean(true); // error
        WritableUtils.writeString(out, VersionMismatch.class.getName());
        WritableUtils.writeString(out, errMsg);
        fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));
        
        responder.doRespond(fakeCall);
      }
    }
    
    private void setupHttpRequestOnIpcPortResponse(@Tainted Server.Connection this) throws IOException {
      @Tainted
      Call fakeCall = new @Tainted Call(0, RpcConstants.INVALID_RETRY_COUNT, null, this);
      fakeCall.setResponse(ByteBuffer.wrap(
          RECEIVED_HTTP_REQ_RESPONSE.getBytes()));
      responder.doRespond(fakeCall);
    }

    /** Reads the connection context following the connection header
     * @param dis - DataInputStream from which to read the header 
     * @throws WrappedRpcServerException - if the header cannot be
     *         deserialized, or the user is not authorized
     */ 
    private void processConnectionContext(@Tainted Server.Connection this, @Tainted DataInputStream dis)
        throws WrappedRpcServerException {
      // allow only one connection context during a session
      if (connectionContextRead) {
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Connection context already processed");
      }
      connectionContext = decodeProtobufFromStream(
          IpcConnectionContextProto.newBuilder(), dis);
      protocolName = connectionContext.hasProtocol() ? connectionContext
          .getProtocol() : null;

      @Tainted
      UserGroupInformation protocolUser = ProtoUtil.getUgi(connectionContext);
      if (saslServer == null) {
        user = protocolUser;
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However, 
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.TOKEN) {
            // Not allowed to doAs if token authentication is used
            throw new @Tainted WrappedRpcServerException(
                RpcErrorCodeProto.FATAL_UNAUTHORIZED,
                new @Tainted AccessControlException("Authenticated user (" + user
                    + ") doesn't match what the client claims to be ("
                    + protocolUser + ")"));
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            @Tainted
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
          }
        }
      }
      authorizeConnection();
      // don't set until after authz because connection isn't established
      connectionContextRead = true;
    }
    
    /**
     * Process a wrapped RPC Request - unwrap the SASL packet and process
     * each embedded RPC request 
     * @param buf - SASL wrapped request of one or more RPCs
     * @throws IOException - SASL packet cannot be unwrapped
     * @throws InterruptedException
     */    
    private void unwrapPacketAndProcessRpcs(@Tainted Server.Connection this, @Tainted byte @Tainted [] inBuf)
        throws WrappedRpcServerException, IOException, InterruptedException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Have read input token of size " + inBuf.length
            + " for processing by saslServer.unwrap()");
      }
      inBuf = saslServer.unwrap(inBuf, 0, inBuf.length);
      @Tainted
      ReadableByteChannel ch = Channels.newChannel(new @Tainted ByteArrayInputStream(
          inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        @Tainted
        int count = -1;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          @Tainted
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(unwrappedData.array());
          unwrappedData = null;
        }
      }
    }
    
    /**
     * Process an RPC Request - handle connection setup and decoding of
     * request into a Call
     * @param buf - contains the RPC request header and the rpc request
     * @throws IOException - internal error that should not be returned to
     *         client, typically failure to respond to client
     * @throws WrappedRpcServerException - an exception to be sent back to
     *         the client that does not require verbose logging by the
     *         Listener thread
     * @throws InterruptedException
     */    
    private void processOneRpc(@Tainted Server.Connection this, @Tainted byte @Tainted [] buf)
        throws IOException, WrappedRpcServerException, InterruptedException {
      @Tainted
      int callId = -1;
      @Tainted
      int retry = RpcConstants.INVALID_RETRY_COUNT;
      try {
        final @Tainted DataInputStream dis =
            new @Tainted DataInputStream(new @Tainted ByteArrayInputStream(buf));
        final @Tainted RpcRequestHeaderProto header =
            decodeProtobufFromStream(RpcRequestHeaderProto.newBuilder(), dis);
        callId = header.getCallId();
        retry = header.getRetryCount();
        if (LOG.isDebugEnabled()) {
          LOG.debug(" got #" + callId);
        }
        checkRpcHeaders(header);
        
        if (callId < 0) { // callIds typically used during connection setup
          processRpcOutOfBandRequest(header, dis);
        } else if (!connectionContextRead) {
          throw new @Tainted WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection context not established");
        } else {
          processRpcRequest(header, dis);
        }
      } catch (@Tainted WrappedRpcServerException wrse) { // inform client of error
        @Tainted
        Throwable ioe = wrse.getCause();
        final @Tainted Call call = new @Tainted Call(callId, retry, null, this);
        setupResponse(authFailedResponse, call,
            RpcStatusProto.FATAL, wrse.getRpcErrorCodeProto(), null,
            ioe.getClass().getName(), ioe.getMessage());
        responder.doRespond(call);
        throw wrse;
      }
    }

    /**
     * Verify RPC header is valid
     * @param header - RPC request header
     * @throws WrappedRpcServerException - header contains invalid values 
     */
    private void checkRpcHeaders(@Tainted Server.Connection this, @Tainted RpcRequestHeaderProto header)
        throws WrappedRpcServerException {
      if (!header.hasRpcOp()) {
        @Tainted
        String err = " IPC Server: No rpc op in rpcRequestHeader";
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      if (header.getRpcOp() != 
          RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET) {
        @Tainted
        String err = "IPC Server does not implement rpc header operation" + 
                header.getRpcOp();
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      // If we know the rpc kind, get its class so that we can deserialize
      // (Note it would make more sense to have the handler deserialize but 
      // we continue with this original design.
      if (!header.hasRpcKind()) {
        @Tainted
        String err = " IPC Server: No rpc kind in rpcRequestHeader";
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
    }

    /**
     * Process an RPC Request - the connection headers and context must
     * have been already read
     * @param header - RPC request header
     * @param dis - stream to request payload
     * @throws WrappedRpcServerException - due to fatal rpc layer issues such
     *   as invalid header or deserialization error. In this case a RPC fatal
     *   status response will later be sent back to client.
     * @throws InterruptedException
     */
    private void processRpcRequest(@Tainted Server.Connection this, @Tainted RpcRequestHeaderProto header,
        @Tainted
        DataInputStream dis) throws WrappedRpcServerException,
        InterruptedException {
      @Tainted
      Class<@Tainted ? extends @Tainted Writable> rpcRequestClass = 
          getRpcRequestWrapper(header.getRpcKind());
      if (rpcRequestClass == null) {
        LOG.warn("Unknown rpc kind "  + header.getRpcKind() + 
            " from client " + getHostAddress());
        final @Tainted String err = "Unknown rpc kind in rpc header"  + 
            header.getRpcKind();
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);   
      }
      @Tainted
      Writable rpcRequest;
      try { //Read the rpc request
        rpcRequest = ReflectionUtils.newInstance(rpcRequestClass, conf);
        rpcRequest.readFields(dis);
      } catch (@Tainted Throwable t) { // includes runtime exception from newInstance
        LOG.warn("Unable to read call parameters for client " +
                 getHostAddress() + "on connection protocol " +
            this.protocolName + " for rpcKind " + header.getRpcKind(),  t);
        @Tainted
        String err = "IPC server unable to read call parameters: "+ t.getMessage();
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
      }
        
      @Tainted
      Call call = new @Tainted Call(header.getCallId(), header.getRetryCount(),
          rpcRequest, this, ProtoUtil.convert(header.getRpcKind()), header
              .getClientId().toByteArray());
      callQueue.put(call);              // queue the call; maybe blocked here
      incRpcCount();  // Increment the rpc count
    }


    /**
     * Establish RPC connection setup by negotiating SASL if required, then
     * reading and authorizing the connection header
     * @param header - RPC header
     * @param dis - stream to request payload
     * @throws WrappedRpcServerException - setup failed due to SASL
     *         negotiation failure, premature or invalid connection context,
     *         or other state errors 
     * @throws IOException - failed to send a response back to the client
     * @throws InterruptedException
     */
    private void processRpcOutOfBandRequest(@Tainted Server.Connection this, @Tainted RpcRequestHeaderProto header,
        @Tainted
        DataInputStream dis) throws WrappedRpcServerException, IOException,
        InterruptedException {
      final @Tainted int callId = header.getCallId();
      if (callId == CONNECTION_CONTEXT_CALL_ID) {
        // SASL must be established prior to connection context
        if (authProtocol == AuthProtocol.SASL && !saslContextEstablished) {
          throw new @Tainted WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection header sent during SASL negotiation");
        }
        // read and authorize the user
        processConnectionContext(dis);
      } else if (callId == AuthProtocol.SASL.callId) {
        // if client was switched to simple, ignore first SASL message
        if (authProtocol != AuthProtocol.SASL) {
          throw new @Tainted WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "SASL protocol not requested by client");
        }
        saslReadAndProcess(dis);
      } else if (callId == PING_CALL_ID) {
        LOG.debug("Received ping message");
      } else {
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Unknown out of band call #" + callId);
      }
    }    

    /**
     * Authorize proxy users to access this server
     * @throws WrappedRpcServerException - user is not allowed to proxy
     */
    private void authorizeConnection(@Tainted Server.Connection this) throws WrappedRpcServerException {
      try {
        // If auth method is TOKEN, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null
            && (authMethod != AuthMethod.TOKEN)) {
          ProxyUsers.authorize(user, this.getHostAddress(), conf);
        }
        authorize(user, protocolName, getHostInetAddress());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully authorized " + connectionContext);
        }
        rpcMetrics.incrAuthorizationSuccesses();
      } catch (@Tainted AuthorizationException ae) {
        LOG.info("Connection from " + this
            + " for protocol " + connectionContext.getProtocol()
            + " is unauthorized for user " + user);
        rpcMetrics.incrAuthorizationFailures();
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ae);
      }
    }
    
    /**
     * Decode the a protobuf from the given input stream 
     * @param builder - Builder of the protobuf to decode
     * @param dis - DataInputStream to read the protobuf
     * @return Message - decoded protobuf
     * @throws WrappedRpcServerException - deserialization failed
     */
    @SuppressWarnings("unchecked")
    private <@Tainted T extends @Tainted Message> @Tainted T decodeProtobufFromStream(@Tainted Server.Connection this, @Tainted Builder builder,
        @Tainted
        DataInputStream dis) throws WrappedRpcServerException {
      try {
        builder.mergeDelimitedFrom(dis);
        return (@Tainted T)builder.build();
      } catch (@Tainted Exception ioe) {
        @Tainted
        Class<@Tainted ? extends java.lang.@Tainted Object> protoClass = builder.getDefaultInstanceForType().getClass();
        throw new @Tainted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
            "Error decoding " + protoClass.getSimpleName() + ": "+ ioe);
      }
    }

    /**
     * Get service class for connection
     * @return the serviceClass
     */
    public @Tainted int getServiceClass(@Tainted Server.Connection this) {
      return serviceClass;
    }

    /**
     * Set service class for connection
     * @param serviceClass the serviceClass to set
     */
    public void setServiceClass(@Tainted Server.Connection this, @Tainted int serviceClass) {
      this.serviceClass = serviceClass;
    }

    private synchronized void close(@Tainted Server.Connection this) {
      disposeSasl();
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(@Tainted Exception e) {
        LOG.debug("Ignoring socket shutdown exception", e);
      }
      if (channel.isOpen()) {
        try {channel.close();} catch(@Tainted Exception e) {}
      }
      try {socket.close();} catch(@Tainted Exception e) {}
    }
  }

  /** Handles queued calls . */
  private class Handler extends @Tainted Thread {
    public @Tainted Handler(@Tainted int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber + " on " + port);
    }

    @Override
    public void run(@Tainted Server.Handler this) {
      LOG.debug(getName() + ": starting");
      SERVER.set(Server.this);
      @Tainted
      ByteArrayOutputStream buf = 
        new @Tainted ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
      while (running) {
        try {
          final @Tainted Call call = callQueue.take(); // pop the queue; maybe blocked here
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": " + call + " for RpcKind " + call.rpcKind);
          }
          @Tainted
          String errorClass = null;
          @Tainted
          String error = null;
          @Tainted
          RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
          @Tainted
          RpcErrorCodeProto detailedErr = null;
          @Tainted
          Writable value = null;

          CurCall.set(call);
          try {
            // Make the call as the user via Subject.doAs, thus associating
            // the call with the Subject
            if (call.connection.user == null) {
              value = call(call.rpcKind, call.connection.protocolName, call.rpcRequest, 
                           call.timestamp);
            } else {
              value = 
                call.connection.user.doAs
                  (new @Tainted PrivilegedExceptionAction<@Tainted Writable>() {
                     @Override
                     public @Tainted Writable run() throws Exception {
                       // make the call
                       return call(call.rpcKind, call.connection.protocolName, 
                                   call.rpcRequest, call.timestamp);

                     }
                   }
                  );
            }
          } catch (@Tainted Throwable e) {
            if (e instanceof @Tainted UndeclaredThrowableException) {
              e = e.getCause();
            }
            @Tainted
            String logMsg = getName() + ", call " + call + ": error: " + e;
            if (e instanceof @Tainted RuntimeException || e instanceof @Tainted Error) {
              // These exception types indicate something is probably wrong
              // on the server side, as opposed to just a normal exceptional
              // result.
              LOG.warn(logMsg, e);
            } else if (exceptionsHandler.isTerse(e.getClass())) {
             // Don't log the whole stack trace of these exceptions.
              // Way too noisy!
              LOG.info(logMsg);
            } else {
              LOG.info(logMsg, e);
            }
            if (e instanceof @Tainted RpcServerException) {
              @Tainted
              RpcServerException rse = ((@Tainted RpcServerException)e); 
              returnStatus = rse.getRpcStatusProto();
              detailedErr = rse.getRpcErrorCodeProto();
            } else {
              returnStatus = RpcStatusProto.ERROR;
              detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
            }
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
            // Remove redundant error class name from the beginning of the stack trace
            @Tainted
            String exceptionHdr = errorClass + ": ";
            if (error.startsWith(exceptionHdr)) {
              error = error.substring(exceptionHdr.length());
            }
          }
          CurCall.set(null);
          synchronized (call.connection.responseQueue) {
            // setupResponse() needs to be sync'ed together with 
            // responder.doResponse() since setupResponse may use
            // SASL to encrypt response data and SASL enforces
            // its own message ordering.
            setupResponse(buf, call, returnStatus, detailedErr, 
                value, errorClass, error);
            
            // Discard the large buf and reset it back to smaller size 
            // to free up heap
            if (buf.size() > maxRespSize) {
              LOG.warn("Large response size " + buf.size() + " for call "
                  + call.toString());
              buf = new @Tainted ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
            }
            responder.doRespond(call);
          }
        } catch (@Tainted InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " unexpectedly interrupted", e);
          }
        } catch (@Tainted Exception e) {
          LOG.info(getName() + " caught an exception", e);
        }
      }
      LOG.debug(getName() + ": exiting");
    }

  }
  
  protected @Tainted Server(@Tainted String bindAddress, @Tainted int port,
                  @Tainted
                  Class<@Tainted ? extends @Tainted Writable> paramClass, @Tainted int handlerCount, 
                  @Tainted
                  Configuration conf)
    throws IOException 
  {
    this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Integer
        .toString(port), null, null);
  }
  
  protected @Tainted Server(@Tainted String bindAddress, @Tainted int port,
      @Tainted
      Class<@Tainted ? extends @Tainted Writable> rpcRequestClass, @Tainted int handlerCount,
      @Tainted
      int numReaders, @Tainted int queueSizePerHandler, @Tainted Configuration conf,
      @Tainted
      String serverName, @Tainted SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager)
    throws IOException {
    this(bindAddress, port, rpcRequestClass, handlerCount, numReaders, 
        queueSizePerHandler, conf, serverName, secretManager, null);
  }
  
  /** 
   * Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
   * from configuration. Otherwise the configuration will be picked up.
   * 
   * If rpcRequestClass is null then the rpcRequestClass must have been 
   * registered via {@link #registerProtocolEngine(RPC.RpcKind,
   *  Class, RPC.RpcInvoker)}
   * This parameter has been retained for compatibility with existing tests
   * and usage.
   */
  @SuppressWarnings("unchecked")
  protected @Tainted Server(@Tainted String bindAddress, @Tainted int port,
      @Tainted
      Class<@Tainted ? extends @Tainted Writable> rpcRequestClass, @Tainted int handlerCount,
      @Tainted
      int numReaders, @Tainted int queueSizePerHandler, @Tainted Configuration conf,
      @Tainted
      String serverName, @Tainted SecretManager<@Tainted ? extends @Tainted TokenIdentifier> secretManager,
      @Tainted
      String portRangeConfig)
    throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.portRangeConfig = portRangeConfig;
    this.port = port;
    this.rpcRequestClass = rpcRequestClass; 
    this.handlerCount = handlerCount;
    this.socketSendBufferSize = 0;
    this.maxDataLength = conf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    if (queueSizePerHandler != -1) {
      this.maxQueueSize = queueSizePerHandler;
    } else {
      this.maxQueueSize = handlerCount * conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);      
    }
    this.maxRespSize = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
    if (numReaders != -1) {
      this.readThreads = numReaders;
    } else {
      this.readThreads = conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    }
    this.callQueue  = new @Tainted LinkedBlockingQueue<@Tainted Call>(maxQueueSize); 
    this.maxIdleTime = 2 * conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
    this.maxConnectionsToNuke = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
    this.thresholdIdleConnections = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
    this.secretManager = (@Tainted SecretManager<@Tainted TokenIdentifier>) secretManager;
    this.authorize = 
      conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, 
                      false);

    // configure supported authentications
    this.enabledAuthMethods = getAuthMethods(secretManager, conf);
    this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);
    
    // Start the listener here and let it bind to the port
    listener = new @Tainted Listener();
    this.port = listener.getAddress().getPort();    
    this.rpcMetrics = RpcMetrics.create(this);
    this.rpcDetailedMetrics = RpcDetailedMetrics.create(this.port);
    this.tcpNoDelay = conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT);

    // Create the responder here
    responder = new @Tainted Responder();
    
    if (secretManager != null) {
      SaslRpcServer.init(conf);
    }
    
    this.exceptionsHandler.addTerseExceptions(StandbyException.class);
  }
  
  private @Tainted RpcSaslProto buildNegotiateResponse(@Tainted Server this, @Tainted List<@Tainted AuthMethod> authMethods)
      throws IOException {
    RpcSaslProto.@Tainted Builder negotiateBuilder = RpcSaslProto.newBuilder();
    if (authMethods.contains(AuthMethod.SIMPLE) && authMethods.size() == 1) {
      // SIMPLE-only servers return success in response to negotiate
      negotiateBuilder.setState(SaslState.SUCCESS);
    } else {
      negotiateBuilder.setState(SaslState.NEGOTIATE);
      for (@Tainted AuthMethod authMethod : authMethods) {
        @Tainted
        SaslRpcServer saslRpcServer = new @Tainted SaslRpcServer(authMethod);      
        SaslAuth.@Tainted Builder builder = negotiateBuilder.addAuthsBuilder()
            .setMethod(authMethod.toString())
            .setMechanism(saslRpcServer.mechanism);
        if (saslRpcServer.protocol != null) {
          builder.setProtocol(saslRpcServer.protocol);
        }
        if (saslRpcServer.serverId != null) {
          builder.setServerId(saslRpcServer.serverId);
        }
      }
    }
    return negotiateBuilder.build();
  }

  // get the security type from the conf. implicitly include token support
  // if a secret manager is provided, or fail if token is the conf value but
  // there is no secret manager
  private @Tainted List<@Tainted AuthMethod> getAuthMethods(@Tainted Server this, @Tainted SecretManager<@Tainted ? extends java.lang.@Tainted Object> secretManager,
                                             @Tainted
                                             Configuration conf) {
    @Tainted
    AuthenticationMethod confAuthenticationMethod =
        SecurityUtil.getAuthenticationMethod(conf);        
    @Tainted
    List<@Tainted AuthMethod> authMethods = new @Tainted ArrayList<@Tainted AuthMethod>();
    if (confAuthenticationMethod == AuthenticationMethod.TOKEN) {
      if (secretManager == null) {
        throw new @Tainted IllegalArgumentException(AuthenticationMethod.TOKEN +
            " authentication requires a secret manager");
      } 
    } else if (secretManager != null) {
      LOG.debug(AuthenticationMethod.TOKEN +
          " authentication enabled for secret manager");
      // most preferred, go to the front of the line!
      authMethods.add(AuthenticationMethod.TOKEN.getAuthMethod());
    }
    authMethods.add(confAuthenticationMethod.getAuthMethod());        
    
    LOG.debug("Server accepts auth methods:" + authMethods);
    return authMethods;
  }
  
  private void closeConnection(@Tainted Server this, @Tainted Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    connection.close();
  }
  
  /**
   * Setup response for the IPC Call.
   * 
   * @param responseBuf buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param status of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(@Tainted Server this, @Tainted ByteArrayOutputStream responseBuf,
                             @Tainted
                             Call call, @Tainted RpcStatusProto status, @Tainted RpcErrorCodeProto erCode,
                             @Tainted
                             Writable rv, @Tainted String errorClass, @Tainted String error) 
  throws IOException {
    responseBuf.reset();
    @Tainted
    DataOutputStream out = new @Tainted DataOutputStream(responseBuf);
    RpcResponseHeaderProto.@Tainted Builder headerBuilder =  
        RpcResponseHeaderProto.newBuilder();
    headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
    headerBuilder.setCallId(call.callId);
    headerBuilder.setRetryCount(call.retryCount);
    headerBuilder.setStatus(status);
    headerBuilder.setServerIpcVersionNum(CURRENT_VERSION);

    if (status == RpcStatusProto.SUCCESS) {
      @Tainted
      RpcResponseHeaderProto header = headerBuilder.build();
      final @Tainted int headerLen = header.getSerializedSize();
      @Tainted
      int fullLength  = CodedOutputStream.computeRawVarint32Size(headerLen) +
          headerLen;
      try {
        if (rv instanceof ProtobufRpcEngine.@Tainted RpcWrapper) {
          ProtobufRpcEngine.@Tainted RpcWrapper resWrapper = 
              (ProtobufRpcEngine.@Tainted RpcWrapper) rv;
          fullLength += resWrapper.getLength();
          out.writeInt(fullLength);
          header.writeDelimitedTo(out);
          rv.write(out);
        } else { // Have to serialize to buffer to get len
          final @Tainted DataOutputBuffer buf = new @Tainted DataOutputBuffer();
          rv.write(buf);
          @Tainted
          byte @Tainted [] data = buf.getData();
          fullLength += buf.getLength();
          out.writeInt(fullLength);
          header.writeDelimitedTo(out);
          out.write(data, 0, buf.getLength());
        }
      } catch (@Tainted Throwable t) {
        LOG.warn("Error serializing call response for call " + call, t);
        // Call back to same function - this is OK since the
        // buffer is reset at the top, and since status is changed
        // to ERROR it won't infinite loop.
        setupResponse(responseBuf, call, RpcStatusProto.ERROR,
            RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
            null, t.getClass().getName(),
            StringUtils.stringifyException(t));
        return;
      }
    } else { // Rpc Failure
      headerBuilder.setExceptionClassName(errorClass);
      headerBuilder.setErrorMsg(error);
      headerBuilder.setErrorDetail(erCode);
      @Tainted
      RpcResponseHeaderProto header = headerBuilder.build();
      @Tainted
      int headerLen = header.getSerializedSize();
      final @Tainted int fullLength  = 
          CodedOutputStream.computeRawVarint32Size(headerLen) + headerLen;
      out.writeInt(fullLength);
      header.writeDelimitedTo(out);
    }
    if (call.connection.useWrap) {
      wrapWithSasl(responseBuf, call);
    }
    call.setResponse(ByteBuffer.wrap(responseBuf.toByteArray()));
  }
  
  /**
   * Setup response for the IPC Call on Fatal Error from a 
   * client that is using old version of Hadoop.
   * The response is serialized using the previous protocol's response
   * layout.
   * 
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponseOldVersionFatal(@Tainted Server this, @Tainted ByteArrayOutputStream response, 
                             @Tainted
                             Call call,
                             @Tainted
                             Writable rv, @Tainted String errorClass, @Tainted String error) 
  throws IOException {
    final @Tainted int OLD_VERSION_FATAL_STATUS = -1;
    response.reset();
    @Tainted
    DataOutputStream out = new @Tainted DataOutputStream(response);
    out.writeInt(call.callId);                // write call id
    out.writeInt(OLD_VERSION_FATAL_STATUS);   // write FATAL_STATUS
    WritableUtils.writeString(out, errorClass);
    WritableUtils.writeString(out, error);

    if (call.connection.useWrap) {
      wrapWithSasl(response, call);
    }
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }
  
  
  private void wrapWithSasl(@Tainted Server this, @Tainted ByteArrayOutputStream response, @Tainted Call call)
      throws IOException {
    if (call.connection.saslServer != null) {
      @Tainted
      byte @Tainted [] token = response.toByteArray();
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (call.connection.saslServer) {
        token = call.connection.saslServer.wrap(token, 0, token.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      response.reset();
      // rebuild with sasl header and payload
      @Tainted
      RpcResponseHeaderProto saslHeader = RpcResponseHeaderProto.newBuilder()
          .setCallId(AuthProtocol.SASL.callId)
          .setStatus(RpcStatusProto.SUCCESS)
          .build();
      @Tainted
      RpcSaslProto saslMessage = RpcSaslProto.newBuilder()
          .setState(SaslState.WRAP)
          .setToken(ByteString.copyFrom(token, 0, token.length))
          .build();
      @Tainted
      RpcResponseMessageWrapper saslResponse =
          new @Tainted RpcResponseMessageWrapper(saslHeader, saslMessage);

      @Tainted
      DataOutputStream out = new @Tainted DataOutputStream(response);
      out.writeInt(saslResponse.getLength());
      saslResponse.write(out);
    }
  }
  
  @Tainted
  Configuration getConf(@Tainted Server this) {
    return conf;
  }
  
  /** Sets the socket buffer size used for responding to RPCs */
  public void setSocketSendBufSize(@Tainted Server this, @Tainted int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start(@Tainted Server this) {
    responder.start();
    listener.start();
    handlers = new @Tainted Handler @Tainted [handlerCount];
    
    for (@Tainted int i = 0; i < handlerCount; i++) {
      handlers[i] = new @Tainted Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop(@Tainted Server this) {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (@Tainted int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
    if (this.rpcDetailedMetrics != null) {
      this.rpcDetailedMetrics.shutdown();
    }
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join(@Tainted Server this) throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized @Tainted InetSocketAddress getListenerAddress(@Tainted Server this) {
    return listener.getAddress();
  }
  
  /** 
   * Called for each call. 
   * @deprecated Use  {@link #call(RPC.RpcKind, String,
   *  Writable, long)} instead
   */
  @Deprecated
  public @Tainted Writable call(@Tainted Server this, @Tainted Writable param, @Tainted long receiveTime) throws Exception {
    return call(RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime);
  }
  
  /** Called for each call. */
  public abstract @Tainted Writable call(@Tainted Server this, RPC.@Tainted RpcKind rpcKind, @Tainted String protocol,
      @Tainted
      Writable param, @Tainted long receiveTime) throws Exception;
  
  /**
   * Authorize the incoming client connection.
   * 
   * @param user client user
   * @param protocolName - the protocol
   * @param addr InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  private void authorize(@Tainted Server this, @Tainted UserGroupInformation user, @Tainted String protocolName,
      @Tainted
      InetAddress addr) throws AuthorizationException {
    if (authorize) {
      if (protocolName == null) {
        throw new @Tainted AuthorizationException("Null protocol not authorized");
      }
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> protocol = null;
      try {
        protocol = getProtocolClass(protocolName, getConf());
      } catch (@Tainted ClassNotFoundException cfne) {
        throw new @Tainted AuthorizationException("Unknown protocol: " + 
                                         protocolName);
      }
      serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
    }
  }
  
  /**
   * Get the port on which the IPC Server is listening for incoming connections.
   * This could be an ephemeral port too, in which case we return the real
   * port on which the Server has bound.
   * @return port on which IPC Server is listening
   */
  public @Tainted int getPort(@Tainted Server this) {
    return port;
  }
  
  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public @Tainted int getNumOpenConnections(@Tainted Server this) {
    return numConnections;
  }
  
  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public @Tainted int getCallQueueLen(@Tainted Server this) {
    return callQueue.size();
  }
  
  /**
   * The maximum size of the rpc call queue of this server.
   * @return The maximum size of the rpc call queue.
   */
  public @Tainted int getMaxQueueSize(@Tainted Server this) {
    return maxQueueSize;
  }

  /**
   * The number of reader threads for this server.
   * @return The number of reader threads.
   */
  public @Tainted int getNumReaders(@Tainted Server this) {
    return readThreads;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be 
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static @Tainted int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.
  
  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large 
   * buffer.  
   *
   * @see WritableByteChannel#write(ByteBuffer)
   */
  private @Tainted int channelWrite(@Tainted Server this, @Tainted WritableByteChannel channel, 
                           @Tainted
                           ByteBuffer buffer) throws IOException {
    
    @Tainted
    int count =  (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                 channel.write(buffer) : channelIO(null, channel, buffer);
    if (count > 0) {
      rpcMetrics.incrSentBytes(count);
    }
    return count;
  }
  
  
  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * ByteBuffer increases. There should not be any performance degredation.
   * 
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  private @Tainted int channelRead(@Tainted Server this, @Tainted ReadableByteChannel channel, 
                          @Tainted
                          ByteBuffer buffer) throws IOException {
    
    @Tainted
    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      rpcMetrics.incrReceivedBytes(count);
    }
    return count;
  }
  
  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   * 
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static @Tainted int channelIO(@Tainted ReadableByteChannel readCh, 
                               @Tainted
                               WritableByteChannel writeCh,
                               @Tainted
                               ByteBuffer buf) throws IOException {
    
    @Tainted
    int originalLimit = buf.limit();
    @Tainted
    int initialRemaining = buf.remaining();
    @Tainted
    int ret = 0;
    
    while (buf.remaining() > 0) {
      try {
        @Tainted
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);
        
        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf); 
        
        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);        
      }
    }

    @Tainted
    int nBytes = initialRemaining - buf.remaining(); 
    return (nBytes > 0) ? nBytes : ret;
  }
}
