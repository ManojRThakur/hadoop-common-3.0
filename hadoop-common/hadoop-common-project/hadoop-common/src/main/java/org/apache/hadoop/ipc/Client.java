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
import static org.apache.hadoop.ipc.RpcConstants.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;
import javax.security.sasl.Sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedOutputStream;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
@InterfaceAudience.LimitedPrivate(value = { "Common", "HDFS", "MapReduce", "Yarn" })
@InterfaceStability.Evolving
public class Client {
  
  public static final @Tainted Log LOG = LogFactory.getLog(Client.class);

  /** A counter for generating call IDs. */
  private static final @Tainted AtomicInteger callIdCounter = new @Tainted AtomicInteger();

  private static final @Tainted ThreadLocal<@Tainted Integer> callId = new @Tainted ThreadLocal<@Tainted Integer>();
  private static final @Tainted ThreadLocal<@Tainted Integer> retryCount = new @Tainted ThreadLocal<@Tainted Integer>();

  /** Set call id and retry count for the next call. */
  public static void setCallIdAndRetryCount(@Tainted int cid, @Tainted int rc) {
    Preconditions.checkArgument(cid != RpcConstants.INVALID_CALL_ID);
    Preconditions.checkState(callId.get() == null);
    Preconditions.checkArgument(rc != RpcConstants.INVALID_RETRY_COUNT);

    callId.set(cid);
    retryCount.set(rc);
  }

  private @Tainted Hashtable<@Tainted ConnectionId, @Tainted Connection> connections =
    new @Tainted Hashtable<@Tainted ConnectionId, @Tainted Connection>();

  private @Tainted Class<@Tainted ? extends @Tainted Writable> valueClass;   // class of call values
  private @Tainted AtomicBoolean running = new @Tainted AtomicBoolean(true); // if client runs
  final private @Tainted Configuration conf;

  private @Tainted SocketFactory socketFactory;           // how to create sockets
  private @Tainted int refCount = 1;

  private final @Tainted int connectionTimeout;

  private final @Tainted boolean fallbackAllowed;
  private final @Tainted byte @Tainted [] clientId;
  
  /**
   * Executor on which IPC calls' parameters are sent.
   * Deferring the sending of parameters to a separate
   * thread isolates them from thread interruptions in the
   * calling code.
   */
  private final @Tainted ExecutorService sendParamsExecutor;
  private final static @Tainted ClientExecutorServiceFactory clientExcecutorFactory =
      new @Tainted ClientExecutorServiceFactory();

  private static class ClientExecutorServiceFactory {
    private @Tainted int executorRefCount = 0;
    private @Tainted ExecutorService clientExecutor = null;
    
    /**
     * Get Executor on which IPC calls' parameters are sent.
     * If the internal reference counter is zero, this method
     * creates the instance of Executor. If not, this method
     * just returns the reference of clientExecutor.
     * 
     * @return An ExecutorService instance
     */
    synchronized @Tainted ExecutorService refAndGetInstance(Client.@Tainted ClientExecutorServiceFactory this) {
      if (executorRefCount == 0) {
        clientExecutor = Executors.newCachedThreadPool(
            new @Tainted ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("IPC Parameter Sending Thread #%d")
            .build());
      }
      executorRefCount++;
      
      return clientExecutor;
    }
    
    /**
     * Cleanup Executor on which IPC calls' parameters are sent.
     * If reference counter is zero, this method discards the
     * instance of the Executor. If not, this method
     * just decrements the internal reference counter.
     * 
     * @return An ExecutorService instance if it exists.
     *   Null is returned if not.
     */
    synchronized @Tainted ExecutorService unrefAndCleanup(Client.@Tainted ClientExecutorServiceFactory this) {
      executorRefCount--;
      assert(executorRefCount >= 0);
      
      if (executorRefCount == 0) {
        clientExecutor.shutdown();
        try {
          if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
            clientExecutor.shutdownNow();
          }
        } catch (@Tainted InterruptedException e) {
          LOG.error("Interrupted while waiting for clientExecutor" +
              "to stop", e);
          clientExecutor.shutdownNow();
        }
        clientExecutor = null;
      }
      
      return clientExecutor;
    }
  };
  
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  final public static void setPingInterval(@Tainted Configuration conf, @Tainted int pingInterval) {
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  final static @Tainted int getPingInterval(@Tainted Configuration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }

  /**
   * The time after which a RPC will timeout.
   * If ping is not enabled (via ipc.client.ping), then the timeout value is the 
   * same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   */
  final public static @Tainted int getTimeout(@Tainted Configuration conf) {
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true)) {
      return getPingInterval(conf);
    }
    return -1;
  }
  /**
   * set the connection timeout value in configuration
   * 
   * @param conf Configuration
   * @param timeout the socket connect timeout value
   */
  public static final void setConnectTimeout(@Tainted Configuration conf, @Tainted int timeout) {
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY, timeout);
  }

  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount(@Tainted Client this) {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount(@Tainted Client this) {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized @Tainted boolean isZeroReference(@Tainted Client this) {
    return refCount==0;
  }

  /** Check the rpc response header. */
  void checkResponse(@Tainted Client this, @Tainted RpcResponseHeaderProto header) throws IOException {
    if (header == null) {
      throw new @Tainted IOException("Response is null.");
    }
    if (header.hasClientId()) {
      // check client IDs
      final @Tainted byte @Tainted [] id = header.getClientId().toByteArray();
      if (!Arrays.equals(id, RpcConstants.DUMMY_CLIENT_ID)) {
        if (!Arrays.equals(id, clientId)) {
          throw new @Tainted IOException("Client IDs not matched: local ID="
              + StringUtils.byteToHexString(clientId) + ", ID in reponse="
              + StringUtils.byteToHexString(header.getClientId().toByteArray()));
        }
      }
    }
  }

  @Tainted
  Call createCall(@Tainted Client this, RPC.@Tainted RpcKind rpcKind, @Tainted Writable rpcRequest) {
    return new @Tainted Call(rpcKind, rpcRequest);
  }

  /** 
   * Class that represents an RPC call
   */
  static class Call {
    final @Tainted int id;               // call id
    final @Tainted int retry;           // retry count
    final @Tainted Writable rpcRequest;  // the serialized rpc request
    @Tainted
    Writable rpcResponse;       // null if rpc has error
    @Tainted
    IOException error;          // exception, null if success
    final RPC.@Tainted RpcKind rpcKind;      // Rpc EngineKind
    @Tainted
    boolean done;               // true when call is done

    private @Tainted Call(RPC.@Tainted RpcKind rpcKind, @Tainted Writable param) {
      this.rpcKind = rpcKind;
      this.rpcRequest = param;

      final @Tainted Integer id = callId.get();
      if (id == null) {
        this.id = nextCallId();
      } else {
        callId.set(null);
        this.id = id;
      }
      
      final @Tainted Integer rc = retryCount.get();
      if (rc == null) {
        this.retry = 0;
      } else {
        this.retry = rc;
      }
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete(Client.@Tainted Call this) {
      this.done = true;
      notify();                                 // notify caller
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(Client.@Tainted Call this, @Tainted IOException error) {
      this.error = error;
      callComplete();
    }
    
    /** Set the return value when there is no error. 
     * Notify the caller the call is done.
     * 
     * @param rpcResponse return value of the rpc call.
     */
    public synchronized void setRpcResponse(Client.@Tainted Call this, @Tainted Writable rpcResponse) {
      this.rpcResponse = rpcResponse;
      callComplete();
    }
    
    public synchronized @Tainted Writable getRpcResponse(Client.@Tainted Call this) {
      return rpcResponse;
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends @Tainted Thread {
    private @Tainted InetSocketAddress server;             // server ip:port
    private final @Tainted ConnectionId remoteId;                // connection id
    private @Tainted AuthMethod authMethod; // authentication method
    private @Tainted AuthProtocol authProtocol;
    private @Tainted int serviceClass;
    private @Tainted SaslRpcClient saslRpcClient;
    
    private @Tainted Socket socket = null;                 // connected socket
    private @Tainted DataInputStream in;
    private @Tainted DataOutputStream out;
    private @Tainted int rpcTimeout;
    private @Tainted int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final @Tainted RetryPolicy connectionRetryPolicy;
    private @Tainted int maxRetriesOnSocketTimeouts;
    private @Tainted boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private @Tainted boolean doPing; //do we need to send ping message
    private @Tainted int pingInterval; // how often sends ping to the server in msecs
    private @Tainted ByteArrayOutputStream pingRequest; // ping message
    
    // currently active calls
    private @Tainted Hashtable<@Tainted Integer, @Tainted Call> calls = new @Tainted Hashtable<@Tainted Integer, @Tainted Call>();
    private @Tainted AtomicLong lastActivity = new @Tainted AtomicLong();// last I/O activity time
    private @Tainted AtomicBoolean shouldCloseConnection = new @Tainted AtomicBoolean();  // indicate if the connection is closed
    private @Tainted IOException closeException; // close reason
    
    private final @Tainted Object sendRpcRequestLock = new @Tainted Object();

    public @Tainted Connection(@Tainted ConnectionId remoteId, @Tainted int serviceClass) throws IOException {
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw NetUtils.wrapException(server.getHostName(),
            server.getPort(),
            null,
            0,
            new @Tainted UnknownHostException());
      }
      this.rpcTimeout = remoteId.getRpcTimeout();
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
      this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.doPing = remoteId.getDoPing();
      if (doPing) {
        // construct a RPC header with the callId as the ping callId
        pingRequest = new @Tainted ByteArrayOutputStream();
        @Tainted
        RpcRequestHeaderProto pingHeader = ProtoUtil
            .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                OperationProto.RPC_FINAL_PACKET, PING_CALL_ID,
                RpcConstants.INVALID_RETRY_COUNT, clientId);
        pingHeader.writeDelimitedTo(pingRequest);
      }
      this.pingInterval = remoteId.getPingInterval();
      this.serviceClass = serviceClass;
      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is " + this.pingInterval + " ms.");
      }

      @Tainted
      UserGroupInformation ticket = remoteId.getTicket();
      // try SASL if security is enabled or if the ugi contains tokens.
      // this causes a SIMPLE client with tokens to attempt SASL
      @Tainted
      boolean trySasl = UserGroupInformation.isSecurityEnabled() ||
                        (ticket != null && !ticket.getTokens().isEmpty());
      this.authProtocol = trySasl ? AuthProtocol.SASL : AuthProtocol.NONE;
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          server.toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch(@Tainted Client.Connection this) {
      lastActivity.set(Time.now());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized @Tainted boolean addCall(@Tainted Client.Connection this, @Tainted Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends @Tainted FilterInputStream {
      /* constructor */
      protected @Tainted PingInputStream(@Tainted InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed or 
       * is not configured to have a RPC timeout, send a ping.
       * (if rpcTimeout is not set to be 0, then RPC should timeout.
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(@Tainted Client.Connection.PingInputStream this, @Tainted SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get() || rpcTimeout > 0) {
          throw e;
        } else {
          sendPing();
        }
      }
      
      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public @Tainted int read(@Tainted Client.Connection.PingInputStream this) throws IOException {
        do {
          try {
            return super.read();
          } catch (@Tainted SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public @Tainted int read(@Tainted Client.Connection.PingInputStream this, @Tainted byte @Tainted [] buf, @Tainted int off, @Tainted int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (@Tainted SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
    }
    
    private synchronized void disposeSasl(@Tainted Client.Connection this) {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (@Tainted IOException ignored) {
        }
      }
    }
    
    private synchronized @Tainted boolean shouldAuthenticateOverKrb(@Tainted Client.Connection this) throws IOException {
      @Tainted
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      @Tainted
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      @Tainted
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS && loginUser != null &&
      // Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
        return true;
      }
      return false;
    }
    
    private synchronized @Tainted AuthMethod setupSaslConnection(@Tainted Client.Connection this, final @Tainted InputStream in2, 
        final @Tainted OutputStream out2) throws IOException, InterruptedException {
      saslRpcClient = new @Tainted SaslRpcClient(remoteId.getTicket(),
          remoteId.getProtocol(), remoteId.getAddress(), conf);
      return saslRpcClient.saslConnect(in2, out2);
    }

    /**
     * Update the server address if the address corresponding to the host
     * name has changed.
     *
     * @return true if an addr change was detected.
     * @throws IOException when the hostname cannot be resolved.
     */
    private synchronized @Tainted boolean updateAddress(@Tainted Client.Connection this) throws IOException {
      // Do a fresh lookup with the old host name.
      @Tainted
      InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                               server.getHostName(), server.getPort());

      if (!server.equals(currentAddr)) {
        LOG.warn("Address change detected. Old: " + server.toString() +
                                 " New: " + currentAddr.toString());
        server = currentAddr;
        return true;
      }
      return false;
    }
    
    private synchronized void setupConnection(@Tainted Client.Connection this) throws IOException {
      @Tainted
      short ioFailures = 0;
      @Tainted
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          
          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */
          @Tainted
          UserGroupInformation ticket = remoteId.getTicket();
          if (ticket != null && ticket.hasKerberosCredentials()) {
            @Tainted
            KerberosInfo krbInfo = 
              remoteId.getProtocol().getAnnotation(KerberosInfo.class);
            if (krbInfo != null && krbInfo.clientPrincipal() != null) {
              @Tainted
              String host = 
                SecurityUtil.getHostFromPrincipal(remoteId.getTicket().getUserName());
              
              // If host name is a valid local address then bind socket to it
              @Tainted
              InetAddress localAddr = NetUtils.getLocalInetAddress(host);
              if (localAddr != null) {
                this.socket.bind(new @Tainted InetSocketAddress(localAddr, 0));
              }
            }
          }
          
          NetUtils.connect(this.socket, server, connectionTimeout);
          if (rpcTimeout > 0) {
            pingInterval = rpcTimeout;  // rpcTimeout overwrites pingInterval
          }
          this.socket.setSoTimeout(pingInterval);
          return;
        } catch (@Tainted ConnectTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionTimeout(timeoutFailures++,
              maxRetriesOnSocketTimeouts, toe);
        } catch (@Tainted IOException ie) {
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionFailure(ioFailures++, ie);
        }
      }
    }

    /**
     * If multiple clients with the same principal try to connect to the same
     * server at the same time, the server assumes a replay attack is in
     * progress. This is a feature of kerberos. In order to work around this,
     * what is done is that the client backs off randomly and tries to initiate
     * the connection again. The other problem is to do with ticket expiry. To
     * handle that, a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        @Tainted Client.Connection this, final @Tainted int currRetries, final @Tainted int maxRetries, final @Tainted Exception ex,
        final @Tainted Random rand, final @Tainted UserGroupInformation ugi) throws IOException,
        InterruptedException {
      ugi.doAs(new @Tainted PrivilegedExceptionAction<@Tainted Object>() {
        @Override
        public @Tainted Object run() throws IOException, InterruptedException {
          final @Tainted short MAX_BACKOFF = 5000;
          closeConnection();
          disposeSasl();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("Exception encountered while connecting to "
                    + "the server : " + ex);
              }
              // try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              // have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
              return null;
            } else {
              @Tainted
              String msg = "Couldn't setup connection for "
                  + UserGroupInformation.getLoginUser().getUserName() + " to "
                  + remoteId;
              LOG.warn(msg);
              throw (@Tainted IOException) new @Tainted IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to "
                + "the server : " + ex);
          }
          if (ex instanceof @Tainted RemoteException)
            throw (@Tainted RemoteException) ex;
          throw new @Tainted IOException(ex);
        }
      });
    }

    
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     */
    private synchronized void setupIOstreams(@Tainted Client.Connection this) {
      if (socket != null || shouldCloseConnection.get()) {
        return;
      } 
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        @Tainted
        short numRetries = 0;
        final @Tainted short MAX_RETRIES = 5;
        @Tainted
        Random rand = null;
        while (true) {
          setupConnection();
          @Tainted
          InputStream inStream = NetUtils.getInputStream(socket);
          @Tainted
          OutputStream outStream = NetUtils.getOutputStream(socket);
          writeConnectionHeader(outStream);
          if (authProtocol == AuthProtocol.SASL) {
            final @Tainted InputStream in2 = inStream;
            final @Tainted OutputStream out2 = outStream;
            @Tainted
            UserGroupInformation ticket = remoteId.getTicket();
            if (ticket.getRealUser() != null) {
              ticket = ticket.getRealUser();
            }
            try {
              authMethod = ticket
                  .doAs(new @Tainted PrivilegedExceptionAction<@Tainted AuthMethod>() {
                    @Override
                    public @Tainted AuthMethod run()
                        throws IOException, InterruptedException {
                      return setupSaslConnection(in2, out2);
                    }
                  });
            } catch (@Tainted Exception ex) {
              authMethod = saslRpcClient.getAuthMethod();
              if (rand == null) {
                rand = new @Tainted Random();
              }
              handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand,
                  ticket);
              continue;
            }
            if (authMethod != AuthMethod.SIMPLE) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              inStream = saslRpcClient.getInputStream(inStream);
              outStream = saslRpcClient.getOutputStream(outStream);
              // for testing
              remoteId.saslQop =
                  (@Tainted String)saslRpcClient.getNegotiatedProperty(Sasl.QOP);
            } else if (UserGroupInformation.isSecurityEnabled() &&
                       !fallbackAllowed) {
              throw new @Tainted IOException("Server asks us to fall back to SIMPLE " +
                  "auth, but this client is configured to only allow secure " +
                  "connections.");
            }
          }
        
          if (doPing) {
            inStream = new @Tainted PingInputStream(inStream);
          }
          this.in = new @Tainted DataInputStream(new @Tainted BufferedInputStream(inStream));

          // SASL may have already buffered the stream
          if (!(outStream instanceof @Tainted BufferedOutputStream)) {
            outStream = new @Tainted BufferedOutputStream(outStream);
          }
          this.out = new @Tainted DataOutputStream(outStream);
          
          writeConnectionContext(remoteId, authMethod);

          // update last activity time
          touch();

          // start the receiver thread after the socket connection has been set
          // up
          start();
          return;
        }
      } catch (@Tainted Throwable t) {
        if (t instanceof @Tainted IOException) {
          markClosed((@Tainted IOException)t);
        } else {
          markClosed(new @Tainted IOException("Couldn't set up IO streams", t));
        }
        close();
      }
    }
    
    private void closeConnection(@Tainted Client.Connection this) {
      if (socket == null) {
        return;
      }
      // close the current connection
      try {
        socket.close();
      } catch (@Tainted IOException e) {
        LOG.warn("Not able to close a socket", e);
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures due to timeout on connect
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff 1 second and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionTimeout(
        @Tainted Client.Connection this, @Tainted
        int curRetries, @Tainted int maxRetries, @Tainted IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); maxRetries=" + maxRetries);
    }

    private void handleConnectionFailure(@Tainted Client.Connection this, @Tainted int curRetries, @Tainted IOException ioe
        ) throws IOException {
      closeConnection();

      final @Tainted RetryAction action;
      try {
        action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
      } catch(@Tainted Exception e) {
        throw e instanceof @Tainted IOException? (@Tainted IOException)e: new @Tainted IOException(e);
      }
      if (action.action == RetryAction.RetryDecision.FAIL) {
        if (action.reason != null) {
          LOG.warn("Failed to connect to server: " + server + ": "
              + action.reason, ioe);
        }
        throw ioe;
      }

      try {
        Thread.sleep(action.delayMillis);
      } catch (@Tainted InterruptedException e) {
        throw (@Tainted IOException)new @Tainted InterruptedIOException("Interrupted: action="
            + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
    }

    /**
     * Write the connection header - this is sent when connection is established
     * +----------------------------------+
     * |  "hrpc" 4 bytes                  |      
     * +----------------------------------+
     * |  Version (1 byte)                |
     * +----------------------------------+
     * |  Service Class (1 byte)          |
     * +----------------------------------+
     * |  AuthProtocol (1 byte)           |      
     * +----------------------------------+
     */
    private void writeConnectionHeader(@Tainted Client.Connection this, @Tainted OutputStream outStream)
        throws IOException {
      @Tainted
      DataOutputStream out = new @Tainted DataOutputStream(new @Tainted BufferedOutputStream(outStream));
      // Write out the header, version and authentication method
      out.write(RpcConstants.HEADER.array());
      out.write(RpcConstants.CURRENT_VERSION);
      out.write(serviceClass);
      out.write(authProtocol.callId);
      out.flush();
    }
    
    /* Write the connection context header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeConnectionContext(@Tainted Client.Connection this, @Tainted ConnectionId remoteId,
                                        @Tainted
                                        AuthMethod authMethod)
                                            throws IOException {
      // Write out the ConnectionHeader
      @Tainted
      IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
          RPC.getProtocolName(remoteId.getProtocol()),
          remoteId.getTicket(),
          authMethod);
      @Tainted
      RpcRequestHeaderProto connectionContextHeader = ProtoUtil
          .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
              OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
              RpcConstants.INVALID_RETRY_COUNT, clientId);
      @Tainted
      RpcRequestMessageWrapper request =
          new @Tainted RpcRequestMessageWrapper(connectionContextHeader, message);
      
      // Write out the packet length
      out.writeInt(request.getLength());
      request.write(out);
    }
    
    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed, 
     * or the client is marked as not running.
     * 
     * Return true if it is time to read a response; false otherwise.
     */
    private synchronized @Tainted boolean waitForWork(@Tainted Client.Connection this) {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        @Tainted
        long timeout = maxIdleTime-
              (Time.now()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (@Tainted InterruptedException e) {}
        }
      }
      
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests 
        markClosed((@Tainted IOException)new @Tainted IOException().initCause(
            new @Tainted InterruptedException()));
        return false;
      }
    }

    public @Tainted InetSocketAddress getRemoteAddress(@Tainted Client.Connection this) {
      return server;
    }

    /* Send a ping to the server if the time elapsed 
     * since last I/O activity is equal to or greater than the ping interval
     */
    private synchronized void sendPing(@Tainted Client.Connection this) throws IOException {
      @Tainted
      long curTime = Time.now();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (out) {
          out.writeInt(pingRequest.size());
          pingRequest.writeTo(out);
          out.flush();
        }
      }
    }

    @Override
    public void run(@Tainted Client.Connection this) {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections " 
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveRpcResponse();
        }
      } catch (@Tainted Throwable t) {
        // This truly is unexpected, since we catch IOException in receiveResponse
        // -- this is only to be really sure that we don't leave a client hanging
        // forever.
        LOG.warn("Unexpected error reading responses on connection " + this, t);
        markClosed(new @Tainted IOException("Error reading responses", t));
      }
      
      close();
      
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /** Initiates a rpc call by sending the rpc request to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     * @param call - the rpc request
     */
    public void sendRpcRequest(@Tainted Client.Connection this, final @Tainted Call call)
        throws InterruptedException, IOException {
      if (shouldCloseConnection.get()) {
        return;
      }

      // Serialize the call to be sent. This is done from the actual
      // caller thread, rather than the sendParamsExecutor thread,
      
      // so that if the serialization throws an error, it is reported
      // properly. This also parallelizes the serialization.
      //
      // Format of a call on the wire:
      // 0) Length of rest below (1 + 2)
      // 1) RpcRequestHeader  - is serialized Delimited hence contains length
      // 2) RpcRequest
      //
      // Items '1' and '2' are prepared here. 
      final @Tainted DataOutputBuffer d = new @Tainted DataOutputBuffer();
      @Tainted
      RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
          call.rpcKind, OperationProto.RPC_FINAL_PACKET, call.id, call.retry,
          clientId);
      header.writeDelimitedTo(d);
      call.rpcRequest.write(d);

      synchronized (sendRpcRequestLock) {
        @Tainted
        Future<@Tainted ? extends java.lang.@Tainted Object> senderFuture = sendParamsExecutor.submit(new @Tainted Runnable() {
          @Override
          public void run() {
            try {
              synchronized (Connection.this.out) {
                if (shouldCloseConnection.get()) {
                  return;
                }
                
                if (LOG.isDebugEnabled())
                  LOG.debug(getName() + " sending #" + call.id);
         
                @Tainted
                byte @Tainted [] data = d.getData();
                @Tainted
                int totalLength = d.getLength();
                out.writeInt(totalLength); // Total Length
                out.write(data, 0, totalLength);// RpcRequestHeader + RpcRequest
                out.flush();
              }
            } catch (@Tainted IOException e) {
              // exception at this point would leave the connection in an
              // unrecoverable state (eg half a call left on the wire).
              // So, close the connection, killing any outstanding calls
              markClosed(e);
            } finally {
              //the buffer is just an in-memory buffer, but it is still polite to
              // close early
              IOUtils.closeStream(d);
            }
          }
        });
      
        try {
          senderFuture.get();
        } catch (@Tainted ExecutionException e) {
          @Tainted
          Throwable cause = e.getCause();
          
          // cause should only be a RuntimeException as the Runnable above
          // catches IOException
          if (cause instanceof @Tainted RuntimeException) {
            throw (@Tainted RuntimeException) cause;
          } else {
            throw new @Tainted RuntimeException("unexpected checked exception", cause);
          }
        }
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveRpcResponse(@Tainted Client.Connection this) {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();
      
      try {
        @Tainted
        int totalLen = in.readInt();
        @Tainted
        RpcResponseHeaderProto header = 
            RpcResponseHeaderProto.parseDelimitedFrom(in);
        checkResponse(header);

        @Tainted
        int headerLen = header.getSerializedSize();
        headerLen += CodedOutputStream.computeRawVarint32Size(headerLen);

        @Tainted
        int callId = header.getCallId();
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + callId);

        @Tainted
        Call call = calls.get(callId);
        @Tainted
        RpcStatusProto status = header.getStatus();
        if (status == RpcStatusProto.SUCCESS) {
          @Tainted
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          calls.remove(callId);
          call.setRpcResponse(value);
          
          // verify that length was correct
          // only for ProtobufEngine where len can be verified easily
          if (call.getRpcResponse() instanceof ProtobufRpcEngine.@Tainted RpcWrapper) {
            ProtobufRpcEngine.@Tainted RpcWrapper resWrapper = 
                (ProtobufRpcEngine.@Tainted RpcWrapper) call.getRpcResponse();
            if (totalLen != headerLen + resWrapper.getLength()) { 
              throw new @Tainted RpcClientException(
                  "RPC response length mismatch on rpc success");
            }
          }
        } else { // Rpc Request failed
          // Verify that length was correct
          if (totalLen != headerLen) {
            throw new @Tainted RpcClientException(
                "RPC response length mismatch on rpc error");
          }
          
          final @Tainted String exceptionClassName = header.hasExceptionClassName() ?
                header.getExceptionClassName() : 
                  "ServerDidNotSetExceptionClassName";
          final @Tainted String errorMsg = header.hasErrorMsg() ? 
                header.getErrorMsg() : "ServerDidNotSetErrorMsg" ;
          final @Tainted RpcErrorCodeProto erCode = 
                    (header.hasErrorDetail() ? header.getErrorDetail() : null);
          if (erCode == null) {
             LOG.warn("Detailed error code not set by server on rpc error");
          }
          @Tainted
          RemoteException re = 
              ( (erCode == null) ? 
                  new @Tainted RemoteException(exceptionClassName, errorMsg) :
              new @Tainted RemoteException(exceptionClassName, errorMsg, erCode));
          if (status == RpcStatusProto.ERROR) {
            calls.remove(callId);
            call.setException(re);
          } else if (status == RpcStatusProto.FATAL) {
            // Close the connection
            markClosed(re);
          }
        }
      } catch (@Tainted IOException e) {
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(@Tainted Client.Connection this, @Tainted IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }
    
    /** Close the connection. */
    private synchronized void close(@Tainted Client.Connection this) {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new @Tainted IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls(@Tainted Client.Connection this) {
      @Tainted
      Iterator<@Tainted Entry<@Tainted Integer, @Tainted Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        @Tainted
        Call c = itor.next().getValue(); 
        itor.remove();
        c.setException(closeException); // local exception
      }
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public @Tainted Client(@Tainted Class<@Tainted ? extends @Tainted Writable> valueClass, @Tainted Configuration conf, 
      @Tainted
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
    this.fallbackAllowed = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.clientId = ClientId.getClientId();
    this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public @Tainted Client(@Tainted Class<@Tainted ? extends @Tainted Writable> valueClass, @Tainted Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
 
  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  @Tainted
  SocketFactory getSocketFactory(@Tainted Client this) {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop(@Tainted Client this) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    synchronized (connections) {
      for (@Tainted Connection conn : connections.values()) {
        conn.interrupt();
      }
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (@Tainted InterruptedException e) {
      }
    }
    
    clientExcecutorFactory.unrefAndCleanup();
  }

  /**
   * Same as {@link #call(RPC.RpcKind, Writable, ConnectionId)}
   *  for RPC_BUILTIN
   */
  public @Tainted Writable call(@Tainted Client this, @Tainted Writable param, @Tainted InetSocketAddress address)
      throws IOException {
    return call(RPC.RpcKind.RPC_BUILTIN, param, address);
    
  }
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * @deprecated Use {@link #call(RPC.RpcKind, Writable,
   *  ConnectionId)} instead 
   */
  @Deprecated
  public @Tainted Writable call(@Tainted Client this, RPC.@Tainted RpcKind rpcKind, @Tainted Writable param, @Tainted InetSocketAddress address)
  throws IOException {
      return call(rpcKind, param, address, null);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> with the <code>ticket</code> credentials, returning 
   * the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   * @deprecated Use {@link #call(RPC.RpcKind, Writable, 
   * ConnectionId)} instead 
   */
  @Deprecated
  public @Tainted Writable call(@Tainted Client this, RPC.@Tainted RpcKind rpcKind, @Tainted Writable param, @Tainted InetSocketAddress addr, 
      @Tainted
      UserGroupInformation ticket) throws IOException {
    @Tainted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, null, ticket, 0,
        conf);
    return call(rpcKind, param, remoteId);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol, 
   * with the <code>ticket</code> credentials and <code>rpcTimeout</code> as 
   * timeout, returning the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception. 
   * @deprecated Use {@link #call(RPC.RpcKind, Writable,
   *  ConnectionId)} instead 
   */
  @Deprecated
  public @Tainted Writable call(@Tainted Client this, RPC.@Tainted RpcKind rpcKind, @Tainted Writable param, @Tainted InetSocketAddress addr, 
                       @Tainted
                       Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted UserGroupInformation ticket,
                       @Tainted
                       int rpcTimeout) throws IOException {
    @Tainted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(rpcKind, param, remoteId);
  }

  
  /**
   * Same as {@link #call(RPC.RpcKind, Writable, InetSocketAddress,
   * Class, UserGroupInformation, int, Configuration)}
   * except that rpcKind is writable.
   */
  public @Tainted Writable call(@Tainted Client this, @Tainted Writable param, @Tainted InetSocketAddress addr,
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted UserGroupInformation ticket,
      @Tainted
      int rpcTimeout, @Tainted Configuration conf) throws IOException {
    @Tainted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId);
  }
  
  /**
   * Same as {@link #call(Writable, InetSocketAddress,
   * Class, UserGroupInformation, int, Configuration)}
   * except that specifying serviceClass.
   */
  public @Tainted Writable call(@Tainted Client this, @Tainted Writable param, @Tainted InetSocketAddress addr,
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted UserGroupInformation ticket,
      @Tainted
      int rpcTimeout, @Tainted int serviceClass, @Tainted Configuration conf)
      throws IOException {
    @Tainted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId, serviceClass);
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, <code>rpcTimeout</code> as
   * timeout and <code>conf</code> as conf for this connection, returning the
   * value. Throws exceptions if there are network problems or if the remote
   * code threw an exception.
   */
  public @Tainted Writable call(@Tainted Client this, RPC.@Tainted RpcKind rpcKind, @Tainted Writable param, @Tainted InetSocketAddress addr, 
                       @Tainted
                       Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted UserGroupInformation ticket,
                       @Tainted
                       int rpcTimeout, @Tainted Configuration conf) throws IOException {
    @Tainted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(rpcKind, param, remoteId);
  }
  
  /**
   * Same as {link {@link #call(RPC.RpcKind, Writable, ConnectionId)}
   * except the rpcKind is RPC_BUILTIN
   */
  public @Tainted Writable call(@Tainted Client this, @Tainted Writable param, @Tainted ConnectionId remoteId)
      throws IOException {
     return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId);
  }
  
  /**
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   */
  public @Tainted Writable call(@Tainted Client this, RPC.@Tainted RpcKind rpcKind, @Tainted Writable rpcRequest,
      @Tainted
      ConnectionId remoteId) throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT);
  }

  /** 
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   * 
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param serviceClass - service class for RPC
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   */
  public @Tainted Writable call(@Tainted Client this, RPC.@Tainted RpcKind rpcKind, @Tainted Writable rpcRequest,
      @Tainted
      ConnectionId remoteId, @Tainted int serviceClass) throws IOException {
    final @Tainted Call call = createCall(rpcKind, rpcRequest);
    @Tainted
    Connection connection = getConnection(remoteId, call, serviceClass);
    try {
      connection.sendRpcRequest(call);                 // send the rpc request
    } catch (@Tainted RejectedExecutionException e) {
      throw new @Tainted IOException("connection has been closed", e);
    } catch (@Tainted InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("interrupted waiting to send rpc request to server", e);
      throw new @Tainted IOException(e);
    }

    @Tainted
    boolean interrupted = false;
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (@Tainted InterruptedException ie) {
          // save the fact that we were interrupted
          interrupted = true;
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();
      }

      if (call.error != null) {
        if (call.error instanceof @Tainted RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          @Tainted
          InetSocketAddress address = connection.getRemoteAddress();
          throw NetUtils.wrapException(address.getHostName(),
                  address.getPort(),
                  NetUtils.getHostname(),
                  0,
                  call.error);
        }
      } else {
        return call.getRpcResponse();
      }
    }
  }

  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @Tainted
  Set<@Tainted ConnectionId> getConnectionIds(@Tainted Client this) {
    synchronized (connections) {
      return connections.keySet();
    }
  }
  
  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused. */
  private @Tainted Connection getConnection(@Tainted Client this, @Tainted ConnectionId remoteId,
      @Tainted
      Call call, @Tainted int serviceClass) throws IOException {
    if (!running.get()) {
      // the client is stopped
      throw new @Tainted IOException("The client is stopped");
    }
    @Tainted
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new @Tainted Connection(remoteId, serviceClass);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));
    
    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  }
  
  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, protocol, ticket>
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class ConnectionId {
    @Tainted
    InetSocketAddress address;
    @Tainted
    UserGroupInformation ticket;
    final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol;
    private static final @Tainted int PRIME = 16777619;
    private final @Tainted int rpcTimeout;
    private final @Tainted int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final @Tainted RetryPolicy connectionRetryPolicy;
    // the max. no. of retries for socket connections on time out exceptions
    private final @Tainted int maxRetriesOnSocketTimeouts;
    private final @Tainted boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final @Tainted boolean doPing; //do we need to send ping message
    private final @Tainted int pingInterval; // how often sends ping to the server in msecs
    private @Tainted String saslQop; // here for testing
    
    @Tainted
    ConnectionId(@Tainted InetSocketAddress address, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol, 
                 @Tainted
                 UserGroupInformation ticket, @Tainted int rpcTimeout, @Tainted int maxIdleTime, 
                 @Tainted
                 RetryPolicy connectionRetryPolicy, @Tainted int maxRetriesOnSocketTimeouts,
                 @Tainted
                 boolean tcpNoDelay, @Tainted boolean doPing, @Tainted int pingInterval) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
      this.maxIdleTime = maxIdleTime;
      this.connectionRetryPolicy = connectionRetryPolicy;
      this.maxRetriesOnSocketTimeouts = maxRetriesOnSocketTimeouts;
      this.tcpNoDelay = tcpNoDelay;
      this.doPing = doPing;
      this.pingInterval = pingInterval;
    }
    
    @Tainted
    InetSocketAddress getAddress(Client.@Tainted ConnectionId this) {
      return address;
    }
    
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> getProtocol(Client.@Tainted ConnectionId this) {
      return protocol;
    }
    
    @Tainted
    UserGroupInformation getTicket(Client.@Tainted ConnectionId this) {
      return ticket;
    }
    
    private @Tainted int getRpcTimeout(Client.@Tainted ConnectionId this) {
      return rpcTimeout;
    }
    
    @Tainted
    int getMaxIdleTime(Client.@Tainted ConnectionId this) {
      return maxIdleTime;
    }
    
    /** max connection retries on socket time outs */
    public @Tainted int getMaxRetriesOnSocketTimeouts(Client.@Tainted ConnectionId this) {
      return maxRetriesOnSocketTimeouts;
    }
    
    @Tainted
    boolean getTcpNoDelay(Client.@Tainted ConnectionId this) {
      return tcpNoDelay;
    }
    
    @Tainted
    boolean getDoPing(Client.@Tainted ConnectionId this) {
      return doPing;
    }
    
    @Tainted
    int getPingInterval(Client.@Tainted ConnectionId this) {
      return pingInterval;
    }
    
    @VisibleForTesting
    @Tainted
    String getSaslQop(Client.@Tainted ConnectionId this) {
      return saslQop;
    }
    
    static @Tainted ConnectionId getConnectionId(@Tainted InetSocketAddress addr,
        @Tainted
        Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted UserGroupInformation ticket, @Tainted int rpcTimeout,
        @Tainted
        Configuration conf) throws IOException {
      return getConnectionId(addr, protocol, ticket, rpcTimeout, null, conf);
    }

    /**
     * Returns a ConnectionId object. 
     * @param addr Remote address for the connection.
     * @param protocol Protocol for RPC.
     * @param ticket UGI
     * @param rpcTimeout timeout
     * @param conf Configuration object
     * @return A ConnectionId instance
     * @throws IOException
     */
    static @Tainted ConnectionId getConnectionId(@Tainted InetSocketAddress addr,
        @Tainted
        Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted UserGroupInformation ticket, @Tainted int rpcTimeout,
        @Tainted
        RetryPolicy connectionRetryPolicy, @Tainted Configuration conf) throws IOException {

      if (connectionRetryPolicy == null) {
        final @Tainted int max = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
        connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            max, 1, TimeUnit.SECONDS);
      }

      @Tainted
      boolean doPing =
        conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
      return new @Tainted ConnectionId(addr, protocol, ticket, rpcTimeout,
          conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
              CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT),
          connectionRetryPolicy,
          conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT),
          conf.getBoolean(CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
              CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT),
          doPing, 
          (doPing ? Client.getPingInterval(conf) : 0));
    }
    
    static @Tainted boolean isEqual(@Tainted Object a, @Tainted Object b) {
      return a == null ? b == null : a.equals(b);
    }

    @Override
    public @Tainted boolean equals(Client.@Tainted ConnectionId this, @Tainted Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof @Tainted ConnectionId) {
        @Tainted
        ConnectionId that = (@Tainted ConnectionId) obj;
        return isEqual(this.address, that.address)
            && this.doPing == that.doPing
            && this.maxIdleTime == that.maxIdleTime
            && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
            && this.pingInterval == that.pingInterval
            && isEqual(this.protocol, that.protocol)
            && this.rpcTimeout == that.rpcTimeout
            && this.tcpNoDelay == that.tcpNoDelay
            && isEqual(this.ticket, that.ticket);
      }
      return false;
    }
    
    @Override
    public @Tainted int hashCode(Client.@Tainted ConnectionId this) {
      @Tainted
      int result = connectionRetryPolicy.hashCode();
      result = PRIME * result + ((address == null) ? 0 : address.hashCode());
      result = PRIME * result + (doPing ? 1231 : 1237);
      result = PRIME * result + maxIdleTime;
      result = PRIME * result + pingInterval;
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + rpcTimeout;
      result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
      result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
      return result;
    }
    
    @Override
    public @Tainted String toString(Client.@Tainted ConnectionId this) {
      return address.toString();
    }
  }  

  /**
   * Returns the next valid sequential call ID by incrementing an atomic counter
   * and masking off the sign bit.  Valid call IDs are non-negative integers in
   * the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
   * purposes.  The values can overflow back to 0 and be reused.  Note that prior
   * versions of the client did not mask off the sign bit, so a server may still
   * see a negative call ID if it receives connections from an old client.
   * 
   * @return next call ID
   */
  public static @Tainted int nextCallId() {
    return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
  }
}
