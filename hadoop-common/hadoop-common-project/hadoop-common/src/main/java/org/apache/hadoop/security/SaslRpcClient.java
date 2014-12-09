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

package org.apache.hadoop.security;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.util.ProtoUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
/**
 * A utility class that encapsulates SASL logic for RPC client
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcClient {
  public static final @Tainted Log LOG = LogFactory.getLog(SaslRpcClient.class);

  private final @Tainted UserGroupInformation ugi;
  private final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol;
  private final @Tainted InetSocketAddress serverAddr;  
  private final @Tainted Configuration conf;

  private @Tainted SaslClient saslClient;
  private @Tainted AuthMethod authMethod;
  
  private static final @Tainted RpcRequestHeaderProto saslHeader = ProtoUtil
      .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
          OperationProto.RPC_FINAL_PACKET, AuthProtocol.SASL.callId,
          RpcConstants.INVALID_RETRY_COUNT, RpcConstants.DUMMY_CLIENT_ID);
  private static final @Tainted RpcSaslProto negotiateRequest =
      RpcSaslProto.newBuilder().setState(SaslState.NEGOTIATE).build();
  
  /**
   * Create a SaslRpcClient that can be used by a RPC client to negotiate
   * SASL authentication with a RPC server
   * @param ugi - connecting user
   * @param protocol - RPC protocol
   * @param serverAddr - InetSocketAddress of remote server
   * @param conf - Configuration
   */
  public @Tainted SaslRpcClient(@Tainted UserGroupInformation ugi, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol,
      @Tainted
      InetSocketAddress serverAddr, @Tainted Configuration conf) {
    this.ugi = ugi;
    this.protocol = protocol;
    this.serverAddr = serverAddr;
    this.conf = conf;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  public @Tainted Object getNegotiatedProperty(@Tainted SaslRpcClient this, @Tainted String key) {
    return (saslClient != null) ? saslClient.getNegotiatedProperty(key) : null;
  }
  

  // the RPC Client has an inelegant way of handling expiration of TGTs
  // acquired via a keytab.  any connection failure causes a relogin, so
  // the Client needs to know what authMethod was being attempted if an
  // exception occurs.  the SASL prep for a kerberos connection should
  // ideally relogin if necessary instead of exposing this detail to the
  // Client
  @InterfaceAudience.Private
  public @Tainted AuthMethod getAuthMethod(@Tainted SaslRpcClient this) {
    return authMethod;
  }
  
  /**
   * Instantiate a sasl client for the first supported auth type in the
   * given list.  The auth type must be defined, enabled, and the user
   * must possess the required credentials, else the next auth is tried.
   * 
   * @param authTypes to attempt in the given order
   * @return SaslAuth of instantiated client
   * @throws AccessControlException - client doesn't support any of the auths
   * @throws IOException - misc errors
   */
  private @Tainted SaslAuth selectSaslClient(@Tainted SaslRpcClient this, @Tainted List<@Tainted SaslAuth> authTypes)
      throws SaslException, AccessControlException, IOException {
    @Tainted
    SaslAuth selectedAuthType = null;
    @Tainted
    boolean switchToSimple = false;
    for (@Tainted SaslAuth authType : authTypes) {
      if (!isValidAuthType(authType)) {
        continue; // don't know what it is, try next
      }
      @Tainted
      AuthMethod authMethod = AuthMethod.valueOf(authType.getMethod());
      if (authMethod == AuthMethod.SIMPLE) {
        switchToSimple = true;
      } else {
        saslClient = createSaslClient(authType);
        if (saslClient == null) { // client lacks credentials, try next
          continue;
        }
      }
      selectedAuthType = authType;
      break;
    }
    if (saslClient == null && !switchToSimple) {
      @Tainted
      List<@Tainted String> serverAuthMethods = new @Tainted ArrayList<@Tainted String>();
      for (@Tainted SaslAuth authType : authTypes) {
        serverAuthMethods.add(authType.getMethod());
      }
      throw new @Tainted AccessControlException(
          "Client cannot authenticate via:" + serverAuthMethods);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Use " + selectedAuthType.getMethod() +
          " authentication for protocol " + protocol.getSimpleName());
    }
    return selectedAuthType;
  }
  

  private @Tainted boolean isValidAuthType(@Tainted SaslRpcClient this, @Tainted SaslAuth authType) {
    @Tainted
    AuthMethod authMethod;
    try {
      authMethod = AuthMethod.valueOf(authType.getMethod());
    } catch (@Tainted IllegalArgumentException iae) { // unknown auth
      authMethod = null;
    }
    // do we know what it is?  is it using our mechanism?
    return authMethod != null &&
           authMethod.getMechanismName().equals(authType.getMechanism());
  }  
  
  /**
   * Try to create a SaslClient for an authentication type.  May return
   * null if the type isn't supported or the client lacks the required
   * credentials.
   * 
   * @param authType - the requested authentication method
   * @return SaslClient for the authType or null
   * @throws SaslException - error instantiating client
   * @throws IOException - misc errors
   */
  private @Tainted SaslClient createSaslClient(@Tainted SaslRpcClient this, @Tainted SaslAuth authType)
      throws SaslException, IOException {
    @Tainted
    String saslUser = null;
    // SASL requires the client and server to use the same proto and serverId
    // if necessary, auth types below will verify they are valid
    final @Tainted String saslProtocol = authType.getProtocol();
    final @Tainted String saslServerName = authType.getServerId();
    @Tainted
    Map<@Tainted String, @Tainted String> saslProperties = SaslRpcServer.SASL_PROPS;
    @Tainted
    CallbackHandler saslCallback = null;
    
    final @Tainted AuthMethod method = AuthMethod.valueOf(authType.getMethod());
    switch (method) {
      case TOKEN: {
        @Tainted
        Token<@Tainted ? extends java.lang.@Tainted Object> token = getServerToken(authType);
        if (token == null) {
          return null; // tokens aren't supported or user doesn't have one
        }
        saslCallback = new @Tainted SaslClientCallbackHandler(token);
        break;
      }
      case KERBEROS: {
        if (ugi.getRealAuthenticationMethod().getAuthMethod() !=
            AuthMethod.KERBEROS) {
          return null; // client isn't using kerberos
        }
        @Tainted
        String serverPrincipal = getServerPrincipal(authType);
        if (serverPrincipal == null) {
          return null; // protocol doesn't use kerberos
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("RPC Server's Kerberos principal name for protocol="
              + protocol.getCanonicalName() + " is " + serverPrincipal);
        }
        break;
      }
      default:
        throw new @Tainted IOException("Unknown authentication method " + method);
    }
    
    @Tainted
    String mechanism = method.getMechanismName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating SASL " + mechanism + "(" + method + ") "
          + " client to authenticate to service at " + saslServerName);
    }
    return Sasl.createSaslClient(
        new @Tainted String @Tainted [] { mechanism }, saslUser, saslProtocol, saslServerName,
        saslProperties, saslCallback);
  }
  
  /**
   * Try to locate the required token for the server.
   * 
   * @param authType of the SASL client
   * @return Token<?> for server, or null if no token available
   * @throws IOException - token selector cannot be instantiated
   */
  private @Tainted Token<@Tainted ? extends java.lang.@Tainted Object> getServerToken(@Tainted SaslRpcClient this, @Tainted SaslAuth authType) throws IOException {
    @Tainted
    TokenInfo tokenInfo = SecurityUtil.getTokenInfo(protocol, conf);
    LOG.debug("Get token info proto:"+protocol+" info:"+tokenInfo);
    if (tokenInfo == null) { // protocol has no support for tokens
      return null;
    }
    @Tainted
    TokenSelector<@Tainted ? extends java.lang.@Tainted Object> tokenSelector = null;
    try {
      tokenSelector = tokenInfo.value().newInstance();
    } catch (@Tainted InstantiationException e) {
      throw new @Tainted IOException(e.toString());
    } catch (@Tainted IllegalAccessException e) {
      throw new @Tainted IOException(e.toString());
    }
    return tokenSelector.selectToken(
        SecurityUtil.buildTokenService(serverAddr), ugi.getTokens());
  }
  
  /**
   * Get the remote server's principal.  The value will be obtained from
   * the config and cross-checked against the server's advertised principal.
   * 
   * @param authType of the SASL client
   * @return String of the server's principal
   * @throws IOException - error determining configured principal
   */
  @VisibleForTesting
  @Tainted
  String getServerPrincipal(@Tainted SaslRpcClient this, @Tainted SaslAuth authType) throws IOException {
    @Tainted
    KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
    LOG.debug("Get kerberos info proto:"+protocol+" info:"+krbInfo);
    if (krbInfo == null) { // protocol has no support for kerberos
      return null;
    }
    @Tainted
    String serverKey = krbInfo.serverPrincipal();
    if (serverKey == null) {
      throw new @Tainted IllegalArgumentException(
          "Can't obtain server Kerberos config key from protocol="
              + protocol.getCanonicalName());
    }
    // construct server advertised principal for comparision
    @Tainted
    String serverPrincipal = new @Tainted KerberosPrincipal(
        authType.getProtocol() + "/" + authType.getServerId()).getName();
    @Tainted
    boolean isPrincipalValid = false;

    // use the pattern if defined
    @Tainted
    String serverKeyPattern = conf.get(serverKey + ".pattern");
    if (serverKeyPattern != null && !serverKeyPattern.isEmpty()) {
      @Tainted
      Pattern pattern = GlobPattern.compile(serverKeyPattern);
      isPrincipalValid = pattern.matcher(serverPrincipal).matches();
    } else {
      // check that the server advertised principal matches our conf
      @Tainted
      String confPrincipal = SecurityUtil.getServerPrincipal(
          conf.get(serverKey), serverAddr.getAddress());
      if (confPrincipal == null || confPrincipal.isEmpty()) {
        throw new @Tainted IllegalArgumentException(
            "Failed to specify server's Kerberos principal name");
      }
      @Tainted
      KerberosName name = new @Tainted KerberosName(confPrincipal);
      if (name.getHostName() == null) {
        throw new @Tainted IllegalArgumentException(
            "Kerberos principal name does NOT have the expected hostname part: "
                + confPrincipal);
      }
      isPrincipalValid = serverPrincipal.equals(confPrincipal);
    }
    if (!isPrincipalValid) {
      throw new @Tainted IllegalArgumentException(
          "Server has invalid Kerberos principal: " + serverPrincipal);
    }
    return serverPrincipal;
  }
  

  /**
   * Do client side SASL authentication with server via the given InputStream
   * and OutputStream
   * 
   * @param inS
   *          InputStream to use
   * @param outS
   *          OutputStream to use
   * @return AuthMethod used to negotiate the connection
   * @throws IOException
   */
  public @Tainted AuthMethod saslConnect(@Tainted SaslRpcClient this, @Tainted InputStream inS, @Tainted OutputStream outS)
      throws IOException {
    @Tainted
    DataInputStream inStream = new @Tainted DataInputStream(new @Tainted BufferedInputStream(inS));
    @Tainted
    DataOutputStream outStream = new @Tainted DataOutputStream(new @Tainted BufferedOutputStream(
        outS));
    
    // redefined if/when a SASL negotiation starts, can be queried if the
    // negotiation fails
    authMethod = AuthMethod.SIMPLE;
    
    sendSaslMessage(outStream, negotiateRequest);
    
    // loop until sasl is complete or a rpc error occurs
    @Tainted
    boolean done = false;
    do {
      @Tainted
      int totalLen = inStream.readInt();
      @Tainted
      RpcResponseMessageWrapper responseWrapper =
          new @Tainted RpcResponseMessageWrapper();
      responseWrapper.readFields(inStream);
      @Tainted
      RpcResponseHeaderProto header = responseWrapper.getMessageHeader();
      switch (header.getStatus()) {
        case ERROR: // might get a RPC error during 
        case FATAL:
          throw new @Tainted RemoteException(header.getExceptionClassName(),
                                    header.getErrorMsg());
        default: break;
      }
      if (totalLen != responseWrapper.getLength()) {
        throw new @Tainted SaslException("Received malformed response length");
      }
      
      if (header.getCallId() != AuthProtocol.SASL.callId) {
        throw new @Tainted SaslException("Non-SASL response during negotiation");
      }
      @Tainted
      RpcSaslProto saslMessage =
          RpcSaslProto.parseFrom(responseWrapper.getMessageBytes());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received SASL message "+saslMessage);
      }
      // handle sasl negotiation process
      RpcSaslProto.@Tainted Builder response = null;
      switch (saslMessage.getState()) {
        case NEGOTIATE: {
          // create a compatible SASL client, throws if no supported auths
          @Tainted
          SaslAuth saslAuthType = selectSaslClient(saslMessage.getAuthsList());
          // define auth being attempted, caller can query if connect fails
          authMethod = AuthMethod.valueOf(saslAuthType.getMethod());
          
          @Tainted
          byte @Tainted [] responseToken = null;
          if (authMethod == AuthMethod.SIMPLE) { // switching to SIMPLE
            done = true; // not going to wait for success ack
          } else {
            @Tainted
            byte @Tainted [] challengeToken = null;
            if (saslAuthType.hasChallenge()) {
              // server provided the first challenge
              challengeToken = saslAuthType.getChallenge().toByteArray();
              saslAuthType =
                  SaslAuth.newBuilder(saslAuthType).clearChallenge().build();
            } else if (saslClient.hasInitialResponse()) {
              challengeToken = new @Tainted byte @Tainted [0];
            }
            responseToken = (challengeToken != null)
                ? saslClient.evaluateChallenge(challengeToken)
                    : new @Tainted byte @Tainted [0];
          }
          response = createSaslReply(SaslState.INITIATE, responseToken);
          response.addAuths(saslAuthType);
          break;
        }
        case CHALLENGE: {
          if (saslClient == null) {
            // should probably instantiate a client to allow a server to
            // demand a specific negotiation
            throw new @Tainted SaslException("Server sent unsolicited challenge");
          }
          @Tainted
          byte @Tainted [] responseToken = saslEvaluateToken(saslMessage, false);
          response = createSaslReply(SaslState.RESPONSE, responseToken);
          break;
        }
        case SUCCESS: {
          // simple server sends immediate success to a SASL client for
          // switch to simple
          if (saslClient == null) {
            authMethod = AuthMethod.SIMPLE;
          } else {
            saslEvaluateToken(saslMessage, true);
          }
          done = true;
          break;
        }
        default: {
          throw new @Tainted SaslException(
              "RPC client doesn't support SASL " + saslMessage.getState());
        }
      }
      if (response != null) {
        sendSaslMessage(outStream, response.build());
      }
    } while (!done);
    return authMethod;
  }
  
  private void sendSaslMessage(@Tainted SaslRpcClient this, @Tainted DataOutputStream out, @Tainted RpcSaslProto message)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending sasl message "+message);
    }
    @Tainted
    RpcRequestMessageWrapper request =
        new @Tainted RpcRequestMessageWrapper(saslHeader, message);
    out.writeInt(request.getLength());
    request.write(out);
    out.flush();    
  }
  
  /**
   * Evaluate the server provided challenge.  The server must send a token
   * if it's not done.  If the server is done, the challenge token is
   * optional because not all mechanisms send a final token for the client to
   * update its internal state.  The client must also be done after
   * evaluating the optional token to ensure a malicious server doesn't
   * prematurely end the negotiation with a phony success.
   *  
   * @param saslResponse - client response to challenge
   * @param serverIsDone - server negotiation state
   * @throws SaslException - any problems with negotiation
   */
  private @Tainted byte @Tainted [] saslEvaluateToken(@Tainted SaslRpcClient this, @Tainted RpcSaslProto saslResponse,
      @Tainted
      boolean serverIsDone) throws SaslException {
    @Tainted
    byte @Tainted [] saslToken = null;
    if (saslResponse.hasToken()) {
      saslToken = saslResponse.getToken().toByteArray();
      saslToken = saslClient.evaluateChallenge(saslToken);
    } else if (!serverIsDone) {
      // the server may only omit a token when it's done
      throw new @Tainted SaslException("Server challenge contains no token");
    }
    if (serverIsDone) {
      // server tried to report success before our client completed
      if (!saslClient.isComplete()) {
        throw new @Tainted SaslException("Client is out of sync with server");
      }
      // a client cannot generate a response to a success message
      if (saslToken != null) {
        throw new @Tainted SaslException("Client generated spurious response");        
      }
    }
    return saslToken;
  }
  
  private RpcSaslProto.@Tainted Builder createSaslReply(@Tainted SaslRpcClient this, @Tainted SaslState state,
                                               @Tainted
                                               byte @Tainted [] responseToken) {
    RpcSaslProto.@Tainted Builder response = RpcSaslProto.newBuilder();
    response.setState(state);
    if (responseToken != null) {
      response.setToken(ByteString.copyFrom(responseToken));
    }
    return response;
  }

  private @Tainted boolean useWrap(@Tainted SaslRpcClient this) {
    // getNegotiatedProperty throws if client isn't complete
    @Tainted
    String qop = (@Tainted String) saslClient.getNegotiatedProperty(Sasl.QOP);
    // SASL wrapping is only used if the connection has a QOP, and
    // the value is not auth.  ex. auth-int & auth-priv
    return qop != null && !"auth".equalsIgnoreCase(qop);
  }
  
  /**
   * Get SASL wrapped InputStream if SASL QoP requires unwrapping,
   * otherwise return original stream.  Can be called only after
   * saslConnect() has been called.
   * 
   * @param in - InputStream used to make the connection
   * @return InputStream that may be using SASL unwrap
   * @throws IOException
   */
  public @Tainted InputStream getInputStream(@Tainted SaslRpcClient this, @Tainted InputStream in) throws IOException {
    if (useWrap()) {
      in = new @Tainted WrappedInputStream(in);
    }
    return in;
  }

  /**
   * Get SASL wrapped OutputStream if SASL QoP requires wrapping,
   * otherwise return original stream.  Can be called only after
   * saslConnect() has been called.
   * 
   * @param in - InputStream used to make the connection
   * @return InputStream that may be using SASL unwrap
   * @throws IOException
   */
  public @Tainted OutputStream getOutputStream(@Tainted SaslRpcClient this, @Tainted OutputStream out) throws IOException {
    if (useWrap()) {
      // the client and server negotiate a maximum buffer size that can be
      // wrapped
      @Tainted
      String maxBuf = (@Tainted String)saslClient.getNegotiatedProperty(Sasl.RAW_SEND_SIZE);
      out = new @Tainted BufferedOutputStream(new @Tainted WrappedOutputStream(out),
                                     Integer.parseInt(maxBuf));
    }
    return out;
  }

  // ideally this should be folded into the RPC decoding loop but it's
  // currently split across Client and SaslRpcClient...
  class WrappedInputStream extends @Tainted FilterInputStream {
    private @Tainted ByteBuffer unwrappedRpcBuffer = ByteBuffer.allocate(0);
    public @Tainted WrappedInputStream(@Tainted InputStream in) throws IOException {
      super(in);
    }
    
    @Override
    public @Tainted int read(@Tainted SaslRpcClient.WrappedInputStream this) throws IOException {
      @Tainted
      byte @Tainted [] b = new @Tainted byte @Tainted [1];
      @Tainted
      int n = read(b, 0, 1);
      return (n != -1) ? b[0] : -1;
    }
    
    @Override
    public @Tainted int read(@Tainted SaslRpcClient.WrappedInputStream this, @Tainted byte b @Tainted []) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public @Tainted int read(@Tainted SaslRpcClient.WrappedInputStream this, @Tainted byte @Tainted [] buf, @Tainted int off, @Tainted int len) throws IOException {
      synchronized(unwrappedRpcBuffer) {
        // fill the buffer with the next RPC message
        if (unwrappedRpcBuffer.remaining() == 0) {
          readNextRpcPacket();
        }
        // satisfy as much of the request as possible
        @Tainted
        int readLen = Math.min(len, unwrappedRpcBuffer.remaining());
        unwrappedRpcBuffer.get(buf, off, readLen);
        return readLen;
      }
    }
    
    // all messages must be RPC SASL wrapped, else an exception is thrown
    private void readNextRpcPacket(@Tainted SaslRpcClient.WrappedInputStream this) throws IOException {
      LOG.debug("reading next wrapped RPC packet");
      @Tainted
      DataInputStream dis = new @Tainted DataInputStream(in);
      @Tainted
      int rpcLen = dis.readInt();
      @Tainted
      byte @Tainted [] rpcBuf = new @Tainted byte @Tainted [rpcLen];
      dis.readFully(rpcBuf);
      
      // decode the RPC header
      @Tainted
      ByteArrayInputStream bis = new @Tainted ByteArrayInputStream(rpcBuf);
      RpcResponseHeaderProto.@Tainted Builder headerBuilder =
          RpcResponseHeaderProto.newBuilder();
      headerBuilder.mergeDelimitedFrom(bis);
      
      @Tainted
      boolean isWrapped = false;
      // Must be SASL wrapped, verify and decode.
      if (headerBuilder.getCallId() == AuthProtocol.SASL.callId) {
        RpcSaslProto.@Tainted Builder saslMessage = RpcSaslProto.newBuilder();
        saslMessage.mergeDelimitedFrom(bis);
        if (saslMessage.getState() == SaslState.WRAP) {
          isWrapped = true;
          @Tainted
          byte @Tainted [] token = saslMessage.getToken().toByteArray();
          if (LOG.isDebugEnabled()) {
            LOG.debug("unwrapping token of length:" + token.length);
          }
          token = saslClient.unwrap(token, 0, token.length);
          unwrappedRpcBuffer = ByteBuffer.wrap(token);
        }
      }
      if (!isWrapped) {
        throw new @Tainted SaslException("Server sent non-wrapped response");
      }
    }
  }

  class WrappedOutputStream extends @Tainted FilterOutputStream {
    public @Tainted WrappedOutputStream(@Tainted OutputStream out) throws IOException {
      super(out);
    }
    @Override
    public void write(@Tainted SaslRpcClient.WrappedOutputStream this, @Tainted byte @Tainted [] buf, @Tainted int off, @Tainted int len) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("wrapping token of length:" + len);
      }
      buf = saslClient.wrap(buf, off, len);
      @Tainted
      RpcSaslProto saslMessage = RpcSaslProto.newBuilder()
          .setState(SaslState.WRAP)
          .setToken(ByteString.copyFrom(buf, 0, buf.length))
          .build();
      @Tainted
      RpcRequestMessageWrapper request =
          new @Tainted RpcRequestMessageWrapper(saslHeader, saslMessage);
      @Tainted
      DataOutputStream dob = new @Tainted DataOutputStream(out);
      dob.writeInt(request.getLength());
      request.write(dob);
     }
  }
  
  /** Release resources used by wrapped saslClient */
  public void dispose(@Tainted SaslRpcClient this) throws SaslException {
    if (saslClient != null) {
      saslClient.dispose();
      saslClient = null;
    }
  }

  private static class SaslClientCallbackHandler implements @Tainted CallbackHandler {
    private final @Tainted String userName;
    private final @Tainted char @Tainted [] userPassword;

    public @Tainted SaslClientCallbackHandler(@Tainted Token<@Tainted ? extends @Tainted TokenIdentifier> token) {
      this.userName = SaslRpcServer.encodeIdentifier(token.getIdentifier());
      this.userPassword = SaslRpcServer.encodePassword(token.getPassword());
    }

    @Override
    public void handle(SaslRpcClient.@Tainted SaslClientCallbackHandler this, @Tainted Callback @Tainted [] callbacks)
        throws UnsupportedCallbackException {
      @Tainted
      NameCallback nc = null;
      @Tainted
      PasswordCallback pc = null;
      @Tainted
      RealmCallback rc = null;
      for (@Tainted Callback callback : callbacks) {
        if (callback instanceof @Tainted RealmChoiceCallback) {
          continue;
        } else if (callback instanceof @Tainted NameCallback) {
          nc = (@Tainted NameCallback) callback;
        } else if (callback instanceof @Tainted PasswordCallback) {
          pc = (@Tainted PasswordCallback) callback;
        } else if (callback instanceof @Tainted RealmCallback) {
          rc = (@Tainted RealmCallback) callback;
        } else {
          throw new @Tainted UnsupportedCallbackException(callback,
              "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting username: " + userName);
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting userPassword");
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting realm: "
              + rc.getDefaultText());
        rc.setText(rc.getDefaultText());
      }
    }
  }
}
