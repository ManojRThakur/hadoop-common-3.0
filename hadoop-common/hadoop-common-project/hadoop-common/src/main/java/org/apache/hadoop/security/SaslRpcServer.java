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
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Connection;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A utility class for dealing with SASL on RPC server
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcServer {
  public static final @Tainted Log LOG = LogFactory.getLog(SaslRpcServer.class);
  public static final @Tainted String SASL_DEFAULT_REALM = "default";
  public static final @Tainted Map<@Tainted String, @Tainted String> SASL_PROPS = 
      new @Tainted TreeMap<@Tainted String, @Tainted String>();

  public static enum QualityOfProtection {

@Tainted  AUTHENTICATION("auth"),

@Tainted  INTEGRITY("auth-int"),

@Tainted  PRIVACY("auth-conf");
    
    public final @Tainted String saslQop;
    
    private @Tainted QualityOfProtection(@Tainted String saslQop) {
      this.saslQop = saslQop;
    }
    
    public @Tainted String getSaslQop(SaslRpcServer.@Tainted QualityOfProtection this) {
      return saslQop;
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public @Tainted AuthMethod authMethod;
  public @Tainted String mechanism;
  public @Tainted String protocol;
  public @Tainted String serverId;
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public @Tainted SaslRpcServer(@Tainted AuthMethod authMethod) throws IOException {
    this.authMethod = authMethod;
    mechanism = authMethod.getMechanismName();    
    switch (authMethod) {
      case SIMPLE: {
        return; // no sasl for simple
      }
      case TOKEN: {
        protocol = "";
        serverId = SaslRpcServer.SASL_DEFAULT_REALM;
        break;
      }
      case KERBEROS: {
        @Tainted
        String fullName = UserGroupInformation.getCurrentUser().getUserName();
        if (LOG.isDebugEnabled())
          LOG.debug("Kerberos principal name is " + fullName);
        // don't use KerberosName because we don't want auth_to_local
        @Tainted
        String @Tainted [] parts = fullName.split("[/@]", 3);
        protocol = parts[0];
        // should verify service host is present here rather than in create()
        // but lazy tests are using a UGI that isn't a SPN...
        serverId = (parts.length < 2) ? "" : parts[1];
        break;
      }
      default:
        // we should never be able to get here
        throw new @Tainted AccessControlException(
            "Server does not support SASL " + authMethod);
    }
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public @Tainted SaslServer create(@Tainted SaslRpcServer this, @Tainted Connection connection,
                           @Tainted
                           SecretManager<@Tainted TokenIdentifier> secretManager
      ) throws IOException, InterruptedException {
    @Tainted
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    final @Tainted CallbackHandler callback;
    switch (authMethod) {
      case TOKEN: {
        callback = new @Tainted SaslDigestCallbackHandler(secretManager, connection);
        break;
      }
      case KERBEROS: {
        if (serverId.isEmpty()) {
          throw new @Tainted AccessControlException(
              "Kerberos principal name does NOT have the expected "
                  + "hostname part: " + ugi.getUserName());
        }
        callback = new @Tainted SaslGssCallbackHandler();
        break;
      }
      default:
        // we should never be able to get here
        throw new @Tainted AccessControlException(
            "Server does not support SASL " + authMethod);
    }
    
    @Tainted
    SaslServer saslServer = ugi.doAs(
        new @Tainted PrivilegedExceptionAction<@Tainted SaslServer>() {
          @Override
          public @Tainted SaslServer run() throws SaslException  {
            return Sasl.createSaslServer(mechanism, protocol, serverId,
                SaslRpcServer.SASL_PROPS, callback);
          }
        });
    if (saslServer == null) {
      throw new @Tainted AccessControlException(
          "Unable to find SASL server implementation for " + mechanism);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created SASL server with mechanism = " + mechanism);
    }
    return saslServer;
  }

  public static void init(@Tainted Configuration conf) {
    @Tainted
    QualityOfProtection saslQOP = QualityOfProtection.AUTHENTICATION;
    @Tainted
    String rpcProtection = conf.get("hadoop.rpc.protection",
        QualityOfProtection.AUTHENTICATION.name().toLowerCase());
    if (QualityOfProtection.INTEGRITY.name().toLowerCase()
        .equals(rpcProtection)) {
      saslQOP = QualityOfProtection.INTEGRITY;
    } else if (QualityOfProtection.PRIVACY.name().toLowerCase().equals(
        rpcProtection)) {
      saslQOP = QualityOfProtection.PRIVACY;
    }
    
    SASL_PROPS.put(Sasl.QOP, saslQOP.getSaslQop());
    SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
    Security.addProvider(new SaslPlainServer.@Tainted SecurityProvider());
  }
  
  static @Tainted String encodeIdentifier(@Tainted byte @Tainted [] identifier) {
    return new @Tainted String(Base64.encodeBase64(identifier));
  }

  static @Tainted byte @Tainted [] decodeIdentifier(@Tainted String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
  }

  public static <@Tainted T extends @Tainted TokenIdentifier> @Tainted T getIdentifier(@Tainted String id,
      @Tainted
      SecretManager<@Tainted T> secretManager) throws InvalidToken {
    @Tainted
    byte @Tainted [] tokenId = decodeIdentifier(id);
    @Tainted
    T tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new @Tainted DataInputStream(new @Tainted ByteArrayInputStream(
          tokenId)));
    } catch (@Tainted IOException e) {
      throw (@Tainted InvalidToken) new @Tainted InvalidToken(
          "Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  static @Tainted char @Tainted [] encodePassword(@Tainted byte @Tainted [] password) {
    return new @Tainted String(Base64.encodeBase64(password)).toCharArray();
  }

  /** Splitting fully qualified Kerberos name into parts */
  public static @Tainted String @Tainted [] splitKerberosName(@Tainted String fullName) {
    return fullName.split("[/@]");
  }

  /** Authentication method */
  @InterfaceStability.Evolving
  public static enum AuthMethod {

@Tainted  SIMPLE((@Untainted byte) 80, ""),

@Tainted  KERBEROS((@Untainted byte) 81, "GSSAPI"),
    @Deprecated

@Tainted  DIGEST((@Untainted byte) 82, "DIGEST-MD5"),

@Tainted  TOKEN((@Untainted byte) 82, "DIGEST-MD5"),

@Tainted  PLAIN((@Untainted byte) 83, "PLAIN");

    /** The code for this method. */
    public final @Untainted byte code;
    public final @Untainted String mechanismName;

    private @Tainted AuthMethod(@Untainted byte code, @Untainted String mechanismName) {
      this.code = code;
      this.mechanismName = mechanismName;
    }

    private static final @Tainted int FIRST_CODE = values()[0].code;

    /** Return the object represented by the code. */
    private static @Tainted AuthMethod valueOf(byte code) {
      final @Tainted int i = (code & 0xff) - FIRST_CODE;
      return i < 0 || i >= values().length ? null : values()[i];
    }

    /** Return the SASL mechanism name */
    public @Untainted String getMechanismName(SaslRpcServer.@Tainted AuthMethod this) {
      return mechanismName;
    }

    /** Read from in */
    public static @Tainted AuthMethod read(@Tainted DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /** Write to out */
    public void write(SaslRpcServer.@Tainted AuthMethod this, @Tainted DataOutput out) throws IOException {
      out.write(code);
    }
  };

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  @InterfaceStability.Evolving
  public static class SaslDigestCallbackHandler implements @Tainted CallbackHandler {
    private @Tainted SecretManager<@Tainted TokenIdentifier> secretManager;
    private @Tainted Server.@Tainted Connection connection; 
    
    public @Tainted SaslDigestCallbackHandler(
        @Tainted
        SecretManager<@Tainted TokenIdentifier> secretManager,
        @Tainted
        Server.@Tainted Connection connection) {
      this.secretManager = secretManager;
      this.connection = connection;
    }

    private @Tainted char @Tainted [] getPassword(SaslRpcServer.@Tainted SaslDigestCallbackHandler this, @Tainted TokenIdentifier tokenid) throws InvalidToken,
        StandbyException, RetriableException, IOException {
      return encodePassword(secretManager.retriableRetrievePassword(tokenid));
    }

    @Override
    public void handle(SaslRpcServer.@Tainted SaslDigestCallbackHandler this, @Tainted Callback @Tainted [] callbacks) throws InvalidToken,
        UnsupportedCallbackException, StandbyException, RetriableException,
        IOException {
      @Tainted
      NameCallback nc = null;
      @Tainted
      PasswordCallback pc = null;
      @Tainted
      AuthorizeCallback ac = null;
      for (@Tainted Callback callback : callbacks) {
        if (callback instanceof @Tainted AuthorizeCallback) {
          ac = (@Tainted AuthorizeCallback) callback;
        } else if (callback instanceof @Tainted NameCallback) {
          nc = (@Tainted NameCallback) callback;
        } else if (callback instanceof @Tainted PasswordCallback) {
          pc = (@Tainted PasswordCallback) callback;
        } else if (callback instanceof @Tainted RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new @Tainted UnsupportedCallbackException(callback,
              "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        @Tainted
        TokenIdentifier tokenIdentifier = getIdentifier(nc.getDefaultName(),
            secretManager);
        @Tainted
        char @Tainted [] password = getPassword(tokenIdentifier);
        @Tainted
        UserGroupInformation user = null;
        user = tokenIdentifier.getUser(); // may throw exception
        connection.attemptingUser = user;
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL server DIGEST-MD5 callback: setting password "
              + "for client: " + tokenIdentifier.getUser());
        }
        pc.setPassword(password);
      }
      if (ac != null) {
        @Tainted
        String authid = ac.getAuthenticationID();
        @Tainted
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled()) {
            @Tainted
            String username =
              getIdentifier(authzid, secretManager).getUser().getUserName();
            LOG.debug("SASL server DIGEST-MD5 callback: setting "
                + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism */
  @InterfaceStability.Evolving
  public static class SaslGssCallbackHandler implements @Tainted CallbackHandler {

    @Override
    public void handle(SaslRpcServer.@Tainted SaslGssCallbackHandler this, @Tainted Callback @Tainted [] callbacks) throws
        UnsupportedCallbackException {
      @Tainted
      AuthorizeCallback ac = null;
      for (@Tainted Callback callback : callbacks) {
        if (callback instanceof @Tainted AuthorizeCallback) {
          ac = (@Tainted AuthorizeCallback) callback;
        } else {
          throw new @Tainted UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }
      if (ac != null) {
        @Tainted
        String authid = ac.getAuthenticationID();
        @Tainted
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled())
            LOG.debug("SASL server GSSAPI callback: setting "
                + "canonicalized client ID: " + authzid);
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}
