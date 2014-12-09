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
import java.security.Provider;
import java.util.Map;

import javax.security.auth.callback.*;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SaslPlainServer implements @Tainted SaslServer {
  @SuppressWarnings("serial")
  public static class SecurityProvider extends @Tainted Provider {
    public @Tainted SecurityProvider() {
      super("SaslPlainServer", 1.0, "SASL PLAIN Authentication Server");
      put("SaslServerFactory.PLAIN",
          SaslPlainServerFactory.class.getName());
    }
  }

  public static class SaslPlainServerFactory implements @Tainted SaslServerFactory {
    @Override
    public @Tainted SaslServer createSaslServer(SaslPlainServer.@Tainted SaslPlainServerFactory this, @Tainted String mechanism, @Tainted String protocol,
        @Tainted
        String serverName, @Tainted Map<@Tainted String, @Tainted ? extends java.lang.@Tainted Object> props, @Tainted CallbackHandler cbh)
            throws SaslException {
      return "PLAIN".equals(mechanism) ? new @Tainted SaslPlainServer(cbh) : null; 
    }
    @Override
    public @Tainted String @Tainted [] getMechanismNames(SaslPlainServer.@Tainted SaslPlainServerFactory this, @Tainted Map<@Tainted String, @Tainted ? extends java.lang.@Tainted Object> props){
      return (props == null) || "false".equals(props.get(Sasl.POLICY_NOPLAINTEXT))
          ? new @Tainted String @Tainted []{"PLAIN"}
          : new @Tainted String @Tainted [0];
    }
  }
  
  private @Tainted CallbackHandler cbh;
  private @Tainted boolean completed;
  private @Tainted String authz;
  
  @Tainted
  SaslPlainServer(@Tainted CallbackHandler callback) {
    this.cbh = callback;
  }

  @Override
  public @Tainted String getMechanismName(@Tainted SaslPlainServer this) {
    return "PLAIN";
  }
  
  @Override
  public @Tainted byte @Tainted [] evaluateResponse(@Tainted SaslPlainServer this, @Tainted byte @Tainted [] response) throws SaslException {
    if (completed) {
      throw new @Tainted IllegalStateException("PLAIN authentication has completed");
    }
    if (response == null) {
      throw new @Tainted IllegalArgumentException("Received null response");
    }
    try {
      @Tainted
      String payload;
      try {
        payload = new @Tainted String(response, "UTF-8");
      } catch (@Tainted Exception e) {
        throw new @Tainted IllegalArgumentException("Received corrupt response", e);
      }
      // [ authz, authn, password ]
      @Tainted
      String @Tainted [] parts = payload.split("\u0000", 3);
      if (parts.length != 3) {
        throw new @Tainted IllegalArgumentException("Received corrupt response");
      }
      if (parts[0].isEmpty()) { // authz = authn
        parts[0] = parts[1];
      }
      
      @Tainted
      NameCallback nc = new @Tainted NameCallback("SASL PLAIN");
      nc.setName(parts[1]);
      @Tainted
      PasswordCallback pc = new @Tainted PasswordCallback("SASL PLAIN", false);
      pc.setPassword(parts[2].toCharArray());
      @Tainted
      AuthorizeCallback ac = new @Tainted AuthorizeCallback(parts[1], parts[0]);
      cbh.handle(new @Tainted Callback @Tainted []{nc, pc, ac});      
      if (ac.isAuthorized()) {
        authz = ac.getAuthorizedID();
      }
    } catch (@Tainted Exception e) {
      throw new @Tainted SaslException("PLAIN auth failed: " + e.getMessage());
    } finally {
      completed = true;
    }
    return null;
  }

  private void throwIfNotComplete(@Tainted SaslPlainServer this) {
    if (!completed) {
      throw new @Tainted IllegalStateException("PLAIN authentication not completed");
    }
  }
  
  @Override
  public @Tainted boolean isComplete(@Tainted SaslPlainServer this) {
    return completed;
  }

  @Override
  public @Tainted String getAuthorizationID(@Tainted SaslPlainServer this) {
    throwIfNotComplete();
    return authz;
  }
  
  @Override
  public @Tainted Object getNegotiatedProperty(@Tainted SaslPlainServer this, @Tainted String propName) {
    throwIfNotComplete();      
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }
  
  @Override
  public @Tainted byte @Tainted [] wrap(@Tainted SaslPlainServer this, @Tainted byte @Tainted [] outgoing, @Tainted int offset, @Tainted int len)
      throws SaslException {
    throwIfNotComplete();
    throw new @Tainted IllegalStateException(
        "PLAIN supports neither integrity nor privacy");      
  }
  
  @Override
  public @Tainted byte @Tainted [] unwrap(@Tainted SaslPlainServer this, @Tainted byte @Tainted [] incoming, @Tainted int offset, @Tainted int len)
      throws SaslException {
    throwIfNotComplete();
    throw new @Tainted IllegalStateException(
        "PLAIN supports neither integrity nor privacy");      
  }
  
  @Override
  public void dispose(@Tainted SaslPlainServer this) throws SaslException {
    cbh = null;
    authz = null;
  }
}