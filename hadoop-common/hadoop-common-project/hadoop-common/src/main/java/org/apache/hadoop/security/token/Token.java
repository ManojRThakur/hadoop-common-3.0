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

package org.apache.hadoop.security.token;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The client-side form of the token.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Token<@Tainted T extends @Tainted TokenIdentifier> implements @Tainted Writable {
  public static final @Tainted Log LOG = LogFactory.getLog(Token.class);
  
  private static @Tainted Map<@Tainted Text, @Tainted Class<@Tainted ? extends @Tainted TokenIdentifier>> tokenKindMap;
  
  private @Tainted byte @Tainted [] identifier;
  private @Tainted byte @Tainted [] password;
  private @Tainted Text kind;
  private @Tainted Text service;
  private @Tainted TokenRenewer renewer;
  
  /**
   * Construct a token given a token identifier and a secret manager for the
   * type of the token identifier.
   * @param id the token identifier
   * @param mgr the secret manager
   */
  public @Tainted Token(@Tainted T id, @Tainted SecretManager<@Tainted T> mgr) {
    password = mgr.createPassword(id);
    identifier = id.getBytes();
    kind = id.getKind();
    service = new @Tainted Text();
  }
 
  /**
   * Construct a token from the components.
   * @param identifier the token identifier
   * @param password the token's password
   * @param kind the kind of token
   * @param service the service for this token
   */
  public @Tainted Token(@Tainted byte @Tainted [] identifier, @Tainted byte @Tainted [] password, @Tainted Text kind, @Tainted Text service) {
    this.identifier = identifier;
    this.password = password;
    this.kind = kind;
    this.service = service;
  }

  /**
   * Default constructor
   */
  public @Tainted Token() {
    identifier = new @Tainted byte @Tainted [0];
    password = new @Tainted byte @Tainted [0];
    kind = new @Tainted Text();
    service = new @Tainted Text();
  }

  /**
   * Clone a token.
   * @param other the token to clone
   */
  public @Tainted Token(@Tainted Token<@Tainted T> other) {
    this.identifier = other.identifier;
    this.password = other.password;
    this.kind = other.kind;
    this.service = other.service;
  }

  /**
   * Get the token identifier's byte representation
   * @return the token identifier's byte representation
   */
  public @Tainted byte @Tainted [] getIdentifier(@Tainted Token<T> this) {
    return identifier;
  }
  
  private static synchronized @Tainted Class<@Tainted ? extends @Tainted TokenIdentifier>
      getClassForIdentifier(@Tainted Text kind) {
    if (tokenKindMap == null) {
      tokenKindMap = Maps.newHashMap();
      for (@Tainted TokenIdentifier id : ServiceLoader.load(TokenIdentifier.class)) {
        tokenKindMap.put(id.getKind(), id.getClass());
      }
    }
    @Tainted
    Class<@Tainted ? extends @Tainted TokenIdentifier> cls = tokenKindMap.get(kind);
    if (cls == null) {
      LOG.warn("Cannot find class for token kind " + kind);
       return null;
    }
    return cls;
  }
  
  /**
   * Get the token identifier object, or null if it could not be constructed
   * (because the class could not be loaded, for example).
   * @return the token identifier, or null
   * @throws IOException 
   */
  @SuppressWarnings("unchecked")
  public @Tainted T decodeIdentifier(@Tainted Token<T> this) throws IOException {
    @Tainted
    Class<@Tainted ? extends @Tainted TokenIdentifier> cls = getClassForIdentifier(getKind());
    if (cls == null) {
      return null;
    }
    @Tainted
    TokenIdentifier tokenIdentifier = ReflectionUtils.newInstance(cls, null);
    @Tainted
    ByteArrayInputStream buf = new @Tainted ByteArrayInputStream(identifier);
    @Tainted
    DataInputStream in = new @Tainted DataInputStream(buf);  
    tokenIdentifier.readFields(in);
    in.close();
    return (T) tokenIdentifier;
  }
  
  /**
   * Get the token password/secret
   * @return the token password/secret
   */
  public @Tainted byte @Tainted [] getPassword(@Tainted Token<T> this) {
    return password;
  }
  
  /**
   * Get the token kind
   * @return the kind of the token
   */
  public synchronized @Tainted Text getKind(@Tainted Token<T> this) {
    return kind;
  }

  /**
   * Set the token kind. This is only intended to be used by services that
   * wrap another service's token, such as HFTP wrapping HDFS.
   * @param newKind
   */
  @InterfaceAudience.Private
  public synchronized void setKind(@Tainted Token<T> this, @Tainted Text newKind) {
    kind = newKind;
    renewer = null;
  }

  /**
   * Get the service on which the token is supposed to be used
   * @return the service name
   */
  public @Tainted Text getService(@Tainted Token<T> this) {
    return service;
  }
  
  /**
   * Set the service on which the token is supposed to be used
   * @param newService the service name
   */
  public void setService(@Tainted Token<T> this, @Tainted Text newService) {
    service = newService;
  }

  /**
   * Indicates whether the token is a clone.  Used by HA failover proxy
   * to indicate a token should not be visible to the user via
   * UGI.getCredentials()
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class PrivateToken<@Tainted T extends @Tainted TokenIdentifier> extends @Tainted Token<T> {
    public @Tainted PrivateToken(@Tainted Token<@Tainted T> token) {
      super(token);
    }
  }

  @Override
  public void readFields(@Tainted Token<T> this, @Tainted DataInput in) throws IOException {
    @Tainted
    int len = WritableUtils.readVInt(in);
    if (identifier == null || identifier.length != len) {
      identifier = new @Tainted byte @Tainted [len];
    }
    in.readFully(identifier);
    len = WritableUtils.readVInt(in);
    if (password == null || password.length != len) {
      password = new @Tainted byte @Tainted [len];
    }
    in.readFully(password);
    kind.readFields(in);
    service.readFields(in);
  }

  @Override
  public void write(@Tainted Token<T> this, @Tainted DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, identifier.length);
    out.write(identifier);
    WritableUtils.writeVInt(out, password.length);
    out.write(password);
    kind.write(out);
    service.write(out);
  }

  /**
   * Generate a string with the url-quoted base64 encoded serialized form
   * of the Writable.
   * @param obj the object to serialize
   * @return the encoded string
   * @throws IOException
   */
  private static @Tainted String encodeWritable(@Tainted Writable obj) throws IOException {
    @Tainted
    DataOutputBuffer buf = new @Tainted DataOutputBuffer();
    obj.write(buf);
    @Tainted
    Base64 encoder = new @Tainted Base64(0, null, true);
    @Tainted
    byte @Tainted [] raw = new @Tainted byte @Tainted [buf.getLength()];
    System.arraycopy(buf.getData(), 0, raw, 0, buf.getLength());
    return encoder.encodeToString(raw);
  }
  
  /**
   * Modify the writable to the value from the newValue
   * @param obj the object to read into
   * @param newValue the string with the url-safe base64 encoded bytes
   * @throws IOException
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  private static void decodeWritable(@Tainted Writable obj, 
                                     @Untainted String newValue) throws IOException {
    @Tainted
    Base64 decoder = new @Tainted Base64(0, null, true);

    DataInputBuffer buf = new DataInputBuffer();
    @Tainted byte @Tainted [] decoded = decoder.decode(newValue);
    buf.reset(decoded, decoded.length);

    //safe because we just read from newValue a trusted value, included manual inspection of DataInputBuffer
    //which is backed by a byteArrayInputStream
    obj.readFields((@Untainted DataInputBuffer)  buf);
  }

  /**
   * Encode this token as a url safe string
   * @return the encoded string
   * @throws IOException
   */
  public @Tainted String encodeToUrlString(@Tainted Token<T> this) throws IOException {
    return encodeWritable(this);
  }
  
  /**
   * Decode the given url safe string into this token.
   * @param newValue the encoded string
   * @throws IOException
   */
  //ostrusted this might be to stringent for the given API
  public void decodeFromUrlString(@Tainted Token<T> this, @Untainted String newValue) throws IOException {
    decodeWritable(this, newValue);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public @Tainted boolean equals(@Tainted Token<T> this, @Tainted Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      @Tainted
      Token<T> r = (@Tainted Token<T>) right;
      return Arrays.equals(identifier, r.identifier) &&
             Arrays.equals(password, r.password) &&
             kind.equals(r.kind) &&
             service.equals(r.service);
    }
  }
  
  @Override
  public @Tainted int hashCode(@Tainted Token<T> this) {
    return WritableComparator.hashBytes(identifier, identifier.length);
  }
  
  private static void addBinaryBuffer(@Tainted StringBuilder buffer, @Tainted byte @Tainted [] bytes) {
    for (@Tainted int idx = 0; idx < bytes.length; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        buffer.append(' ');
      }
      @Tainted
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        buffer.append('0');
      }
      buffer.append(num);
    }
  }
  
  private void identifierToString(@Tainted Token<T> this, @Tainted StringBuilder buffer) {
    T id = null;
    try {
      id = decodeIdentifier();
    } catch (@Tainted IOException e) {
      // handle in the finally block
    } finally {
      if (id != null) {
        buffer.append("(").append(id).append(")");
      } else {
        addBinaryBuffer(buffer, identifier);
      }
    }
  }

  @Override
  public @Tainted String toString(@Tainted Token<T> this) {
    @Tainted
    StringBuilder buffer = new @Tainted StringBuilder();
    buffer.append("Kind: ");
    buffer.append(kind.toString());
    buffer.append(", Service: ");
    buffer.append(service.toString());
    buffer.append(", Ident: ");
    identifierToString(buffer);
    return buffer.toString();
  }
  
  private static @Tainted ServiceLoader<@Tainted TokenRenewer> renewers =
      ServiceLoader.load(TokenRenewer.class);

  private synchronized @Tainted TokenRenewer getRenewer(@Tainted Token<T> this) throws IOException {
    if (renewer != null) {
      return renewer;
    }
    renewer = TRIVIAL_RENEWER;
    synchronized (renewers) {
      for (@Tainted TokenRenewer canidate : renewers) {
        if (canidate.handleKind(this.kind)) {
          renewer = canidate;
          return renewer;
        }
      }
    }
    LOG.warn("No TokenRenewer defined for token kind " + this.kind);
    return renewer;
  }

  /**
   * Is this token managed so that it can be renewed or cancelled?
   * @return true, if it can be renewed and cancelled.
   */
  public @Tainted boolean isManaged(@Tainted Token<T> this) throws IOException {
    return getRenewer().isManaged(this);
  }

  /**
   * Renew this delegation token
   * @return the new expiration time
   * @throws IOException
   * @throws InterruptedException
   */
  public @Tainted long renew(@Tainted Token<T> this, @Tainted Configuration conf
                    ) throws IOException, InterruptedException {
    return getRenewer().renew(this, conf);
  }
  
  /**
   * Cancel this delegation token
   * @throws IOException
   * @throws InterruptedException
   */
  public void cancel(@Tainted Token<T> this, @Tainted Configuration conf
                     ) throws IOException, InterruptedException {
    getRenewer().cancel(this, conf);
  }
  
  /**
   * A trivial renewer for token kinds that aren't managed. Sub-classes need
   * to implement getKind for their token kind.
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class TrivialRenewer extends @Tainted TokenRenewer {
    
    // define the kind for this renewer
    protected @Tainted Text getKind(Token.@Tainted TrivialRenewer this) {
      return null;
    }

    @Override
    public @Tainted boolean handleKind(Token.@Tainted TrivialRenewer this, @Tainted Text kind) {
      return kind.equals(getKind());
    }

    @Override
    public @Tainted boolean isManaged(Token.@Tainted TrivialRenewer this, @Tainted Token<@Tainted ? extends java.lang.@Tainted Object> token) {
      return false;
    }

    @Override
    public @Tainted long renew(Token.@Tainted TrivialRenewer this, @Tainted Token<@Tainted ? extends java.lang.@Tainted Object> token, @Tainted Configuration conf) {
      throw new @Tainted UnsupportedOperationException("Token renewal is not supported "+
                                              " for " + token.kind + " tokens");
    }

    @Override
    public void cancel(Token.@Tainted TrivialRenewer this, @Tainted Token<@Tainted ? extends java.lang.@Tainted Object> token, @Tainted Configuration conf) throws IOException,
        InterruptedException {
      throw new @Tainted UnsupportedOperationException("Token cancel is not supported " +
          " for " + token.kind + " tokens");
    }

  }
  private static final @Tainted TokenRenewer TRIVIAL_RENEWER = new @Tainted TrivialRenewer();
}
