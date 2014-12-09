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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import com.google.common.annotations.VisibleForTesting;

public class ProtocolSignature implements @Tainted Writable {
  static {               // register a ctor
    WritableFactories.setFactory
      (ProtocolSignature.class,
       new @Tainted WritableFactory() {
         @Override
        public @Tainted Writable newInstance() { return new @Tainted ProtocolSignature(); }
       });
  }

  private @Tainted long version;
  private @Tainted int @Tainted [] methods = null; // an array of method hash codes
  
  /**
   * default constructor
   */
  public @Tainted ProtocolSignature() {
  }
  
  /**
   * Constructor
   * 
   * @param version server version
   * @param methodHashcodes hash codes of the methods supported by server
   */
  public @Tainted ProtocolSignature(@Tainted long version, @Tainted int @Tainted [] methodHashcodes) {
    this.version = version;
    this.methods = methodHashcodes;
  }
  
  public @Tainted long getVersion(@Tainted ProtocolSignature this) {
    return version;
  }
  
  public @Tainted int @Tainted [] getMethods(@Tainted ProtocolSignature this) {
    return methods;
  }

  @Override
  public void readFields(@Tainted ProtocolSignature this, @Tainted DataInput in) throws IOException {
    version = in.readLong();
    @Tainted
    boolean hasMethods = in.readBoolean();
    if (hasMethods) {
      @Tainted
      int numMethods = in.readInt();
      methods = new @Tainted int @Tainted [numMethods];
      for (@Tainted int i=0; i<numMethods; i++) {
        methods[i] = in.readInt();
      }
    }
  }

  @Override
  public void write(@Tainted ProtocolSignature this, @Tainted DataOutput out) throws IOException {
    out.writeLong(version);
    if (methods == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(methods.length);
      for (@Tainted int method : methods) {
        out.writeInt(method);
      }
    }
  }

  /**
   * Calculate a method's hash code considering its method
   * name, returning type, and its parameter types
   * 
   * @param method a method
   * @return its hash code
   */
  static @Tainted int getFingerprint(@Tainted Method method) {
    @Tainted
    int hashcode = method.getName().hashCode();
    hashcode =  hashcode + 31*method.getReturnType().getName().hashCode();
    for (@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> type : method.getParameterTypes()) {
      hashcode = 31*hashcode ^ type.getName().hashCode();
    }
    return hashcode;
  }

  /**
   * Convert an array of Method into an array of hash codes
   * 
   * @param methods
   * @return array of hash codes
   */
  private static @Tainted int @Tainted [] getFingerprints(@Tainted Method @Tainted [] methods) {
    if (methods == null) {
      return null;
    }
    @Tainted
    int @Tainted [] hashCodes = new @Tainted int @Tainted [methods.length];
    for (@Tainted int i = 0; i<methods.length; i++) {
      hashCodes[i] = getFingerprint(methods[i]);
    }
    return hashCodes;
  }

  /**
   * Get the hash code of an array of methods
   * Methods are sorted before hashcode is calculated.
   * So the returned value is irrelevant of the method order in the array.
   * 
   * @param methods an array of methods
   * @return the hash code
   */
  static @Tainted int getFingerprint(@Tainted Method @Tainted [] methods) {
    return getFingerprint(getFingerprints(methods));
  }
  
  /**
   * Get the hash code of an array of hashcodes
   * Hashcodes are sorted before hashcode is calculated.
   * So the returned value is irrelevant of the hashcode order in the array.
   * 
   * @param methods an array of methods
   * @return the hash code
   */
  static @Tainted int getFingerprint(@Tainted int @Tainted [] hashcodes) {
    Arrays.sort(hashcodes);
    return Arrays.hashCode(hashcodes);
    
  }
  private static class ProtocolSigFingerprint {
    private @Tainted ProtocolSignature signature;
    private @Tainted int fingerprint;
    
    @Tainted
    ProtocolSigFingerprint(@Tainted ProtocolSignature sig, @Tainted int fingerprint) {
      this.signature = sig;
      this.fingerprint = fingerprint;
    }
  }
  
  /**
   * A cache that maps a protocol's name to its signature & finger print
   */
  private final static @Tainted HashMap<@Tainted String, @Tainted ProtocolSigFingerprint> 
     PROTOCOL_FINGERPRINT_CACHE = 
       new @Tainted HashMap<@Tainted String, @Tainted ProtocolSigFingerprint>();
  
  @VisibleForTesting
  public static void resetCache() {
    PROTOCOL_FINGERPRINT_CACHE.clear();
  }
  
  /**
   * Return a protocol's signature and finger print from cache
   * 
   * @param protocol a protocol class
   * @param serverVersion protocol version
   * @return its signature and finger print
   */
  private static @Tainted ProtocolSigFingerprint getSigFingerprint(
      @Tainted
      Class <@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted long serverVersion) {
    @Tainted
    String protocolName = RPC.getProtocolName(protocol);
    synchronized (PROTOCOL_FINGERPRINT_CACHE) {
      @Tainted
      ProtocolSigFingerprint sig = PROTOCOL_FINGERPRINT_CACHE.get(protocolName);
      if (sig == null) {
        @Tainted
        int @Tainted [] serverMethodHashcodes = getFingerprints(protocol.getMethods());
        sig = new @Tainted ProtocolSigFingerprint(
            new @Tainted ProtocolSignature(serverVersion, serverMethodHashcodes),
            getFingerprint(serverMethodHashcodes));
        PROTOCOL_FINGERPRINT_CACHE.put(protocolName, sig);
      }
      return sig;    
    }
  }
  
  /**
   * Get a server protocol's signature
   * 
   * @param clientMethodsHashCode client protocol methods hashcode
   * @param serverVersion server protocol version
   * @param protocol protocol
   * @return the server's protocol signature
   */
  public static @Tainted ProtocolSignature getProtocolSignature(
      @Tainted
      int clientMethodsHashCode,
      @Tainted
      long serverVersion,
      @Tainted
      Class<@Tainted ? extends @Tainted VersionedProtocol> protocol) {
    // try to get the finger print & signature from the cache
    @Tainted
    ProtocolSigFingerprint sig = getSigFingerprint(protocol, serverVersion);
    
    // check if the client side protocol matches the one on the server side
    if (clientMethodsHashCode == sig.fingerprint) {
      return new @Tainted ProtocolSignature(serverVersion, null);  // null indicates a match
    } 
    
    return sig.signature;
  }
  
  public static @Tainted ProtocolSignature getProtocolSignature(@Tainted String protocolName,
      @Tainted
      long version) throws ClassNotFoundException {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> protocol = Class.forName(protocolName);
    return getSigFingerprint(protocol, version).signature;
  }
  
  /**
   * Get a server protocol's signature
   *
   * @param server server implementation
   * @param protocol server protocol
   * @param clientVersion client's version
   * @param clientMethodsHash client's protocol's hash code
   * @return the server protocol's signature
   * @throws IOException if any error occurs
   */
  @SuppressWarnings("unchecked")
  public static @Tainted ProtocolSignature getProtocolSignature(@Tainted VersionedProtocol server,
      @Tainted
      String protocol,
      @Tainted
      long clientVersion, @Tainted int clientMethodsHash) throws IOException {
    @Tainted
    Class<@Tainted ? extends @Tainted VersionedProtocol> inter;
    try {
      inter = (@Tainted Class<@Tainted ? extends @Tainted VersionedProtocol>)Class.forName(protocol);
    } catch (@Tainted Exception e) {
      throw new @Tainted IOException(e);
    }
    @Tainted
    long serverVersion = server.getProtocolVersion(protocol, clientVersion);
    return ProtocolSignature.getProtocolSignature(
        clientMethodsHash, serverVersion, inter);
  }
}
