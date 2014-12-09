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
package org.apache.hadoop.net;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Specialized SocketFactory to create sockets with a SOCKS proxy
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class StandardSocketFactory extends @Tainted SocketFactory {

  /**
   * Default empty constructor (for use with the reflection API).
   */
  public @Tainted StandardSocketFactory() {
  }

  @Override
  public @Tainted Socket createSocket(@Tainted StandardSocketFactory this) throws IOException {
    /*
     * NOTE: This returns an NIO socket so that it has an associated 
     * SocketChannel. As of now, this unfortunately makes streams returned
     * by Socket.getInputStream() and Socket.getOutputStream() unusable
     * (because a blocking read on input stream blocks write on output stream
     * and vice versa).
     * 
     * So users of these socket factories should use 
     * NetUtils.getInputStream(socket) and 
     * NetUtils.getOutputStream(socket) instead.
     * 
     * A solution for hiding from this from user is to write a 
     * 'FilterSocket' on the lines of FilterInputStream and extend it by
     * overriding getInputStream() and getOutputStream().
     */
    return SocketChannel.open().socket();
  }

  @Override
  public @Tainted Socket createSocket(@Tainted StandardSocketFactory this, @Tainted InetAddress addr, @Tainted int port) throws IOException {

    @Tainted
    Socket socket = createSocket();
    socket.connect(new @Tainted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @Tainted Socket createSocket(@Tainted StandardSocketFactory this, @Tainted InetAddress addr, @Tainted int port,
      @Tainted
      InetAddress localHostAddr, @Tainted int localPort) throws IOException {

    @Tainted
    Socket socket = createSocket();
    socket.bind(new @Tainted InetSocketAddress(localHostAddr, localPort));
    socket.connect(new @Tainted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @Tainted Socket createSocket(@Tainted StandardSocketFactory this, @Tainted String host, @Tainted int port) throws IOException,
      UnknownHostException {

    @Tainted
    Socket socket = createSocket();
    socket.connect(new @Tainted InetSocketAddress(host, port));
    return socket;
  }

  @Override
  public @Tainted Socket createSocket(@Tainted StandardSocketFactory this, @Tainted String host, @Tainted int port,
      @Tainted
      InetAddress localHostAddr, @Tainted int localPort) throws IOException,
      UnknownHostException {

    @Tainted
    Socket socket = createSocket();
    socket.bind(new @Tainted InetSocketAddress(localHostAddr, localPort));
    socket.connect(new @Tainted InetSocketAddress(host, port));
    return socket;
  }

  @Override
  public @Tainted boolean equals(@Tainted StandardSocketFactory this, @Tainted Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    return obj.getClass().equals(this.getClass());
  }

  @Override
  public @Tainted int hashCode(@Tainted StandardSocketFactory this) {
    return this.getClass().hashCode();
  }

}
