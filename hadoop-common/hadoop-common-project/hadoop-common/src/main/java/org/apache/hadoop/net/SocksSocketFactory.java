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
import java.net.Proxy;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Specialized SocketFactory to create sockets with a SOCKS proxy
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SocksSocketFactory extends @Tainted SocketFactory implements
    @Tainted
    Configurable {

  private @Tainted Configuration conf;

  private @Tainted Proxy proxy;

  /**
   * Default empty constructor (for use with the reflection API).
   */
  public @Tainted SocksSocketFactory() {
    this.proxy = Proxy.NO_PROXY;
  }

  /**
   * Constructor with a supplied Proxy
   * 
   * @param proxy the proxy to use to create sockets
   */
  public @Tainted SocksSocketFactory(@Tainted Proxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public @Tainted Socket createSocket(@Tainted SocksSocketFactory this) throws IOException {

    return new @Tainted Socket(proxy);
  }

  @Override
  public @Tainted Socket createSocket(@Tainted SocksSocketFactory this, @Tainted InetAddress addr, @Tainted int port) throws IOException {

    @Tainted
    Socket socket = createSocket();
    socket.connect(new @Tainted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @Tainted Socket createSocket(@Tainted SocksSocketFactory this, @Tainted InetAddress addr, @Tainted int port,
      @Tainted
      InetAddress localHostAddr, @Tainted int localPort) throws IOException {

    @Tainted
    Socket socket = createSocket();
    socket.bind(new @Tainted InetSocketAddress(localHostAddr, localPort));
    socket.connect(new @Tainted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @Tainted Socket createSocket(@Tainted SocksSocketFactory this, @Tainted String host, @Tainted int port) throws IOException,
      UnknownHostException {

    @Tainted
    Socket socket = createSocket();
    socket.connect(new @Tainted InetSocketAddress(host, port));
    return socket;
  }

  @Override
  public @Tainted Socket createSocket(@Tainted SocksSocketFactory this, @Tainted String host, @Tainted int port,
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
  public @Tainted int hashCode(@Tainted SocksSocketFactory this) {
    return proxy.hashCode();
  }

  @Override
  public @Tainted boolean equals(@Tainted SocksSocketFactory this, @Tainted Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof @Tainted SocksSocketFactory))
      return false;
    final @Tainted SocksSocketFactory other = (@Tainted SocksSocketFactory) obj;
    if (proxy == null) {
      if (other.proxy != null)
        return false;
    } else if (!proxy.equals(other.proxy))
      return false;
    return true;
  }

  @Override
  public @Tainted Configuration getConf(@Tainted SocksSocketFactory this) {
    return this.conf;
  }

  @Override
  public void setConf(@Tainted SocksSocketFactory this, @Tainted Configuration conf) {
    this.conf = conf;
    @Tainted
    String proxyStr = conf.get("hadoop.socks.server");
    if ((proxyStr != null) && (proxyStr.length() > 0)) {
      setProxy(proxyStr);
    }
  }

  /**
   * Set the proxy of this socket factory as described in the string
   * parameter
   * 
   * @param proxyStr the proxy address using the format "host:port"
   */
  private void setProxy(@Tainted SocksSocketFactory this, @Tainted String proxyStr) {
    @Tainted
    String @Tainted [] strs = proxyStr.split(":", 2);
    if (strs.length != 2)
      throw new @Tainted RuntimeException("Bad SOCKS proxy parameter: " + proxyStr);
    @Tainted
    String host = strs[0];
    @Tainted
    int port = Integer.parseInt(strs[1]);
    this.proxy =
        new @Tainted Proxy(Proxy.Type.SOCKS, InetSocketAddress.createUnresolved(host,
            port));
  }
}
