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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Vector;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

/**
 * 
 * A class that provides direct and reverse lookup functionalities, allowing
 * the querying of specific network interfaces or nameservers.
 * 
 * 
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DNS {

  private static final @Tainted Log LOG = LogFactory.getLog(DNS.class);

  /**
   * The cached hostname -initially null.
   */

  private static final @Tainted String cachedHostname = resolveLocalHostname();
  private static final @Tainted String cachedHostAddress = resolveLocalHostIPAddress();
  private static final @Tainted String LOCALHOST = "localhost";

  /**
   * Returns the hostname associated with the specified IP address by the
   * provided nameserver.
   *
   * Loopback addresses 
   * @param hostIp The address to reverse lookup
   * @param ns The host name of a reachable DNS server
   * @return The host name associated with the provided IP
   * @throws NamingException If a NamingException is encountered
   */
  public static @Tainted String reverseDns(@Tainted InetAddress hostIp, @Tainted String ns)
    throws NamingException {
    //
    // Builds the reverse IP lookup form
    // This is formed by reversing the IP numbers and appending in-addr.arpa
    //
    @Tainted
    String @Tainted [] parts = hostIp.getHostAddress().split("\\.");
    @Tainted
    String reverseIP = parts[3] + "." + parts[2] + "." + parts[1] + "."
      + parts[0] + ".in-addr.arpa";

    @Tainted
    DirContext ictx = new @Tainted InitialDirContext();
    @Tainted
    Attributes attribute;
    try {
      attribute = ictx.getAttributes("dns://"               // Use "dns:///" if the default
                         + ((ns == null) ? "" : ns) +
                         // nameserver is to be used
                         "/" + reverseIP, new @Tainted String @Tainted [] { "PTR" });
    } finally {
      ictx.close();
    }

    @Tainted
    String hostname = attribute.get("PTR").get().toString();
    @Tainted
    int hostnameLength = hostname.length();
    if (hostname.charAt(hostnameLength - 1) == '.') {
      hostname = hostname.substring(0, hostnameLength - 1);
    }
    return hostname;
  }

  /**
   * @return NetworkInterface for the given subinterface name (eg eth0:0)
   *    or null if no interface with the given name can be found  
   */
  private static @Tainted NetworkInterface getSubinterface(@Tainted String strInterface)
      throws SocketException {
    @Tainted
    Enumeration<@Tainted NetworkInterface> nifs = 
      NetworkInterface.getNetworkInterfaces();
      
    while (nifs.hasMoreElements()) {
      @Tainted
      Enumeration<@Tainted NetworkInterface> subNifs = 
        nifs.nextElement().getSubInterfaces();

      while (subNifs.hasMoreElements()) {
        @Tainted
        NetworkInterface nif = subNifs.nextElement();
        if (nif.getName().equals(strInterface)) {
          return nif;
        }
      }
    }
    return null;
  }

  /**
   * @param nif network interface to get addresses for
   * @return set containing addresses for each subinterface of nif,
   *    see below for the rationale for using an ordered set
   */
  private static @Tainted LinkedHashSet<@Tainted InetAddress> getSubinterfaceInetAddrs(
      @Tainted
      NetworkInterface nif) {
    @Tainted
    LinkedHashSet<@Tainted InetAddress> addrs = new @Tainted LinkedHashSet<@Tainted InetAddress>();
    @Tainted
    Enumeration<@Tainted NetworkInterface> subNifs = nif.getSubInterfaces();
    while (subNifs.hasMoreElements()) {
      @Tainted
      NetworkInterface subNif = subNifs.nextElement();
      addrs.addAll(Collections.list(subNif.getInetAddresses()));
    }
    return addrs;
  }

  /**
   * Like {@link DNS#getIPs(String, boolean), but returns all
   * IPs associated with the given interface and its subinterfaces.
   */
  public static @Tainted String @Tainted [] getIPs(@Tainted String strInterface)
      throws UnknownHostException {
    return getIPs(strInterface, true);
  }

  /**
   * Returns all the IPs associated with the provided interface, if any, in
   * textual form.
   * 
   * @param strInterface
   *            The name of the network interface or sub-interface to query
   *            (eg eth0 or eth0:0) or the string "default"
   * @param returnSubinterfaces
   *            Whether to return IPs associated with subinterfaces of
   *            the given interface
   * @return A string vector of all the IPs associated with the provided
   *         interface. The local host IP is returned if the interface
   *         name "default" is specified or there is an I/O error looking
   *         for the given interface.
   * @throws UnknownHostException
   *             If the given interface is invalid
   * 
   */
  public static @Tainted String @Tainted [] getIPs(@Tainted String strInterface,
      @Tainted
      boolean returnSubinterfaces) throws UnknownHostException {
    if ("default".equals(strInterface)) {
      return new @Tainted String @Tainted [] { cachedHostAddress };
    }
    @Tainted
    NetworkInterface netIf;
    try {
      netIf = NetworkInterface.getByName(strInterface);
      if (netIf == null) {
        netIf = getSubinterface(strInterface);
      }
    } catch (@Tainted SocketException e) {
      LOG.warn("I/O error finding interface " + strInterface +
          ": " + e.getMessage());
      return new @Tainted String @Tainted [] { cachedHostAddress };
    }
    if (netIf == null) {
      throw new @Tainted UnknownHostException("No such interface " + strInterface);
    }

    // NB: Using a LinkedHashSet to preserve the order for callers
    // that depend on a particular element being 1st in the array.
    // For example, getDefaultIP always returns the first element.
    @Tainted
    LinkedHashSet<@Tainted InetAddress> allAddrs = new @Tainted LinkedHashSet<@Tainted InetAddress>();
    allAddrs.addAll(Collections.list(netIf.getInetAddresses()));
    if (!returnSubinterfaces) {
      allAddrs.removeAll(getSubinterfaceInetAddrs(netIf));
    }

    @Tainted
    String ips @Tainted [] = new @Tainted String @Tainted [allAddrs.size()];
    @Tainted
    int i = 0;
    for (@Tainted InetAddress addr : allAddrs) {
      ips[i++] = addr.getHostAddress();
    }
    return ips;
  }


  /**
   * Returns the first available IP address associated with the provided
   * network interface or the local host IP if "default" is given.
   *
   * @param strInterface
   *            The name of the network interface or subinterface to query
   *             (e.g. eth0 or eth0:0) or the string "default"
   * @return The IP address in text form, the local host IP is returned
   *         if the interface name "default" is specified
   * @throws UnknownHostException
   *             If the given interface is invalid
   */
  public static @Tainted String getDefaultIP(@Tainted String strInterface)
    throws UnknownHostException {
    @Tainted
    String @Tainted [] ips = getIPs(strInterface);
    return ips[0];
  }

  /**
   * Returns all the host names associated by the provided nameserver with the
   * address bound to the specified network interface
   *
   * @param strInterface
   *            The name of the network interface or subinterface to query
   *            (e.g. eth0 or eth0:0)
   * @param nameserver
   *            The DNS host name
   * @return A string vector of all host names associated with the IPs tied to
   *         the specified interface
   * @throws UnknownHostException if the given interface is invalid
   */
  public static @Tainted String @Tainted [] getHosts(@Tainted String strInterface, @Tainted String nameserver)
    throws UnknownHostException {
    @Tainted
    String @Tainted [] ips = getIPs(strInterface);
    @Tainted
    Vector<@Tainted String> hosts = new @Tainted Vector<@Tainted String>();
    for (@Tainted int ctr = 0; ctr < ips.length; ctr++) {
      try {
        hosts.add(reverseDns(InetAddress.getByName(ips[ctr]),
                             nameserver));
      } catch (@Tainted UnknownHostException ignored) {
      } catch (@Tainted NamingException ignored) {
      }
    }
    if (hosts.isEmpty()) {
      LOG.warn("Unable to determine hostname for interface " + strInterface);
      return new @Tainted String @Tainted [] { cachedHostname };
    } else {
      return hosts.toArray(new @Tainted String @Tainted [hosts.size()]);
    }
  }


  /**
   * Determine the local hostname; retrieving it from cache if it is known
   * If we cannot determine our host name, return "localhost"
   * @return the local hostname or "localhost"
   */
  private static @Tainted String resolveLocalHostname() {
    @Tainted
    String localhost;
    try {
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (@Tainted UnknownHostException e) {
      LOG.warn("Unable to determine local hostname "
          + "-falling back to \"" + LOCALHOST + "\"", e);
      localhost = LOCALHOST;
    }
    return localhost;
  }


  /**
   * Get the IPAddress of the local host as a string.
   * This will be a loop back value if the local host address cannot be
   * determined.
   * If the loopback address of "localhost" does not resolve, then the system's
   * network is in such a state that nothing is going to work. A message is
   * logged at the error level and a null pointer returned, a pointer
   * which will trigger failures later on the application
   * @return the IPAddress of the local host or null for a serious problem.
   */
  private static @Tainted String resolveLocalHostIPAddress() {
    @Tainted
    String address;
      try {
        address = InetAddress.getLocalHost().getHostAddress();
      } catch (@Tainted UnknownHostException e) {
        LOG.warn("Unable to determine address of the host"
                + "-falling back to \"" + LOCALHOST + "\" address", e);
        try {
          address = InetAddress.getByName(LOCALHOST).getHostAddress();
        } catch (@Tainted UnknownHostException noLocalHostAddressException) {
          //at this point, deep trouble
          LOG.error("Unable to determine local loopback address "
                  + "of \"" + LOCALHOST + "\" " +
                  "-this system's network configuration is unsupported", e);
          address = null;
        }
      }
    return address;
  }

  /**
   * Returns all the host names associated by the default nameserver with the
   * address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @return The list of host names associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   * 
   */
  public static @Tainted String @Tainted [] getHosts(@Tainted String strInterface)
    throws UnknownHostException {
    return getHosts(strInterface, null);
  }

  /**
   * Returns the default (first) host name associated by the provided
   * nameserver with the address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @param nameserver
   *            The DNS host name
   * @return The default host names associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   */
  public static @Tainted String getDefaultHost(@Tainted String strInterface, @Tainted String nameserver)
    throws UnknownHostException {
    if ("default".equals(strInterface)) {
      return cachedHostname;
    }

    if ("default".equals(nameserver)) {
      return getDefaultHost(strInterface);
    }

    @Tainted
    String @Tainted [] hosts = getHosts(strInterface, nameserver);
    return hosts[0];
  }

  /**
   * Returns the default (first) host name associated by the default
   * nameserver with the address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0).
   *            Must not be null.
   * @return The default host name associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   */
  public static @Tainted String getDefaultHost(@Tainted String strInterface)
    throws UnknownHostException {
    return getDefaultHost(strInterface, null);
  }

}
