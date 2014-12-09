/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

import com.google.common.annotations.VisibleForTesting;

//this will need to be replaced someday when there is a suitable replacement
import sun.net.dns.ResolverConfiguration;
import sun.net.util.IPAddressUtil;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SecurityUtil {
  public static final @Tainted Log LOG = LogFactory.getLog(SecurityUtil.class);
  public static final @Tainted String HOSTNAME_PATTERN = "_HOST";

  // controls whether buildTokenService will use an ip or host/ip as given
  // by the user
  @VisibleForTesting
  static @Tainted boolean useIpForTokenService;
  @VisibleForTesting
  static @Tainted HostResolver hostResolver;

  private static @Tainted SSLFactory sslFactory;

  static {
    @Tainted
    Configuration conf = new @Tainted Configuration();
    @Tainted
    boolean useIp = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT);
    setTokenServiceUseIp(useIp);
    if (HttpConfig.isSecure()) {
      sslFactory = new @Tainted SSLFactory(SSLFactory.Mode.CLIENT, conf);
      try {
        sslFactory.init();
      } catch (@Tainted Exception ex) {
        throw new @Tainted RuntimeException(ex);
      }
    }
  }
  
  /**
   * For use only by tests and initialization
   */
  @InterfaceAudience.Private
  static void setTokenServiceUseIp(@Tainted boolean flag) {
    useIpForTokenService = flag;
    hostResolver = !useIpForTokenService
        ? new @Tainted QualifiedHostResolver()
        : new @Tainted StandardHostResolver();
  }
  
  /**
   * Find the original TGT within the current subject's credentials. Cross-realm
   * TGT's of the form "krbtgt/TWO.COM@ONE.COM" may be present.
   * 
   * @return The TGT from the current subject
   * @throws IOException
   *           if TGT can't be found
   */
  private static @Tainted KerberosTicket getTgtFromSubject() throws IOException {
    @Tainted
    Subject current = Subject.getSubject(AccessController.getContext());
    if (current == null) {
      throw new @Tainted IOException(
          "Can't get TGT from current Subject, because it is null");
    }
    @Tainted
    Set<@Tainted KerberosTicket> tickets = current
        .getPrivateCredentials(KerberosTicket.class);
    for (@Tainted KerberosTicket t : tickets) {
      if (isOriginalTGT(t))
        return t;
    }
    throw new @Tainted IOException("Failed to find TGT from current Subject:"+current);
  }
  
  /**
   * TGS must have the server principal of the form "krbtgt/FOO@FOO".
   * @param principal
   * @return true or false
   */
  static @Tainted boolean 
  isTGSPrincipal(@Tainted KerberosPrincipal principal) {
    if (principal == null)
      return false;
    if (principal.getName().equals("krbtgt/" + principal.getRealm() + 
        "@" + principal.getRealm())) {
      return true;
    }
    return false;
  }
  
  /**
   * Check whether the server principal is the TGS's principal
   * @param ticket the original TGT (the ticket that is obtained when a 
   * kinit is done)
   * @return true or false
   */
  protected static @Tainted boolean isOriginalTGT(@Tainted KerberosTicket ticket) {
    return isTGSPrincipal(ticket.getServer());
  }

  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal
   * names. It replaces hostname pattern with hostname, which should be
   * fully-qualified domain name. If hostname is null or "0.0.0.0", it uses
   * dynamically looked-up fqdn of the current host instead.
   * 
   * @param principalConfig
   *          the Kerberos principal name conf value to convert
   * @param hostname
   *          the fully-qualified domain name used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static @Tainted String getServerPrincipal(@Tainted String principalConfig,
      @Tainted
      String hostname) throws IOException {
    @Tainted
    String @Tainted [] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      return replacePattern(components, hostname);
    }
  }
  
  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal names.
   * This method is similar to {@link #getServerPrincipal(String, String)},
   * except 1) the reverse DNS lookup from addr to hostname is done only when
   * necessary, 2) param addr can't be null (no default behavior of using local
   * hostname when addr is null).
   * 
   * @param principalConfig
   *          Kerberos principal name pattern to convert
   * @param addr
   *          InetAddress of the host used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static @Tainted String getServerPrincipal(@Tainted String principalConfig,
      @Tainted
      InetAddress addr) throws IOException {
    @Tainted
    String @Tainted [] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      if (addr == null) {
        throw new @Tainted IOException("Can't replace " + HOSTNAME_PATTERN
            + " pattern since client address is null");
      }
      return replacePattern(components, addr.getCanonicalHostName());
    }
  }
  
  private static @Tainted String @Tainted [] getComponents(@Tainted String principalConfig) {
    if (principalConfig == null)
      return null;
    return principalConfig.split("[/@]");
  }
  
  private static @Tainted String replacePattern(@Tainted String @Tainted [] components, @Tainted String hostname)
      throws IOException {
    @Tainted
    String fqdn = hostname;
    if (fqdn == null || fqdn.isEmpty() || fqdn.equals("0.0.0.0")) {
      fqdn = getLocalHostName();
    }
    return components[0] + "/" + fqdn.toLowerCase(Locale.US) + "@" + components[2];
  }
  
  static @Tainted String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  /**
   * Login as a principal specified in config. Substitute $host in
   * user's Kerberos principal name with a dynamically looked-up fully-qualified
   * domain name of the current host.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final @Tainted Configuration conf,
      final @Tainted String keytabFileKey, final @Tainted String userNameKey) throws IOException {
    login(conf, keytabFileKey, userNameKey, getLocalHostName());
  }

  /**
   * Login as a principal specified in config. Substitute $host in user's Kerberos principal 
   * name with hostname. If non-secure mode - return. If no keytab available -
   * bail out with an exception
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @param hostname
   *          hostname to use for substitution
   * @throws IOException if the config doesn't specify a keytab
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final @Tainted Configuration conf,
      final @Tainted String keytabFileKey, final @Tainted String userNameKey, @Tainted String hostname)
      throws IOException {
    
    if(! UserGroupInformation.isSecurityEnabled()) 
      return;
    
    @Tainted
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new @Tainted IOException("Running in secure mode, but config doesn't have a keytab");
    }

    @Tainted
    String principalConfig = conf.get(userNameKey, System
        .getProperty("user.name"));
    @Tainted
    String principalName = SecurityUtil.getServerPrincipal(principalConfig,
        hostname);
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename);
  }

  /**
   * create the service name for a Delegation token
   * @param uri of the service
   * @param defPort is used if the uri lacks a port
   * @return the token service, or null if no authority
   * @see #buildTokenService(InetSocketAddress)
   */
  public static @Tainted String buildDTServiceName(@Tainted URI uri, @Tainted int defPort) {
    @Tainted
    String authority = uri.getAuthority();
    if (authority == null) {
      return null;
    }
    @Tainted
    InetSocketAddress addr = NetUtils.createSocketAddr(authority, defPort);
    return buildTokenService(addr).toString();
   }
  
  /**
   * Get the host name from the principal name of format <service>/host@realm.
   * @param principalName principal name of format as described above
   * @return host name if the the string conforms to the above format, else null
   */
  public static @Tainted String getHostFromPrincipal(@Tainted String principalName) {
    return new @Tainted HadoopKerberosName(principalName).getHostName();
  }

  private static @Tainted ServiceLoader<@Tainted SecurityInfo> securityInfoProviders = 
    ServiceLoader.load(SecurityInfo.class);
  private static @Tainted SecurityInfo @Tainted [] testProviders = new @Tainted SecurityInfo @Tainted [0];

  /**
   * Test setup method to register additional providers.
   * @param providers a list of high priority providers to use
   */
  @InterfaceAudience.Private
  public static void setSecurityInfoProviders(@Tainted SecurityInfo @Tainted ... providers) {
    testProviders = providers;
  }
  
  /**
   * Look up the KerberosInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol the protocol class to get the information for
   * @param conf configuration object
   * @return the KerberosInfo or null if it has no KerberosInfo defined
   */
  public static @Tainted KerberosInfo 
  getKerberosInfo(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted Configuration conf) {
    synchronized (testProviders) {
      for(@Tainted SecurityInfo provider: testProviders) {
        @Tainted
        KerberosInfo result = provider.getKerberosInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      }
    }
    
    synchronized (securityInfoProviders) {
      for(@Tainted SecurityInfo provider: securityInfoProviders) {
        @Tainted
        KerberosInfo result = provider.getKerberosInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }
 
  /**
   * Look up the TokenInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol The protocol class to get the information for.
   * @param conf Configuration object
   * @return the TokenInfo or null if it has no KerberosInfo defined
   */
  public static @Tainted TokenInfo getTokenInfo(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol, @Tainted Configuration conf) {
    synchronized (testProviders) {
      for(@Tainted SecurityInfo provider: testProviders) {
        @Tainted
        TokenInfo result = provider.getTokenInfo(protocol, conf);
        if (result != null) {
          return result;
        }      
      }
    }
    
    synchronized (securityInfoProviders) {
      for(@Tainted SecurityInfo provider: securityInfoProviders) {
        @Tainted
        TokenInfo result = provider.getTokenInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      } 
    }
    
    return null;
  }

  /**
   * Decode the given token's service field into an InetAddress
   * @param token from which to obtain the service
   * @return InetAddress for the service
   */
  public static @Tainted InetSocketAddress getTokenServiceAddr(@Tainted Token<@Tainted ? extends java.lang.@Tainted Object> token) {
    return NetUtils.createSocketAddr(token.getService().toString());
  }

  /**
   * Set the given token's service to the format expected by the RPC client 
   * @param token a delegation token
   * @param addr the socket for the rpc connection
   */
  public static void setTokenService(@Tainted Token<@Tainted ? extends java.lang.@Tainted Object> token, @Tainted InetSocketAddress addr) {
    @Tainted
    Text service = buildTokenService(addr);
    if (token != null) {
      token.setService(service);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired token "+token);  // Token#toString() prints service
      }
    } else {
      LOG.warn("Failed to get token for service "+service);
    }
  }
  
  /**
   * Construct the service key for a token
   * @param addr InetSocketAddress of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static @Tainted Text buildTokenService(@Tainted InetSocketAddress addr) {
    @Tainted
    String host = null;
    if (useIpForTokenService) {
      if (addr.isUnresolved()) { // host has no ip address
        throw new @Tainted IllegalArgumentException(
            new @Tainted UnknownHostException(addr.getHostName())
        );
      }
      host = addr.getAddress().getHostAddress();
    } else {
      host = addr.getHostName().toLowerCase();
    }
    return new @Tainted Text(host + ":" + addr.getPort());
  }

  /**
   * Construct the service key for a token
   * @param uri of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static @Tainted Text buildTokenService(@Tainted URI uri) {
    return buildTokenService(NetUtils.createSocketAddr(uri.getAuthority()));
  }
  
  /**
   * Perform the given action as the daemon's login user. If the login
   * user cannot be determined, this will log a FATAL error and exit
   * the whole JVM.
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T doAsLoginUserOrFatal(@Tainted PrivilegedAction<@Tainted T> action) { 
    if (UserGroupInformation.isSecurityEnabled()) {
      @Tainted
      UserGroupInformation ugi = null;
      try { 
        ugi = UserGroupInformation.getLoginUser();
      } catch (@Tainted IOException e) {
        LOG.fatal("Exception while getting login user", e);
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
      return ugi.doAs(action);
    } else {
      return action.run();
    }
  }
  
  /**
   * Perform the given action as the daemon's login user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T doAsLoginUser(@Tainted PrivilegedExceptionAction<@Tainted T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getLoginUser(), action);
  }

  /**
   * Perform the given action as the daemon's current user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T doAsCurrentUser(@Tainted PrivilegedExceptionAction<@Tainted T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getCurrentUser(), action);
  }

  private static <@Tainted T extends java.lang.@Tainted Object> @Tainted T doAsUser(@Tainted UserGroupInformation ugi,
      @Tainted
      PrivilegedExceptionAction<@Tainted T> action) throws IOException {
    try {
      return ugi.doAs(action);
    } catch (@Tainted InterruptedException ie) {
      throw new @Tainted IOException(ie);
    }
  }

  /**
   * Open a (if need be) secure connection to a URL in a secure environment
   * that is using SPNEGO to authenticate its URLs. All Namenode and Secondary
   * Namenode URLs that are protected via SPNEGO should be accessed via this
   * method.
   *
   * @param url to authenticate via SPNEGO.
   * @return A connection that has been authenticated via SPNEGO
   * @throws IOException If unable to authenticate via SPNEGO
   */
  public static @Tainted URLConnection openSecureHttpConnection(@Tainted URL url) throws IOException {
    if (!HttpConfig.isSecure() && !UserGroupInformation.isSecurityEnabled()) {
      return url.openConnection();
    }

    AuthenticatedURL.@Tainted Token token = new AuthenticatedURL.@Tainted Token();
    try {
      return new @Tainted AuthenticatedURL(null, sslFactory).openConnection(url, token);
    } catch (@Tainted AuthenticationException e) {
      throw new @Tainted IOException("Exception trying to open authenticated connection to "
              + url, e);
    }
  }

  /**
   * Resolves a host subject to the security requirements determined by
   * hadoop.security.token.service.use_ip.
   * 
   * @param hostname host or ip to resolve
   * @return a resolved host
   * @throws UnknownHostException if the host doesn't exist
   */
  @InterfaceAudience.Private
  public static
  @Tainted
  InetAddress getByName(@Tainted String hostname) throws UnknownHostException {
    return hostResolver.getByName(hostname);
  }
  
  interface HostResolver {
    @Tainted
    InetAddress getByName(SecurityUtil.@Tainted HostResolver this, @Tainted String host) throws UnknownHostException;    
  }
  
  /**
   * Uses standard java host resolution
   */
  static class StandardHostResolver implements @Tainted HostResolver {
    @Override
    public @Tainted InetAddress getByName(SecurityUtil.@Tainted StandardHostResolver this, @Tainted String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }
  }
  
  /**
   * This an alternate resolver with important properties that the standard
   * java resolver lacks:
   * 1) The hostname is fully qualified.  This avoids security issues if not
   *    all hosts in the cluster do not share the same search domains.  It
   *    also prevents other hosts from performing unnecessary dns searches.
   *    In contrast, InetAddress simply returns the host as given.
   * 2) The InetAddress is instantiated with an exact host and IP to prevent
   *    further unnecessary lookups.  InetAddress may perform an unnecessary
   *    reverse lookup for an IP.
   * 3) A call to getHostName() will always return the qualified hostname, or
   *    more importantly, the IP if instantiated with an IP.  This avoids
   *    unnecessary dns timeouts if the host is not resolvable.
   * 4) Point 3 also ensures that if the host is re-resolved, ex. during a
   *    connection re-attempt, that a reverse lookup to host and forward
   *    lookup to IP is not performed since the reverse/forward mappings may
   *    not always return the same IP.  If the client initiated a connection
   *    with an IP, then that IP is all that should ever be contacted.
   *    
   * NOTE: this resolver is only used if:
   *       hadoop.security.token.service.use_ip=false 
   */
  protected static class QualifiedHostResolver implements @Tainted HostResolver {
    @SuppressWarnings("unchecked")
    private @Tainted List<@Tainted String> searchDomains =
        ResolverConfiguration.open().searchlist();
    
    /**
     * Create an InetAddress with a fully qualified hostname of the given
     * hostname.  InetAddress does not qualify an incomplete hostname that
     * is resolved via the domain search list.
     * {@link InetAddress#getCanonicalHostName()} will fully qualify the
     * hostname, but it always return the A record whereas the given hostname
     * may be a CNAME.
     * 
     * @param host a hostname or ip address
     * @return InetAddress with the fully qualified hostname or ip
     * @throws UnknownHostException if host does not exist
     */
    @Override
    public @Tainted InetAddress getByName(SecurityUtil.@Tainted QualifiedHostResolver this, @Tainted String host) throws UnknownHostException {
      @Tainted
      InetAddress addr = null;

      if (IPAddressUtil.isIPv4LiteralAddress(host)) {
        // use ipv4 address as-is
        @Tainted
        byte @Tainted [] ip = IPAddressUtil.textToNumericFormatV4(host);
        addr = InetAddress.getByAddress(host, ip);
      } else if (IPAddressUtil.isIPv6LiteralAddress(host)) {
        // use ipv6 address as-is
        @Tainted
        byte @Tainted [] ip = IPAddressUtil.textToNumericFormatV6(host);
        addr = InetAddress.getByAddress(host, ip);
      } else if (host.endsWith(".")) {
        // a rooted host ends with a dot, ex. "host."
        // rooted hosts never use the search path, so only try an exact lookup
        addr = getByExactName(host);
      } else if (host.contains(".")) {
        // the host contains a dot (domain), ex. "host.domain"
        // try an exact host lookup, then fallback to search list
        addr = getByExactName(host);
        if (addr == null) {
          addr = getByNameWithSearch(host);
        }
      } else {
        // it's a simple host with no dots, ex. "host"
        // try the search list, then fallback to exact host
        @Tainted
        InetAddress loopback = InetAddress.getByName(null);
        if (host.equalsIgnoreCase(loopback.getHostName())) {
          addr = InetAddress.getByAddress(host, loopback.getAddress());
        } else {
          addr = getByNameWithSearch(host);
          if (addr == null) {
            addr = getByExactName(host);
          }
        }
      }
      // unresolvable!
      if (addr == null) {
        throw new @Tainted UnknownHostException(host);
      }
      return addr;
    }

    @Tainted
    InetAddress getByExactName(SecurityUtil.@Tainted QualifiedHostResolver this, @Tainted String host) {
      @Tainted
      InetAddress addr = null;
      // InetAddress will use the search list unless the host is rooted
      // with a trailing dot.  The trailing dot will disable any use of the
      // search path in a lower level resolver.  See RFC 1535.
      @Tainted
      String fqHost = host;
      if (!fqHost.endsWith(".")) fqHost += ".";
      try {
        addr = getInetAddressByName(fqHost);
        // can't leave the hostname as rooted or other parts of the system
        // malfunction, ex. kerberos principals are lacking proper host
        // equivalence for rooted/non-rooted hostnames
        addr = InetAddress.getByAddress(host, addr.getAddress());
      } catch (@Tainted UnknownHostException e) {
        // ignore, caller will throw if necessary
      }
      return addr;
    }

    @Tainted
    InetAddress getByNameWithSearch(SecurityUtil.@Tainted QualifiedHostResolver this, @Tainted String host) {
      @Tainted
      InetAddress addr = null;
      if (host.endsWith(".")) { // already qualified?
        addr = getByExactName(host); 
      } else {
        for (@Tainted String domain : searchDomains) {
          @Tainted
          String dot = !domain.startsWith(".") ? "." : "";
          addr = getByExactName(host + dot + domain);
          if (addr != null) break;
        }
      }
      return addr;
    }

    // implemented as a separate method to facilitate unit testing
    @Tainted
    InetAddress getInetAddressByName(SecurityUtil.@Tainted QualifiedHostResolver this, @Tainted String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }

    void setSearchDomains(SecurityUtil.@Tainted QualifiedHostResolver this, @Tainted String @Tainted ... domains) {
      searchDomains = Arrays.asList(domains);
    }
  }

  public static @Tainted AuthenticationMethod getAuthenticationMethod(@Tainted Configuration conf) {
    @Tainted
    String value = conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    try {
      return Enum.valueOf(AuthenticationMethod.class, value.toUpperCase(Locale.ENGLISH));
    } catch (@Tainted IllegalArgumentException iae) {
      throw new @Tainted IllegalArgumentException("Invalid attribute value for " +
          HADOOP_SECURITY_AUTHENTICATION + " of " + value);
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void setAuthenticationMethod(
      @Tainted
      AuthenticationMethod authenticationMethod, @Tainted Configuration conf) {
    if (authenticationMethod == null) {
      authenticationMethod = AuthenticationMethod.SIMPLE;
    }

    //ostrusted, AuthenticationMethod is an enum composed of Trusted fields, its toString method is trusted
    conf.set(HADOOP_SECURITY_AUTHENTICATION, (@Untainted String) authenticationMethod.toString().toLowerCase(Locale.ENGLISH));
  }
}
