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
package org.apache.hadoop.security.ssl;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.util.ReflectionUtils;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;

/**
 * Factory that creates SSLEngine and SSLSocketFactory instances using
 * Hadoop configuration information.
 * <p/>
 * This SSLFactory uses a {@link ReloadingX509TrustManager} instance,
 * which reloads public keys if the truststore file changes.
 * <p/>
 * This factory is used to configure HTTPS in Hadoop HTTP based endpoints, both
 * client and server.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SSLFactory implements @Tainted ConnectionConfigurator {

  @InterfaceAudience.Private
  public static enum Mode {  @Tainted  CLIENT,  @Tainted  SERVER }

  public static final @Tainted String SSL_REQUIRE_CLIENT_CERT_KEY =
    "hadoop.ssl.require.client.cert";
  public static final @Tainted String SSL_HOSTNAME_VERIFIER_KEY =
    "hadoop.ssl.hostname.verifier";
  public static final @Tainted String SSL_CLIENT_CONF_KEY =
    "hadoop.ssl.client.conf";
  public static final @Tainted String SSL_SERVER_CONF_KEY =
    "hadoop.ssl.server.conf";
  public static final @Tainted String SSLCERTIFICATE = IBM_JAVA?"ibmX509":"SunX509"; 

  public static final @Tainted boolean DEFAULT_SSL_REQUIRE_CLIENT_CERT = false;

  public static final @Tainted String KEYSTORES_FACTORY_CLASS_KEY =
    "hadoop.ssl.keystores.factory.class";

  private @Tainted Configuration conf;
  private @Tainted Mode mode;
  private @Tainted boolean requireClientCert;
  private @Tainted SSLContext context;
  private @Tainted HostnameVerifier hostnameVerifier;
  private @Tainted KeyStoresFactory keystoresFactory;

  /**
   * Creates an SSLFactory.
   *
   * @param mode SSLFactory mode, client or server.
   * @param conf Hadoop configuration from where the SSLFactory configuration
   * will be read.
   */
  public @Tainted SSLFactory(@Tainted Mode mode, @Tainted Configuration conf) {
    this.conf = conf;
    if (mode == null) {
      throw new @Tainted IllegalArgumentException("mode cannot be NULL");
    }
    this.mode = mode;
    requireClientCert = conf.getBoolean(SSL_REQUIRE_CLIENT_CERT_KEY,
                                        DEFAULT_SSL_REQUIRE_CLIENT_CERT);
    @Tainted
    Configuration sslConf = readSSLConfiguration(mode);

    @Tainted
    Class<@Tainted ? extends @Tainted KeyStoresFactory> klass
      = conf.getClass(KEYSTORES_FACTORY_CLASS_KEY,
                      FileBasedKeyStoresFactory.class, KeyStoresFactory.class);
    keystoresFactory = ReflectionUtils.newInstance(klass, sslConf);
  }

  private @Tainted Configuration readSSLConfiguration(@Tainted SSLFactory this, @Tainted Mode mode) {
    @Tainted
    Configuration sslConf = new @Tainted Configuration(false);
    sslConf.setBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, requireClientCert);
    @Tainted
    String sslConfResource;
    if (mode == Mode.CLIENT) {
      sslConfResource = conf.get(SSL_CLIENT_CONF_KEY, "ssl-client.xml");
    } else {
      sslConfResource = conf.get(SSL_SERVER_CONF_KEY, "ssl-server.xml");
    }
    sslConf.addResource(sslConfResource);
    return sslConf;
  }

  /**
   * Initializes the factory.
   *
   * @throws  GeneralSecurityException thrown if an SSL initialization error
   * happened.
   * @throws IOException thrown if an IO error happened while reading the SSL
   * configuration.
   */
  public void init(@Tainted SSLFactory this) throws GeneralSecurityException, IOException {
    keystoresFactory.init(mode);
    context = SSLContext.getInstance("TLS");
    context.init(keystoresFactory.getKeyManagers(),
                 keystoresFactory.getTrustManagers(), null);

    hostnameVerifier = getHostnameVerifier(conf);
  }

  private @Tainted HostnameVerifier getHostnameVerifier(@Tainted SSLFactory this, @Tainted Configuration conf)
    throws GeneralSecurityException, IOException {
    @Tainted
    HostnameVerifier hostnameVerifier;
    @Tainted
    String verifier =
      conf.get(SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT").trim().toUpperCase();
    if (verifier.equals("DEFAULT")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT;
    } else if (verifier.equals("DEFAULT_AND_LOCALHOST")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT_AND_LOCALHOST;
    } else if (verifier.equals("STRICT")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT;
    } else if (verifier.equals("STRICT_IE6")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT_IE6;
    } else if (verifier.equals("ALLOW_ALL")) {
      hostnameVerifier = SSLHostnameVerifier.ALLOW_ALL;
    } else {
      throw new @Tainted GeneralSecurityException("Invalid hostname verifier: " +
                                         verifier);
    }
    return hostnameVerifier;
  }

  /**
   * Releases any resources being used.
   */
  public void destroy(@Tainted SSLFactory this) {
    keystoresFactory.destroy();
  }
  /**
   * Returns the SSLFactory KeyStoresFactory instance.
   *
   * @return the SSLFactory KeyStoresFactory instance.
   */
  public @Tainted KeyStoresFactory getKeystoresFactory(@Tainted SSLFactory this) {
    return keystoresFactory;
  }

  /**
   * Returns a configured SSLEngine.
   *
   * @return the configured SSLEngine.
   * @throws GeneralSecurityException thrown if the SSL engine could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public @Tainted SSLEngine createSSLEngine(@Tainted SSLFactory this)
    throws GeneralSecurityException, IOException {
    @Tainted
    SSLEngine sslEngine = context.createSSLEngine();
    if (mode == Mode.CLIENT) {
      sslEngine.setUseClientMode(true);
    } else {
      sslEngine.setUseClientMode(false);
      sslEngine.setNeedClientAuth(requireClientCert);
    }
    return sslEngine;
  }

  /**
   * Returns a configured SSLServerSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public @Tainted SSLServerSocketFactory createSSLServerSocketFactory(@Tainted SSLFactory this)
    throws GeneralSecurityException, IOException {
    if (mode != Mode.SERVER) {
      throw new @Tainted IllegalStateException("Factory is in CLIENT mode");
    }
    return context.getServerSocketFactory();
  }

  /**
   * Returns a configured SSLSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public @Tainted SSLSocketFactory createSSLSocketFactory(@Tainted SSLFactory this)
    throws GeneralSecurityException, IOException {
    if (mode != Mode.CLIENT) {
      throw new @Tainted IllegalStateException("Factory is in CLIENT mode");
    }
    return context.getSocketFactory();
  }

  /**
   * Returns the hostname verifier it should be used in HttpsURLConnections.
   *
   * @return the hostname verifier.
   */
  public @Tainted HostnameVerifier getHostnameVerifier(@Tainted SSLFactory this) {
    if (mode != Mode.CLIENT) {
      throw new @Tainted IllegalStateException("Factory is in CLIENT mode");
    }
    return hostnameVerifier;
  }

  /**
   * Returns if client certificates are required or not.
   *
   * @return if client certificates are required or not.
   */
  public @Tainted boolean isClientCertRequired(@Tainted SSLFactory this) {
    return requireClientCert;
  }

  /**
   * If the given {@link HttpURLConnection} is an {@link HttpsURLConnection}
   * configures the connection with the {@link SSLSocketFactory} and
   * {@link HostnameVerifier} of this SSLFactory, otherwise does nothing.
   *
   * @param conn the {@link HttpURLConnection} instance to configure.
   * @return the configured {@link HttpURLConnection} instance.
   *
   * @throws IOException if an IO error occurred.
   */
  @Override
  public @Tainted HttpURLConnection configure(@Tainted SSLFactory this, @Tainted HttpURLConnection conn)
    throws IOException {
    if (conn instanceof @Tainted HttpsURLConnection) {
      @Tainted
      HttpsURLConnection sslConn = (@Tainted HttpsURLConnection) conn;
      try {
        sslConn.setSSLSocketFactory(createSSLSocketFactory());
      } catch (@Tainted GeneralSecurityException ex) {
        throw new @Tainted IOException(ex);
      }
      sslConn.setHostnameVerifier(getHostnameVerifier());
      conn = sslConn;
    }
    return conn;
  }
}
