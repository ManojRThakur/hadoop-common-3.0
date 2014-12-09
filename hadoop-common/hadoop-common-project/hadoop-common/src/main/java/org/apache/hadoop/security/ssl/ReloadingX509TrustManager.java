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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TrustManager} implementation that reloads its configuration when
 * the truststore file on disk changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ReloadingX509TrustManager
  implements @Tainted X509TrustManager, @Tainted Runnable {

  private static final @Tainted Log LOG =
    LogFactory.getLog(ReloadingX509TrustManager.class);

  private @Tainted String type;
  private @Tainted File file;
  private @Tainted String password;
  private @Tainted long lastLoaded;
  private @Tainted long reloadInterval;
  private @Tainted AtomicReference<@Tainted X509TrustManager> trustManagerRef;

  private volatile @Tainted boolean running;
  private @Tainted Thread reloader;

  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying trustore file has changed.
   *
   * @param type type of truststore file, typically 'jks'.
   * @param location local path to the truststore file.
   * @param password password of the truststore file.
   * @param reloadInterval interval to check if the truststore file has
   * changed, in milliseconds.
   * @throws IOException thrown if the truststore could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   * initialized due to a security error.
   */
  public @Tainted ReloadingX509TrustManager(@Tainted String type, @Tainted String location,
                                   @Tainted
                                   String password, @Tainted long reloadInterval)
    throws IOException, GeneralSecurityException {
    this.type = type;
    file = new @Tainted File(location);
    this.password = password;
    trustManagerRef = new @Tainted AtomicReference<@Tainted X509TrustManager>();
    trustManagerRef.set(loadTrustManager());
    this.reloadInterval = reloadInterval;
  }

  /**
   * Starts the reloader thread.
   */
  public void init(@Tainted ReloadingX509TrustManager this) {
    reloader = new @Tainted Thread(this, "Truststore reloader thread");
    reloader.setDaemon(true);
    running =  true;
    reloader.start();
  }

  /**
   * Stops the reloader thread.
   */
  public void destroy(@Tainted ReloadingX509TrustManager this) {
    running = false;
    reloader.interrupt();
  }

  /**
   * Returns the reload check interval.
   *
   * @return the reload check interval, in milliseconds.
   */
  public @Tainted long getReloadInterval(@Tainted ReloadingX509TrustManager this) {
    return reloadInterval;
  }

  @Override
  public void checkClientTrusted(@Tainted ReloadingX509TrustManager this, @Tainted X509Certificate @Tainted [] chain, @Tainted String authType)
    throws CertificateException {
    @Tainted
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkClientTrusted(chain, authType);
    } else {
      throw new @Tainted CertificateException("Unknown client chain certificate: " +
                                     chain[0].toString());
    }
  }

  @Override
  public void checkServerTrusted(@Tainted ReloadingX509TrustManager this, @Tainted X509Certificate @Tainted [] chain, @Tainted String authType)
    throws CertificateException {
    @Tainted
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkServerTrusted(chain, authType);
    } else {
      throw new @Tainted CertificateException("Unknown server chain certificate: " +
                                     chain[0].toString());
    }
  }

  private static final @Tainted X509Certificate @Tainted [] EMPTY = new @Tainted X509Certificate @Tainted [0];
  @Override
  public @Tainted X509Certificate @Tainted [] getAcceptedIssuers(@Tainted ReloadingX509TrustManager this) {
    @Tainted
    X509Certificate @Tainted [] issuers = EMPTY;
    @Tainted
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      issuers = tm.getAcceptedIssuers();
    }
    return issuers;
  }

  @Tainted
  boolean needsReload(@Tainted ReloadingX509TrustManager this) {
    @Tainted
    boolean reload = true;
    if (file.exists()) {
      if (file.lastModified() == lastLoaded) {
        reload = false;
      }
    } else {
      lastLoaded = 0;
    }
    return reload;
  }

  @Tainted
  X509TrustManager loadTrustManager(@Tainted ReloadingX509TrustManager this)
  throws IOException, GeneralSecurityException {
    @Tainted
    X509TrustManager trustManager = null;
    @Tainted
    KeyStore ks = KeyStore.getInstance(type);
    lastLoaded = file.lastModified();
    @Tainted
    FileInputStream in = new @Tainted FileInputStream(file);
    try {
      ks.load(in, password.toCharArray());
      LOG.debug("Loaded truststore '" + file + "'");
    } finally {
      in.close();
    }

    @Tainted
    TrustManagerFactory trustManagerFactory = 
      TrustManagerFactory.getInstance(SSLFactory.SSLCERTIFICATE);
    trustManagerFactory.init(ks);
    @Tainted
    TrustManager @Tainted [] trustManagers = trustManagerFactory.getTrustManagers();
    for (@Tainted TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof @Tainted X509TrustManager) {
        trustManager = (@Tainted X509TrustManager) trustManager1;
        break;
      }
    }
    return trustManager;
  }

  @Override
  public void run(@Tainted ReloadingX509TrustManager this) {
    while (running) {
      try {
        Thread.sleep(reloadInterval);
      } catch (@Tainted InterruptedException e) {
        //NOP
      }
      if (running && needsReload()) {
        try {
          trustManagerRef.set(loadTrustManager());
        } catch (@Tainted Exception ex) {
          LOG.warn("Could not load truststore (keep using existing one) : " +
                   ex.toString(), ex);
        }
      }
    }
  }

}
