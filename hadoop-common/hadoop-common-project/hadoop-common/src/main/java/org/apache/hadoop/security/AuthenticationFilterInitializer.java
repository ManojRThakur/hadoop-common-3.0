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
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Initializes hadoop-auth AuthenticationFilter which provides support for
 * Kerberos HTTP SPNEGO authentication.
 * <p/>
 * It enables anonymous access, simple/speudo and Kerberos HTTP SPNEGO
 * authentication  for Hadoop JobTracker, NameNode, DataNodes and
 * TaskTrackers.
 * <p/>
 * Refer to the <code>core-default.xml</code> file, after the comment
 * 'HTTP Authentication' for details on the configuration options.
 * All related configuration properties have 'hadoop.http.authentication.'
 * as prefix.
 */
public class AuthenticationFilterInitializer extends @Tainted FilterInitializer {

  static final @Tainted String PREFIX = "hadoop.http.authentication.";

  static final @Tainted String SIGNATURE_SECRET_FILE = AuthenticationFilter.SIGNATURE_SECRET + ".file";

  /**
   * Initializes hadoop-auth AuthenticationFilter.
   * <p/>
   * Propagates to hadoop-auth AuthenticationFilter configuration all Hadoop
   * configuration properties prefixed with "hadoop.http.authentication."
   *
   * @param container The filter container
   * @param conf Configuration for run-time parameters
   */
  @Override
  public void initFilter(@Tainted AuthenticationFilterInitializer this, @Tainted FilterContainer container, @Tainted Configuration conf) {
    @Tainted
    Map<@Tainted String, @Tainted String> filterConfig = new @Tainted HashMap<@Tainted String, @Tainted String>();

    //setting the cookie path to root '/' so it is used for all resources.
    filterConfig.put(AuthenticationFilter.COOKIE_PATH, "/");

    for (Map.@Tainted Entry<@Tainted String, @Untainted String> entry : conf) {
      @Tainted
      String name = entry.getKey();
      if (name.startsWith(PREFIX)) {
        @Tainted
        String value = conf.get(name);
        name = name.substring(PREFIX.length());
        filterConfig.put(name, value);
      }
    }

    @Tainted
    String signatureSecretFile = filterConfig.get(SIGNATURE_SECRET_FILE);
    if (signatureSecretFile == null) {
      throw new @Tainted RuntimeException("Undefined property: " + SIGNATURE_SECRET_FILE);      
    }
    
    try {
      @Tainted
      StringBuilder secret = new @Tainted StringBuilder();
      @Tainted
      Reader reader = new @Tainted FileReader(signatureSecretFile);
      @Tainted
      int c = reader.read();
      while (c > -1) {
        secret.append((@Tainted char)c);
        c = reader.read();
      }
      reader.close();
      filterConfig.put(AuthenticationFilter.SIGNATURE_SECRET, secret.toString());
    } catch (@Tainted IOException ex) {
      throw new @Tainted RuntimeException("Could not read HTTP signature secret file: " + signatureSecretFile);            
    }

    //Resolve _HOST into bind address
    @Tainted
    String bindAddress = conf.get(HttpServer.BIND_ADDRESS);
    @Tainted
    String principal = filterConfig.get(KerberosAuthenticationHandler.PRINCIPAL);
    if (principal != null) {
      try {
        principal = SecurityUtil.getServerPrincipal(principal, bindAddress);
      }
      catch (@Tainted IOException ex) {
        throw new @Tainted RuntimeException("Could not resolve Kerberos principal name: " + ex.toString(), ex);
      }
      filterConfig.put(KerberosAuthenticationHandler.PRINCIPAL, principal);
    }

    container.addFilter("authentication",
                        AuthenticationFilter.class.getName(),
                        filterConfig);
  }

}
