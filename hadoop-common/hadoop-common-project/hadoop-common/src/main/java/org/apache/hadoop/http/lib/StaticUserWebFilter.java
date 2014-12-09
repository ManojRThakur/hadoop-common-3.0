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
package org.apache.hadoop.http.lib;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;

import javax.servlet.Filter;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;

/**
 * Provides a servlet filter that pretends to authenticate a fake user (Dr.Who)
 * so that the web UI is usable for a secure cluster without authentication.
 */
public class StaticUserWebFilter extends @Tainted FilterInitializer {
  static final @Tainted String DEPRECATED_UGI_KEY = "dfs.web.ugi";

  private static final @Tainted Log LOG = LogFactory.getLog(StaticUserWebFilter.class);

  static class User implements @Tainted Principal {
    private final @Tainted String name;
    public @Tainted User(@Tainted String name) {
      this.name = name;
    }
    @Override
    public @Tainted String getName(StaticUserWebFilter.@Tainted User this) {
      return name;
    }
    @Override
    public @Tainted int hashCode(StaticUserWebFilter.@Tainted User this) {
      return name.hashCode();
    }
    @Override
    public @Tainted boolean equals(StaticUserWebFilter.@Tainted User this, @Tainted Object other) {
      if (other == this) {
        return true;
      } else if (other == null || other.getClass() != getClass()) {
        return false;
      }
      return ((@Tainted User) other).name.equals(name);
    }
    @Override
    public @Tainted String toString(StaticUserWebFilter.@Tainted User this) {
      return name;
    }    
  }

  public static class StaticUserFilter implements @Tainted Filter {
    private @Tainted User user;
    private @Tainted String username;

    @Override
    public void destroy(StaticUserWebFilter.@Tainted StaticUserFilter this) {
      // NOTHING
    }

    @Override
    public void doFilter(StaticUserWebFilter.@Tainted StaticUserFilter this, @Tainted ServletRequest request, @Tainted ServletResponse response,
                         @Tainted
                         FilterChain chain
                         ) throws IOException, ServletException {
      @Tainted
      HttpServletRequest httpRequest = (@Tainted HttpServletRequest) request;
      // if the user is already authenticated, don't override it
      if (httpRequest.getRemoteUser() != null) {
        chain.doFilter(request, response);
      } else {
        @Tainted
        HttpServletRequestWrapper wrapper = 
            new @Tainted HttpServletRequestWrapper(httpRequest) {
          @Override
          public @Tainted Principal getUserPrincipal() {
            return user;
          }
          @Override
          public @Tainted String getRemoteUser() {
            return username;
          }
        };
        chain.doFilter(wrapper, response);
      }
    }

    @Override
    public void init(StaticUserWebFilter.@Tainted StaticUserFilter this, @Tainted FilterConfig conf) throws ServletException {
      this.username = conf.getInitParameter(HADOOP_HTTP_STATIC_USER);
      this.user = new @Tainted User(username);
    }
    
  }

  @Override
  public void initFilter(@Tainted StaticUserWebFilter this, @Tainted FilterContainer container, @Tainted Configuration conf) {
    @Tainted
    HashMap<@Tainted String, @Tainted String> options = new @Tainted HashMap<@Tainted String, @Tainted String>();
    
    @Tainted
    String username = getUsernameFromConf(conf);
    options.put(HADOOP_HTTP_STATIC_USER, username);

    container.addFilter("static_user_filter", 
                        StaticUserFilter.class.getName(), 
                        options);
  }

  /**
   * Retrieve the static username from the configuration.
   */
  static @Tainted String getUsernameFromConf(@Tainted Configuration conf) {
    @Tainted
    String oldStyleUgi = conf.get(DEPRECATED_UGI_KEY);
    if (oldStyleUgi != null) {
      // We can't use the normal configuration deprecation mechanism here
      // since we need to split out the username from the configured UGI.
      LOG.warn(DEPRECATED_UGI_KEY + " should not be used. Instead, use " + 
          HADOOP_HTTP_STATIC_USER + ".");
      @Tainted
      String @Tainted [] parts = oldStyleUgi.split(",");
      return parts[0];
    } else {
      return conf.get(HADOOP_HTTP_STATIC_USER,
        DEFAULT_HADOOP_HTTP_STATIC_USER);
    }
  }

}
