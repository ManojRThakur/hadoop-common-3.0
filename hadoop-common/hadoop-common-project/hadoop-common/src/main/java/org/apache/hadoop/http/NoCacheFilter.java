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
package org.apache.hadoop.http;

import org.checkerframework.checker.tainting.qual.Tainted;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class NoCacheFilter implements @Tainted Filter {

  @Override
  public void init(@Tainted NoCacheFilter this, @Tainted FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(@Tainted NoCacheFilter this, @Tainted ServletRequest req, @Tainted ServletResponse res,
                       @Tainted
                       FilterChain chain)
    throws IOException, ServletException {
    @Tainted
    HttpServletResponse httpRes = (@Tainted HttpServletResponse) res;
    httpRes.setHeader("Cache-Control", "no-cache");
    @Tainted
    long now = System.currentTimeMillis();
    httpRes.addDateHeader("Expires", now);
    httpRes.addDateHeader("Date", now);
    httpRes.addHeader("Pragma", "no-cache");
    chain.doFilter(req, res);
  }

  @Override
  public void destroy(@Tainted NoCacheFilter this) {
  }

}
