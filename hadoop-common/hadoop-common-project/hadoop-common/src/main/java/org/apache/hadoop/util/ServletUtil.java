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
package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.*;
import java.util.Calendar;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ServletUtil {
  /**
   * Initial HTML header
   */
  public static @Tainted PrintWriter initHTML(@Tainted ServletResponse response, @Tainted String title
      ) throws IOException {
    response.setContentType("text/html");
    @Tainted
    PrintWriter out = response.getWriter();
    out.println("<html>\n"
        + "<link rel='stylesheet' type='text/css' href='/static/hadoop.css'>\n"
        + "<title>" + title + "</title>\n"
        + "<body>\n"
        + "<h1>" + title + "</h1>\n");
    return out;
  }

  /**
   * Get a parameter from a ServletRequest.
   * Return null if the parameter contains only white spaces.
   */
  public static @Tainted String getParameter(@Tainted ServletRequest request, @Tainted String name) {
    @Tainted
    String s = request.getParameter(name);
    if (s == null) {
      return null;
    }
    s = s.trim();
    return s.length() == 0? null: s;
  }
  
  /**
   * @return a long value as passed in the given parameter, throwing
   * an exception if it is not present or if it is not a valid number.
   */
  public static @Tainted long parseLongParam(@Tainted ServletRequest request, @Tainted String param)
      throws IOException {
    @Tainted
    String paramStr = request.getParameter(param);
    if (paramStr == null) {
      throw new @Tainted IOException("Invalid request has no " + param + " parameter");
    }
    
    return Long.valueOf(paramStr);
  }

  public static final @Tainted String HTML_TAIL = "<hr />\n"
    + "<a href='http://hadoop.apache.org/core'>Hadoop</a>, " 
    + Calendar.getInstance().get(Calendar.YEAR) + ".\n"
    + "</body></html>";
  
  /**
   * HTML footer to be added in the jsps.
   * @return the HTML footer.
   */
  public static @Tainted String htmlFooter() {
    return HTML_TAIL;
  }
  
  /**
   * Generate the percentage graph and returns HTML representation string
   * of the same.
   * 
   * @param perc The percentage value for which graph is to be generated
   * @param width The width of the display table
   * @return HTML String representation of the percentage graph
   * @throws IOException
   */
  public static @Tainted String percentageGraph(@Tainted int perc, @Tainted int width) throws IOException {
    assert perc >= 0; assert perc <= 100;

    @Tainted
    StringBuilder builder = new @Tainted StringBuilder();

    builder.append("<table border=\"1px\" width=\""); builder.append(width);
    builder.append("px\"><tr>");
    if(perc > 0) {
      builder.append("<td cellspacing=\"0\" class=\"perc_filled\" width=\"");
      builder.append(perc); builder.append("%\"></td>");
    }if(perc < 100) {
      builder.append("<td cellspacing=\"0\" class=\"perc_nonfilled\" width=\"");
      builder.append(100 - perc); builder.append("%\"></td>");
    }
    builder.append("</tr></table>");
    return builder.toString();
  }
  
  /**
   * Generate the percentage graph and returns HTML representation string
   * of the same.
   * @param perc The percentage value for which graph is to be generated
   * @param width The width of the display table
   * @return HTML String representation of the percentage graph
   * @throws IOException
   */
  public static @Tainted String percentageGraph(@Tainted float perc, @Tainted int width) throws IOException {
    return percentageGraph((@Tainted int)perc, width);
  }

  /**
   * Escape and encode a string regarded as within the query component of an URI.
   * @param value the value to encode
   * @return encoded query, null if the default charset is not supported
   */
  public static @Tainted String encodeQueryValue(final @Tainted String value) {
    try {
      return URIUtil.encodeWithinQuery(value, "UTF-8");
    } catch (@Tainted URIException e) {
      throw new @Tainted AssertionError("JVM does not support UTF-8"); // should never happen!
    }
  }

  /**
   * Escape and encode a string regarded as the path component of an URI.
   * @param path the path component to encode
   * @return encoded path, null if UTF-8 is not supported
   */
  public static @Tainted String encodePath(final @Tainted String path) {
    try {
      return URIUtil.encodePath(path, "UTF-8");
    } catch (@Tainted URIException e) {
      throw new @Tainted AssertionError("JVM does not support UTF-8"); // should never happen!
    }
  }

  /**
   * Parse and decode the path component from the given request.
   * @param request Http request to parse
   * @param servletName the name of servlet that precedes the path
   * @return decoded path component, null if UTF-8 is not supported
   */
  public static @Tainted String getDecodedPath(final @Tainted HttpServletRequest request, @Tainted String servletName) {
    try {
      return URIUtil.decode(getRawPath(request, servletName), "UTF-8");
    } catch (@Tainted URIException e) {
      throw new @Tainted AssertionError("JVM does not support UTF-8"); // should never happen!
    }
  }

  /**
   * Parse the path component from the given request and return w/o decoding.
   * @param request Http request to parse
   * @param servletName the name of servlet that precedes the path
   * @return path component, null if the default charset is not supported
   */
  public static @Tainted String getRawPath(final @Tainted HttpServletRequest request, @Tainted String servletName) {
    Preconditions.checkArgument(request.getRequestURI().startsWith(servletName+"/"));
    return request.getRequestURI().substring(servletName.length());
  }
}