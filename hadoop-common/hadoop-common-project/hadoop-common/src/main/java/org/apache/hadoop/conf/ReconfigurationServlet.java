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

package org.apache.hadoop.conf;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import org.apache.commons.logging.*;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.Collection;
import java.util.Enumeration;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.util.StringUtils;

/**
 * A servlet for changing a node's configuration.
 *
 * Reloads the configuration file, verifies whether changes are
 * possible and asks the admin to approve the change.
 *
 */
public class ReconfigurationServlet extends @Tainted HttpServlet {
  
  private static final @Tainted long serialVersionUID = 1L;

  private static final @Tainted Log LOG =
    LogFactory.getLog(ReconfigurationServlet.class);

  // the prefix used to fing the attribute holding the reconfigurable 
  // for a given request
  //
  // we get the attribute prefix + servlet path
  public static final @Tainted String CONF_SERVLET_RECONFIGURABLE_PREFIX =
    "conf.servlet.reconfigurable.";
  
  @Override
  public void init(@Tainted ReconfigurationServlet this) throws ServletException {
    super.init();
  }

  private @Tainted Reconfigurable getReconfigurable(@Tainted ReconfigurationServlet this, @Tainted HttpServletRequest req) {
    LOG.info("servlet path: " + req.getServletPath());
    LOG.info("getting attribute: " + CONF_SERVLET_RECONFIGURABLE_PREFIX +
             req.getServletPath());
    return (@Tainted Reconfigurable)
      this.getServletContext().getAttribute(CONF_SERVLET_RECONFIGURABLE_PREFIX +
                                            req.getServletPath());
  }

  private void printHeader(@Tainted ReconfigurationServlet this, @Tainted PrintWriter out, @Tainted String nodeName) {
    out.print("<html><head>");
    out.printf("<title>%s Reconfiguration Utility</title>\n", 
               StringEscapeUtils.escapeHtml(nodeName));
    out.print("</head><body>\n");
    out.printf("<h1>%s Reconfiguration Utility</h1>\n",
               StringEscapeUtils.escapeHtml(nodeName));
  }

  private void printFooter(@Tainted ReconfigurationServlet this, @Tainted PrintWriter out) {
    out.print("</body></html>\n");
  }
  
  /**
   * Print configuration options that can be changed.
   */
  private void printConf(@Tainted ReconfigurationServlet this, @Tainted PrintWriter out, @Tainted Reconfigurable reconf) {
    @Tainted Configuration oldConf = reconf.getConf();
    @Tainted Configuration newConf = new @Tainted Configuration();

    @Tainted Collection<ReconfigurationUtil.@Tainted PropertyChange> changes =
      ReconfigurationUtil.getChangedProperties(newConf, oldConf);

    @Tainted boolean changeOK = true;
    
    out.println("<form action=\"\" method=\"post\">");
    out.println("<table border=\"1\">");
    out.println("<tr><th>Property</th><th>Old value</th>");
    out.println("<th>New value </th><th></th></tr>");
    for (ReconfigurationUtil.@Tainted PropertyChange c: changes) {
      out.print("<tr><td>");
      if (!reconf.isPropertyReconfigurable(c.prop)) {
        out.print("<font color=\"red\">" + 
                  StringEscapeUtils.escapeHtml(c.prop) + "</font>");
        changeOK = false;
      } else {
        out.print(StringEscapeUtils.escapeHtml(c.prop));
        out.print("<input type=\"hidden\" name=\"" +
                  StringEscapeUtils.escapeHtml(c.prop) + "\" value=\"" +
                  StringEscapeUtils.escapeHtml(c.newVal) + "\"/>");
      }
      out.print("</td><td>" +
                (c.oldVal == null ? "<it>default</it>" : 
                 StringEscapeUtils.escapeHtml(c.oldVal)) +
                "</td><td>" +
                (c.newVal == null ? "<it>default</it>" : 
                 StringEscapeUtils.escapeHtml(c.newVal)) +
                "</td>");
      out.print("</tr>\n");
    }
    out.println("</table>");
    if (!changeOK) {
      out.println("<p><font color=\"red\">WARNING: properties marked red" +
                  " will not be changed until the next restart.</font></p>");
    }
    out.println("<input type=\"submit\" value=\"Apply\" />");
    out.println("</form>");
  }

  @SuppressWarnings("unchecked")
  private @Tainted Enumeration<@Tainted String> getParams(@Tainted ReconfigurationServlet this, @Tainted HttpServletRequest req) {
    return (@Tainted Enumeration<@Tainted String>) req.getParameterNames();
  }

  /**
   * Apply configuratio changes after admin has approved them.
   */
  @SuppressWarnings("ostrusted:argument.type.incompatible") //buzzsaw, see below
  private void applyChanges(@Tainted ReconfigurationServlet this, @Tainted PrintWriter out, @Tainted Reconfigurable reconf,
                            @Tainted
                            HttpServletRequest req) 
    throws IOException, ReconfigurationException {
    @Tainted Configuration oldConf = reconf.getConf();
    @Tainted Configuration newConf = new @Tainted Configuration();

    @Tainted Enumeration<@Tainted String> params = getParams(req);

    synchronized(oldConf) {
      while (params.hasMoreElements()) {
        @Tainted String rawParam = params.nextElement();
        @Tainted String param = StringEscapeUtils.unescapeHtml( rawParam );

        @SuppressWarnings("ostrusted:cast.unsafe") // Buzzsaw or bug, not validation directly from http request.
        @Untainted String value = (@Untainted String) StringEscapeUtils.unescapeHtml( req.getParameter(rawParam) );
        if (value != null) {
          if (value.equals( newConf.getRaw( param ) ) || value.equals("default") ||
              value.equals("null") || value.isEmpty()) {
            if ((value.equals("default") || value.equals("null") || 
                 value.isEmpty()) && 
                oldConf.getRaw(param) != null) {
              out.println("<p>Changed \"" + 
                          StringEscapeUtils.escapeHtml(param) + "\" from \"" +
                          StringEscapeUtils.escapeHtml(oldConf.getRaw(param)) +
                          "\" to default</p>");
              reconf.reconfigureProperty(param, null);
            } else if (!value.equals("default") && !value.equals("null") &&
                       !value.isEmpty() && 
                       (oldConf.getRaw(param) == null || 
                        !oldConf.getRaw(param).equals(value))) {
              // change from default or value to different value
              if (oldConf.getRaw(param) == null) {
                out.println("<p>Changed \"" + 
                            StringEscapeUtils.escapeHtml(param) + 
                            "\" from default to \"" +
                            StringEscapeUtils.escapeHtml(value) + "\"</p>");
              } else {
                out.println("<p>Changed \"" + 
                            StringEscapeUtils.escapeHtml(param) + "\" from \"" +
                            StringEscapeUtils.escapeHtml(oldConf. getRaw(param)) +
                            "\" to \"" +
                            StringEscapeUtils.escapeHtml(value) + "\"</p>");
              }
              //ostrusted, value read from a HttpServletReques, the whole intent of this class is
              //to reconfigure from a servlet request, we suggest that this is either checked or secured
              //in some other fashion, so this is a buzzsaw
              reconf.reconfigureProperty(param, value);
            } else {
              LOG.info("property " + param + " unchanged");
            }
          } else {
            // parameter value != newConf value
            out.println("<p>\"" + StringEscapeUtils.escapeHtml(param) + 
                        "\" not changed because value has changed from \"" +
                        StringEscapeUtils.escapeHtml(value) + "\" to \"" +
                        StringEscapeUtils.escapeHtml(newConf.getRaw(param)) +
                        "\" since approval</p>");
          }
        }
      }
    }
  }

  @Override
  protected void doGet(@Tainted ReconfigurationServlet this, @Tainted HttpServletRequest req, @Tainted HttpServletResponse resp)
    throws ServletException, IOException {
    LOG.info("GET");
    @Tainted
    PrintWriter out = resp.getWriter();
    
    @Tainted
    Reconfigurable reconf = getReconfigurable(req);
    @Tainted String nodeName = reconf.getClass().getCanonicalName();

    printHeader(out, nodeName);
    printConf(out, reconf);
    printFooter(out);
  }

  @Override
  protected void doPost(@Tainted ReconfigurationServlet this, @Tainted HttpServletRequest req, @Tainted HttpServletResponse resp)
    throws ServletException, IOException {
    LOG.info("POST");
    @Tainted
    PrintWriter out = resp.getWriter();

    @Tainted
    Reconfigurable reconf = getReconfigurable(req);
    @Tainted
    String nodeName = reconf.getClass().getCanonicalName();

    printHeader(out, nodeName);

    try { 
      applyChanges(out, reconf, req);
    } catch (@Tainted ReconfigurationException e) {
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 
                     StringUtils.stringifyException(e));
      return;
    }

    out.println("<p><a href=\"" + req.getServletPath() + "\">back</a></p>");
    printFooter(out);
  }

}
