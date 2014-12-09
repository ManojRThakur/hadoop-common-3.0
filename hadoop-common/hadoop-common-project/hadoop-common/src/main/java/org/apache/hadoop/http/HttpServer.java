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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.InterruptedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLServerSocketFactory;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.ConfServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.jmx.JMXJsonServlet;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.metrics.MetricsServlet;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.mortbay.io.Buffer;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.MimeTypes;
import org.mortbay.jetty.RequestLog;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/logs/" -> points to the log directory
 *   "/static/" -> points to common static files (src/webapps/static)
 *   "/" -> the jsp server code from (src/webapps/<name>)
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "HBase"})
@InterfaceStability.Evolving
public class HttpServer implements @Tainted FilterContainer {
  public static final @Tainted Log LOG = LogFactory.getLog(HttpServer.class);

  static final @Tainted String FILTER_INITIALIZER_PROPERTY
      = "hadoop.http.filter.initializers";
  static final @Tainted String HTTP_MAX_THREADS = "hadoop.http.max.threads";

  // The ServletContext attribute where the daemon Configuration
  // gets stored.
  public static final @Tainted String CONF_CONTEXT_ATTRIBUTE = "hadoop.conf";
  public static final @Tainted String ADMINS_ACL = "admins.acl";
  public static final @Tainted String SPNEGO_FILTER = "SpnegoFilter";
  public static final @Tainted String NO_CACHE_FILTER = "NoCacheFilter";

  public static final @Tainted String BIND_ADDRESS = "bind.address";

  private @Tainted AccessControlList adminsAcl;

  private @Tainted SSLFactory sslFactory;
  protected final @Tainted Server webServer;
  protected final @Tainted Connector listener;
  protected final @Tainted WebAppContext webAppContext;
  protected final @Tainted boolean findPort;
  protected final @Tainted Map<@Tainted Context, @Tainted Boolean> defaultContexts =
      new @Tainted HashMap<@Tainted Context, @Tainted Boolean>();
  protected final @Tainted List<@Tainted String> filterNames = new @Tainted ArrayList<@Tainted String>();
  static final @Tainted String STATE_DESCRIPTION_ALIVE = " - alive";
  static final @Tainted String STATE_DESCRIPTION_NOT_LIVE = " - not live";

  private final @Tainted boolean listenerStartedExternally;
  
  /**
   * Class to construct instances of HTTP server with specific options.
   */
  public static class Builder {
    @Tainted
    String name;
    @Untainted String bindAddress;
    @Tainted
    Integer port;
    @Tainted
    Boolean findPort;
    @Tainted
    Configuration conf;
    @Tainted
    Connector connector;
    @Tainted
    String @Tainted [] pathSpecs;
    @Tainted
    AccessControlList adminsAcl;
    @Tainted
    boolean securityEnabled = false;
    @Tainted
    String usernameConfKey = null;
    @Tainted
    String keytabConfKey = null;
    
    public @Tainted Builder setName(HttpServer.@Tainted Builder this, @Tainted String name){
      this.name = name;
      return this;
    }
    
    public @Tainted Builder setBindAddress(HttpServer.@Tainted Builder this, @Untainted String bindAddress){
      this.bindAddress = bindAddress;
      return this;
    }
    
    public @Tainted Builder setPort(HttpServer.@Tainted Builder this, @Tainted int port) {
      this.port = port;
      return this;
    }
    
    public @Tainted Builder setFindPort(HttpServer.@Tainted Builder this, @Tainted boolean findPort) {
      this.findPort = findPort;
      return this;
    }
    
    public @Tainted Builder setConf(HttpServer.@Tainted Builder this, @Tainted Configuration conf) {
      this.conf = conf;
      return this;
    }
    
    public @Tainted Builder setConnector(HttpServer.@Tainted Builder this, @Tainted Connector connector) {
      this.connector = connector;
      return this;
    }
    
    public @Tainted Builder setPathSpec(HttpServer.@Tainted Builder this, @Tainted String @Tainted [] pathSpec) {
      this.pathSpecs = pathSpec;
      return this;
    }
    
    public @Tainted Builder setACL(HttpServer.@Tainted Builder this, @Tainted AccessControlList acl) {
      this.adminsAcl = acl;
      return this;
    }
    
    public @Tainted Builder setSecurityEnabled(HttpServer.@Tainted Builder this, @Tainted boolean securityEnabled) {
      this.securityEnabled = securityEnabled;
      return this;
    }
    
    public @Tainted Builder setUsernameConfKey(HttpServer.@Tainted Builder this, @Tainted String usernameConfKey) {
      this.usernameConfKey = usernameConfKey;
      return this;
    }
    
    public @Tainted Builder setKeytabConfKey(HttpServer.@Tainted Builder this, @Tainted String keytabConfKey) {
      this.keytabConfKey = keytabConfKey;
      return this;
    }
    
    public @Tainted HttpServer build(HttpServer.@Tainted Builder this) throws IOException {
      if (this.name == null) {
        throw new @Tainted HadoopIllegalArgumentException("name is not set");
      }
      if (this.bindAddress == null) {
        throw new @Tainted HadoopIllegalArgumentException("bindAddress is not set");
      }
      if (this.port == null) {
        throw new @Tainted HadoopIllegalArgumentException("port is not set");
      }
      if (this.findPort == null) {
        throw new @Tainted HadoopIllegalArgumentException("findPort is not set");
      }
      
      if (this.conf == null) {
        conf = new @Tainted Configuration();
      }
      
      @Tainted
      HttpServer server = new @Tainted HttpServer(this.name, this.bindAddress, this.port,
      this.findPort, this.conf, this.adminsAcl, this.connector, this.pathSpecs);
      if (this.securityEnabled) {
        server.initSpnego(this.conf, this.usernameConfKey, this.keytabConfKey);
      }
      return server;
    }
  }
  
  /** Same as this(name, bindAddress, port, findPort, null); */
  @Deprecated
  public @Tainted HttpServer(@Tainted String name, @Untainted String bindAddress, @Tainted int port, @Tainted boolean findPort
      ) throws IOException {
    this(name, bindAddress, port, findPort, new @Tainted Configuration());
  }
  
  @Deprecated
  public @Tainted HttpServer(@Tainted String name, @Untainted String bindAddress, @Tainted int port,
      @Tainted
      boolean findPort, @Tainted Configuration conf, @Tainted Connector connector) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, connector, null);
  }

  /**
   * Create a status server on the given port. Allows you to specify the
   * path specifications that this server will be serving so that they will be
   * added to the filters properly.  
   * 
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   * @param pathSpecs Path specifications that this httpserver will be serving. 
   *        These will be added to any filters.
   */
  @Deprecated
  public @Tainted HttpServer(@Tainted String name, @Untainted String bindAddress, @Tainted int port,
      @Tainted
      boolean findPort, @Tainted Configuration conf, @Tainted String @Tainted [] pathSpecs) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, null, pathSpecs);
  }
  
  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   */
  @Deprecated
  public @Tainted HttpServer(@Tainted String name, @Untainted String bindAddress, @Tainted int port,
      @Tainted
      boolean findPort, @Tainted Configuration conf) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, null, null);
  }

  @Deprecated
  public @Tainted HttpServer(@Tainted String name, @Untainted String bindAddress, @Tainted int port,
      @Tainted
      boolean findPort, @Tainted Configuration conf, @Tainted AccessControlList adminsAcl) 
      throws IOException {
    this(name, bindAddress, port, findPort, conf, adminsAcl, null, null);
  }

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   * @param adminsAcl {@link AccessControlList} of the admins
   */
  @Deprecated
  public @Tainted HttpServer(@Tainted String name, @Untainted String bindAddress, @Tainted int port,
      @Tainted
      boolean findPort, @Tainted Configuration conf, @Tainted AccessControlList adminsAcl, 
      @Tainted
      Connector connector) throws IOException {
    this(name, bindAddress, port, findPort, conf, adminsAcl, connector, null);
  }

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   * @param adminsAcl {@link AccessControlList} of the admins
   * @param connector A jetty connection listener
   * @param pathSpecs Path specifications that this httpserver will be serving. 
   *        These will be added to any filters.
   */
  public @Tainted HttpServer(@Tainted String name, @Untainted String bindAddress, @Tainted int port,
      @Tainted
      boolean findPort, @Tainted Configuration conf, @Tainted AccessControlList adminsAcl, 
      @Tainted
      Connector connector, @Tainted String @Tainted [] pathSpecs) throws IOException {
    webServer = new @Tainted Server();
    this.findPort = findPort;
    this.adminsAcl = adminsAcl;
    
    if(connector == null) {
      listenerStartedExternally = false;
      if (HttpConfig.isSecure()) {
        sslFactory = new @Tainted SSLFactory(SSLFactory.Mode.SERVER, conf);
        try {
          sslFactory.init();
        } catch (@Tainted GeneralSecurityException ex) {
          throw new @Tainted IOException(ex);
        }
        @Tainted
        SslSocketConnector sslListener = new @Tainted SslSocketConnector() {
          @Override
          protected @Tainted SSLServerSocketFactory createFactory() throws Exception {
            return sslFactory.createSSLServerSocketFactory();
          }
        };
        listener = sslListener;
      } else {
        listener = createBaseListener(conf);
      }
      listener.setHost(bindAddress);
      listener.setPort(port);
    } else {
      listenerStartedExternally = true;
      listener = connector;
    }
    
    webServer.addConnector(listener);

    @Tainted
    int maxThreads = conf.getInt(HTTP_MAX_THREADS, -1);
    // If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
    // default value (currently 250).
    @Tainted
    QueuedThreadPool threadPool = maxThreads == -1 ?
        new @Tainted QueuedThreadPool() : new @Tainted QueuedThreadPool(maxThreads);
    threadPool.setDaemon(true);
    webServer.setThreadPool(threadPool);

    final @Tainted String appDir = getWebAppsPath(name);
    @Tainted
    ContextHandlerCollection contexts = new @Tainted ContextHandlerCollection();
    @Tainted
    RequestLog requestLog = HttpRequestLog.getRequestLog(name);

    if (requestLog != null) {
      @Tainted
      RequestLogHandler requestLogHandler = new @Tainted RequestLogHandler();
      requestLogHandler.setRequestLog(requestLog);
      @Tainted
      HandlerCollection handlers = new @Tainted HandlerCollection();
      handlers.setHandlers(new @Tainted Handler @Tainted [] {requestLogHandler, contexts});
      webServer.setHandler(handlers);
    }
    else {
      webServer.setHandler(contexts);
    }

    webAppContext = new @Tainted WebAppContext();
    webAppContext.setDisplayName(name);
    webAppContext.setContextPath("/");
    webAppContext.setWar(appDir + "/" + name);
    webAppContext.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    webAppContext.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
    addNoCacheFilter(webAppContext);
    webServer.addHandler(webAppContext);

    addDefaultApps(contexts, appDir, conf);
        
    addGlobalFilter("safety", QuotingInputFilter.class.getName(), null);
    final @Tainted FilterInitializer @Tainted [] initializers = getFilterInitializers(conf); 
    if (initializers != null) {
      conf = new @Tainted Configuration(conf);
      conf.set(BIND_ADDRESS, bindAddress);
      for(@Tainted FilterInitializer c : initializers) {
        c.initFilter(this, conf);
      }
    }

    addDefaultServlets();

    if (pathSpecs != null) {
      for (@Tainted String path : pathSpecs) {
        LOG.info("adding path spec: " + path);
        addFilterPathMapping(path, webAppContext);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void addNoCacheFilter(@Tainted HttpServer this, @Tainted WebAppContext ctxt) {
    defineFilter(ctxt, NO_CACHE_FILTER,
      NoCacheFilter.class.getName(), Collections.EMPTY_MAP, new @Tainted String @Tainted [] { "/*"});
  }

  /**
   * Create a required listener for the Jetty instance listening on the port
   * provided. This wrapper and all subclasses must create at least one
   * listener.
   */
  public @Tainted Connector createBaseListener(@Tainted HttpServer this, @Tainted Configuration conf) throws IOException {
    return HttpServer.createDefaultChannelConnector();
  }
  
  @InterfaceAudience.Private
  public static @Tainted Connector createDefaultChannelConnector() {
    @Tainted
    SelectChannelConnector ret = new @Tainted SelectChannelConnector();
    ret.setLowResourceMaxIdleTime(10000);
    ret.setAcceptQueueSize(128);
    ret.setResolveNames(false);
    ret.setUseDirectBuffers(false);
    if(Shell.WINDOWS) {
      // result of setting the SO_REUSEADDR flag is different on Windows
      // http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
      // without this 2 NN's can start on the same machine and listen on 
      // the same port with indeterminate routing of incoming requests to them
      ret.setReuseAddress(false);
    }
    ret.setHeaderBufferSize(1024*64);
    return ret;
  }

  /** Get an array of FilterConfiguration specified in the conf */
  private static @Tainted FilterInitializer @Tainted [] getFilterInitializers(@Tainted Configuration conf) {
    if (conf == null) {
      return null;
    }

    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] classes = conf.getClasses(FILTER_INITIALIZER_PROPERTY);
    if (classes == null) {
      return null;
    }

    @Tainted
    FilterInitializer @Tainted [] initializers = new @Tainted FilterInitializer @Tainted [classes.length];
    for(@Tainted int i = 0; i < classes.length; i++) {
      initializers[i] = (@Tainted FilterInitializer)ReflectionUtils.newInstance(
          classes[i], conf);
    }
    return initializers;
  }

  /**
   * Add default apps.
   * @param appDir The application directory
   * @throws IOException
   */
  protected void addDefaultApps(@Tainted HttpServer this, @Tainted ContextHandlerCollection parent,
      final @Tainted String appDir, @Tainted Configuration conf) throws IOException {
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined. 
    @Tainted
    String logDir = System.getProperty("hadoop.log.dir");
    if (logDir != null) {
      @Tainted
      Context logContext = new @Tainted Context(parent, "/logs");
      logContext.setResourceBase(logDir);
      logContext.addServlet(AdminAuthorizedServlet.class, "/*");
      if (conf.getBoolean(
          CommonConfigurationKeys.HADOOP_JETTY_LOGS_SERVE_ALIASES,
          CommonConfigurationKeys.DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES)) {
        logContext.getInitParams().put(
            "org.mortbay.jetty.servlet.Default.aliases", "true");
      }
      logContext.setDisplayName("logs");
      setContextAttributes(logContext, conf);
      addNoCacheFilter(webAppContext);
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    @Tainted
    Context staticContext = new @Tainted Context(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    staticContext.setDisplayName("static");
    setContextAttributes(staticContext, conf);
    defaultContexts.put(staticContext, true);
  }
  
  private void setContextAttributes(@Tainted HttpServer this, @Tainted Context context, @Tainted Configuration conf) {
    context.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    context.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
  }

  /**
   * Add default servlets.
   */
  protected void addDefaultServlets(@Tainted HttpServer this) {
    // set up default servlets
    addServlet("stacks", "/stacks", StackServlet.class);
    addServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
    addServlet("metrics", "/metrics", MetricsServlet.class);
    addServlet("jmx", "/jmx", JMXJsonServlet.class);
    addServlet("conf", "/conf", ConfServlet.class);
  }

  public void addContext(@Tainted HttpServer this, @Tainted Context ctxt, @Tainted boolean isFiltered)
      throws IOException {
    webServer.addHandler(ctxt);
    addNoCacheFilter(webAppContext);
    defaultContexts.put(ctxt, isFiltered);
  }

  /**
   * Add a context 
   * @param pathSpec The path spec for the context
   * @param dir The directory containing the context
   * @param isFiltered if true, the servlet is added to the filter path mapping 
   * @throws IOException
   */
  protected void addContext(@Tainted HttpServer this, @Tainted String pathSpec, @Tainted String dir, @Tainted boolean isFiltered) throws IOException {
    if (0 == webServer.getHandlers().length) {
      throw new @Tainted RuntimeException("Couldn't find handler");
    }
    @Tainted
    WebAppContext webAppCtx = new @Tainted WebAppContext();
    webAppCtx.setContextPath(pathSpec);
    webAppCtx.setWar(dir);
    addContext(webAppCtx, true);
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(@Tainted HttpServer this, @Tainted String name, @Tainted Object value) {
    webAppContext.setAttribute(name, value);
  }

  /** 
   * Add a Jersey resource package.
   * @param packageName The Java package name containing the Jersey resource.
   * @param pathSpec The path spec for the servlet
   */
  public void addJerseyResourcePackage(@Tainted HttpServer this, final @Tainted String packageName,
      final @Tainted String pathSpec) {
    LOG.info("addJerseyResourcePackage: packageName=" + packageName
        + ", pathSpec=" + pathSpec);
    final @Tainted ServletHolder sh = new @Tainted ServletHolder(ServletContainer.class);
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
        "com.sun.jersey.api.core.PackagesResourceConfig");
    sh.setInitParameter("com.sun.jersey.config.property.packages", packageName);
    webAppContext.addServlet(sh, pathSpec);
  }

  /**
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addServlet(@Tainted HttpServer this, @Tainted String name, @Tainted String pathSpec,
      @Tainted
      Class<@Tainted ? extends @Tainted HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz, false);
    addFilterPathMapping(pathSpec, webAppContext);
  }

  /**
   * Add an internal servlet in the server. 
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters are not enabled. 
   * 
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addInternalServlet(@Tainted HttpServer this, @Tainted String name, @Tainted String pathSpec,
      @Tainted
      Class<@Tainted ? extends @Tainted HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz, false);
  }

  /**
   * Add an internal servlet in the server, specifying whether or not to
   * protect with Kerberos authentication. 
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   +   * servlets added using this method, filters (except internal Kerberos
   * filters) are not enabled. 
   * 
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   * @param requireAuth Require Kerberos authenticate to access servlet
   */
  public void addInternalServlet(@Tainted HttpServer this, @Tainted String name, @Tainted String pathSpec, 
      @Tainted
      Class<@Tainted ? extends @Tainted HttpServlet> clazz, @Tainted boolean requireAuth) {
    @Tainted
    ServletHolder holder = new @Tainted ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);

    if(requireAuth && UserGroupInformation.isSecurityEnabled()) {
       LOG.info("Adding Kerberos (SPNEGO) filter to " + name);
       @Tainted
       ServletHandler handler = webAppContext.getServletHandler();
       @Tainted
       FilterMapping fmap = new @Tainted FilterMapping();
       fmap.setPathSpec(pathSpec);
       fmap.setFilterName(SPNEGO_FILTER);
       fmap.setDispatches(Handler.ALL);
       handler.addFilterMapping(fmap);
    }
  }

  @Override
  public void addFilter(@Tainted HttpServer this, @Tainted String name, @Tainted String classname,
      @Tainted
      Map<@Tainted String, @Tainted String> parameters) {

    final @Tainted String @Tainted [] USER_FACING_URLS = new String @Tainted [] { "*.html", "*.jsp" };
    defineFilter(webAppContext, name, classname, parameters, USER_FACING_URLS);
    LOG.info("Added filter " + name + " (class=" + classname
        + ") to context " + webAppContext.getDisplayName());
    final @Tainted String @Tainted [] ALL_URLS = new String @Tainted [] { "/*" };
    for (Map.@Tainted Entry<@Tainted Context, @Tainted Boolean> e : defaultContexts.entrySet()) {
      if (e.getValue()) {
        @Tainted
        Context ctx = e.getKey();
        defineFilter(ctx, name, classname, parameters, ALL_URLS);
        LOG.info("Added filter " + name + " (class=" + classname
            + ") to context " + ctx.getDisplayName());
      }
    }
    filterNames.add(name);
  }

  @Override
  public void addGlobalFilter(@Tainted HttpServer this, @Tainted String name, @Tainted String classname,
      @Tainted
      Map<@Tainted String, @Tainted String> parameters) {
    final @Tainted String @Tainted [] ALL_URLS = new String @Tainted [] { "/*" };
    defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
    for (@Tainted Context ctx : defaultContexts.keySet()) {
      defineFilter(ctx, name, classname, parameters, ALL_URLS);
    }
    LOG.info("Added global filter '" + name + "' (class=" + classname + ")");
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  public void defineFilter(@Tainted HttpServer this, @Tainted Context ctx, @Tainted String name,
      @Tainted
      String classname, @Tainted Map<@Tainted String, @Tainted String> parameters, @Tainted String @Tainted [] urls) {

    @Tainted
    FilterHolder holder = new @Tainted FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    holder.setInitParameters(parameters);
    @Tainted
    FilterMapping fmap = new @Tainted FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(Handler.ALL);
    fmap.setFilterName(name);
    @Tainted
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilter(holder, fmap);
  }

  /**
   * Add the path spec to the filter path mapping.
   * @param pathSpec The path spec
   * @param webAppCtx The WebApplicationContext to add to
   */
  protected void addFilterPathMapping(@Tainted HttpServer this, @Tainted String pathSpec,
      @Tainted
      Context webAppCtx) {
    @Tainted
    ServletHandler handler = webAppCtx.getServletHandler();
    for(@Tainted String name : filterNames) {
      @Tainted
      FilterMapping fmap = new @Tainted FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(Handler.ALL);
      handler.addFilterMapping(fmap);
    }
  }
  
  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public @Tainted Object getAttribute(@Tainted HttpServer this, @Tainted String name) {
    return webAppContext.getAttribute(name);
  }
  
  public @Tainted WebAppContext getWebAppContext(@Tainted HttpServer this){
    return this.webAppContext;
  }

  /**
   * Get the pathname to the webapps files.
   * @param appName eg "secondary" or "datanode"
   * @return the pathname as a URL
   * @throws FileNotFoundException if 'webapps' directory cannot be found on CLASSPATH.
   */
  protected @Tainted String getWebAppsPath(@Tainted HttpServer this, @Tainted String appName) throws FileNotFoundException {
    @Tainted
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null) 
      throw new @Tainted FileNotFoundException("webapps/" + appName
          + " not found in CLASSPATH");
    @Tainted
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Get the port that the server is on
   * @return the port
   */
  public @Tainted int getPort(@Tainted HttpServer this) {
    return webServer.getConnectors()[0].getLocalPort();
  }

  /**
   * Set the min, max number of worker threads (simultaneous connections).
   */
  public void setThreads(@Tainted HttpServer this, @Tainted int min, @Tainted int max) {
    @Tainted
    QueuedThreadPool pool = (@Tainted QueuedThreadPool) webServer.getThreadPool() ;
    pool.setMinThreads(min);
    pool.setMaxThreads(max);
  }

  /**
   * Configure an ssl listener on the server.
   * @param addr address to listen on
   * @param keystore location of the keystore
   * @param storPass password for the keystore
   * @param keyPass password for the key
   * @deprecated Use {@link #addSslListener(InetSocketAddress, Configuration, boolean)}
   */
  @Deprecated
  public void addSslListener(@Tainted HttpServer this, @Tainted InetSocketAddress addr, @Tainted String keystore,
      @Tainted
      String storPass, @Tainted String keyPass) throws IOException {
    if (webServer.isStarted()) {
      throw new @Tainted IOException("Failed to add ssl listener");
    }
    @Tainted
    SslSocketConnector sslListener = new @Tainted SslSocketConnector();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(keystore);
    sslListener.setPassword(storPass);
    sslListener.setKeyPassword(keyPass);
    webServer.addConnector(sslListener);
  }

  /**
   * Configure an ssl listener on the server.
   * @param addr address to listen on
   * @param sslConf conf to retrieve ssl options
   * @param needCertsAuth whether x509 certificate authentication is required
   */
  public void addSslListener(@Tainted HttpServer this, @Tainted InetSocketAddress addr, @Tainted Configuration sslConf,
      @Tainted
      boolean needCertsAuth) throws IOException {
    if (webServer.isStarted()) {
      throw new @Tainted IOException("Failed to add ssl listener");
    }
    if (needCertsAuth) {
      // setting up SSL truststore for authenticating clients
      System.setProperty("javax.net.ssl.trustStore", sslConf.get(
          "ssl.server.truststore.location", ""));
      System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
          "ssl.server.truststore.password", ""));
      System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
          "ssl.server.truststore.type", "jks"));
    }
    @Tainted
    SslSocketConnector sslListener = new @Tainted SslSocketConnector();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(sslConf.get("ssl.server.keystore.location"));
    sslListener.setPassword(sslConf.get("ssl.server.keystore.password", ""));
    sslListener.setKeyPassword(sslConf.get("ssl.server.keystore.keypassword", ""));
    sslListener.setKeystoreType(sslConf.get("ssl.server.keystore.type", "jks"));
    sslListener.setNeedClientAuth(needCertsAuth);
    webServer.addConnector(sslListener);
  }
  
  protected void initSpnego(@Tainted HttpServer this, @Tainted Configuration conf,
      @Tainted
      String usernameConfKey, @Tainted String keytabConfKey) throws IOException {
    @Tainted
    Map<@Tainted String, @Tainted String> params = new @Tainted HashMap<@Tainted String, @Tainted String>();
    @Tainted
    String principalInConf = conf.get(usernameConfKey);
    if (principalInConf != null && !principalInConf.isEmpty()) {
      params.put("kerberos.principal",
                 SecurityUtil.getServerPrincipal(principalInConf, listener.getHost()));
    }
    @Tainted
    String httpKeytab = conf.get(keytabConfKey);
    if (httpKeytab != null && !httpKeytab.isEmpty()) {
      params.put("kerberos.keytab", httpKeytab);
    }
    params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");
  
    defineFilter(webAppContext, SPNEGO_FILTER,
                 AuthenticationFilter.class.getName(), params, null);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start(@Tainted HttpServer this) throws IOException {
    try {
      try {
        openListener();
        LOG.info("Jetty bound to port " + listener.getLocalPort());
        webServer.start();
      } catch (@Tainted IOException ex) {
        LOG.info("HttpServer.start() threw a non Bind IOException", ex);
        throw ex;
      } catch (@Tainted MultiException ex) {
        LOG.info("HttpServer.start() threw a MultiException", ex);
        throw ex;
      }
      // Make sure there is no handler failures.
      @Tainted
      Handler @Tainted [] handlers = webServer.getHandlers();
      for (@Tainted int i = 0; i < handlers.length; i++) {
        if (handlers[i].isFailed()) {
          throw new @Tainted IOException(
              "Problem in starting http server. Server handlers failed");
        }
      }
      // Make sure there are no errors initializing the context.
      @Tainted
      Throwable unavailableException = webAppContext.getUnavailableException();
      if (unavailableException != null) {
        // Have to stop the webserver, or else its non-daemon threads
        // will hang forever.
        webServer.stop();
        throw new @Tainted IOException("Unable to initialize WebAppContext",
            unavailableException);
      }
    } catch (@Tainted IOException e) {
      throw e;
    } catch (@Tainted InterruptedException e) {
      throw (@Tainted IOException) new @Tainted InterruptedIOException(
          "Interrupted while starting HTTP server").initCause(e);
    } catch (@Tainted Exception e) {
      throw new @Tainted IOException("Problem starting http server", e);
    }
  }

  /**
   * Open the main listener for the server
   * @throws Exception
   */
  void openListener(@Tainted HttpServer this) throws Exception {
    if (listener.getLocalPort() != -1) { // it's already bound
      return;
    }
    if (listenerStartedExternally) { // Expect that listener was started securely
      throw new @Tainted Exception("Expected webserver's listener to be started " +
          "previously but wasn't");
    }
    @Tainted
    int port = listener.getPort();
    while (true) {
      // jetty has a bug where you can't reopen a listener that previously
      // failed to open w/o issuing a close first, even if the port is changed
      try {
        listener.close();
        listener.open();
        break;
      } catch (@Tainted BindException ex) {
        if (port == 0 || !findPort) {
          @Tainted
          BindException be = new @Tainted BindException(
              "Port in use: " + listener.getHost() + ":" + listener.getPort());
          be.initCause(ex);
          throw be;
        }
      }
      // try the next port number
      listener.setPort(++port);
      Thread.sleep(100);
    }
  }
  
  /**
   * Return the bind address of the listener.
   * @return InetSocketAddress of the listener
   */
  public @Tainted InetSocketAddress getListenerAddress(@Tainted HttpServer this) {
    @Tainted
    int port = listener.getLocalPort();
    if (port == -1) { // not bound, return requested port
      port = listener.getPort();
    }
    return new @Tainted InetSocketAddress(listener.getHost(), port);
  }
  
  /**
   * stop the server
   */
  public void stop(@Tainted HttpServer this) throws Exception {
    @Tainted
    MultiException exception = null;
    try {
      listener.close();
    } catch (@Tainted Exception e) {
      LOG.error("Error while stopping listener for webapp"
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    try {
      if (sslFactory != null) {
          sslFactory.destroy();
      }
    } catch (@Tainted Exception e) {
      LOG.error("Error while destroying the SSLFactory"
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    try {
      // clear & stop webAppContext attributes to avoid memory leaks.
      webAppContext.clearAttributes();
      webAppContext.stop();
    } catch (@Tainted Exception e) {
      LOG.error("Error while stopping web app context for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }
    try {
      webServer.stop();
    } catch (@Tainted Exception e) {
      LOG.error("Error while stopping web server for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    if (exception != null) {
      exception.ifExceptionThrow();
    }

  }

  private @Tainted MultiException addMultiException(@Tainted HttpServer this, @Tainted MultiException exception, @Tainted Exception e) {
    if(exception == null){
      exception = new @Tainted MultiException();
    }
    exception.add(e);
    return exception;
  }

  public void join(@Tainted HttpServer this) throws InterruptedException {
    webServer.join();
  }

  /**
   * Test for the availability of the web server
   * @return true if the web server is started, false otherwise
   */
  public @Tainted boolean isAlive(@Tainted HttpServer this) {
    return webServer != null && webServer.isStarted();
  }

  /**
   * Return the host and port of the HttpServer, if live
   * @return the classname and any HTTP URL
   */
  @Override
  public @Tainted String toString(@Tainted HttpServer this) {
    return listener != null ?
        ("HttpServer at http://" + listener.getHost() + ":" + listener.getLocalPort() + "/"
            + (isAlive() ? STATE_DESCRIPTION_ALIVE : STATE_DESCRIPTION_NOT_LIVE))
        : "Inactive HttpServer";
  }

  /**
   * Checks the user has privileges to access to instrumentation servlets.
   * <p/>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to FALSE
   * (default value) it always returns TRUE.
   * <p/>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to TRUE
   * it will check that if the current user is in the admin ACLS. If the user is
   * in the admin ACLs it returns TRUE, otherwise it returns FALSE.
   *
   * @param servletContext the servlet context.
   * @param request the servlet request.
   * @param response the servlet response.
   * @return TRUE/FALSE based on the logic decribed above.
   */
  public static @Tainted boolean isInstrumentationAccessAllowed(
    @Tainted
    ServletContext servletContext, @Tainted HttpServletRequest request,
    @Tainted
    HttpServletResponse response) throws IOException {
    @Tainted
    Configuration conf =
      (@Tainted Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);

    @Tainted
    boolean access = true;
    @Tainted
    boolean adminAccess = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
      false);
    if (adminAccess) {
      access = hasAdministratorAccess(servletContext, request, response);
    }
    return access;
  }

  /**
   * Does the user sending the HttpServletRequest has the administrator ACLs? If
   * it isn't the case, response will be modified to send an error to the user.
   * 
   * @param servletContext
   * @param request
   * @param response used to send the error response if user does not have admin access.
   * @return true if admin-authorized, false otherwise
   * @throws IOException
   */
  public static @Tainted boolean hasAdministratorAccess(
      @Tainted
      ServletContext servletContext, @Tainted HttpServletRequest request,
      @Tainted
      HttpServletResponse response) throws IOException {
    @Tainted
    Configuration conf =
        (@Tainted Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
    // If there is no authorization, anybody has administrator access.
    if (!conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      return true;
    }

    @Tainted
    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                         "Unauthenticated users are not " +
                         "authorized to access this page.");
      return false;
    }
    
    if (servletContext.getAttribute(ADMINS_ACL) != null &&
        !userHasAdministratorAccess(servletContext, remoteUser)) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "User "
          + remoteUser + " is unauthorized to access this page.");
      return false;
    }

    return true;
  }

  /**
   * Get the admin ACLs from the given ServletContext and check if the given
   * user is in the ACL.
   * 
   * @param servletContext the context containing the admin ACL.
   * @param remoteUser the remote user to check for.
   * @return true if the user is present in the ACL, false if no ACL is set or
   *         the user is not present
   */
  public static @Tainted boolean userHasAdministratorAccess(@Tainted ServletContext servletContext,
      @Tainted
      String remoteUser) {
    @Tainted
    AccessControlList adminsAcl = (@Tainted AccessControlList) servletContext
        .getAttribute(ADMINS_ACL);
    @Tainted
    UserGroupInformation remoteUserUGI =
        UserGroupInformation.createRemoteUser(remoteUser);
    return adminsAcl != null && adminsAcl.isUserAllowed(remoteUserUGI);
  }

  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends @Tainted HttpServlet {
    private static final @Tainted long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServer.@Tainted StackServlet this, @Tainted HttpServletRequest request, @Tainted HttpServletResponse response)
      throws ServletException, IOException {
      if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                     request, response)) {
        return;
      }
      response.setContentType("text/plain; charset=UTF-8");
      @Tainted
      PrintWriter out = response.getWriter();
      ReflectionUtils.printThreadInfo(out, "");
      out.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);      
    }
  }
  
  /**
   * A Servlet input filter that quotes all HTML active characters in the
   * parameter names and values. The goal is to quote the characters to make
   * all of the servlets resistant to cross-site scripting attacks.
   */
  public static class QuotingInputFilter implements @Tainted Filter {
    private @Tainted FilterConfig config;

    public static class RequestQuoter extends @Tainted HttpServletRequestWrapper {
      private final @Tainted HttpServletRequest rawRequest;
      public @Tainted RequestQuoter(@Tainted HttpServletRequest rawRequest) {
        super(rawRequest);
        this.rawRequest = rawRequest;
      }
      
      /**
       * Return the set of parameter names, quoting each name.
       */
      @SuppressWarnings("unchecked")
      @Override
      public @Tainted Enumeration<@Tainted String> getParameterNames(HttpServer.QuotingInputFilter.@Tainted RequestQuoter this) {
        return new @Tainted Enumeration<@Tainted String>() {
          private @Tainted Enumeration<@Tainted String> rawIterator =
            rawRequest.getParameterNames();
          @Override
          public @Tainted boolean hasMoreElements() {
            return rawIterator.hasMoreElements();
          }

          @Override
          public @Tainted String nextElement() {
            return HtmlQuoting.quoteHtmlChars(rawIterator.nextElement());
          }
        };
      }
      
      /**
       * Unquote the name and quote the value.
       */
      @Override
      public @Tainted String getParameter(HttpServer.QuotingInputFilter.@Tainted RequestQuoter this, @Tainted String name) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getParameter
                                     (HtmlQuoting.unquoteHtmlChars(name)));
      }
      
      @Override
      public @Tainted String @Tainted [] getParameterValues(HttpServer.QuotingInputFilter.@Tainted RequestQuoter this, @Tainted String name) {
        @Tainted
        String unquoteName = HtmlQuoting.unquoteHtmlChars(name);
        @Tainted
        String @Tainted [] unquoteValue = rawRequest.getParameterValues(unquoteName);
        if (unquoteValue == null) {
          return null;
        }
        @Tainted
        String @Tainted [] result = new @Tainted String @Tainted [unquoteValue.length];
        for(@Tainted int i=0; i < result.length; ++i) {
          result[i] = HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
        }
        return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      public @Tainted Map<@Tainted String, @Tainted String @Tainted []> getParameterMap(HttpServer.QuotingInputFilter.@Tainted RequestQuoter this) {
        @Tainted
        Map<@Tainted String, @Tainted String @Tainted []> result = new @Tainted HashMap<@Tainted String, @Tainted String @Tainted []>();
        @Tainted
        Map<@Tainted String, @Tainted String @Tainted []> raw = rawRequest.getParameterMap();
        for (Map.@Tainted Entry<@Tainted String, @Tainted String @Tainted []> item: raw.entrySet()) {
          @Tainted
          String @Tainted [] rawValue = item.getValue();
          @Tainted
          String @Tainted [] cookedValue = new @Tainted String @Tainted [rawValue.length];
          for(@Tainted int i=0; i< rawValue.length; ++i) {
            cookedValue[i] = HtmlQuoting.quoteHtmlChars(rawValue[i]);
          }
          result.put(HtmlQuoting.quoteHtmlChars(item.getKey()), cookedValue);
        }
        return result;
      }
      
      /**
       * Quote the url so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public @Tainted StringBuffer getRequestURL(HttpServer.QuotingInputFilter.@Tainted RequestQuoter this){
        @Tainted
        String url = rawRequest.getRequestURL().toString();
        return new @Tainted StringBuffer(HtmlQuoting.quoteHtmlChars(url));
      }
      
      /**
       * Quote the server name so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public @Tainted String getServerName(HttpServer.QuotingInputFilter.@Tainted RequestQuoter this) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getServerName());
      }
    }

    @Override
    public void init(HttpServer.@Tainted QuotingInputFilter this, @Tainted FilterConfig config) throws ServletException {
      this.config = config;
    }

    @Override
    public void destroy(HttpServer.@Tainted QuotingInputFilter this) {
    }

    @Override
    public void doFilter(HttpServer.@Tainted QuotingInputFilter this, @Tainted ServletRequest request, 
                         @Tainted
                         ServletResponse response,
                         @Tainted
                         FilterChain chain
                         ) throws IOException, ServletException {
      @Tainted
      HttpServletRequestWrapper quoted = 
        new @Tainted RequestQuoter((@Tainted HttpServletRequest) request);
      @Tainted
      HttpServletResponse httpResponse = (@Tainted HttpServletResponse) response;

      @Tainted
      String mime = inferMimeType(request);
      if (mime == null) {
        httpResponse.setContentType("text/plain; charset=utf-8");
      } else if (mime.startsWith("text/html")) {
        // HTML with unspecified encoding, we want to
        // force HTML with utf-8 encoding
        // This is to avoid the following security issue:
        // http://openmya.hacker.jp/hasegawa/security/utf7cs.html
        httpResponse.setContentType("text/html; charset=utf-8");
      } else if (mime.startsWith("application/xml")) {
        httpResponse.setContentType("text/xml; charset=utf-8");
      }
      chain.doFilter(quoted, httpResponse);
    }

    /**
     * Infer the mime type for the response based on the extension of the request
     * URI. Returns null if unknown.
     */
    private @Tainted String inferMimeType(HttpServer.@Tainted QuotingInputFilter this, @Tainted ServletRequest request) {
      @Tainted
      String path = ((@Tainted HttpServletRequest)request).getRequestURI();
      @Tainted
      ContextHandler.@Tainted SContext sContext = (@Tainted ContextHandler.@Tainted SContext)config.getServletContext();
      @Tainted
      MimeTypes mimes = sContext.getContextHandler().getMimeTypes();
      @Tainted
      Buffer mimeBuffer = mimes.getMimeByExtension(path);
      return (mimeBuffer == null) ? null : mimeBuffer.toString();
    }

  }
}
