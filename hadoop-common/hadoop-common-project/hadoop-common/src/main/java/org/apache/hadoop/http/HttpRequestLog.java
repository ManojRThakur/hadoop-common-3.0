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
import java.util.HashMap;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogConfigurationException;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.mortbay.jetty.NCSARequestLog;
import org.mortbay.jetty.RequestLog;

/**
 * RequestLog object for use with Http
 */
public class HttpRequestLog {

  public static final @Tainted Log LOG = LogFactory.getLog(HttpRequestLog.class);
  private static final @Tainted HashMap<@Tainted String, @Tainted String> serverToComponent;

  static {
    serverToComponent = new @Tainted HashMap<@Tainted String, @Tainted String>();
    serverToComponent.put("cluster", "resourcemanager");
    serverToComponent.put("hdfs", "namenode");
    serverToComponent.put("node", "nodemanager");
  }

  public static @Tainted RequestLog getRequestLog(@Tainted String name) {

    @Tainted
    String lookup = serverToComponent.get(name);
    if (lookup != null) {
      name = lookup;
    }
    @Tainted
    String loggerName = "http.requests." + name;
    @Tainted
    String appenderName = name + "requestlog";
    @Tainted
    Log logger = LogFactory.getLog(loggerName);

    if (logger instanceof @Tainted Log4JLogger) {
      @Tainted
      Log4JLogger httpLog4JLog = (@Tainted Log4JLogger)logger;
      @Tainted
      Logger httpLogger = httpLog4JLog.getLogger();
      @Tainted
      Appender appender = null;

      try {
        appender = httpLogger.getAppender(appenderName);
      } catch (@Tainted LogConfigurationException e) {
        LOG.warn("Http request log for " + loggerName
            + " could not be created");
        throw e;
      }

      if (appender == null) {
        LOG.info("Http request log for " + loggerName
            + " is not defined");
        return null;
      }

      if (appender instanceof @Tainted HttpRequestLogAppender) {
        @Tainted
        HttpRequestLogAppender requestLogAppender
          = (@Tainted HttpRequestLogAppender)appender;
        @Tainted
        NCSARequestLog requestLog = new @Tainted NCSARequestLog();
        requestLog.setFilename(requestLogAppender.getFilename());
        requestLog.setRetainDays(requestLogAppender.getRetainDays());
        return requestLog;
      }
      else {
        LOG.warn("Jetty request log for " + loggerName
            + " was of the wrong class");
        return null;
      }
    }
    else {
      LOG.warn("Jetty request log can only be enabled using Log4j");
      return null;
    }
  }
}
