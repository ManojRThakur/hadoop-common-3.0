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
package org.apache.hadoop.metrics;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Utility class to simplify creation and reporting of hadoop metrics.
 *
 * For examples of usage, see NameNodeMetrics.
 * @see org.apache.hadoop.metrics.MetricsRecord
 * @see org.apache.hadoop.metrics.MetricsContext
 * @see org.apache.hadoop.metrics.ContextFactory
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class MetricsUtil {
    
  public static final @Tainted Log LOG =
    LogFactory.getLog(MetricsUtil.class);

  /**
   * Don't allow creation of a new instance of Metrics
   */
  private @Tainted MetricsUtil() {}
    
  public static @Tainted MetricsContext getContext(@Tainted String contextName) {
    return getContext(contextName, contextName);
  }

  /**
   * Utility method to return the named context.
   * If the desired context cannot be created for any reason, the exception
   * is logged, and a null context is returned.
   */
  public static @Tainted MetricsContext getContext(@Tainted String refName, @Tainted String contextName) {
    @Tainted
    MetricsContext metricsContext;
    try {
      metricsContext =
        ContextFactory.getFactory().getContext(refName, contextName);
      if (!metricsContext.isMonitoring()) {
        metricsContext.startMonitoring();
      }
    } catch (@Tainted Exception ex) {
      LOG.error("Unable to create metrics context " + contextName, ex);
      metricsContext = ContextFactory.getNullContext(contextName);
    }
    return metricsContext;
  }

  /**
   * Utility method to create and return new metrics record instance within the
   * given context. This record is tagged with the host name.
   *
   * @param context the context
   * @param recordName name of the record
   * @return newly created metrics record
   */
  public static @Tainted MetricsRecord createRecord(@Tainted MetricsContext context, 
                                           @Tainted
                                           String recordName) 
  {
    @Tainted
    MetricsRecord metricsRecord = context.createRecord(recordName);
    metricsRecord.setTag("hostName", getHostName());
    return metricsRecord;        
  }
    
  /**
   * Returns the host name.  If the host name is unobtainable, logs the
   * exception and returns "unknown".
   */
  private static @Tainted String getHostName() {
    @Tainted
    String hostName = null;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (@Tainted UnknownHostException ex) {
      LOG.info("Unable to obtain hostName", ex);
      hostName = "unknown";
    }
    return hostName;
  }

}
