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
package org.apache.hadoop.metrics.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 
 * This is the registry for metrics.
 * Related set of metrics should be declared in a holding class and registered
 * in a registry for those metrics which is also stored in the the holding class.
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class MetricsRegistry {
  private @Tainted Map<@Tainted String, @Tainted MetricsBase> metricsList = new @Tainted HashMap<@Tainted String, @Tainted MetricsBase>();

  public @Tainted MetricsRegistry() {
  }
  
  /**
   * 
   * @return number of metrics in the registry
   */
  public @Tainted int size(@Tainted MetricsRegistry this) {
    return metricsList.size();
  }
  
  /**
   * Add a new metrics to the registry
   * @param metricsName - the name
   * @param theMetricsObj - the metrics
   * @throws IllegalArgumentException if a name is already registered
   */
  public synchronized void add(@Tainted MetricsRegistry this, final @Tainted String metricsName, final @Tainted MetricsBase theMetricsObj) {
    if (metricsList.containsKey(metricsName)) {
      throw new @Tainted IllegalArgumentException("Duplicate metricsName:" + metricsName);
    }
    metricsList.put(metricsName, theMetricsObj);
  }

  
  /**
   * 
   * @param metricsName
   * @return the metrics if there is one registered by the supplied name.
   *         Returns null if none is registered
   */
  public synchronized @Tainted MetricsBase get(@Tainted MetricsRegistry this, final @Tainted String metricsName) {
    return metricsList.get(metricsName);
  }
  
  
  /**
   * 
   * @return the list of metrics names
   */
  public synchronized @Tainted Collection<@Tainted String> getKeyList(@Tainted MetricsRegistry this) {
    return metricsList.keySet();
  }
  
  /**
   * 
   * @return the list of metrics
   */
  public synchronized @Tainted Collection<@Tainted MetricsBase> getMetricsList(@Tainted MetricsRegistry this) {
    return metricsList.values();
  }
}
