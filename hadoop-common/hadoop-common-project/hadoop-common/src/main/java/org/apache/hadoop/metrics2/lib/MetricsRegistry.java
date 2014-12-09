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

package org.apache.hadoop.metrics2.lib;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;

/**
 * An optional metrics registry class for creating and maintaining a
 * collection of MetricsMutables, making writing metrics source easier.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsRegistry {
  private final @Tainted Map<@Tainted String, @Tainted MutableMetric> metricsMap = Maps.newLinkedHashMap();
  private final @Tainted Map<@Tainted String, @Tainted MetricsTag> tagsMap = Maps.newLinkedHashMap();
  private final @Tainted MetricsInfo metricsInfo;

  /**
   * Construct the registry with a record name
   * @param name  of the record of the metrics
   */
  public @Tainted MetricsRegistry(@Tainted String name) {
    metricsInfo = Interns.info(name, name);
  }

  /**
   * Construct the registry with a metadata object
   * @param info  the info object for the metrics record/group
   */
  public @Tainted MetricsRegistry(@Tainted MetricsInfo info) {
    metricsInfo = info;
  }

  /**
   * @return the info object of the metrics registry
   */
  public @Tainted MetricsInfo info(@Tainted MetricsRegistry this) {
    return metricsInfo;
  }

  /**
   * Get a metric by name
   * @param name  of the metric
   * @return the metric object
   */
  public synchronized @Tainted MutableMetric get(@Tainted MetricsRegistry this, @Tainted String name) {
    return metricsMap.get(name);
  }

  /**
   * Get a tag by name
   * @param name  of the tag
   * @return the tag object
   */
  public synchronized @Tainted MetricsTag getTag(@Tainted MetricsRegistry this, @Tainted String name) {
    return tagsMap.get(name);
  }

  /**
   * Create a mutable integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  public @Tainted MutableCounterInt newCounter(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc, @Tainted int iVal) {
    return newCounter(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public synchronized @Tainted MutableCounterInt newCounter(@Tainted MetricsRegistry this, @Tainted MetricsInfo info, @Tainted int iVal) {
    checkMetricName(info.name());
    @Tainted
    MutableCounterInt ret = new @Tainted MutableCounterInt(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable long integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  public @Tainted MutableCounterLong newCounter(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc, @Tainted long iVal) {
    return newCounter(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable long integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public synchronized
  @Tainted
  MutableCounterLong newCounter(@Tainted MetricsRegistry this, @Tainted MetricsInfo info, @Tainted long iVal) {
    checkMetricName(info.name());
    @Tainted
    MutableCounterLong ret = new @Tainted MutableCounterLong(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable integer gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public @Tainted MutableGaugeInt newGauge(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc, @Tainted int iVal) {
    return newGauge(Interns.info(name, desc), iVal);
  }
  /**
   * Create a mutable integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public synchronized @Tainted MutableGaugeInt newGauge(@Tainted MetricsRegistry this, @Tainted MetricsInfo info, @Tainted int iVal) {
    checkMetricName(info.name());
    @Tainted
    MutableGaugeInt ret = new @Tainted MutableGaugeInt(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable long integer gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public @Tainted MutableGaugeLong newGauge(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc, @Tainted long iVal) {
    return newGauge(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable long integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public synchronized @Tainted MutableGaugeLong newGauge(@Tainted MetricsRegistry this, @Tainted MetricsInfo info, @Tainted long iVal) {
    checkMetricName(info.name());
    @Tainted
    MutableGaugeLong ret = new @Tainted MutableGaugeLong(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable metric that estimates quantiles of a stream of values
   * @param name of the metric
   * @param desc metric description
   * @param sampleName of the metric (e.g., "Ops")
   * @param valueName of the metric (e.g., "Time" or "Latency")
   * @param interval rollover interval of estimator in seconds
   * @return a new quantile estimator object
   */
  public synchronized @Tainted MutableQuantiles newQuantiles(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc,
      @Tainted
      String sampleName, @Tainted String valueName, @Tainted int interval) {
    checkMetricName(name);
    @Tainted
    MutableQuantiles ret = 
        new @Tainted MutableQuantiles(name, desc, sampleName, valueName, interval);
    metricsMap.put(name, ret);
    return ret;
  }
  
  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @param extended    produce extended stat (stdev, min/max etc.) if true.
   * @return a new mutable stat metric object
   */
  public synchronized @Tainted MutableStat newStat(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc,
      @Tainted
      String sampleName, @Tainted String valueName, @Tainted boolean extended) {
    checkMetricName(name);
    @Tainted
    MutableStat ret =
        new @Tainted MutableStat(name, desc, sampleName, valueName, extended);
    metricsMap.put(name, ret);
    return ret;
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @return a new mutable metric object
   */
  public @Tainted MutableStat newStat(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc,
                             @Tainted
                             String sampleName, @Tainted String valueName) {
    return newStat(name, desc, sampleName, valueName, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @return a new mutable metric object
   */
  public @Tainted MutableRate newRate(@Tainted MetricsRegistry this, @Tainted String name) {
    return newRate(name, name, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @param description of the metric
   * @return a new mutable rate metric object
   */
  public @Tainted MutableRate newRate(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String description) {
    return newRate(name, description, false);
  }

  /**
   * Create a mutable rate metric (for throughput measurement)
   * @param name  of the metric
   * @param desc  description
   * @param extended  produce extended stat (stdev/min/max etc.) if true
   * @return a new mutable rate metric object
   */
  public @Tainted MutableRate newRate(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc, @Tainted boolean extended) {
    return newRate(name, desc, extended, true);
  }

  @InterfaceAudience.Private
  public synchronized @Tainted MutableRate newRate(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String desc,
      @Tainted
      boolean extended, @Tainted boolean returnExisting) {
    if (returnExisting) {
      @Tainted
      MutableMetric rate = metricsMap.get(name);
      if (rate != null) {
        if (rate instanceof @Tainted MutableRate) return (@Tainted MutableRate) rate;
        throw new @Tainted MetricsException("Unexpected metrics type "+ rate.getClass()
                                   +" for "+ name);
      }
    }
    checkMetricName(name);
    @Tainted
    MutableRate ret = new @Tainted MutableRate(name, desc, extended);
    metricsMap.put(name, ret);
    return ret;
  }

  synchronized void add(@Tainted MetricsRegistry this, @Tainted String name, @Tainted MutableMetric metric) {
    checkMetricName(name);
    metricsMap.put(name, metric);
  }

  /**
   * Add sample to a stat metric by name.
   * @param name  of the metric
   * @param value of the snapshot to add
   */
  public synchronized void add(@Tainted MetricsRegistry this, @Tainted String name, @Tainted long value) {
    @Tainted
    MutableMetric m = metricsMap.get(name);

    if (m != null) {
      if (m instanceof @Tainted MutableStat) {
        ((@Tainted MutableStat) m).add(value);
      }
      else {
        throw new @Tainted MetricsException("Unsupported add(value) for metric "+ name);
      }
    }
    else {
      metricsMap.put(name, newRate(name)); // default is a rate metric
      add(name, value);
    }
  }

  /**
   * Set the metrics context tag
   * @param name of the context
   * @return the registry itself as a convenience
   */
  public @Tainted MetricsRegistry setContext(@Tainted MetricsRegistry this, @Tainted String name) {
    return tag(MsInfo.Context, name, true);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return the registry (for keep adding tags)
   */
  public @Tainted MetricsRegistry tag(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String description, @Tainted String value) {
    return tag(name, description, value, false);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @param override  existing tag if true
   * @return the registry (for keep adding tags)
   */
  public @Tainted MetricsRegistry tag(@Tainted MetricsRegistry this, @Tainted String name, @Tainted String description, @Tainted String value,
                             @Tainted
                             boolean override) {
    return tag(Interns.info(name, description), value, override);
  }

  /**
   * Add a tag to the metrics
   * @param info  metadata of the tag
   * @param value of the tag
   * @param override existing tag if true
   * @return the registry (for keep adding tags etc.)
   */
  public synchronized
  @Tainted
  MetricsRegistry tag(@Tainted MetricsRegistry this, @Tainted MetricsInfo info, @Tainted String value, @Tainted boolean override) {
    if (!override) checkTagName(info.name());
    tagsMap.put(info.name(), Interns.tag(info, value));
    return this;
  }

  public @Tainted MetricsRegistry tag(@Tainted MetricsRegistry this, @Tainted MetricsInfo info, @Tainted String value) {
    return tag(info, value, false);
  }

  @Tainted
  Collection<@Tainted MetricsTag> tags(@Tainted MetricsRegistry this) {
    return tagsMap.values();
  }

  @Tainted
  Collection<@Tainted MutableMetric> metrics(@Tainted MetricsRegistry this) {
    return metricsMap.values();
  }

  private void checkMetricName(@Tainted MetricsRegistry this, @Tainted String name) {
    if (metricsMap.containsKey(name)) {
      throw new @Tainted MetricsException("Metric name "+ name +" already exists!");
    }
  }

  private void checkTagName(@Tainted MetricsRegistry this, @Tainted String name) {
    if (tagsMap.containsKey(name)) {
      throw new @Tainted MetricsException("Tag "+ name +" already exists!");
    }
  }

  /**
   * Sample all the mutable metrics and put the snapshot in the builder
   * @param builder to contain the metrics snapshot
   * @param all get all the metrics even if the values are not changed.
   */
  public synchronized void snapshot(@Tainted MetricsRegistry this, @Tainted MetricsRecordBuilder builder, @Tainted boolean all) {
    for (@Tainted MetricsTag tag : tags()) {
      builder.add(tag);
    }
    for (@Tainted MutableMetric metric : metrics()) {
      metric.snapshot(builder, all);
    }
  }

  @Override public @Tainted String toString(@Tainted MetricsRegistry this) {
    return Objects.toStringHelper(this)
        .add("info", metricsInfo).add("tags", tags()).add("metrics", metrics())
        .toString();
  }
}
