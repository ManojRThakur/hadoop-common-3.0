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

package org.apache.hadoop.metrics2.impl;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.HashMap;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import static org.apache.hadoop.metrics2.impl.MetricsConfig.*;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.metrics2.util.Contracts.*;

/**
 * An adapter class for metrics source and associated filter and jmx impl
 */
class MetricsSourceAdapter implements @Tainted DynamicMBean {

  private static final @Tainted Log LOG = LogFactory.getLog(MetricsSourceAdapter.class);

  private final @Tainted String prefix;
  private final @Tainted String name;
  private final @Tainted MetricsSource source;
  private final @Tainted MetricsFilter recordFilter;
  private final @Tainted MetricsFilter metricFilter;
  private final @Tainted HashMap<@Tainted String, @Tainted Attribute> attrCache;
  private final @Tainted MBeanInfoBuilder infoBuilder;
  private final @Tainted Iterable<@Tainted MetricsTag> injectedTags;

  private @Tainted Iterable<@Tainted MetricsRecordImpl> lastRecs;
  private @Tainted long jmxCacheTS = 0;
  private @Tainted int jmxCacheTTL;
  private @Tainted MBeanInfo infoCache;
  private @Tainted ObjectName mbeanName;
  private final @Tainted boolean startMBeans;

  @Tainted
  MetricsSourceAdapter(@Tainted String prefix, @Tainted String name, @Tainted String description,
                       @Tainted
                       MetricsSource source, @Tainted Iterable<@Tainted MetricsTag> injectedTags,
                       @Tainted
                       MetricsFilter recordFilter, @Tainted MetricsFilter metricFilter,
                       @Tainted
                       int jmxCacheTTL, @Tainted boolean startMBeans) {
    this.prefix = checkNotNull(prefix, "prefix");
    this.name = checkNotNull(name, "name");
    this.source = checkNotNull(source, "source");
    attrCache = Maps.newHashMap();
    infoBuilder = new @Tainted MBeanInfoBuilder(name, description);
    this.injectedTags = injectedTags;
    this.recordFilter = recordFilter;
    this.metricFilter = metricFilter;
    this.jmxCacheTTL = checkArg(jmxCacheTTL, jmxCacheTTL > 0, "jmxCacheTTL");
    this.startMBeans = startMBeans;
  }

  @Tainted
  MetricsSourceAdapter(@Tainted String prefix, @Tainted String name, @Tainted String description,
                       @Tainted
                       MetricsSource source, @Tainted Iterable<@Tainted MetricsTag> injectedTags,
                       @Tainted
                       int period, @Tainted MetricsConfig conf) {
    this(prefix, name, description, source, injectedTags,
         conf.getFilter(RECORD_FILTER_KEY),
         conf.getFilter(METRIC_FILTER_KEY),
         period + 1, // hack to avoid most of the "innocuous" races.
         conf.getBoolean(START_MBEANS_KEY, true));
  }

  void start(@Tainted MetricsSourceAdapter this) {
    if (startMBeans) startMBeans();
  }

  @Override
  public @Tainted Object getAttribute(@Tainted MetricsSourceAdapter this, @Tainted String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    updateJmxCache();
    synchronized(this) {
      @Tainted
      Attribute a = attrCache.get(attribute);
      if (a == null) {
        throw new @Tainted AttributeNotFoundException(attribute +" not found");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(attribute +": "+ a);
      }
      return a.getValue();
    }
  }

  @Override
  public void setAttribute(@Tainted MetricsSourceAdapter this, @Tainted Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
             MBeanException, ReflectionException {
    throw new @Tainted UnsupportedOperationException("Metrics are read-only.");
  }

  @Override
  public @Tainted AttributeList getAttributes(@Tainted MetricsSourceAdapter this, @Tainted String @Tainted [] attributes) {
    updateJmxCache();
    synchronized(this) {
      @Tainted
      AttributeList ret = new @Tainted AttributeList();
      for (@Tainted String key : attributes) {
        @Tainted
        Attribute attr = attrCache.get(key);
        if (LOG.isDebugEnabled()) {
          LOG.debug(key +": "+ attr);
        }
        ret.add(attr);
      }
      return ret;
    }
  }

  @Override
  public @Tainted AttributeList setAttributes(@Tainted MetricsSourceAdapter this, @Tainted AttributeList attributes) {
    throw new @Tainted UnsupportedOperationException("Metrics are read-only.");
  }

  @Override
  public @Tainted Object invoke(@Tainted MetricsSourceAdapter this, @Tainted String actionName, @Tainted Object @Tainted [] params, @Tainted String @Tainted [] signature)
      throws MBeanException, ReflectionException {
    throw new @Tainted UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public @Tainted MBeanInfo getMBeanInfo(@Tainted MetricsSourceAdapter this) {
    updateJmxCache();
    return infoCache;
  }

  private void updateJmxCache(@Tainted MetricsSourceAdapter this) {
    @Tainted
    boolean getAllMetrics = false;
    synchronized(this) {
      if (Time.now() - jmxCacheTS >= jmxCacheTTL) {
        // temporarilly advance the expiry while updating the cache
        jmxCacheTS = Time.now() + jmxCacheTTL;
        if (lastRecs == null) {
          getAllMetrics = true;
        }
      }
      else {
        return;
      }
    }

    if (getAllMetrics) {
      @Tainted
      MetricsCollectorImpl builder = new @Tainted MetricsCollectorImpl();
      getMetrics(builder, true);
    }

    synchronized(this) {
      @Tainted
      int oldCacheSize = attrCache.size();
      @Tainted
      int newCacheSize = updateAttrCache();
      if (oldCacheSize < newCacheSize) {
        updateInfoCache();
      }
      jmxCacheTS = Time.now();
      lastRecs = null;  // in case regular interval update is not running
    }
  }

  @Tainted
  Iterable<@Tainted MetricsRecordImpl> getMetrics(@Tainted MetricsSourceAdapter this, @Tainted MetricsCollectorImpl builder,
                                         @Tainted
                                         boolean all) {
    builder.setRecordFilter(recordFilter).setMetricFilter(metricFilter);
    synchronized(this) {
      if (lastRecs == null && jmxCacheTS == 0) {
        all = true; // Get all the metrics to populate the sink caches
      }
    }
    try {
      source.getMetrics(builder, all);
    } catch (@Tainted Exception e) {
      LOG.error("Error getting metrics from source "+ name, e);
    }
    for (@Tainted MetricsRecordBuilderImpl rb : builder) {
      for (@Tainted MetricsTag t : injectedTags) {
        rb.add(t);
      }
    }
    synchronized(this) {
      lastRecs = builder.getRecords();
      return lastRecs;
    }
  }

  synchronized void stop(@Tainted MetricsSourceAdapter this) {
    stopMBeans();
  }

  synchronized void startMBeans(@Tainted MetricsSourceAdapter this) {
    if (mbeanName != null) {
      LOG.warn("MBean "+ name +" already initialized!");
      LOG.debug("Stacktrace: ", new @Tainted Throwable());
      return;
    }
    mbeanName = MBeans.register(prefix, name, this);
    LOG.debug("MBean for source "+ name +" registered.");
  }

  synchronized void stopMBeans(@Tainted MetricsSourceAdapter this) {
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
      mbeanName = null;
    }
  }

  private void updateInfoCache(@Tainted MetricsSourceAdapter this) {
    LOG.debug("Updating info cache...");
    infoCache = infoBuilder.reset(lastRecs).get();
    LOG.debug("Done");
  }

  private @Tainted int updateAttrCache(@Tainted MetricsSourceAdapter this) {
    LOG.debug("Updating attr cache...");
    @Tainted
    int recNo = 0;
    @Tainted
    int numMetrics = 0;
    for (@Tainted MetricsRecordImpl record : lastRecs) {
      for (@Tainted MetricsTag t : record.tags()) {
        setAttrCacheTag(t, recNo);
        ++numMetrics;
      }
      for (@Tainted AbstractMetric m : record.metrics()) {
        setAttrCacheMetric(m, recNo);
        ++numMetrics;
      }
      ++recNo;
    }
    LOG.debug("Done. # tags & metrics="+ numMetrics);
    return numMetrics;
  }

  private static @Tainted String tagName(@Tainted String name, @Tainted int recNo) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder(name.length() + 16);
    sb.append("tag.").append(name);
    if (recNo > 0) {
      sb.append('.').append(recNo);
    }
    return sb.toString();
  }

  private void setAttrCacheTag(@Tainted MetricsSourceAdapter this, @Tainted MetricsTag tag, @Tainted int recNo) {
    @Tainted
    String key = tagName(tag.name(), recNo);
    attrCache.put(key, new @Tainted Attribute(key, tag.value()));
  }

  private static @Tainted String metricName(@Tainted String name, @Tainted int recNo) {
    if (recNo == 0) {
      return name;
    }
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder(name.length() + 12);
    sb.append(name);
    if (recNo > 0) {
      sb.append('.').append(recNo);
    }
    return sb.toString();
  }

  private void setAttrCacheMetric(@Tainted MetricsSourceAdapter this, @Tainted AbstractMetric metric, @Tainted int recNo) {
    @Tainted
    String key = metricName(metric.name(), recNo);
    attrCache.put(key, new @Tainted Attribute(key, metric.value()));
  }

  @Tainted
  String name(@Tainted MetricsSourceAdapter this) {
    return name;
  }

  @Tainted
  MetricsSource source(@Tainted MetricsSourceAdapter this) {
    return source;
  }
}
