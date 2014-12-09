/*
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

package org.apache.hadoop.metrics2.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

/**
 * A metrics cache for sinks that don't support sparse updates.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsCache {
  static final @Tainted Log LOG = LogFactory.getLog(MetricsCache.class);
  static final @Tainted int MAX_RECS_PER_NAME_DEFAULT = 1000;

  private final @Tainted Map<@Tainted String, @Tainted RecordCache> map = Maps.newHashMap();
  private final @Tainted int maxRecsPerName;

  class RecordCache
      extends @Tainted LinkedHashMap<@Tainted Collection<@Tainted MetricsTag>, @Tainted Record> {
    private static final @Tainted long serialVersionUID = 1L;
    private @Tainted boolean gotOverflow = false;

    @Override
    protected @Tainted boolean removeEldestEntry(@Tainted MetricsCache.RecordCache this, Map.@Tainted Entry<@Tainted Collection<@Tainted MetricsTag>,
                                                  @Tainted
                                                  Record> eldest) {
      @Tainted
      boolean overflow = size() > maxRecsPerName;
      if (overflow && !gotOverflow) {
        LOG.warn("Metrics cache overflow at "+ size() +" for "+ eldest);
        gotOverflow = true;
      }
      return overflow;
    }
  }

  /**
   * Cached record
   */
  public static class Record {
    final @Tainted Map<@Tainted String, @Tainted String> tags = Maps.newHashMap();
    final @Tainted Map<@Tainted String, @Tainted AbstractMetric> metrics = Maps.newHashMap();

    /**
     * Lookup a tag value
     * @param key name of the tag
     * @return the tag value
     */
    public @Tainted String getTag(MetricsCache.@Tainted Record this, @Tainted String key) {
      return tags.get(key);
    }

    /**
     * Lookup a metric value
     * @param key name of the metric
     * @return the metric value
     */
    public @Tainted Number getMetric(MetricsCache.@Tainted Record this, @Tainted String key) {
      @Tainted
      AbstractMetric metric = metrics.get(key);
      return metric != null ? metric.value() : null;
    }

    /**
     * Lookup a metric instance
     * @param key name of the metric
     * @return the metric instance
     */
    public @Tainted AbstractMetric getMetricInstance(MetricsCache.@Tainted Record this, @Tainted String key) {
      return metrics.get(key);
    }

    /**
     * @return the entry set of the tags of the record
     */
    public @Tainted Set<Map.@Tainted Entry<@Tainted String, @Tainted String>> tags(MetricsCache.@Tainted Record this) {
      return tags.entrySet();
    }

    /**
     * @deprecated use metricsEntrySet() instead
     * @return entry set of metrics
     */
    @Deprecated
    public @Tainted Set<Map.@Tainted Entry<@Tainted String, @Tainted Number>> metrics(MetricsCache.@Tainted Record this) {
      @Tainted
      Map<@Tainted String, @Tainted Number> map = new @Tainted LinkedHashMap<@Tainted String, @Tainted Number>(
          metrics.size());
      for (Map.@Tainted Entry<@Tainted String, @Tainted AbstractMetric> mapEntry : metrics.entrySet()) {
        map.put(mapEntry.getKey(), mapEntry.getValue().value());
      }
      return map.entrySet();
    }

    /**
     * @return entry set of metrics
     */
    public @Tainted Set<Map.@Tainted Entry<@Tainted String, @Tainted AbstractMetric>> metricsEntrySet(MetricsCache.@Tainted Record this) {
      return metrics.entrySet();
    }

    @Override public @Tainted String toString(MetricsCache.@Tainted Record this) {
      return Objects.toStringHelper(this)
          .add("tags", tags).add("metrics", metrics)
          .toString();
    }
  }

  public @Tainted MetricsCache() {
    this(MAX_RECS_PER_NAME_DEFAULT);
  }

  /**
   * Construct a metrics cache
   * @param maxRecsPerName  limit of the number records per record name
   */
  public @Tainted MetricsCache(@Tainted int maxRecsPerName) {
    this.maxRecsPerName = maxRecsPerName;
  }

  /**
   * Update the cache and return the current cached record
   * @param mr the update record
   * @param includingTags cache tag values (for later lookup by name) if true
   * @return the updated cache record
   */
  public @Tainted Record update(@Tainted MetricsCache this, @Tainted MetricsRecord mr, @Tainted boolean includingTags) {
    @Tainted
    String name = mr.name();
    @Tainted
    RecordCache recordCache = map.get(name);
    if (recordCache == null) {
      recordCache = new @Tainted RecordCache();
      map.put(name, recordCache);
    }
    @Tainted
    Collection<@Tainted MetricsTag> tags = mr.tags();
    @Tainted
    Record record = recordCache.get(tags);
    if (record == null) {
      record = new @Tainted Record();
      recordCache.put(tags, record);
    }
    for (@Tainted AbstractMetric m : mr.metrics()) {
      record.metrics.put(m.name(), m);
    }
    if (includingTags) {
      // mostly for some sinks that include tags as part of a dense schema
      for (@Tainted MetricsTag t : mr.tags()) {
        record.tags.put(t.name(), t.value());
      }
    }
    return record;
  }

  /**
   * Update the cache and return the current cache record
   * @param mr the update record
   * @return the updated cache record
   */
  public @Tainted Record update(@Tainted MetricsCache this, @Tainted MetricsRecord mr) {
    return update(mr, false);
  }

  /**
   * Get the cached record
   * @param name of the record
   * @param tags of the record
   * @return the cached record or null
   */
  public @Tainted Record get(@Tainted MetricsCache this, @Tainted String name, @Tainted Collection<@Tainted MetricsTag> tags) {
    @Tainted
    RecordCache rc = map.get(name);
    if (rc == null) return null;
    return rc.get(tags);
  }
}
