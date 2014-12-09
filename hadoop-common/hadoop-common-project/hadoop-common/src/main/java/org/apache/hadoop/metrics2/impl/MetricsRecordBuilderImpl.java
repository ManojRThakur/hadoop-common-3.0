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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.util.Time;

/**
 * {@link MetricsRecordBuilder} implementation used for building metrics records
 * by the {@link MetricsCollector}. It provides the following functionality:
 * <ul>
 * <li>Allows configuring filters for metrics.
 * </ul>
 *
 */
class MetricsRecordBuilderImpl extends @Tainted MetricsRecordBuilder {
  private final @Tainted MetricsCollector parent;
  private final @Tainted long timestamp;
  private final @Tainted MetricsInfo recInfo;
  private final @Tainted List<@Tainted AbstractMetric> metrics;
  private final @Tainted List<@Tainted MetricsTag> tags;
  private final @Tainted MetricsFilter recordFilter, metricFilter;
  private final @Tainted boolean acceptable;

  /**
   * @param parent {@link MetricsCollector} using this record builder
   * @param info metrics information
   * @param rf
   * @param mf
   * @param acceptable
   */
  @Tainted
  MetricsRecordBuilderImpl(@Tainted MetricsCollector parent, @Tainted MetricsInfo info,
      @Tainted
      MetricsFilter rf, @Tainted MetricsFilter mf, @Tainted boolean acceptable) {
    this.parent = parent;
    timestamp = Time.now();
    recInfo = info;
    metrics = Lists.newArrayList();
    tags = Lists.newArrayList();
    recordFilter = rf;
    metricFilter = mf;
    this.acceptable = acceptable;
  }

  @Override
  public @Tainted MetricsCollector parent(@Tainted MetricsRecordBuilderImpl this) { return parent; }

  @Override
  public @Tainted MetricsRecordBuilderImpl tag(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsInfo info, @Tainted String value) {
    if (acceptable) {
      tags.add(Interns.tag(info, value));
    }
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl add(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsTag tag) {
    tags.add(tag);
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl add(@Tainted MetricsRecordBuilderImpl this, @Tainted AbstractMetric metric) {
    metrics.add(metric);
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl addCounter(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsInfo info, @Tainted int value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @Tainted MetricCounterInt(info, value));
    }
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl addCounter(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsInfo info, @Tainted long value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @Tainted MetricCounterLong(info, value));
    }
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl addGauge(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsInfo info, @Tainted int value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @Tainted MetricGaugeInt(info, value));
    }
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl addGauge(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsInfo info, @Tainted long value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @Tainted MetricGaugeLong(info, value));
    }
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl addGauge(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsInfo info, @Tainted float value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @Tainted MetricGaugeFloat(info, value));
    }
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl addGauge(@Tainted MetricsRecordBuilderImpl this, @Tainted MetricsInfo info, @Tainted double value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @Tainted MetricGaugeDouble(info, value));
    }
    return this;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl setContext(@Tainted MetricsRecordBuilderImpl this, @Tainted String value) {
    return tag(MsInfo.Context, value);
  }

  public @Tainted MetricsRecordImpl getRecord(@Tainted MetricsRecordBuilderImpl this) {
    if (acceptable && (recordFilter == null || recordFilter.accepts(tags))) {
      return new @Tainted MetricsRecordImpl(recInfo, timestamp, tags(), metrics());
    }
    return null;
  }

  @Tainted
  List<@Tainted MetricsTag> tags(@Tainted MetricsRecordBuilderImpl this) {
    return Collections.unmodifiableList(tags);
  }

  @Tainted
  List<@Tainted AbstractMetric> metrics(@Tainted MetricsRecordBuilderImpl this) {
    return Collections.unmodifiableList(metrics);
  }
}
