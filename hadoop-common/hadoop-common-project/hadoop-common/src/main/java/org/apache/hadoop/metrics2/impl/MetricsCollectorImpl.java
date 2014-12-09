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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsFilter;
import static org.apache.hadoop.metrics2.lib.Interns.*;

class MetricsCollectorImpl implements @Tainted MetricsCollector,
    @Tainted
    Iterable<@Tainted MetricsRecordBuilderImpl> {

  private final @Tainted List<@Tainted MetricsRecordBuilderImpl> rbs = Lists.newArrayList();
  private @Tainted MetricsFilter recordFilter;
  private @Tainted MetricsFilter metricFilter;

  @Override
  public @Tainted MetricsRecordBuilderImpl addRecord(@Tainted MetricsCollectorImpl this, @Tainted MetricsInfo info) {
    @Tainted
    boolean acceptable = recordFilter == null ||
                         recordFilter.accepts(info.name());
    @Tainted
    MetricsRecordBuilderImpl rb = new @Tainted MetricsRecordBuilderImpl(this, info,
        recordFilter, metricFilter, acceptable);
    if (acceptable) rbs.add(rb);
    return rb;
  }

  @Override
  public @Tainted MetricsRecordBuilderImpl addRecord(@Tainted MetricsCollectorImpl this, @Tainted String name) {
    return addRecord(info(name, name +" record"));
  }

  public @Tainted List<@Tainted MetricsRecordImpl> getRecords(@Tainted MetricsCollectorImpl this) {
    @Tainted
    List<@Tainted MetricsRecordImpl> recs = Lists.newArrayListWithCapacity(rbs.size());
    for (@Tainted MetricsRecordBuilderImpl rb : rbs) {
      @Tainted
      MetricsRecordImpl mr = rb.getRecord();
      if (mr != null) {
        recs.add(mr);
      }
    }
    return recs;
  }

  @Override
  public @Tainted Iterator<@Tainted MetricsRecordBuilderImpl> iterator(@Tainted MetricsCollectorImpl this) {
    return rbs.iterator();
  }

  void clear(@Tainted MetricsCollectorImpl this) { rbs.clear(); }

  @Tainted
  MetricsCollectorImpl setRecordFilter(@Tainted MetricsCollectorImpl this, @Tainted MetricsFilter rf) {
    recordFilter = rf;
    return this;
  }

  @Tainted
  MetricsCollectorImpl setMetricFilter(@Tainted MetricsCollectorImpl this, @Tainted MetricsFilter mf) {
    metricFilter = mf;
    return this;
  }
}
