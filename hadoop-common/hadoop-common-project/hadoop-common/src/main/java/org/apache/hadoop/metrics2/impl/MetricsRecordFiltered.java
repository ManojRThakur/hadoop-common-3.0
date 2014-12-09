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
import java.util.Collection;

import com.google.common.collect.AbstractIterator;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

class MetricsRecordFiltered extends @Tainted AbstractMetricsRecord {
  private final @Tainted MetricsRecord delegate;
  private final @Tainted MetricsFilter filter;

  @Tainted
  MetricsRecordFiltered(@Tainted MetricsRecord delegate, @Tainted MetricsFilter filter) {
    this.delegate = delegate;
    this.filter = filter;
  }

  @Override public @Tainted long timestamp(@Tainted MetricsRecordFiltered this) {
    return delegate.timestamp();
  }

  @Override public @Tainted String name(@Tainted MetricsRecordFiltered this) {
    return delegate.name();
  }

  @Override public @Tainted String description(@Tainted MetricsRecordFiltered this) {
    return delegate.description();
  }

  @Override public @Tainted String context(@Tainted MetricsRecordFiltered this) {
    return delegate.context();
  }

  @Override public @Tainted Collection<@Tainted MetricsTag> tags(@Tainted MetricsRecordFiltered this) {
    return delegate.tags();
  }

  @Override public @Tainted Iterable<@Tainted AbstractMetric> metrics(@Tainted MetricsRecordFiltered this) {
    return new @Tainted Iterable<@Tainted AbstractMetric>() {
      final @Tainted Iterator<@Tainted AbstractMetric> it = delegate.metrics().iterator();
      @Override public @Tainted Iterator<@Tainted AbstractMetric> iterator() {
        return new @Tainted AbstractIterator<@Tainted AbstractMetric>() {
          @Override public AbstractMetric computeNext() {
            while (it.hasNext()) {
              AbstractMetric next = it.next();
              if (filter.accepts(next.name())) {
                return next;
              }
            }
            return endOfData();
          }
        };
      }
    };
  }
}
