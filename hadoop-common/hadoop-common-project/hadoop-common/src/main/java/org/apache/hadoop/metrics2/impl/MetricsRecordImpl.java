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
import java.util.List;

import static com.google.common.base.Preconditions.*;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsTag;
import static org.apache.hadoop.metrics2.util.Contracts.*;

class MetricsRecordImpl extends @Tainted AbstractMetricsRecord {
  protected static final @Tainted String DEFAULT_CONTEXT = "default";

  private final @Tainted long timestamp;
  private final @Tainted MetricsInfo info;
  private final @Tainted List<@Tainted MetricsTag> tags;
  private final @Tainted Iterable<@Tainted AbstractMetric> metrics;

  /**
   * Construct a metrics record
   * @param info  {@link MetricsInfo} of the record
   * @param timestamp of the record
   * @param tags  of the record
   * @param metrics of the record
   */
  public @Tainted MetricsRecordImpl(@Tainted MetricsInfo info, @Tainted long timestamp,
                           @Tainted
                           List<@Tainted MetricsTag> tags,
                           @Tainted
                           Iterable<@Tainted AbstractMetric> metrics) {
    this.timestamp = checkArg(timestamp, timestamp > 0, "timestamp");
    this.info = checkNotNull(info, "info");
    this.tags = checkNotNull(tags, "tags");
    this.metrics = checkNotNull(metrics, "metrics");
  }

  @Override public @Tainted long timestamp(@Tainted MetricsRecordImpl this) {
    return timestamp;
  }

  @Override public @Tainted String name(@Tainted MetricsRecordImpl this) {
    return info.name();
  }

  @Tainted
  MetricsInfo info(@Tainted MetricsRecordImpl this) {
    return info;
  }

  @Override public @Tainted String description(@Tainted MetricsRecordImpl this) {
    return info.description();
  }

  @Override public @Tainted String context(@Tainted MetricsRecordImpl this) {
    // usually the first tag
    for (@Tainted MetricsTag t : tags) {
      if (t.info() == MsInfo.Context) {
        return t.value();
      }
    }
    return DEFAULT_CONTEXT;
  }

  @Override
  public @Tainted List<@Tainted MetricsTag> tags(@Tainted MetricsRecordImpl this) {
    return tags; // already unmodifiable from MetricsRecordBuilderImpl#tags
  }

  @Override public @Tainted Iterable<@Tainted AbstractMetric> metrics(@Tainted MetricsRecordImpl this) {
    return metrics;
  }
}
