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

/**
 * An immutable element for the sink queues.
 */
class MetricsBuffer implements @Tainted Iterable<MetricsBuffer.@Tainted Entry> {

  private final @Tainted Iterable<@Tainted Entry> mutable;

  @Tainted
  MetricsBuffer(@Tainted Iterable<MetricsBuffer.@Tainted Entry> mutable) {
    this.mutable = mutable;
  }

  @Override
  public @Tainted Iterator<@Tainted Entry> iterator(@Tainted MetricsBuffer this) {
    return mutable.iterator();
  }

  static class Entry {
    private final @Tainted String sourceName;
    private final @Tainted Iterable<@Tainted MetricsRecordImpl> records;

    @Tainted
    Entry(@Tainted String name, @Tainted Iterable<@Tainted MetricsRecordImpl> records) {
      sourceName = name;
      this.records = records;
    }

    @Tainted
    String name(MetricsBuffer.@Tainted Entry this) {
      return sourceName;
    }

    @Tainted
    Iterable<@Tainted MetricsRecordImpl> records(MetricsBuffer.@Tainted Entry this) {
      return records;
    }
  }

}
