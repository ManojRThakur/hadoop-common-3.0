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

package org.apache.hadoop.metrics2.impl;

import org.checkerframework.checker.tainting.qual.Tainted;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import org.apache.hadoop.metrics2.MetricsRecord;

abstract class AbstractMetricsRecord implements @Tainted MetricsRecord {

  @Override public @Tainted boolean equals(@Tainted AbstractMetricsRecord this, @Tainted Object obj) {
    if (obj instanceof @Tainted MetricsRecord) {
      final @Tainted MetricsRecord other = (@Tainted MetricsRecord) obj;
      return Objects.equal(timestamp(), other.timestamp()) &&
             Objects.equal(name(), other.name()) &&
             Objects.equal(description(), other.description()) &&
             Objects.equal(tags(), other.tags()) &&
             Iterables.elementsEqual(metrics(), other.metrics());
    }
    return false;
  }

  // Should make sense most of the time when the record is used as a key
  @Override public @Tainted int hashCode(@Tainted AbstractMetricsRecord this) {
    return Objects.hashCode(name(), description(), tags());
  }

  @Override public @Tainted String toString(@Tainted AbstractMetricsRecord this) {
    return Objects.toStringHelper(this)
        .add("timestamp", timestamp())
        .add("name", name())
        .add("description", description())
        .add("tags", tags())
        .add("metrics", Iterables.toString(metrics()))
        .toString();
  }
}
