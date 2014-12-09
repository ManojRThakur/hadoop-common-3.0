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

package org.apache.hadoop.metrics2;

import org.checkerframework.checker.tainting.qual.Tainted;
import com.google.common.base.Objects;
import static com.google.common.base.Preconditions.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The immutable metric
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractMetric implements @Tainted MetricsInfo {
  private final @Tainted MetricsInfo info;

  /**
   * Construct the metric
   * @param info  about the metric
   */
  protected @Tainted AbstractMetric(@Tainted MetricsInfo info) {
    this.info = checkNotNull(info, "metric info");
  }

  @Override public @Tainted String name(@Tainted AbstractMetric this) {
    return info.name();
  }

  @Override public @Tainted String description(@Tainted AbstractMetric this) {
    return info.description();
  }

  protected @Tainted MetricsInfo info(@Tainted AbstractMetric this) {
    return info;
  }

  /**
   * Get the value of the metric
   * @return the value of the metric
   */
  public abstract @Tainted Number value(@Tainted AbstractMetric this);

  /**
   * Get the type of the metric
   * @return the type of the metric
   */
  public abstract @Tainted MetricType type(@Tainted AbstractMetric this);

  /**
   * Accept a visitor interface
   * @param visitor of the metric
   */
  public abstract void visit(@Tainted AbstractMetric this, @Tainted MetricsVisitor visitor);

  @Override public @Tainted boolean equals(@Tainted AbstractMetric this, @Tainted Object obj) {
    if (obj instanceof @Tainted AbstractMetric) {
      final @Tainted AbstractMetric other = (@Tainted AbstractMetric) obj;
      return Objects.equal(info, other.info()) &&
             Objects.equal(value(), other.value());
    }
    return false;
  }

  @Override public @Tainted int hashCode(@Tainted AbstractMetric this) {
    return Objects.hashCode(info, value());
  }

  @Override public @Tainted String toString(@Tainted AbstractMetric this) {
    return Objects.toStringHelper(this)
        .add("info", info)
        .add("value", value())
        .toString();
  }
}
