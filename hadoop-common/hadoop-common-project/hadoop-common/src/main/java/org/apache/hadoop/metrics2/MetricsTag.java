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
 * Immutable tag for metrics (for grouping on host/queue/username etc.)
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsTag implements @Tainted MetricsInfo {
  private final @Tainted MetricsInfo info;
  private final @Tainted String value;

  /**
   * Construct the tag with name, description and value
   * @param info  of the tag
   * @param value of the tag
   */
  public @Tainted MetricsTag(@Tainted MetricsInfo info, @Tainted String value) {
    this.info = checkNotNull(info, "tag info");
    this.value = value;
  }

  @Override public @Tainted String name(@Tainted MetricsTag this) {
    return info.name();
  }

  @Override public @Tainted String description(@Tainted MetricsTag this) {
    return info.description();
  }

  /**
   * @return the info object of the tag
   */
  public @Tainted MetricsInfo info(@Tainted MetricsTag this) {
    return info;
  }

  /**
   * Get the value of the tag
   * @return  the value
   */
  public @Tainted String value(@Tainted MetricsTag this) {
    return value;
  }

  @Override public @Tainted boolean equals(@Tainted MetricsTag this, @Tainted Object obj) {
    if (obj instanceof @Tainted MetricsTag) {
      final @Tainted MetricsTag other = (@Tainted MetricsTag) obj;
      return Objects.equal(info, other.info()) &&
             Objects.equal(value, other.value());
    }
    return false;
  }

  @Override public @Tainted int hashCode(@Tainted MetricsTag this) {
    return Objects.hashCode(info, value);
  }

  @Override public @Tainted String toString(@Tainted MetricsTag this) {
    return Objects.toStringHelper(this)
        .add("info", info)
        .add("value", value())
        .toString();
  }
}
