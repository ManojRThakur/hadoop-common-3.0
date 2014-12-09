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

package org.apache.hadoop.metrics2.lib;

import org.checkerframework.checker.tainting.qual.Tainted;
import com.google.common.base.Objects;
import static com.google.common.base.Preconditions.*;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Making implementing metric info a little easier
 */
class MetricsInfoImpl implements @Tainted MetricsInfo {
  private final @Tainted String name;
  private final @Tainted String description;

  @Tainted
  MetricsInfoImpl(@Tainted String name, @Tainted String description) {
    this.name = checkNotNull(name, "name");
    this.description = checkNotNull(description, "description");
  }

  @Override public @Tainted String name(@Tainted MetricsInfoImpl this) {
    return name;
  }

  @Override public @Tainted String description(@Tainted MetricsInfoImpl this) {
    return description;
  }

  @Override public @Tainted boolean equals(@Tainted MetricsInfoImpl this, @Tainted Object obj) {
    if (obj instanceof @Tainted MetricsInfo) {
      @Tainted
      MetricsInfo other = (@Tainted MetricsInfo) obj;
      return Objects.equal(name, other.name()) &&
             Objects.equal(description, other.description());
    }
    return false;
  }

  @Override public @Tainted int hashCode(@Tainted MetricsInfoImpl this) {
    return Objects.hashCode(name, description);
  }

  @Override public @Tainted String toString(@Tainted MetricsInfoImpl this) {
    return Objects.toStringHelper(this)
        .add("name", name).add("description", description)
        .toString();
  }
}
