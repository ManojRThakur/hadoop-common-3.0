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
import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Metrics system related metrics info instances
 */
@InterfaceAudience.Private
public enum MsInfo implements @Tainted MetricsInfo {

@Tainted  NumActiveSources("Number of active metrics sources"),

@Tainted  NumAllSources("Number of all registered metrics sources"),

@Tainted  NumActiveSinks("Number of active metrics sinks"),

@Tainted  NumAllSinks("Number of all registered metrics sinks"),

@Tainted  Context("Metrics context"),

@Tainted  Hostname("Local hostname"),

@Tainted  SessionId("Session ID"),

@Tainted  ProcessName("Process name");

  private final @Tainted String desc;

  @Tainted
  MsInfo(@Tainted String desc) {
    this.desc = desc;
  }

  @Override public @Tainted String description(@Tainted MsInfo this) {
    return desc;
  }

  @Override public @Tainted String toString(@Tainted MsInfo this) {
    return Objects.toStringHelper(this)
        .add("name", name()).add("description", desc)
        .toString();
  }
}
