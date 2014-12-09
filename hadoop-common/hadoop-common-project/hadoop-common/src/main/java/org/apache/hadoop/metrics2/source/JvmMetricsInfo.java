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

package org.apache.hadoop.metrics2.source;

import org.checkerframework.checker.tainting.qual.Tainted;
import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * JVM and logging related metrics info instances
 */
@InterfaceAudience.Private
public enum JvmMetricsInfo implements @Tainted MetricsInfo {

@Tainted  JvmMetrics("JVM related metrics etc."), // record info
  // metrics

@Tainted  MemNonHeapUsedM("Non-heap memory used in MB"),

@Tainted  MemNonHeapCommittedM("Non-heap memory committed in MB"),

@Tainted  MemNonHeapMaxM("Non-heap memory max in MB"),

@Tainted  MemHeapUsedM("Heap memory used in MB"),

@Tainted  MemHeapCommittedM("Heap memory committed in MB"),

@Tainted  MemHeapMaxM("Heap memory max in MB"),

@Tainted  MemMaxM("Max memory size in MB"),

@Tainted  GcCount("Total GC count"),

@Tainted  GcTimeMillis("Total GC time in milliseconds"),

@Tainted  ThreadsNew("Number of new threads"),

@Tainted  ThreadsRunnable("Number of runnable threads"),

@Tainted  ThreadsBlocked("Number of blocked threads"),

@Tainted  ThreadsWaiting("Number of waiting threads"),

@Tainted  ThreadsTimedWaiting("Number of timed waiting threads"),

@Tainted  ThreadsTerminated("Number of terminated threads"),

@Tainted  LogFatal("Total number of fatal log events"),

@Tainted  LogError("Total number of error log events"),

@Tainted  LogWarn("Total number of warning log events"),

@Tainted  LogInfo("Total number of info log events");

  private final @Tainted String desc;

  @Tainted
  JvmMetricsInfo(@Tainted String desc) { this.desc = desc; }

  @Override public @Tainted String description(@Tainted JvmMetricsInfo this) { return desc; }

  @Override public @Tainted String toString(@Tainted JvmMetricsInfo this) {
  return Objects.toStringHelper(this)
      .add("name", name()).add("description", desc)
      .toString();
  }
}
