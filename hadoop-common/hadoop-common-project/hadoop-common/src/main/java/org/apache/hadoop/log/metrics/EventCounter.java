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
package org.apache.hadoop.log.metrics;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A log4J Appender that simply counts logging events in three levels:
 * fatal, error and warn. The class name is used in log4j.properties
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EventCounter extends @Tainted AppenderSkeleton {
  private static final @Tainted int FATAL = 0;
  private static final @Tainted int ERROR = 1;
  private static final @Tainted int WARN = 2;
  private static final @Tainted int INFO = 3;

  private static class EventCounts {
    private final @Tainted long @Tainted [] counts = new long @Tainted [] {0, 0, 0, 0};

    private synchronized void incr(EventCounter.@Tainted EventCounts this, @Tainted int i) {
      ++counts[i];
    }

    private synchronized @Tainted long get(EventCounter.@Tainted EventCounts this, @Tainted int i) {
      return counts[i];
    }
  }

  private static @Tainted EventCounts counts = new @Tainted EventCounts();

  @InterfaceAudience.Private
  public static @Tainted long getFatal() {
    return counts.get(FATAL);
  }

  @InterfaceAudience.Private
  public static @Tainted long getError() {
    return counts.get(ERROR);
  }

  @InterfaceAudience.Private
  public static @Tainted long getWarn() {
    return counts.get(WARN);
  }

  @InterfaceAudience.Private
  public static @Tainted long getInfo() {
    return counts.get(INFO);
  }

  @Override
  public void append(@Tainted EventCounter this, @Tainted LoggingEvent event) {
    @Tainted
    Level level = event.getLevel();
    // depends on the api, == might not work
    // see HADOOP-7055 for details
    if (level.equals(Level.INFO)) {
      counts.incr(INFO);
    }
    else if (level.equals(Level.WARN)) {
      counts.incr(WARN);
    }
    else if (level.equals(Level.ERROR)) {
      counts.incr(ERROR);
    }
    else if (level.equals(Level.FATAL)) {
      counts.incr(FATAL);
    }
  }

  @Override
  public void close(@Tainted EventCounter this) {
  }

  @Override
  public @Tainted boolean requiresLayout(@Tainted EventCounter this) {
    return false;
  }
}
