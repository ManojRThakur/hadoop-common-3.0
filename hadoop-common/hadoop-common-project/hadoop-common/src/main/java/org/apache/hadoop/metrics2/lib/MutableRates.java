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
import java.lang.reflect.Method;
import java.util.Set;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * Helper class to manage a group of mutable rate metrics
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableRates extends @Tainted MutableMetric {
  static final @Tainted Log LOG = LogFactory.getLog(MutableRates.class);
  private final @Tainted MetricsRegistry registry;
  private final @Tainted Set<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>> protocolCache = Sets.newHashSet();

  @Tainted
  MutableRates(@Tainted MetricsRegistry registry) {
    this.registry = checkNotNull(registry, "metrics registry");
  }

  /**
   * Initialize the registry with all the methods in a protocol
   * so they all show up in the first snapshot.
   * Convenient for JMX implementations.
   * @param protocol the protocol class
   */
  public void init(@Tainted MutableRates this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> protocol) {
    if (protocolCache.contains(protocol)) return;
    protocolCache.add(protocol);
    for (@Tainted Method method : protocol.getDeclaredMethods()) {
      @Tainted
      String name = method.getName();
      LOG.debug(name);
      try { registry.newRate(name, name, false, true); }
      catch (@Tainted Exception e) {
        LOG.error("Error creating rate metrics for "+ method.getName(), e);
      }
    }
  }

  /**
   * Add a rate sample for a rate metric
   * @param name of the rate metric
   * @param elapsed time
   */
  public void add(@Tainted MutableRates this, @Tainted String name, @Tainted long elapsed) {
    registry.add(name, elapsed);
  }

  @Override
  public void snapshot(@Tainted MutableRates this, @Tainted MetricsRecordBuilder rb, @Tainted boolean all) {
    registry.snapshot(rb, all);
  }
}
