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
import java.util.concurrent.atomic.AtomicReference;
import javax.management.ObjectName;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

import com.google.common.annotations.VisibleForTesting;

/**
 * The default metrics system singleton. This class is used by all the daemon
 * processes(such as NameNode, DataNode, JobTracker etc.). During daemon process
 * initialization the processes call {@link DefaultMetricsSystem#init(String)}
 * to initialize the {@link MetricsSystem}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum DefaultMetricsSystem {

@Tainted  INSTANCE; // the singleton

  private @Tainted AtomicReference<@Tainted MetricsSystem> impl =
      new @Tainted AtomicReference<@Tainted MetricsSystem>(new @Tainted MetricsSystemImpl());
  
  @VisibleForTesting
  volatile @Tainted boolean miniClusterMode = false;
  
  transient final @Tainted UniqueNames mBeanNames = new @Tainted UniqueNames();
  transient final @Tainted UniqueNames sourceNames = new @Tainted UniqueNames();

  /**
   * Convenience method to initialize the metrics system
   * @param prefix  for the metrics system configuration
   * @return the metrics system instance
   */
  public static @Tainted MetricsSystem initialize(@Tainted String prefix) {
    return INSTANCE.init(prefix);
  }

  @Tainted
  MetricsSystem init(@Tainted DefaultMetricsSystem this, @Tainted String prefix) {
    return impl.get().init(prefix);
  }

  /**
   * @return the metrics system object
   */
  public static @Tainted MetricsSystem instance() {
    return INSTANCE.getImpl();
  }

  /**
   * Shutdown the metrics system
   */
  public static void shutdown() {
    INSTANCE.shutdownInstance();
  }

  void shutdownInstance(@Tainted DefaultMetricsSystem this) {
    @Tainted
    boolean last = impl.get().shutdown();
    if (last) synchronized(this) {
      mBeanNames.map.clear();
      sourceNames.map.clear();
    }
  }

  @InterfaceAudience.Private
  public static @Tainted MetricsSystem setInstance(@Tainted MetricsSystem ms) {
    return INSTANCE.setImpl(ms);
  }

  @Tainted
  MetricsSystem setImpl(@Tainted DefaultMetricsSystem this, @Tainted MetricsSystem ms) {
    return impl.getAndSet(ms);
  }

  @Tainted
  MetricsSystem getImpl(@Tainted DefaultMetricsSystem this) { return impl.get(); }

  @VisibleForTesting
  public static void setMiniClusterMode(@Tainted boolean choice) {
    INSTANCE.miniClusterMode = choice;
  }

  @VisibleForTesting
  public static @Tainted boolean inMiniClusterMode() {
    return INSTANCE.miniClusterMode;
  }

  @InterfaceAudience.Private
  public static @Tainted ObjectName newMBeanName(@Tainted String name) {
    return INSTANCE.newObjectName(name);
  }

  @InterfaceAudience.Private
  public static @Tainted String sourceName(@Tainted String name, @Tainted boolean dupOK) {
    return INSTANCE.newSourceName(name, dupOK);
  }

  synchronized @Tainted ObjectName newObjectName(@Tainted DefaultMetricsSystem this, @Tainted String name) {
    try {
      if (mBeanNames.map.containsKey(name) && !miniClusterMode) {
        throw new @Tainted MetricsException(name +" already exists!");
      }
      return new @Tainted ObjectName(mBeanNames.uniqueName(name));
    } catch (@Tainted Exception e) {
      throw new @Tainted MetricsException(e);
    }
  }

  synchronized @Tainted String newSourceName(@Tainted DefaultMetricsSystem this, @Tainted String name, @Tainted boolean dupOK) {
    if (sourceNames.map.containsKey(name)) {
      if (dupOK) {
        return name;
      } else if (!miniClusterMode) {
        throw new @Tainted MetricsException("Metrics source "+ name +" already exists!");
      }
    }
    return sourceNames.uniqueName(name);
  }
}
