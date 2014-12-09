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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.concurrent.ThreadFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A thread that has called {@link Thread#setDaemon(boolean) } with true.*/
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class Daemon extends @Tainted Thread {

  {
    setDaemon(true);                              // always a daemon
  }

  /**
   * Provide a factory for named daemon threads,
   * for use in ExecutorServices constructors
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public static class DaemonFactory extends @Tainted Daemon implements @Tainted ThreadFactory {

    @Override
    public @Tainted Thread newThread(Daemon.@Tainted DaemonFactory this, @Tainted Runnable runnable) {
      return new @Tainted Daemon(runnable);
    }

  }

  @Tainted
  Runnable runnable = null;
  /** Construct a daemon thread. */
  public @Tainted Daemon() {
    super();
  }

  /** Construct a daemon thread. */
  public @Tainted Daemon(@Tainted Runnable runnable) {
    super(runnable);
    this.runnable = runnable;
    this.setName(((@Tainted Object)runnable).toString());
  }

  /** Construct a daemon thread to be part of a specified thread group. */
  public @Tainted Daemon(@Tainted ThreadGroup group, @Tainted Runnable runnable) {
    super(group, runnable);
    this.runnable = runnable;
    this.setName(((@Tainted Object)runnable).toString());
  }

  public @Tainted Runnable getRunnable(@Tainted Daemon this) {
    return runnable;
  }
}
