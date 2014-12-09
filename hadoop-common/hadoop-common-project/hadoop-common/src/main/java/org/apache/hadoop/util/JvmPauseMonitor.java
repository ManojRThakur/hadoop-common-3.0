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
package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Class which sets up a simple thread which runs in a loop sleeping
 * for a short interval of time. If the sleep takes significantly longer
 * than its target time, it implies that the JVM or host machine has
 * paused processing, which may cause other problems. If such a pause is
 * detected, the thread logs a message.
 */
@InterfaceAudience.Private
public class JvmPauseMonitor {
  private static final @Tainted Log LOG = LogFactory.getLog(
      JvmPauseMonitor.class);

  /** The target sleep time */
  private static final @Tainted long SLEEP_INTERVAL_MS = 500;
  
  /** log WARN if we detect a pause longer than this threshold */
  private final @Tainted long warnThresholdMs;
  private static final @Tainted String WARN_THRESHOLD_KEY =
      "jvm.pause.warn-threshold.ms";
  private static final @Tainted long WARN_THRESHOLD_DEFAULT = 10000;
  
  /** log INFO if we detect a pause longer than this threshold */
  private final @Tainted long infoThresholdMs;
  private static final @Tainted String INFO_THRESHOLD_KEY =
      "jvm.pause.info-threshold.ms";
  private static final @Tainted long INFO_THRESHOLD_DEFAULT = 1000;

  
  private @Tainted Thread monitorThread;
  private volatile @Tainted boolean shouldRun = true;
  
  public @Tainted JvmPauseMonitor(@Tainted Configuration conf) {
    this.warnThresholdMs = conf.getLong(WARN_THRESHOLD_KEY, WARN_THRESHOLD_DEFAULT);
    this.infoThresholdMs = conf.getLong(INFO_THRESHOLD_KEY, INFO_THRESHOLD_DEFAULT);
  }
  
  public void start(@Tainted JvmPauseMonitor this) {
    Preconditions.checkState(monitorThread == null,
        "Already started");
    monitorThread = new @Tainted Daemon(new @Tainted Monitor());
    monitorThread.start();
  }
  
  public void stop(@Tainted JvmPauseMonitor this) {
    shouldRun = false;
    monitorThread.interrupt();
    try {
      monitorThread.join();
    } catch (@Tainted InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  private @Tainted String formatMessage(@Tainted JvmPauseMonitor this, @Tainted long extraSleepTime,
      @Tainted
      Map<@Tainted String, @Tainted GcTimes> gcTimesAfterSleep,
      @Tainted
      Map<@Tainted String, @Tainted GcTimes> gcTimesBeforeSleep) {
    
    @Tainted
    Set<@Tainted String> gcBeanNames = Sets.intersection(
        gcTimesAfterSleep.keySet(),
        gcTimesBeforeSleep.keySet());
    @Tainted
    List<@Tainted String> gcDiffs = Lists.newArrayList();
    for (@Tainted String name : gcBeanNames) {
      @Tainted
      GcTimes diff = gcTimesAfterSleep.get(name).subtract(
          gcTimesBeforeSleep.get(name));
      if (diff.gcCount != 0) {
        gcDiffs.add("GC pool '" + name + "' had collection(s): " +
            diff.toString());
      }
    }
    
    @Tainted
    String ret = "Detected pause in JVM or host machine (eg GC): " +
        "pause of approximately " + extraSleepTime + "ms\n";
    if (gcDiffs.isEmpty()) {
      ret += "No GCs detected";
    } else {
      ret += Joiner.on("\n").join(gcDiffs);
    }
    return ret;
  }
  
  private @Tainted Map<@Tainted String, @Tainted GcTimes> getGcTimes(@Tainted JvmPauseMonitor this) {
    @Tainted
    Map<@Tainted String, @Tainted GcTimes> map = Maps.newHashMap();
    @Tainted
    List<@Tainted GarbageCollectorMXBean> gcBeans =
        ManagementFactory.getGarbageCollectorMXBeans();
    for (@Tainted GarbageCollectorMXBean gcBean : gcBeans) {
      map.put(gcBean.getName(), new @Tainted GcTimes(gcBean));
    }
    return map;
  }
  
  private static class GcTimes {
    private @Tainted GcTimes(@Tainted GarbageCollectorMXBean gcBean) {
      gcCount = gcBean.getCollectionCount();
      gcTimeMillis = gcBean.getCollectionTime();
    }
    
    private @Tainted GcTimes(@Tainted long count, @Tainted long time) {
      this.gcCount = count;
      this.gcTimeMillis = time;
    }

    private @Tainted GcTimes subtract(JvmPauseMonitor.@Tainted GcTimes this, @Tainted GcTimes other) {
      return new @Tainted GcTimes(this.gcCount - other.gcCount,
          this.gcTimeMillis - other.gcTimeMillis);
    }
    
    @Override
    public @Tainted String toString(JvmPauseMonitor.@Tainted GcTimes this) {
      return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
    }
    
    private @Tainted long gcCount;
    private @Tainted long gcTimeMillis;
  }

  private class Monitor implements @Tainted Runnable {
    @Override
    public void run(@Tainted JvmPauseMonitor.Monitor this) {
      @Tainted
      Stopwatch sw = new @Tainted Stopwatch();
      @Tainted
      Map<@Tainted String, @Tainted GcTimes> gcTimesBeforeSleep = getGcTimes();
      while (shouldRun) {
        sw.reset().start();
        try {
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (@Tainted InterruptedException ie) {
          return;
        }
        @Tainted
        long extraSleepTime = sw.elapsedMillis() - SLEEP_INTERVAL_MS;
        @Tainted
        Map<@Tainted String, @Tainted GcTimes> gcTimesAfterSleep = getGcTimes();

        if (extraSleepTime > warnThresholdMs) {
          LOG.warn(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        } else if (extraSleepTime > infoThresholdMs) {
          LOG.info(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        }
        
        gcTimesBeforeSleep = gcTimesAfterSleep;
      }
    }
  }
  
  /**
   * Simple 'main' to facilitate manual testing of the pause monitor.
   * 
   * This main function just leaks memory into a list. Running this class
   * with a 1GB heap will very quickly go into "GC hell" and result in
   * log messages about the GC pauses.
   */
  public static void main(@Tainted String @Tainted []args) throws Exception {
    new @Tainted JvmPauseMonitor(new @Tainted Configuration()).start();
    @Tainted
    List<@Tainted String> list = Lists.newArrayList();
    @Tainted
    int i = 0;
    while (true) {
      list.add(String.valueOf(i++));
    }
  }
}
