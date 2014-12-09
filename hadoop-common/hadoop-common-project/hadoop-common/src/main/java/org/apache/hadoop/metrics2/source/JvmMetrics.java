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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Map;
import java.util.List;

import com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.log.metrics.EventCounter;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import static org.apache.hadoop.metrics2.source.JvmMetricsInfo.*;
import static org.apache.hadoop.metrics2.impl.MsInfo.*;

/**
 * JVM and logging related metrics.
 * Mostly used by various servers as a part of the metrics they export.
 */
@InterfaceAudience.Private
public class JvmMetrics implements @Tainted MetricsSource {
  enum Singleton {

@Tainted  INSTANCE;

    @Tainted
    JvmMetrics impl;

    synchronized @Tainted JvmMetrics init(JvmMetrics.@Tainted Singleton this, @Tainted String processName, @Tainted String sessionId) {
      if (impl == null) {
        impl = create(processName, sessionId, DefaultMetricsSystem.instance());
      }
      return impl;
    }
  }

  static final @Tainted float M = 1024*1024;

  final @Tainted MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
  final @Tainted List<@Tainted GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
  final @Tainted ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  final @Tainted String processName;
  final @Tainted String sessionId;
  final @Tainted Map<@Tainted String, @Tainted MetricsInfo @Tainted []> gcInfoCache = Maps.newHashMap();

  @Tainted
  JvmMetrics(@Tainted String processName, @Tainted String sessionId) {
    this.processName = processName;
    this.sessionId = sessionId;
  }

  public static @Tainted JvmMetrics create(@Tainted String processName, @Tainted String sessionId,
                                  @Tainted
                                  MetricsSystem ms) {
    return ms.register(JvmMetrics.name(), JvmMetrics.description(),
                       new @Tainted JvmMetrics(processName, sessionId));
  }

  public static @Tainted JvmMetrics initSingleton(@Tainted String processName, @Tainted String sessionId) {
    return Singleton.INSTANCE.init(processName, sessionId);
  }

  @Override
  public void getMetrics(@Tainted JvmMetrics this, @Tainted MetricsCollector collector, @Tainted boolean all) {
    @Tainted
    MetricsRecordBuilder rb = collector.addRecord(JvmMetrics)
        .setContext("jvm").tag(ProcessName, processName)
        .tag(SessionId, sessionId);
    getMemoryUsage(rb);
    getGcUsage(rb);
    getThreadUsage(rb);
    getEventCounters(rb);
  }

  private void getMemoryUsage(@Tainted JvmMetrics this, @Tainted MetricsRecordBuilder rb) {
    @Tainted
    MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    @Tainted
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    @Tainted
    Runtime runtime = Runtime.getRuntime();
    rb.addGauge(MemNonHeapUsedM, memNonHeap.getUsed() / M)
      .addGauge(MemNonHeapCommittedM, memNonHeap.getCommitted() / M)
      .addGauge(MemNonHeapMaxM, memNonHeap.getMax() / M)
      .addGauge(MemHeapUsedM, memHeap.getUsed() / M)
      .addGauge(MemHeapCommittedM, memHeap.getCommitted() / M)
      .addGauge(MemHeapMaxM, memHeap.getMax() / M)
      .addGauge(MemMaxM, runtime.maxMemory() / M);
  }

  private void getGcUsage(@Tainted JvmMetrics this, @Tainted MetricsRecordBuilder rb) {
    @Tainted
    long count = 0;
    @Tainted
    long timeMillis = 0;
    for (@Tainted GarbageCollectorMXBean gcBean : gcBeans) {
      @Tainted
      long c = gcBean.getCollectionCount();
      @Tainted
      long t = gcBean.getCollectionTime();
      @Tainted
      MetricsInfo @Tainted [] gcInfo = getGcInfo(gcBean.getName());
      rb.addCounter(gcInfo[0], c).addCounter(gcInfo[1], t);
      count += c;
      timeMillis += t;
    }
    rb.addCounter(GcCount, count)
      .addCounter(GcTimeMillis, timeMillis);
  }

  private synchronized @Tainted MetricsInfo @Tainted [] getGcInfo(@Tainted JvmMetrics this, @Tainted String gcName) {
    @Tainted
    MetricsInfo @Tainted [] gcInfo = gcInfoCache.get(gcName);
    if (gcInfo == null) {
      gcInfo = new @Tainted MetricsInfo @Tainted [2];
      gcInfo[0] = Interns.info("GcCount"+ gcName, "GC Count for "+ gcName);
      gcInfo[1] = Interns.info("GcTimeMillis"+ gcName, "GC Time for "+ gcName);
      gcInfoCache.put(gcName, gcInfo);
    }
    return gcInfo;
  }

  private void getThreadUsage(@Tainted JvmMetrics this, @Tainted MetricsRecordBuilder rb) {
    @Tainted
    int threadsNew = 0;
    @Tainted
    int threadsRunnable = 0;
    @Tainted
    int threadsBlocked = 0;
    @Tainted
    int threadsWaiting = 0;
    @Tainted
    int threadsTimedWaiting = 0;
    @Tainted
    int threadsTerminated = 0;
    @Tainted
    long threadIds @Tainted [] = threadMXBean.getAllThreadIds();
    for (@Tainted ThreadInfo threadInfo : threadMXBean.getThreadInfo(threadIds, 0)) {
      if (threadInfo == null) continue; // race protection
      switch (threadInfo.getThreadState()) {
        case NEW:           threadsNew++;           break;
        case RUNNABLE:      threadsRunnable++;      break;
        case BLOCKED:       threadsBlocked++;       break;
        case WAITING:       threadsWaiting++;       break;
        case TIMED_WAITING: threadsTimedWaiting++;  break;
        case TERMINATED:    threadsTerminated++;    break;
      }
    }
    rb.addGauge(ThreadsNew, threadsNew)
      .addGauge(ThreadsRunnable, threadsRunnable)
      .addGauge(ThreadsBlocked, threadsBlocked)
      .addGauge(ThreadsWaiting, threadsWaiting)
      .addGauge(ThreadsTimedWaiting, threadsTimedWaiting)
      .addGauge(ThreadsTerminated, threadsTerminated);
  }

  private void getEventCounters(@Tainted JvmMetrics this, @Tainted MetricsRecordBuilder rb) {
    rb.addCounter(LogFatal, EventCounter.getFatal())
      .addCounter(LogError, EventCounter.getError())
      .addCounter(LogWarn, EventCounter.getWarn())
      .addCounter(LogInfo, EventCounter.getInfo());
  }
}
