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
import java.util.Random;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.metrics2.util.Contracts.*;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.util.Time;

/**
 * An adapter class for metrics sink and associated filters
 */
class MetricsSinkAdapter implements SinkQueue.@Tainted Consumer<@Tainted MetricsBuffer> {

  private final @Tainted Log LOG = LogFactory.getLog(MetricsSinkAdapter.class);
  private final @Tainted String name;
  private final @Tainted String description;
  private final @Tainted String context;
  private final @Tainted MetricsSink sink;
  private final @Tainted MetricsFilter sourceFilter;
  private final @Tainted MetricsFilter recordFilter;
  private final @Tainted MetricsFilter metricFilter;
  private final @Tainted SinkQueue<@Tainted MetricsBuffer> queue;
  private final @Tainted Thread sinkThread;
  private volatile @Tainted boolean stopping = false;
  private volatile @Tainted boolean inError = false;
  private final @Tainted int period;
  private final @Tainted int firstRetryDelay;
  private final @Tainted int retryCount;
  private final @Tainted long oobPutTimeout;
  private final @Tainted float retryBackoff;
  private final @Tainted MetricsRegistry registry = new @Tainted MetricsRegistry("sinkadapter");
  private final @Tainted MutableStat latency;
  private final @Tainted MutableCounterInt dropped;
  private final @Tainted MutableGaugeInt qsize;

  @Tainted
  MetricsSinkAdapter(@Tainted String name, @Tainted String description, @Tainted MetricsSink sink,
                     @Tainted
                     String context, @Tainted MetricsFilter sourceFilter,
                     @Tainted
                     MetricsFilter recordFilter, @Tainted MetricsFilter metricFilter,
                     @Tainted
                     int period, @Tainted int queueCapacity, @Tainted int retryDelay,
                     @Tainted
                     float retryBackoff, @Tainted int retryCount) {
    this.name = checkNotNull(name, "name");
    this.description = description;
    this.sink = checkNotNull(sink, "sink object");
    this.context = context;
    this.sourceFilter = sourceFilter;
    this.recordFilter = recordFilter;
    this.metricFilter = metricFilter;
    this.period = checkArg(period, period > 0, "period");
    firstRetryDelay = checkArg(retryDelay, retryDelay > 0, "retry delay");
    this.retryBackoff = checkArg(retryBackoff, retryBackoff>1, "retry backoff");
    oobPutTimeout = (@Tainted long)
        (firstRetryDelay * Math.pow(retryBackoff, retryCount) * 1000);
    this.retryCount = retryCount;
    this.queue = new @Tainted SinkQueue<@Tainted MetricsBuffer>(checkArg(queueCapacity,
        queueCapacity > 0, "queue capacity"));
    latency = registry.newRate("Sink_"+ name, "Sink end to end latency", false);
    dropped = registry.newCounter("Sink_"+ name +"Dropped",
                                  "Dropped updates per sink", 0);
    qsize = registry.newGauge("Sink_"+ name + "Qsize", "Queue size", 0);

    sinkThread = new @Tainted Thread() {
      @Override public void run() {
        publishMetricsFromQueue();
      }
    };
    sinkThread.setName(name);
    sinkThread.setDaemon(true);
  }

  @Tainted
  boolean putMetrics(@Tainted MetricsSinkAdapter this, @Tainted MetricsBuffer buffer, @Tainted long logicalTime) {
    if (logicalTime % period == 0) {
      LOG.debug("enqueue, logicalTime="+ logicalTime);
      if (queue.enqueue(buffer)) return true;
      dropped.incr();
      return false;
    }
    return true; // OK
  }
  
  public @Tainted boolean putMetricsImmediate(@Tainted MetricsSinkAdapter this, @Tainted MetricsBuffer buffer) {
    @Tainted
    WaitableMetricsBuffer waitableBuffer =
        new @Tainted WaitableMetricsBuffer(buffer);
    if (!queue.enqueue(waitableBuffer)) {
      LOG.warn(name + " has a full queue and can't consume the given metrics.");
      dropped.incr();
      return false;
    }
    if (!waitableBuffer.waitTillNotified(oobPutTimeout)) {
      LOG.warn(name +
          " couldn't fulfill an immediate putMetrics request in time." +
          " Abandoning.");
      return false;
    }
    return true;
  }

  void publishMetricsFromQueue(@Tainted MetricsSinkAdapter this) {
    @Tainted
    int retryDelay = firstRetryDelay;
    @Tainted
    int n = retryCount;
    @Tainted
    int minDelay = Math.min(500, retryDelay * 1000); // millis
    @Tainted
    Random rng = new @Tainted Random(System.nanoTime());
    while (!stopping) {
      try {
        queue.consumeAll(this);
        retryDelay = firstRetryDelay;
        n = retryCount;
        inError = false;
      } catch (@Tainted InterruptedException e) {
        LOG.info(name +" thread interrupted.");
      } catch (@Tainted Exception e) {
        if (n > 0) {
          @Tainted
          int retryWindow = Math.max(0, 1000 / 2 * retryDelay - minDelay);
          @Tainted
          int awhile = rng.nextInt(retryWindow) + minDelay;
          if (!inError) {
            LOG.error("Got sink exception, retry in "+ awhile +"ms", e);
          }
          retryDelay *= retryBackoff;
          try { Thread.sleep(awhile); }
          catch (@Tainted InterruptedException e2) {
            LOG.info(name +" thread interrupted while waiting for retry", e2);
          }
          --n;
        } else {
          if (!inError) {
            LOG.error("Got sink exception and over retry limit, "+
                      "suppressing further error messages", e);
          }
          queue.clear();
          inError = true; // Don't keep complaining ad infinitum
        }
      }
    }
  }

  @Override
  public void consume(@Tainted MetricsSinkAdapter this, @Tainted MetricsBuffer buffer) {
    @Tainted
    long ts = 0;
    for (MetricsBuffer.@Tainted Entry entry : buffer) {
      if (sourceFilter == null || sourceFilter.accepts(entry.name())) {
        for (@Tainted MetricsRecordImpl record : entry.records()) {
          if ((context == null || context.equals(record.context())) &&
              (recordFilter == null || recordFilter.accepts(record))) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Pushing record "+ entry.name() +"."+ record.context() +
                        "."+ record.name() +" to "+ name);
            }
            sink.putMetrics(metricFilter == null
                ? record
                : new @Tainted MetricsRecordFiltered(record, metricFilter));
            if (ts == 0) ts = record.timestamp();
          }
        }
      }
    }
    if (ts > 0) {
      sink.flush();
      latency.add(Time.now() - ts);
    }
    if (buffer instanceof @Tainted WaitableMetricsBuffer) {
      ((@Tainted WaitableMetricsBuffer)buffer).notifyAnyWaiters();
    }
    LOG.debug("Done");
  }

  void start(@Tainted MetricsSinkAdapter this) {
    sinkThread.start();
    LOG.info("Sink "+ name +" started");
  }

  void stop(@Tainted MetricsSinkAdapter this) {
    stopping = true;
    sinkThread.interrupt();
    try {
      sinkThread.join();
    } catch (@Tainted InterruptedException e) {
      LOG.warn("Stop interrupted", e);
    }
  }

  @Tainted
  String name(@Tainted MetricsSinkAdapter this) {
    return name;
  }

  @Tainted
  String description(@Tainted MetricsSinkAdapter this) {
    return description;
  }

  void snapshot(@Tainted MetricsSinkAdapter this, @Tainted MetricsRecordBuilder rb, @Tainted boolean all) {
    registry.snapshot(rb, all);
  }

  @Tainted
  MetricsSink sink(@Tainted MetricsSinkAdapter this) {
    return sink;
  }

  static class WaitableMetricsBuffer extends @Tainted MetricsBuffer {
    private final @Tainted Semaphore notificationSemaphore =
        new @Tainted Semaphore(0);

    public @Tainted WaitableMetricsBuffer(@Tainted MetricsBuffer metricsBuffer) {
      super(metricsBuffer);
    }

    public @Tainted boolean waitTillNotified(MetricsSinkAdapter.@Tainted WaitableMetricsBuffer this, @Tainted long millisecondsToWait) {
      try {
        return notificationSemaphore.tryAcquire(millisecondsToWait,
            TimeUnit.MILLISECONDS);
      } catch (@Tainted InterruptedException e) {
        return false;
      }
    }

    public void notifyAnyWaiters(MetricsSinkAdapter.@Tainted WaitableMetricsBuffer this) {
      notificationSemaphore.release();
    }
  }
}
