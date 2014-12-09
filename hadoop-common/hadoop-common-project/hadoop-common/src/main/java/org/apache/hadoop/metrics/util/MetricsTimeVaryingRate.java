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
package org.apache.hadoop.metrics.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The MetricsTimeVaryingRate class is for a rate based metric that
 * naturally varies over time (e.g. time taken to create a file).
 * The rate is averaged at each interval heart beat (the interval
 * is set in the metrics config file).
 * This class also keeps track of the min and max rates along with 
 * a method to reset the min-max.
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class MetricsTimeVaryingRate extends @Tainted MetricsBase {

  private static final @Tainted Log LOG =
    LogFactory.getLog("org.apache.hadoop.metrics.util");

  static class Metrics {
    @Tainted
    int numOperations = 0;
    @Tainted
    long time = 0;  // total time or average time

    void set(MetricsTimeVaryingRate.@Tainted Metrics this, final @Tainted Metrics resetTo) {
      numOperations = resetTo.numOperations;
      time = resetTo.time;
    }
    
    void reset(MetricsTimeVaryingRate.@Tainted Metrics this) {
      numOperations = 0;
      time = 0;
    }
  }
  
  static class MinMax {
    @Tainted
    long minTime = -1;
    @Tainted
    long maxTime = 0;
    
    void set(MetricsTimeVaryingRate.@Tainted MinMax this, final @Tainted MinMax newVal) {
      minTime = newVal.minTime;
      maxTime = newVal.maxTime;
    }
    
    void reset(MetricsTimeVaryingRate.@Tainted MinMax this) {
      minTime = -1;
      maxTime = 0;
    }
    void update(MetricsTimeVaryingRate.@Tainted MinMax this, final @Tainted long time) { // update min max
      minTime = (minTime == -1) ? time : Math.min(minTime, time);
      minTime = Math.min(minTime, time);
      maxTime = Math.max(maxTime, time);
    }
  }
  private @Tainted Metrics currentData;
  private @Tainted Metrics previousIntervalData;
  private @Tainted MinMax minMax;
  
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   */
  public @Tainted MetricsTimeVaryingRate(final @Tainted String nam, final @Tainted MetricsRegistry registry, final @Tainted String description) {
    super(nam, description);
    currentData = new @Tainted Metrics();
    previousIntervalData = new @Tainted Metrics();
    minMax = new @Tainted MinMax();
    registry.add(nam, this);
  }
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * A description of {@link #NO_DESCRIPTION} is used
   */
  public @Tainted MetricsTimeVaryingRate(final @Tainted String nam, @Tainted MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION);

  }
  
  
  /**
   * Increment the metrics for numOps operations
   * @param numOps - number of operations
   * @param time - time for numOps operations
   */
  public synchronized void inc(@Tainted MetricsTimeVaryingRate this, final @Tainted int numOps, final @Tainted long time) {
    currentData.numOperations += numOps;
    currentData.time += time;
    @Tainted
    long timePerOps = time/numOps;
    minMax.update(timePerOps);
  }
  
  /**
   * Increment the metrics for one operation
   * @param time for one operation
   */
  public synchronized void inc(@Tainted MetricsTimeVaryingRate this, final @Tainted long time) {
    currentData.numOperations++;
    currentData.time += time;
    minMax.update(time);
  }
  
  

  private synchronized void intervalHeartBeat(@Tainted MetricsTimeVaryingRate this) {
     previousIntervalData.numOperations = currentData.numOperations;
     previousIntervalData.time = (currentData.numOperations == 0) ?
                             0 : currentData.time / currentData.numOperations;
     currentData.reset();
  }
  
  /**
   * Push the delta  metrics to the mr.
   * The delta is since the last push/interval.
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #getPreviousIntervalAverageTime()} and
   * {@link #getPreviousIntervalNumOps()}
   *
   * @param mr
   */
  @Override
  public synchronized void pushMetric(@Tainted MetricsTimeVaryingRate this, final @Tainted MetricsRecord mr) {
    intervalHeartBeat();
    try {
      mr.incrMetric(getName() + "_num_ops", getPreviousIntervalNumOps());
      mr.setMetric(getName() + "_avg_time", getPreviousIntervalAverageTime());
    } catch (@Tainted Exception e) {
      LOG.info("pushMetric failed for " + getName() + "\n" , e);
    }
  }
  
  /**
   * The number of operations in the previous interval
   * @return - ops in prev interval
   */
  public synchronized @Tainted int getPreviousIntervalNumOps(@Tainted MetricsTimeVaryingRate this) { 
    return previousIntervalData.numOperations;
  }
  
  /**
   * The average rate of an operation in the previous interval
   * @return - the average rate.
   */
  public synchronized @Tainted long getPreviousIntervalAverageTime(@Tainted MetricsTimeVaryingRate this) {
    return previousIntervalData.time;
  } 
  
  /**
   * The min time for a single operation since the last reset
   *  {@link #resetMinMax()}
   * @return min time for an operation
   */
  public synchronized @Tainted long getMinTime(@Tainted MetricsTimeVaryingRate this) {
    return  minMax.minTime;
  }
  
  /**
   * The max time for a single operation since the last reset
   *  {@link #resetMinMax()}
   * @return max time for an operation
   */
  public synchronized @Tainted long getMaxTime(@Tainted MetricsTimeVaryingRate this) {
    return minMax.maxTime;
  }
  
  /**
   * Reset the min max values
   */
  public synchronized void resetMinMax(@Tainted MetricsTimeVaryingRate this) {
    minMax.reset();
  }
}
