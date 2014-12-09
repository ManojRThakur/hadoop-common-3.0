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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.SampleStat;
import static org.apache.hadoop.metrics2.lib.Interns.*;

/**
 * A mutable metric with stats.
 *
 * Useful for keeping throughput/latency stats.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableStat extends @Tainted MutableMetric {
  private final @Tainted MetricsInfo numInfo;
  private final @Tainted MetricsInfo avgInfo;
  private final @Tainted MetricsInfo stdevInfo;
  private final @Tainted MetricsInfo iMinInfo;
  private final @Tainted MetricsInfo iMaxInfo;
  private final @Tainted MetricsInfo minInfo;
  private final @Tainted MetricsInfo maxInfo;

  private final @Tainted SampleStat intervalStat = new @Tainted SampleStat();
  private final @Tainted SampleStat prevStat = new @Tainted SampleStat();
  private final SampleStat.@Tainted MinMax minMax = new SampleStat.@Tainted MinMax();
  private @Tainted long numSamples = 0;
  private @Tainted boolean extended = false;

  /**
   * Construct a sample statistics metric
   * @param name        of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   * @param extended    create extended stats (stdev, min/max etc.) by default.
   */
  public @Tainted MutableStat(@Tainted String name, @Tainted String description,
                     @Tainted
                     String sampleName, @Tainted String valueName, @Tainted boolean extended) {
    @Tainted
    String ucName = StringUtils.capitalize(name);
    @Tainted
    String usName = StringUtils.capitalize(sampleName);
    @Tainted
    String uvName = StringUtils.capitalize(valueName);
    @Tainted
    String desc = StringUtils.uncapitalize(description);
    @Tainted
    String lsName = StringUtils.uncapitalize(sampleName);
    @Tainted
    String lvName = StringUtils.uncapitalize(valueName);
    numInfo = info(ucName +"Num"+ usName, "Number of "+ lsName +" for "+ desc);
    avgInfo = info(ucName +"Avg"+ uvName, "Average "+ lvName +" for "+ desc);
    stdevInfo = info(ucName +"Stdev"+ uvName,
                     "Standard deviation of "+ lvName +" for "+ desc);
    iMinInfo = info(ucName +"IMin"+ uvName,
                    "Interval min "+ lvName +" for "+ desc);
    iMaxInfo = info(ucName + "IMax"+ uvName,
                    "Interval max "+ lvName +" for "+ desc);
    minInfo = info(ucName +"Min"+ uvName, "Min "+ lvName +" for "+ desc);
    maxInfo = info(ucName +"Max"+ uvName, "Max "+ lvName +" for "+ desc);
    this.extended = extended;
  }

  /**
   * Construct a snapshot stat metric with extended stat off by default
   * @param name        of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   */
  public @Tainted MutableStat(@Tainted String name, @Tainted String description,
                     @Tainted
                     String sampleName, @Tainted String valueName) {
    this(name, description, sampleName, valueName, false);
  }

  /**
   * Add a number of samples and their sum to the running stat
   * @param numSamples  number of samples
   * @param sum of the samples
   */
  public synchronized void add(@Tainted MutableStat this, @Tainted long numSamples, @Tainted long sum) {
    intervalStat.add(numSamples, sum);
    setChanged();
  }

  /**
   * Add a snapshot to the metric
   * @param value of the metric
   */
  public synchronized void add(@Tainted MutableStat this, @Tainted long value) {
    intervalStat.add(value);
    minMax.add(value);
    setChanged();
  }

  @Override
  public synchronized void snapshot(@Tainted MutableStat this, @Tainted MetricsRecordBuilder builder, @Tainted boolean all) {
    if (all || changed()) {
      numSamples += intervalStat.numSamples();
      builder.addCounter(numInfo, numSamples)
             .addGauge(avgInfo, lastStat().mean());
      if (extended) {
        builder.addGauge(stdevInfo, lastStat().stddev())
               .addGauge(iMinInfo, lastStat().min())
               .addGauge(iMaxInfo, lastStat().max())
               .addGauge(minInfo, minMax.min())
               .addGauge(maxInfo, minMax.max());
      }
      if (changed()) {
        if (numSamples > 0) {
          intervalStat.copyTo(prevStat);
          intervalStat.reset();
        }
        clearChanged();
      }
    }
  }

  private @Tainted SampleStat lastStat(@Tainted MutableStat this) {
    return changed() ? intervalStat : prevStat;
  }

  /**
   * Reset the all time min max of the metric
   */
  public void resetMinMax(@Tainted MutableStat this) {
    minMax.reset();
  }

}
