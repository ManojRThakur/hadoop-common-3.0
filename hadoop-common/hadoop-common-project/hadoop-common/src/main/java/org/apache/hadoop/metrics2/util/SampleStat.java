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

package org.apache.hadoop.metrics2.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Helper to compute running sample stats
 */
@InterfaceAudience.Private
public class SampleStat {
  private final @Tainted MinMax minmax = new @Tainted MinMax();
  private @Tainted long numSamples = 0;
  private @Tainted double a0;
  private @Tainted double a1;
  private @Tainted double s0;
  private @Tainted double s1;

  /**
   * Construct a new running sample stat
   */
  public @Tainted SampleStat() {
    a0 = s0 = 0.0;
  }

  public void reset(@Tainted SampleStat this) {
    numSamples = 0;
    a0 = s0 = 0.0;
    minmax.reset();
  }

  // We want to reuse the object, sometimes.
  void reset(@Tainted SampleStat this, @Tainted long numSamples, @Tainted double a0, @Tainted double a1, @Tainted double s0, @Tainted double s1,
             @Tainted
             MinMax minmax) {
    this.numSamples = numSamples;
    this.a0 = a0;
    this.a1 = a1;
    this.s0 = s0;
    this.s1 = s1;
    this.minmax.reset(minmax);
  }

  /**
   * Copy the values to other (saves object creation and gc.)
   * @param other the destination to hold our values
   */
  public void copyTo(@Tainted SampleStat this, @Tainted SampleStat other) {
    other.reset(numSamples, a0, a1, s0, s1, minmax);
  }

  /**
   * Add a sample the running stat.
   * @param x the sample number
   * @return  self
   */
  public @Tainted SampleStat add(@Tainted SampleStat this, @Tainted double x) {
    minmax.add(x);
    return add(1, x);
  }

  /**
   * Add some sample and a partial sum to the running stat.
   * Note, min/max is not evaluated using this method.
   * @param nSamples  number of samples
   * @param x the partial sum
   * @return  self
   */
  public @Tainted SampleStat add(@Tainted SampleStat this, @Tainted long nSamples, @Tainted double x) {
    numSamples += nSamples;

    if (numSamples == 1) {
      a0 = a1 = x;
      s0 = 0.0;
    }
    else {
      // The Welford method for numerical stability
      a1 = a0 + (x - a0) / numSamples;
      s1 = s0 + (x - a0) * (x - a1);
      a0 = a1;
      s0 = s1;
    }
    return this;
  }

  /**
   * @return  the total number of samples
   */
  public @Tainted long numSamples(@Tainted SampleStat this) {
    return numSamples;
  }

  /**
   * @return  the arithmetic mean of the samples
   */
  public @Tainted double mean(@Tainted SampleStat this) {
    return numSamples > 0 ? a1 : 0.0;
  }

  /**
   * @return  the variance of the samples
   */
  public @Tainted double variance(@Tainted SampleStat this) {
    return numSamples > 1 ? s1 / (numSamples - 1) : 0.0;
  }

  /**
   * @return  the standard deviation of the samples
   */
  public @Tainted double stddev(@Tainted SampleStat this) {
    return Math.sqrt(variance());
  }

  /**
   * @return  the minimum value of the samples
   */
  public @Tainted double min(@Tainted SampleStat this) {
    return minmax.min();
  }

  /**
   * @return  the maximum value of the samples
   */
  public @Tainted double max(@Tainted SampleStat this) {
    return minmax.max();
  }

  /**
   * Helper to keep running min/max
   */
  @SuppressWarnings("PublicInnerClass")
  public static class MinMax {

    // Float.MAX_VALUE is used rather than Double.MAX_VALUE, even though the
    // min and max variables are of type double.
    // Float.MAX_VALUE is big enough, and using Double.MAX_VALUE makes 
    // Ganglia core due to buffer overflow.
    // The same reasoning applies to the MIN_VALUE counterparts.
    static final @Tainted double DEFAULT_MIN_VALUE = Float.MAX_VALUE;
    static final @Tainted double DEFAULT_MAX_VALUE = Float.MIN_VALUE;

    private @Tainted double min = DEFAULT_MIN_VALUE;
    private @Tainted double max = DEFAULT_MAX_VALUE;

    public void add(SampleStat.@Tainted MinMax this, @Tainted double value) {
      if (value > max) max = value;
      if (value < min) min = value;
    }

    public @Tainted double min(SampleStat.@Tainted MinMax this) { return min; }
    public @Tainted double max(SampleStat.@Tainted MinMax this) { return max; }

    public void reset(SampleStat.@Tainted MinMax this) {
      min = DEFAULT_MIN_VALUE;
      max = DEFAULT_MAX_VALUE;
    }

    public void reset(SampleStat.@Tainted MinMax this, @Tainted MinMax other) {
      min = other.min();
      max = other.max();
    }
  }
}
