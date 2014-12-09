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

package org.apache.hadoop.io;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Writable for Double values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DoubleWritable implements @Tainted WritableComparable<@Tainted DoubleWritable> {

  private @Tainted double value = 0.0;
  
  public @Tainted DoubleWritable() {
    
  }
  
  public @Tainted DoubleWritable(@Tainted double value) {
    set(value);
  }
  
  @Override
  public void readFields(@Tainted DoubleWritable this, @Tainted DataInput in) throws IOException {
    value = in.readDouble();
  }

  @Override
  public void write(@Tainted DoubleWritable this, @Tainted DataOutput out) throws IOException {
    out.writeDouble(value);
  }
  
  public void set(@Tainted DoubleWritable this, @Tainted double value) { this.value = value; }
  
  public @Tainted double get(@Tainted DoubleWritable this) { return value; }

  /**
   * Returns true iff <code>o</code> is a DoubleWritable with the same value.
   */
  @Override
  public @Tainted boolean equals(@Tainted DoubleWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted DoubleWritable)) {
      return false;
    }
    @Tainted
    DoubleWritable other = (@Tainted DoubleWritable)o;
    return this.value == other.value;
  }
  
  @Override
  public @Tainted int hashCode(@Tainted DoubleWritable this) {
    return (@Tainted int)Double.doubleToLongBits(value);
  }
  
  @Override
  public @Tainted int compareTo(@Tainted DoubleWritable this, @Tainted DoubleWritable o) {
    return (value < o.value ? -1 : (value == o.value ? 0 : 1));
  }
  
  @Override
  public @Tainted String toString(@Tainted DoubleWritable this) {
    return Double.toString(value);
  }

  /** A Comparator optimized for DoubleWritable. */ 
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(DoubleWritable.class);
    }

    @Override
    public @Tainted int compare(DoubleWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      @Tainted
      double thisValue = readDouble(b1, s1);
      @Tainted
      double thatValue = readDouble(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(DoubleWritable.class, new @Tainted Comparator());
  }

}

