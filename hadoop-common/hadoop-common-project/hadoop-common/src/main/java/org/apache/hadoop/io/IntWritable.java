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

/** A WritableComparable for ints. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class IntWritable implements @Tainted WritableComparable<@Tainted IntWritable> {
  private @Tainted int value;

  public @Tainted IntWritable() {}

  public @Tainted IntWritable(@Tainted int value) { set(value); }

  /** Set the value of this IntWritable. */
  public void set(@Tainted IntWritable this, @Tainted int value) { this.value = value; }

  /** Return the value of this IntWritable. */
  public @Tainted int get(@Tainted IntWritable this) { return value; }

  @Override
  public void readFields(@Tainted IntWritable this, @Tainted DataInput in) throws IOException {
    value = in.readInt();
  }

  @Override
  public void write(@Tainted IntWritable this, @Tainted DataOutput out) throws IOException {
    out.writeInt(value);
  }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
  @Override
  public @Tainted boolean equals(@Tainted IntWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted IntWritable))
      return false;
    @Tainted
    IntWritable other = (@Tainted IntWritable)o;
    return this.value == other.value;
  }

  @Override
  public @Tainted int hashCode(@Tainted IntWritable this) {
    return value;
  }

  /** Compares two IntWritables. */
  @Override
  public @Tainted int compareTo(@Tainted IntWritable this, @Tainted IntWritable o) {
    @Tainted
    int thisValue = this.value;
    @Tainted
    int thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public @Tainted String toString(@Tainted IntWritable this) {
    return Integer.toString(value);
  }

  /** A Comparator optimized for IntWritable. */ 
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(IntWritable.class);
    }
    
    @Override
    public @Tainted int compare(IntWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      @Tainted
      int thisValue = readInt(b1, s1);
      @Tainted
      int thatValue = readInt(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(IntWritable.class, new @Tainted Comparator());
  }
}

