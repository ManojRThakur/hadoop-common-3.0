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

/** A WritableComparable for longs. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongWritable implements @Tainted WritableComparable<@Tainted LongWritable> {
  private @Tainted long value;

  public @Tainted LongWritable() {}

  public @Tainted LongWritable(@Tainted long value) { set(value); }

  /** Set the value of this LongWritable. */
  public void set(@Tainted LongWritable this, @Tainted long value) { this.value = value; }

  /** Return the value of this LongWritable. */
  public @Tainted long get(@Tainted LongWritable this) { return value; }

  @Override
  public void readFields(@Tainted LongWritable this, @Tainted DataInput in) throws IOException {
    value = in.readLong();
  }

  @Override
  public void write(@Tainted LongWritable this, @Tainted DataOutput out) throws IOException {
    out.writeLong(value);
  }

  /** Returns true iff <code>o</code> is a LongWritable with the same value. */
  @Override
  public @Tainted boolean equals(@Tainted LongWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted LongWritable))
      return false;
    @Tainted
    LongWritable other = (@Tainted LongWritable)o;
    return this.value == other.value;
  }

  @Override
  public @Tainted int hashCode(@Tainted LongWritable this) {
    return (@Tainted int)value;
  }

  /** Compares two LongWritables. */
  @Override
  public @Tainted int compareTo(@Tainted LongWritable this, @Tainted LongWritable o) {
    @Tainted
    long thisValue = this.value;
    @Tainted
    long thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public @Tainted String toString(@Tainted LongWritable this) {
    return Long.toString(value);
  }

  /** A Comparator optimized for LongWritable. */ 
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(LongWritable.class);
    }

    @Override
    public @Tainted int compare(LongWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      @Tainted
      long thisValue = readLong(b1, s1);
      @Tainted
      long thatValue = readLong(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  /** A decreasing Comparator optimized for LongWritable. */ 
  public static class DecreasingComparator extends @Tainted Comparator {
    
    @Override
    public @Tainted int compare(LongWritable.@Tainted DecreasingComparator this, @Tainted WritableComparable a, @Tainted WritableComparable b) {
      return -super.compare(a, b);
    }
    @Override
    public @Tainted int compare(LongWritable.@Tainted DecreasingComparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1, @Tainted byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  static {                                       // register default comparator
    WritableComparator.define(LongWritable.class, new @Tainted Comparator());
  }

}

