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
import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A WritableComparable for floats. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FloatWritable implements @Tainted WritableComparable<@Tainted FloatWritable> {
  private @Tainted float value;

  public @Tainted FloatWritable() {}

  public @Tainted FloatWritable(@Tainted float value) { set(value); }

  /** Set the value of this FloatWritable. */
  public void set(@Tainted FloatWritable this, @Tainted float value) { this.value = value; }

  /** Return the value of this FloatWritable. */
  public @Tainted float get(@Tainted FloatWritable this) { return value; }

  @Override
  public void readFields(@Tainted FloatWritable this, @Tainted DataInput in) throws IOException {
    value = in.readFloat();
  }

  @Override
  public void write(@Tainted FloatWritable this, @Tainted DataOutput out) throws IOException {
    out.writeFloat(value);
  }

  /** Returns true iff <code>o</code> is a FloatWritable with the same value. */
  @Override
  public @Tainted boolean equals(@Tainted FloatWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted FloatWritable))
      return false;
    @Tainted
    FloatWritable other = (@Tainted FloatWritable)o;
    return this.value == other.value;
  }

  @Override
  public @Tainted int hashCode(@Tainted FloatWritable this) {
    return Float.floatToIntBits(value);
  }

  /** Compares two FloatWritables. */
  @Override
  public @Tainted int compareTo(@Tainted FloatWritable this, @Tainted FloatWritable o) {
    @Tainted
    float thisValue = this.value;
    @Tainted
    float thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public @Tainted String toString(@Tainted FloatWritable this) {
    return Float.toString(value);
  }

  /** A Comparator optimized for FloatWritable. */ 
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(FloatWritable.class);
    }
    @Override
    public @Tainted int compare(FloatWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      @Tainted
      float thisValue = readFloat(b1, s1);
      @Tainted
      float thatValue = readFloat(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(FloatWritable.class, new @Tainted Comparator());
  }

}

