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

/** A WritableComparable for a single byte. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ByteWritable implements @Tainted WritableComparable<@Tainted ByteWritable> {
  private @Tainted byte value;

  public @Tainted ByteWritable() {}

  public @Tainted ByteWritable(@Tainted byte value) { set(value); }

  /** Set the value of this ByteWritable. */
  public void set(@Tainted ByteWritable this, @Tainted byte value) { this.value = value; }

  /** Return the value of this ByteWritable. */
  public @Tainted byte get(@Tainted ByteWritable this) { return value; }

  @Override
  public void readFields(@Tainted ByteWritable this, @Tainted DataInput in) throws IOException {
    value = in.readByte();
  }

  @Override
  public void write(@Tainted ByteWritable this, @Tainted DataOutput out) throws IOException {
    out.writeByte(value);
  }

  /** Returns true iff <code>o</code> is a ByteWritable with the same value. */
  @Override
  public @Tainted boolean equals(@Tainted ByteWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted ByteWritable)) {
      return false;
    }
    @Tainted
    ByteWritable other = (@Tainted ByteWritable)o;
    return this.value == other.value;
  }

  @Override
  public @Tainted int hashCode(@Tainted ByteWritable this) {
    return (@Tainted int)value;
  }

  /** Compares two ByteWritables. */
  @Override
  public @Tainted int compareTo(@Tainted ByteWritable this, @Tainted ByteWritable o) {
    @Tainted
    int thisValue = this.value;
    @Tainted
    int thatValue = o.value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  @Override
  public @Tainted String toString(@Tainted ByteWritable this) {
    return Byte.toString(value);
  }

  /** A Comparator optimized for ByteWritable. */ 
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(ByteWritable.class);
    }

    @Override
    public @Tainted int compare(ByteWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      @Tainted
      byte thisValue = b1[s1];
      @Tainted
      byte thatValue = b2[s2];
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(ByteWritable.class, new @Tainted Comparator());
  }
}

