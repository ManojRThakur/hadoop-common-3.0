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

/** A WritableComparable for shorts. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ShortWritable implements @Tainted WritableComparable<@Tainted ShortWritable> {
  private @Tainted short value;

  public @Tainted ShortWritable() {
  }

  public @Tainted ShortWritable(@Tainted short value) {
    set(value);
  }

  /** Set the value of this ShortWritable. */
  public void set(@Tainted ShortWritable this, @Tainted short value) {
    this.value = value;
  }

  /** Return the value of this ShortWritable. */
  public @Tainted short get(@Tainted ShortWritable this) {
    return value;
  }

  /** read the short value */
  @Override
  public void readFields(@Tainted ShortWritable this, @Tainted DataInput in) throws IOException {
    value = in.readShort();
  }

  /** write short value */
  @Override
  public void write(@Tainted ShortWritable this, @Tainted DataOutput out) throws IOException {
    out.writeShort(value);
  }

  /** Returns true iff <code>o</code> is a ShortWritable with the same value. */
  @Override
  public @Tainted boolean equals(@Tainted ShortWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted ShortWritable))
      return false;
    @Tainted
    ShortWritable other = (@Tainted ShortWritable) o;
    return this.value == other.value;
  }

  /** hash code */
  @Override
  public @Tainted int hashCode(@Tainted ShortWritable this) {
    return value;
  }

  /** Compares two ShortWritable. */
  @Override
  public @Tainted int compareTo(@Tainted ShortWritable this, @Tainted ShortWritable o) {
    @Tainted
    short thisValue = this.value;
    @Tainted
    short thatValue = (o).value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  /** Short values in string format */
  @Override
  public @Tainted String toString(@Tainted ShortWritable this) {
    return Short.toString(value);
  }

  /** A Comparator optimized for ShortWritable. */
  public static class Comparator extends @Tainted WritableComparator {

    public @Tainted Comparator() {
      super(ShortWritable.class);
    }
    
    @Override
    public @Tainted int compare(ShortWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1, @Tainted byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      @Tainted
      short thisValue = (@Tainted short) readUnsignedShort(b1, s1);
      @Tainted
      short thatValue = (@Tainted short) readUnsignedShort(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(ShortWritable.class, new @Tainted Comparator());
  }

}
