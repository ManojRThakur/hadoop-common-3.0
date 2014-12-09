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

/** A WritableComparable for integer values stored in variable-length format.
 * Such values take between one and five bytes.  Smaller values take fewer bytes.
 * 
 * @see org.apache.hadoop.io.WritableUtils#readVInt(DataInput)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VIntWritable implements @Tainted WritableComparable<@Tainted VIntWritable> {
  private @Tainted int value;

  public @Tainted VIntWritable() {}

  public @Tainted VIntWritable(@Tainted int value) { set(value); }

  /** Set the value of this VIntWritable. */
  public void set(@Tainted VIntWritable this, @Tainted int value) { this.value = value; }

  /** Return the value of this VIntWritable. */
  public @Tainted int get(@Tainted VIntWritable this) { return value; }

  @Override
  public void readFields(@Tainted VIntWritable this, @Tainted DataInput in) throws IOException {
    value = WritableUtils.readVInt(in);
  }

  @Override
  public void write(@Tainted VIntWritable this, @Tainted DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, value);
  }

  /** Returns true iff <code>o</code> is a VIntWritable with the same value. */
  @Override
  public @Tainted boolean equals(@Tainted VIntWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted VIntWritable))
      return false;
    @Tainted
    VIntWritable other = (@Tainted VIntWritable)o;
    return this.value == other.value;
  }

  @Override
  public @Tainted int hashCode(@Tainted VIntWritable this) {
    return value;
  }

  /** Compares two VIntWritables. */
  @Override
  public @Tainted int compareTo(@Tainted VIntWritable this, @Tainted VIntWritable o) {
    @Tainted
    int thisValue = this.value;
    @Tainted
    int thatValue = o.value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }
  
  @Override
  public @Tainted String toString(@Tainted VIntWritable this) {
    return Integer.toString(value);
  }

}

