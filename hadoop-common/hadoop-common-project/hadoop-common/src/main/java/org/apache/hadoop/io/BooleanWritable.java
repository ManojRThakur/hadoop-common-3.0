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

/** 
 * A WritableComparable for booleans. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BooleanWritable implements @Tainted WritableComparable<@Tainted BooleanWritable> {
  private @Tainted boolean value;

  /** 
   */
  public @Tainted BooleanWritable() {};

  /** 
   */
  public @Tainted BooleanWritable(@Tainted boolean value) {
    set(value);
  }

  /** 
   * Set the value of the BooleanWritable
   */    
  public void set(@Tainted BooleanWritable this, @Tainted boolean value) {
    this.value = value;
  }

  /**
   * Returns the value of the BooleanWritable
   */
  public @Tainted boolean get(@Tainted BooleanWritable this) {
    return value;
  }

  /**
   */
  @Override
  public void readFields(@Tainted BooleanWritable this, @Tainted DataInput in) throws IOException {
    value = in.readBoolean();
  }

  /**
   */
  @Override
  public void write(@Tainted BooleanWritable this, @Tainted DataOutput out) throws IOException {
    out.writeBoolean(value);
  }

  /**
   */
  @Override
  public @Tainted boolean equals(@Tainted BooleanWritable this, @Tainted Object o) {
    if (!(o instanceof @Tainted BooleanWritable)) {
      return false;
    }
    @Tainted
    BooleanWritable other = (@Tainted BooleanWritable) o;
    return this.value == other.value;
  }

  @Override
  public @Tainted int hashCode(@Tainted BooleanWritable this) {
    return value ? 0 : 1;
  }



  /**
   */
  @Override
  public @Tainted int compareTo(@Tainted BooleanWritable this, @Tainted BooleanWritable o) {
    @Tainted
    boolean a = this.value;
    @Tainted
    boolean b = o.value;
    return ((a == b) ? 0 : (a == false) ? -1 : 1);
  }
  
  @Override
  public @Tainted String toString(@Tainted BooleanWritable this) {
    return Boolean.toString(get());
  }

  /** 
   * A Comparator optimized for BooleanWritable. 
   */ 
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(BooleanWritable.class);
    }

    @Override
    public @Tainted int compare(BooleanWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }


  static {
    WritableComparator.define(BooleanWritable.class, new @Tainted Comparator());
  }
}
