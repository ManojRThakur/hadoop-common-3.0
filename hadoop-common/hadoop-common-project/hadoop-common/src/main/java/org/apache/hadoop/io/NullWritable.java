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

/** Singleton Writable with no data. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NullWritable implements @Tainted WritableComparable<@Tainted NullWritable> {

  private static final @Tainted NullWritable THIS = new @Tainted NullWritable();

  private @Tainted NullWritable() {}                       // no public ctor

  /** Returns the single instance of this class. */
  public static @Tainted NullWritable get() { return THIS; }
  
  @Override
  public @Tainted String toString(@Tainted NullWritable this) {
    return "(null)";
  }

  @Override
  public @Tainted int hashCode(@Tainted NullWritable this) { return 0; }
  
  @Override
  public @Tainted int compareTo(@Tainted NullWritable this, @Tainted NullWritable other) {
    return 0;
  }
  @Override
  public @Tainted boolean equals(@Tainted NullWritable this, @Tainted Object other) { return other instanceof @Tainted NullWritable; }
  @Override
  public void readFields(@Tainted NullWritable this, @Tainted DataInput in) throws IOException {}
  @Override
  public void write(@Tainted NullWritable this, @Tainted DataOutput out) throws IOException {}

  /** A Comparator &quot;optimized&quot; for NullWritable. */
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(NullWritable.class);
    }

    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public @Tainted int compare(NullWritable.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      assert 0 == l1;
      assert 0 == l2;
      return 0;
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(NullWritable.class, new @Tainted Comparator());
  }
}

