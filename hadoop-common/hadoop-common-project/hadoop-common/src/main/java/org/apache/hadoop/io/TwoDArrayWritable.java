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
import java.lang.reflect.Array;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A Writable for 2D arrays containing a matrix of instances of a class. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TwoDArrayWritable implements @Tainted Writable {
  private @Tainted Class valueClass;
  private @Tainted Writable @Tainted [] @Tainted [] values;

  public @Tainted TwoDArrayWritable(@Tainted Class valueClass) {
    this.valueClass = valueClass;
  }

  public @Tainted TwoDArrayWritable(@Tainted Class valueClass, @Tainted Writable @Tainted [] @Tainted [] values) {
    this(valueClass);
    this.values = values;
  }

  public @Tainted Object toArray(@Tainted TwoDArrayWritable this) {
    @Tainted
    int dimensions @Tainted [] = new int @Tainted [] {values.length, 0};
    @Tainted
    Object result = Array.newInstance(valueClass, dimensions);
    for (@Tainted int i = 0; i < values.length; i++) {
      @Tainted
      Object resultRow = Array.newInstance(valueClass, values[i].length);
      Array.set(result, i, resultRow);
      for (@Tainted int j = 0; j < values[i].length; j++) {
        Array.set(resultRow, j, values[i][j]);
      }
    }
    return result;
  }

  public void set(@Tainted TwoDArrayWritable this, @Tainted Writable @Tainted [] @Tainted [] values) { this.values = values; }

  public @Tainted Writable @Tainted [] @Tainted [] get(@Tainted TwoDArrayWritable this) { return values; }

  @Override
  public void readFields(@Tainted TwoDArrayWritable this, @Tainted DataInput in) throws IOException {
    // construct matrix
    values = new @Tainted Writable @Tainted [in.readInt()] @Tainted [];          
    for (@Tainted int i = 0; i < values.length; i++) {
      values[i] = new @Tainted Writable @Tainted [in.readInt()];
    }

    // construct values
    for (@Tainted int i = 0; i < values.length; i++) {
      for (@Tainted int j = 0; j < values[i].length; j++) {
        @Tainted
        Writable value;                             // construct value
        try {
          value = (@Tainted Writable)valueClass.newInstance();
        } catch (@Tainted InstantiationException e) {
          throw new @Tainted RuntimeException(e.toString());
        } catch (@Tainted IllegalAccessException e) {
          throw new @Tainted RuntimeException(e.toString());
        }
        value.readFields(in);                       // read a value
        values[i][j] = value;                       // store it in values
      }
    }
  }

  @Override
  public void write(@Tainted TwoDArrayWritable this, @Tainted DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (@Tainted int i = 0; i < values.length; i++) {
      out.writeInt(values[i].length);
    }
    for (@Tainted int i = 0; i < values.length; i++) {
      for (@Tainted int j = 0; j < values[i].length; j++) {
        values[i][j].write(out);
      }
    }
  }
}

