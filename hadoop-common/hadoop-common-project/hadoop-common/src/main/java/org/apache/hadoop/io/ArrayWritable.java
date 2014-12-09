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

/** 
 * A Writable for arrays containing instances of a class. The elements of this
 * writable must all be instances of the same class. If this writable will be
 * the input for a Reducer, you will need to create a subclass that sets the
 * value to be of the proper type.
 *
 * For example:
 * <code>
 * public class IntArrayWritable extends ArrayWritable {
 *   public IntArrayWritable() { 
 *     super(IntWritable.class); 
 *   }	
 * }
 * </code>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayWritable implements @Tainted Writable {
  private @Tainted Class<@Tainted ? extends @Tainted Writable> valueClass;
  private @Tainted Writable @Tainted [] values;

  public @Tainted ArrayWritable(@Tainted Class<@Tainted ? extends @Tainted Writable> valueClass) {
    if (valueClass == null) { 
      throw new @Tainted IllegalArgumentException("null valueClass"); 
    }    
    this.valueClass = valueClass;
  }

  public @Tainted ArrayWritable(@Tainted Class<@Tainted ? extends @Tainted Writable> valueClass, @Tainted Writable @Tainted [] values) {
    this(valueClass);
    this.values = values;
  }

  public @Tainted ArrayWritable(@Tainted String @Tainted [] strings) {
    this(UTF8.class, new @Tainted Writable @Tainted [strings.length]);
    for (@Tainted int i = 0; i < strings.length; i++) {
      values[i] = new @Tainted UTF8(strings[i]);
    }
  }

  public @Tainted Class getValueClass(@Tainted ArrayWritable this) {
    return valueClass;
  }

  public @Tainted String @Tainted [] toStrings(@Tainted ArrayWritable this) {
    @Tainted
    String @Tainted [] strings = new @Tainted String @Tainted [values.length];
    for (@Tainted int i = 0; i < values.length; i++) {
      strings[i] = values[i].toString();
    }
    return strings;
  }

  public @Tainted Object toArray(@Tainted ArrayWritable this) {
    @Tainted
    Object result = Array.newInstance(valueClass, values.length);
    for (@Tainted int i = 0; i < values.length; i++) {
      Array.set(result, i, values[i]);
    }
    return result;
  }

  public void set(@Tainted ArrayWritable this, @Tainted Writable @Tainted [] values) { this.values = values; }

  public @Tainted Writable @Tainted [] get(@Tainted ArrayWritable this) { return values; }

  @Override
  public void readFields(@Tainted ArrayWritable this, @Tainted DataInput in) throws IOException {
    values = new @Tainted Writable @Tainted [in.readInt()];          // construct values
    for (@Tainted int i = 0; i < values.length; i++) {
      @Tainted
      Writable value = WritableFactories.newInstance(valueClass);
      value.readFields(in);                       // read a value
      values[i] = value;                          // store it in values
    }
  }

  @Override
  public void write(@Tainted ArrayWritable this, @Tainted DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (@Tainted int i = 0; i < values.length; i++) {
      values[i].write(out);
    }
  }

}

