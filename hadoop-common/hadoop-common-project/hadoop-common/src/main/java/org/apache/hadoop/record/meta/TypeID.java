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

package org.apache.hadoop.record.meta;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.RecordOutput;

/** 
 * Represents typeID for basic types.
 *  
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TypeID {

  /**
   * constants representing the IDL types we support
   */
  public static final class RIOType {
    public static final @Tainted byte BOOL   = 1;
    public static final @Tainted byte BUFFER = 2;
    public static final @Tainted byte BYTE   = 3;
    public static final @Tainted byte DOUBLE = 4;
    public static final @Tainted byte FLOAT  = 5;
    public static final @Tainted byte INT    = 6;
    public static final @Tainted byte LONG   = 7;
    public static final @Tainted byte MAP    = 8;
    public static final @Tainted byte STRING = 9;
    public static final @Tainted byte STRUCT = 10;
    public static final @Tainted byte VECTOR = 11;
  }

  /**
   * Constant classes for the basic types, so we can share them.
   */
  public static final @Tainted TypeID BoolTypeID = new @Tainted TypeID(RIOType.BOOL);
  public static final @Tainted TypeID BufferTypeID = new @Tainted TypeID(RIOType.BUFFER);
  public static final @Tainted TypeID ByteTypeID = new @Tainted TypeID(RIOType.BYTE);
  public static final @Tainted TypeID DoubleTypeID = new @Tainted TypeID(RIOType.DOUBLE);
  public static final @Tainted TypeID FloatTypeID = new @Tainted TypeID(RIOType.FLOAT);
  public static final @Tainted TypeID IntTypeID = new @Tainted TypeID(RIOType.INT);
  public static final @Tainted TypeID LongTypeID = new @Tainted TypeID(RIOType.LONG);
  public static final @Tainted TypeID StringTypeID = new @Tainted TypeID(RIOType.STRING);
  
  protected @Tainted byte typeVal;

  /**
   * Create a TypeID object 
   */
  @Tainted
  TypeID(@Tainted byte typeVal) {
    this.typeVal = typeVal;
  }

  /**
   * Get the type value. One of the constants in RIOType.
   */
  public @Tainted byte getTypeVal(@Tainted TypeID this) {
    return typeVal;
  }

  /**
   * Serialize the TypeID object
   */
  void write(@Tainted TypeID this, @Tainted RecordOutput rout, @Tainted String tag) throws IOException {
    rout.writeByte(typeVal, tag);
  }
  
  /**
   * Two base typeIDs are equal if they refer to the same type
   */
  @Override
  public @Tainted boolean equals(@Tainted TypeID this, @Tainted Object o) {
    if (this == o) 
      return true;

    if (o == null)
      return false;

    if (this.getClass() != o.getClass())
      return false;

    @Tainted
    TypeID oTypeID = (@Tainted TypeID) o;
    return (this.typeVal == oTypeID.typeVal);
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  @Override
  public @Tainted int hashCode(@Tainted TypeID this) {
    // See 'Effectve Java' by Joshua Bloch
    return 37*17+(@Tainted int)typeVal;
  }
}

