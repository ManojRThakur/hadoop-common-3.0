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
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;

/** 
 * Represents typeID for a struct 
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StructTypeID extends @Tainted TypeID {
  private @Tainted ArrayList<@Tainted FieldTypeInfo> typeInfos = new @Tainted ArrayList<@Tainted FieldTypeInfo>();
  
  @Tainted
  StructTypeID() {
    super(RIOType.STRUCT);
  }
  
  /**
   * Create a StructTypeID based on the RecordTypeInfo of some record
   */
  public @Tainted StructTypeID(@Tainted RecordTypeInfo rti) {
    super(RIOType.STRUCT);
    typeInfos.addAll(rti.getFieldTypeInfos());
  }

  void add (@Tainted StructTypeID this, @Tainted FieldTypeInfo ti) {
    typeInfos.add(ti);
  }
  
  public @Tainted Collection<@Tainted FieldTypeInfo> getFieldTypeInfos(@Tainted StructTypeID this) {
    return typeInfos;
  }
  
  /* 
   * return the StructTypeiD, if any, of the given field 
   */
  @Tainted
  StructTypeID findStruct(@Tainted StructTypeID this, @Tainted String name) {
    // walk through the list, searching. Not the most efficient way, but this
    // in intended to be used rarely, so we keep it simple. 
    // As an optimization, we can keep a hashmap of record name to its RTI, for later.
    for (@Tainted FieldTypeInfo ti : typeInfos) {
      if ((0 == ti.getFieldID().compareTo(name)) && (ti.getTypeID().getTypeVal() == RIOType.STRUCT)) {
        return (@Tainted StructTypeID) ti.getTypeID();
      }
    }
    return null;
  }
  
  @Override
  void write(@Tainted StructTypeID this, @Tainted RecordOutput rout, @Tainted String tag) throws IOException {
    rout.writeByte(typeVal, tag);
    writeRest(rout, tag);
  }

  /* 
   * Writes rest of the struct (excluding type value).
   * As an optimization, this method is directly called by RTI 
   * for the top level record so that we don't write out the byte
   * indicating that this is a struct (since top level records are
   * always structs).
   */
  void writeRest(@Tainted StructTypeID this, @Tainted RecordOutput rout, @Tainted String tag) throws IOException {
    rout.writeInt(typeInfos.size(), tag);
    for (@Tainted FieldTypeInfo ti : typeInfos) {
      ti.write(rout, tag);
    }
  }

  /* 
   * deserialize ourselves. Called by RTI. 
   */
  void read(@Tainted StructTypeID this, @Tainted RecordInput rin, @Tainted String tag) throws IOException {
    // number of elements
    @Tainted
    int numElems = rin.readInt(tag);
    for (@Tainted int i=0; i<numElems; i++) {
      typeInfos.add(genericReadTypeInfo(rin, tag));
    }
  }
  
  // generic reader: reads the next TypeInfo object from stream and returns it
  private @Tainted FieldTypeInfo genericReadTypeInfo(@Tainted StructTypeID this, @Tainted RecordInput rin, @Tainted String tag) throws IOException {
    @Tainted
    String fieldName = rin.readString(tag);
    @Tainted
    TypeID id = genericReadTypeID(rin, tag);
    return new @Tainted FieldTypeInfo(fieldName, id);
  }
  
  // generic reader: reads the next TypeID object from stream and returns it
  private @Tainted TypeID genericReadTypeID(@Tainted StructTypeID this, @Tainted RecordInput rin, @Tainted String tag) throws IOException {
    @Tainted
    byte typeVal = rin.readByte(tag);
    switch (typeVal) {
    case TypeID.RIOType.BOOL: 
      return TypeID.BoolTypeID;
    case TypeID.RIOType.BUFFER: 
      return TypeID.BufferTypeID;
    case TypeID.RIOType.BYTE:
      return TypeID.ByteTypeID;
    case TypeID.RIOType.DOUBLE:
      return TypeID.DoubleTypeID;
    case TypeID.RIOType.FLOAT:
      return TypeID.FloatTypeID;
    case TypeID.RIOType.INT: 
      return TypeID.IntTypeID;
    case TypeID.RIOType.LONG:
      return TypeID.LongTypeID;
    case TypeID.RIOType.MAP:
    {
      @Tainted
      TypeID tIDKey = genericReadTypeID(rin, tag);
      @Tainted
      TypeID tIDValue = genericReadTypeID(rin, tag);
      return new @Tainted MapTypeID(tIDKey, tIDValue);
    }
    case TypeID.RIOType.STRING: 
      return TypeID.StringTypeID;
    case TypeID.RIOType.STRUCT: 
    {
      @Tainted
      StructTypeID stID = new @Tainted StructTypeID();
      @Tainted
      int numElems = rin.readInt(tag);
      for (@Tainted int i=0; i<numElems; i++) {
        stID.add(genericReadTypeInfo(rin, tag));
      }
      return stID;
    }
    case TypeID.RIOType.VECTOR: 
    {
      @Tainted
      TypeID tID = genericReadTypeID(rin, tag);
      return new @Tainted VectorTypeID(tID);
    }
    default:
      // shouldn't be here
      throw new @Tainted IOException("Unknown type read");
    }
  }
  
  @Override
  public @Tainted boolean equals(@Tainted StructTypeID this, @Tainted Object o) {
    return super.equals(o);
  }
  
  @Override
  public @Tainted int hashCode(@Tainted StructTypeID this) { return super.hashCode(); }
}
