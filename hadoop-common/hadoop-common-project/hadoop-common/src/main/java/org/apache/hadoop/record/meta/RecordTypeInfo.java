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
 * A record's Type Information object which can read/write itself. 
 * 
 * Type information for a record comprises metadata about the record, 
 * as well as a collection of type information for each field in the record. 
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RecordTypeInfo extends org.apache.hadoop.record.Record 
{

  private @Tainted String name;
  // A RecordTypeInfo is really just a wrapper around StructTypeID
  @Tainted
  StructTypeID sTid;
   // A RecordTypeInfo object is just a collection of TypeInfo objects for each of its fields.  
  //private ArrayList<FieldTypeInfo> typeInfos = new ArrayList<FieldTypeInfo>();
  // we keep a hashmap of struct/record names and their type information, as we need it to 
  // set filters when reading nested structs. This map is used during deserialization.
  //private Map<String, RecordTypeInfo> structRTIs = new HashMap<String, RecordTypeInfo>();

  /**
   * Create an empty RecordTypeInfo object.
   */
  public @Tainted RecordTypeInfo() {
    sTid = new @Tainted StructTypeID();
  }

  /**
   * Create a RecordTypeInfo object representing a record with the given name
   * @param name Name of the record
   */
  public @Tainted RecordTypeInfo(@Tainted String name) {
    this.name = name;
    sTid = new @Tainted StructTypeID();
  }

  /*
   * private constructor
   */
  private @Tainted RecordTypeInfo(@Tainted String name, @Tainted StructTypeID stid) {
    this.sTid = stid;
    this.name = name;
  }
  
  /**
   * return the name of the record
   */
  public @Tainted String getName(@Tainted RecordTypeInfo this) {
    return name;
  }

  /**
   * set the name of the record
   */
  public void setName(@Tainted RecordTypeInfo this, @Tainted String name) {
    this.name = name;
  }

  /**
   * Add a field. 
   * @param fieldName Name of the field
   * @param tid Type ID of the field
   */
  public void addField(@Tainted RecordTypeInfo this, @Tainted String fieldName, @Tainted TypeID tid) {
    sTid.getFieldTypeInfos().add(new @Tainted FieldTypeInfo(fieldName, tid));
  }
  
  private void addAll(@Tainted RecordTypeInfo this, @Tainted Collection<@Tainted FieldTypeInfo> tis) {
    sTid.getFieldTypeInfos().addAll(tis);
  }

  /**
   * Return a collection of field type infos
   */
  public @Tainted Collection<@Tainted FieldTypeInfo> getFieldTypeInfos(@Tainted RecordTypeInfo this) {
    return sTid.getFieldTypeInfos();
  }
  
  /**
   * Return the type info of a nested record. We only consider nesting 
   * to one level. 
   * @param name Name of the nested record
   */
  public @Tainted RecordTypeInfo getNestedStructTypeInfo(@Tainted RecordTypeInfo this, @Tainted String name) {
    @Tainted
    StructTypeID stid = sTid.findStruct(name);
    if (null == stid) return null;
    return new @Tainted RecordTypeInfo(name, stid);
  }

  /**
   * Serialize the type information for a record
   */
  @Override
  public void serialize(@Tainted RecordTypeInfo this, @Tainted RecordOutput rout, @Tainted String tag) throws IOException {
    // write out any header, version info, here
    rout.startRecord(this, tag);
    rout.writeString(name, tag);
    sTid.writeRest(rout, tag);
    rout.endRecord(this, tag);
  }

  /**
   * Deserialize the type information for a record
   */
  @Override
  public void deserialize(@Tainted RecordTypeInfo this, @Tainted RecordInput rin, @Tainted String tag) throws IOException {
    // read in any header, version info 
    rin.startRecord(tag);
    // name
    this.name = rin.readString(tag);
    sTid.read(rin, tag);
    rin.endRecord(tag);
  }
  
  /**
   * This class doesn't implement Comparable as it's not meant to be used 
   * for anything besides de/serializing.
   * So we always throw an exception.
   * Not implemented. Always returns 0 if another RecordTypeInfo is passed in. 
   */
  @Override
  public @Tainted int compareTo (@Tainted RecordTypeInfo this, final @Tainted Object peer_) throws ClassCastException {
    if (!(peer_ instanceof @Tainted RecordTypeInfo)) {
      throw new @Tainted ClassCastException("Comparing different types of records.");
    }
    throw new @Tainted UnsupportedOperationException("compareTo() is not supported");
  }
}

