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
 * Represents a type information for a field, which is made up of its 
 * ID (name) and its type (a TypeID object).
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldTypeInfo
{

  private @Tainted String fieldID;
  private @Tainted TypeID typeID;

  /**
   * Construct a FiledTypeInfo with the given field name and the type
   */
  @Tainted
  FieldTypeInfo(@Tainted String fieldID, @Tainted TypeID typeID) {
    this.fieldID = fieldID;
    this.typeID = typeID;
  }

  /**
   * get the field's TypeID object
   */
  public @Tainted TypeID getTypeID(@Tainted FieldTypeInfo this) {
    return typeID;
  }
  
  /**
   * get the field's id (name)
   */
  public @Tainted String getFieldID(@Tainted FieldTypeInfo this) {
    return fieldID;
  }
  
  void write(@Tainted FieldTypeInfo this, @Tainted RecordOutput rout, @Tainted String tag) throws IOException {
    rout.writeString(fieldID, tag);
    typeID.write(rout, tag);
  }
  
  /**
   * Two FieldTypeInfos are equal if ach of their fields matches
   */
  @Override
  public @Tainted boolean equals(@Tainted FieldTypeInfo this, @Tainted Object o) {
    if (this == o) 
      return true;
    if (!(o instanceof @Tainted FieldTypeInfo))
      return false;
    @Tainted
    FieldTypeInfo fti = (@Tainted FieldTypeInfo) o;
    // first check if fieldID matches
    if (!this.fieldID.equals(fti.fieldID)) {
      return false;
    }
    // now see if typeID matches
    return (this.typeID.equals(fti.typeID));
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  @Override
  public @Tainted int hashCode(@Tainted FieldTypeInfo this) {
    return 37*17+typeID.hashCode() + 37*17+fieldID.hashCode();
  }
  

  public @Tainted boolean equals(@Tainted FieldTypeInfo this, @Tainted FieldTypeInfo ti) {
    // first check if fieldID matches
    if (!this.fieldID.equals(ti.fieldID)) {
      return false;
    }
    // now see if typeID matches
    return (this.typeID.equals(ti.typeID));
  }

}

