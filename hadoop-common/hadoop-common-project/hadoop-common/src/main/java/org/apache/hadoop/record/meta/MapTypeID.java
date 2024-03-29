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
 * Represents typeID for a Map 
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapTypeID extends @Tainted TypeID {
  
  private @Tainted TypeID typeIDKey; 
  private @Tainted TypeID typeIDValue; 
  
  public @Tainted MapTypeID(@Tainted TypeID typeIDKey, @Tainted TypeID typeIDValue) {
    super(RIOType.MAP);
    this.typeIDKey = typeIDKey;
    this.typeIDValue = typeIDValue;
  }
  
  /**
   * get the TypeID of the map's key element
   */
  public @Tainted TypeID getKeyTypeID(@Tainted MapTypeID this) {
    return this.typeIDKey;
  }
  
  /**
   * get the TypeID of the map's value element
   */
  public @Tainted TypeID getValueTypeID(@Tainted MapTypeID this) {
    return this.typeIDValue;
  }
  
  @Override
  void write(@Tainted MapTypeID this, @Tainted RecordOutput rout, @Tainted String tag) throws IOException {
    rout.writeByte(typeVal, tag);
    typeIDKey.write(rout, tag);
    typeIDValue.write(rout, tag);
  }
  
  /**
   * Two map  typeIDs are equal if their constituent elements have the 
   * same type
   */
  @Override
  public @Tainted boolean equals(@Tainted MapTypeID this, @Tainted Object o) {
    if (!super.equals(o))
      return false;

    @Tainted
    MapTypeID mti = (@Tainted MapTypeID) o;

    return this.typeIDKey.equals(mti.typeIDKey) &&
           this.typeIDValue.equals(mti.typeIDValue);
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  @Override
  public @Tainted int hashCode(@Tainted MapTypeID this) {
    return 37*17+typeIDKey.hashCode() + 37*17+typeIDValue.hashCode();
  }
  
}
