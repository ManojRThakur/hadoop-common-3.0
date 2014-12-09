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
 * Represents typeID for vector. 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VectorTypeID extends @Tainted TypeID {
  private @Tainted TypeID typeIDElement; 
  
  public @Tainted VectorTypeID(@Tainted TypeID typeIDElement) {
    super(RIOType.VECTOR);
    this.typeIDElement = typeIDElement;
  }
  
  public @Tainted TypeID getElementTypeID(@Tainted VectorTypeID this) {
    return this.typeIDElement;
  }
  
  @Override
  void write(@Tainted VectorTypeID this, @Tainted RecordOutput rout, @Tainted String tag) throws IOException {
    rout.writeByte(typeVal, tag);
    typeIDElement.write(rout, tag);
  }
  
  /**
   * Two vector typeIDs are equal if their constituent elements have the 
   * same type
   */
  @Override
  public @Tainted boolean equals(@Tainted VectorTypeID this, @Tainted Object o) {
    if (!super.equals (o))
      return false;

    @Tainted
    VectorTypeID vti = (@Tainted VectorTypeID) o;
    return this.typeIDElement.equals(vti.typeIDElement);
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  @Override
  public @Tainted int hashCode(@Tainted VectorTypeID this) {
    return 37*17+typeIDElement.hashCode();
  }
  
}
