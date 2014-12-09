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

package org.apache.hadoop.record;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.util.TreeMap;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface that all the serializers have to implement.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RecordOutput {
  /**
   * Write a byte to serialized record.
   * @param b Byte to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeByte(@Tainted RecordOutput this, @Tainted byte b, @Tainted String tag) throws IOException;
  
  /**
   * Write a boolean to serialized record.
   * @param b Boolean to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeBool(@Tainted RecordOutput this, @Tainted boolean b, @Tainted String tag) throws IOException;
  
  /**
   * Write an integer to serialized record.
   * @param i Integer to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeInt(@Tainted RecordOutput this, @Tainted int i, @Tainted String tag) throws IOException;
  
  /**
   * Write a long integer to serialized record.
   * @param l Long to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeLong(@Tainted RecordOutput this, @Tainted long l, @Tainted String tag) throws IOException;
  
  /**
   * Write a single-precision float to serialized record.
   * @param f Float to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeFloat(@Tainted RecordOutput this, @Tainted float f, @Tainted String tag) throws IOException;
  
  /**
   * Write a double precision floating point number to serialized record.
   * @param d Double to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeDouble(@Tainted RecordOutput this, @Tainted double d, @Tainted String tag) throws IOException;
  
  /**
   * Write a unicode string to serialized record.
   * @param s String to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeString(@Tainted RecordOutput this, @Tainted String s, @Tainted String tag) throws IOException;
  
  /**
   * Write a buffer to serialized record.
   * @param buf Buffer to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeBuffer(@Tainted RecordOutput this, @Tainted Buffer buf, @Tainted String tag)
    throws IOException;
  
  /**
   * Mark the start of a record to be serialized.
   * @param r Record to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startRecord(@Tainted RecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException;
  
  /**
   * Mark the end of a serialized record.
   * @param r Record to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endRecord(@Tainted RecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException;
  
  /**
   * Mark the start of a vector to be serialized.
   * @param v Vector to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startVector(@Tainted RecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException;
  
  /**
   * Mark the end of a serialized vector.
   * @param v Vector to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endVector(@Tainted RecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException;
  
  /**
   * Mark the start of a map to be serialized.
   * @param m Map to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startMap(@Tainted RecordOutput this, @Tainted TreeMap m, @Tainted String tag) throws IOException;
  
  /**
   * Mark the end of a serialized map.
   * @param m Map to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endMap(@Tainted RecordOutput this, @Tainted TreeMap m, @Tainted String tag) throws IOException;
}
