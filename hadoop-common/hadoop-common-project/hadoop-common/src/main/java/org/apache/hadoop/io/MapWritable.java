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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Writable Map.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapWritable extends @Tainted AbstractMapWritable
  implements @Tainted Map<@Tainted Writable, @Tainted Writable> {

  private @Tainted Map<@Tainted Writable, @Tainted Writable> instance;
  
  /** Default constructor. */
  public @Tainted MapWritable() {
    super();
    this.instance = new @Tainted HashMap<@Tainted Writable, @Tainted Writable>();
  }
  
  /**
   * Copy constructor.
   * 
   * @param other the map to copy from
   */
  public @Tainted MapWritable(@Tainted MapWritable other) {
    this();
    copy(other);
  }
  
  @Override
  public void clear(@Tainted MapWritable this) {
    instance.clear();
  }

  @Override
  public @Tainted boolean containsKey(@Tainted MapWritable this, @Tainted Object key) {
    return instance.containsKey(key);
  }

  @Override
  public @Tainted boolean containsValue(@Tainted MapWritable this, @Tainted Object value) {
    return instance.containsValue(value);
  }

  @Override
  public @Tainted Set<Map.@Tainted Entry<@Tainted Writable, @Tainted Writable>> entrySet(@Tainted MapWritable this) {
    return instance.entrySet();
  }

  @Override
  public @Tainted boolean equals(@Tainted MapWritable this, @Tainted Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof @Tainted MapWritable) {
      @Tainted
      Map map = (@Tainted Map) obj;
      if (size() != map.size()) {
        return false;
      }

      return entrySet().equals(map.entrySet());
    }

    return false;
  }

  @Override
  public @Tainted Writable get(@Tainted MapWritable this, @Tainted Object key) {
    return instance.get(key);
  }
  
  @Override
  public @Tainted int hashCode(@Tainted MapWritable this) {
    return 1 + this.instance.hashCode();
  }

  @Override
  public @Tainted boolean isEmpty(@Tainted MapWritable this) {
    return instance.isEmpty();
  }

  @Override
  public @Tainted Set<@Tainted Writable> keySet(@Tainted MapWritable this) {
    return instance.keySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public @Tainted Writable put(@Tainted MapWritable this, @Tainted Writable key, @Tainted Writable value) {
    addToMap(key.getClass());
    addToMap(value.getClass());
    return instance.put(key, value);
  }

  @Override
  public void putAll(@Tainted MapWritable this, @Tainted Map<@Tainted ? extends @Tainted Writable, @Tainted ? extends @Tainted Writable> t) {
    for (Map.@Tainted Entry<@Tainted ? extends @Tainted Writable, @Tainted ? extends @Tainted Writable> e: t.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public @Tainted Writable remove(@Tainted MapWritable this, @Tainted Object key) {
    return instance.remove(key);
  }

  @Override
  public @Tainted int size(@Tainted MapWritable this) {
    return instance.size();
  }

  @Override
  public @Tainted Collection<@Tainted Writable> values(@Tainted MapWritable this) {
    return instance.values();
  }
  
  // Writable
  
  @Override
  public void write(@Tainted MapWritable this, @Tainted DataOutput out) throws IOException {
    super.write(out);
    
    // Write out the number of entries in the map
    
    out.writeInt(instance.size());

    // Then write out each key/value pair
    
    for (Map.@Tainted Entry<@Tainted Writable, @Tainted Writable> e: instance.entrySet()) {
      out.writeByte(getId(e.getKey().getClass()));
      e.getKey().write(out);
      out.writeByte(getId(e.getValue().getClass()));
      e.getValue().write(out);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(@Tainted MapWritable this, @Tainted DataInput in) throws IOException {
    super.readFields(in);
    
    // First clear the map.  Otherwise we will just accumulate
    // entries every time this method is called.
    this.instance.clear();
    
    // Read the number of entries in the map
    
    @Tainted
    int entries = in.readInt();
    
    // Then read each key/value pair
    
    for (@Tainted int i = 0; i < entries; i++) {
      @Tainted
      Writable key = (@Tainted Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      key.readFields(in);
      
      @Tainted
      Writable value = (@Tainted Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      value.readFields(in);
      instance.put(key, value);
    }
  }
}
