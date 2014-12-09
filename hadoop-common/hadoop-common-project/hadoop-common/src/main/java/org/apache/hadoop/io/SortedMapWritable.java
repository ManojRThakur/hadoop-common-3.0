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
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Writable SortedMap.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SortedMapWritable extends @Tainted AbstractMapWritable
  implements @Tainted SortedMap<@Tainted WritableComparable, @Tainted Writable> {
  
  private @Tainted SortedMap<@Tainted WritableComparable, @Tainted Writable> instance;
  
  /** default constructor. */
  public @Tainted SortedMapWritable() {
    super();
    this.instance = new @Tainted TreeMap<@Tainted WritableComparable, @Tainted Writable>();
  }
  
  /**
   * Copy constructor.
   * 
   * @param other the map to copy from
   */
  public @Tainted SortedMapWritable(@Tainted SortedMapWritable other) {
    this();
    copy(other);
  }

  @Override
  public @Tainted Comparator<@Tainted ? super @Tainted WritableComparable> comparator(@Tainted SortedMapWritable this) {
    // Returning null means we use the natural ordering of the keys
    return null;
  }

  @Override
  public @Tainted WritableComparable firstKey(@Tainted SortedMapWritable this) {
    return instance.firstKey();
  }

  @Override
  public @Tainted SortedMap<@Tainted WritableComparable, @Tainted Writable>
  headMap(@Tainted SortedMapWritable this, @Tainted WritableComparable toKey) {
    
    return instance.headMap(toKey);
  }

  @Override
  public @Tainted WritableComparable lastKey(@Tainted SortedMapWritable this) {
    return instance.lastKey();
  }

  @Override
  public @Tainted SortedMap<@Tainted WritableComparable, @Tainted Writable>
  subMap(@Tainted SortedMapWritable this, @Tainted WritableComparable fromKey, @Tainted WritableComparable toKey) {
    
    return instance.subMap(fromKey, toKey);
  }

  @Override
  public @Tainted SortedMap<@Tainted WritableComparable, @Tainted Writable>
  tailMap(@Tainted SortedMapWritable this, @Tainted WritableComparable fromKey) {
    
    return instance.tailMap(fromKey);
  }

  @Override
  public void clear(@Tainted SortedMapWritable this) {
    instance.clear();
  }

  @Override
  public @Tainted boolean containsKey(@Tainted SortedMapWritable this, @Tainted Object key) {
    return instance.containsKey(key);
  }

  @Override
  public @Tainted boolean containsValue(@Tainted SortedMapWritable this, @Tainted Object value) {
    return instance.containsValue(value);
  }

  @Override
  public @Tainted Set<java.util.Map.@Tainted Entry<@Tainted WritableComparable, @Tainted Writable>> entrySet(@Tainted SortedMapWritable this) {
    return instance.entrySet();
  }

  @Override
  public @Tainted Writable get(@Tainted SortedMapWritable this, @Tainted Object key) {
    return instance.get(key);
  }

  @Override
  public @Tainted boolean isEmpty(@Tainted SortedMapWritable this) {
    return instance.isEmpty();
  }

  @Override
  public @Tainted Set<@Tainted WritableComparable> keySet(@Tainted SortedMapWritable this) {
    return instance.keySet();
  }

  @Override
  public @Tainted Writable put(@Tainted SortedMapWritable this, @Tainted WritableComparable key, @Tainted Writable value) {
    addToMap(key.getClass());
    addToMap(value.getClass());
    return instance.put(key, value);
  }

  @Override
  public void putAll(@Tainted SortedMapWritable this, @Tainted Map<@Tainted ? extends @Tainted WritableComparable, @Tainted ? extends @Tainted Writable> t) {
    for (Map.@Tainted Entry<@Tainted ? extends @Tainted WritableComparable, @Tainted ? extends @Tainted Writable> e:
      t.entrySet()) {
      
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public @Tainted Writable remove(@Tainted SortedMapWritable this, @Tainted Object key) {
    return instance.remove(key);
  }

  @Override
  public @Tainted int size(@Tainted SortedMapWritable this) {
    return instance.size();
  }

  @Override
  public @Tainted Collection<@Tainted Writable> values(@Tainted SortedMapWritable this) {
    return instance.values();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(@Tainted SortedMapWritable this, @Tainted DataInput in) throws IOException {
    super.readFields(in);
    
    // Read the number of entries in the map
    
    @Tainted
    int entries = in.readInt();
    
    // Then read each key/value pair
    
    for (@Tainted int i = 0; i < entries; i++) {
      @Tainted
      WritableComparable key =
        (@Tainted WritableComparable) ReflectionUtils.newInstance(getClass(
            in.readByte()), getConf());
      
      key.readFields(in);
      
      @Tainted
      Writable value = (@Tainted Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      value.readFields(in);
      instance.put(key, value);
    }
  }

  @Override
  public void write(@Tainted SortedMapWritable this, @Tainted DataOutput out) throws IOException {
    super.write(out);
    
    // Write out the number of entries in the map
    
    out.writeInt(instance.size());
    
    // Then write out each key/value pair
    
    for (Map.@Tainted Entry<@Tainted WritableComparable, @Tainted Writable> e: instance.entrySet()) {
      out.writeByte(getId(e.getKey().getClass()));
      e.getKey().write(out);
      out.writeByte(getId(e.getValue().getClass()));
      e.getValue().write(out);
    }
  }

  @Override
  public @Tainted boolean equals(@Tainted SortedMapWritable this, @Tainted Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof @Tainted SortedMapWritable) {
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
  public @Tainted int hashCode(@Tainted SortedMapWritable this) {
    return instance.hashCode();
  }
}
