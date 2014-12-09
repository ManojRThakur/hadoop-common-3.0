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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

/**
 * Abstract base class for MapWritable and SortedMapWritable
 * 
 * Unlike org.apache.nutch.crawl.MapWritable, this class allows creation of
 * MapWritable&lt;Writable, MapWritable&gt; so the CLASS_TO_ID and ID_TO_CLASS
 * maps travel with the class instead of being static.
 * 
 * Class ids range from 1 to 127 so there can be at most 127 distinct classes
 * in any specific map instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractMapWritable implements @Tainted Writable, @Tainted Configurable {
  private @Tainted AtomicReference<@Tainted Configuration> conf;
  
  /* Class to id mappings */
  @VisibleForTesting
  @Tainted
  Map<@Tainted Class, @Tainted Byte> classToIdMap = new @Tainted ConcurrentHashMap<@Tainted Class, @Tainted Byte>();
  
  /* Id to Class mappings */
  @VisibleForTesting
  @Tainted
  Map<@Tainted Byte, @Tainted Class> idToClassMap = new @Tainted ConcurrentHashMap<@Tainted Byte, @Tainted Class>();
  
  /* The number of new classes (those not established by the constructor) */
  private volatile @Tainted byte newClasses = 0;
  
  /** @return the number of known classes */
  @Tainted
  byte getNewClasses(@Tainted AbstractMapWritable this) {
    return newClasses;
  }

  /**
   * Used to add "predefined" classes and by Writable to copy "new" classes.
   */
  private synchronized void addToMap(@Tainted AbstractMapWritable this, @Tainted Class clazz, @Tainted byte id) {
    if (classToIdMap.containsKey(clazz)) {
      @Tainted
      byte b = classToIdMap.get(clazz);
      if (b != id) {
        throw new @Tainted IllegalArgumentException ("Class " + clazz.getName() +
          " already registered but maps to " + b + " and not " + id);
      }
    }
    if (idToClassMap.containsKey(id)) {
      @Tainted
      Class c = idToClassMap.get(id);
      if (!c.equals(clazz)) {
        throw new @Tainted IllegalArgumentException("Id " + id + " exists but maps to " +
            c.getName() + " and not " + clazz.getName());
      }
    }
    classToIdMap.put(clazz, id);
    idToClassMap.put(id, clazz);
  }
  
  /** Add a Class to the maps if it is not already present. */ 
  protected synchronized void addToMap(@Tainted AbstractMapWritable this, @Tainted Class clazz) {
    if (classToIdMap.containsKey(clazz)) {
      return;
    }
    if (newClasses + 1 > Byte.MAX_VALUE) {
      throw new @Tainted IndexOutOfBoundsException("adding an additional class would" +
      " exceed the maximum number allowed");
    }
    @Tainted
    byte id = ++newClasses;
    addToMap(clazz, id);
  }

  /** @return the Class class for the specified id */
  protected @Tainted Class getClass(@Tainted AbstractMapWritable this, @Tainted byte id) {
    return idToClassMap.get(id);
  }

  /** @return the id for the specified Class */
  protected @Tainted byte getId(@Tainted AbstractMapWritable this, @Tainted Class clazz) {
    return classToIdMap.containsKey(clazz) ? classToIdMap.get(clazz) : -1;
  }

  /** Used by child copy constructors. */
  protected synchronized void copy(@Tainted AbstractMapWritable this, @Tainted Writable other) {
    if (other != null) {
      try {
        @Tainted
        DataOutputBuffer out = new @Tainted DataOutputBuffer();
        other.write(out);
        @Tainted
        DataInputBuffer in = new @Tainted DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        readFields(in);
        
      } catch (@Tainted IOException e) {
        throw new @Tainted IllegalArgumentException("map cannot be copied: " +
            e.getMessage());
      }
      
    } else {
      throw new @Tainted IllegalArgumentException("source map cannot be null");
    }
  }
  
  /** constructor. */
  protected @Tainted AbstractMapWritable() {
    this.conf = new @Tainted AtomicReference<@Tainted Configuration>();

    addToMap(ArrayWritable.class,
        Byte.valueOf(Integer.valueOf(-127).byteValue())); 
    addToMap(BooleanWritable.class,
        Byte.valueOf(Integer.valueOf(-126).byteValue()));
    addToMap(BytesWritable.class,
        Byte.valueOf(Integer.valueOf(-125).byteValue()));
    addToMap(FloatWritable.class,
        Byte.valueOf(Integer.valueOf(-124).byteValue()));
    addToMap(IntWritable.class,
        Byte.valueOf(Integer.valueOf(-123).byteValue()));
    addToMap(LongWritable.class,
        Byte.valueOf(Integer.valueOf(-122).byteValue()));
    addToMap(MapWritable.class,
        Byte.valueOf(Integer.valueOf(-121).byteValue()));
    addToMap(MD5Hash.class,
        Byte.valueOf(Integer.valueOf(-120).byteValue()));
    addToMap(NullWritable.class,
        Byte.valueOf(Integer.valueOf(-119).byteValue()));
    addToMap(ObjectWritable.class,
        Byte.valueOf(Integer.valueOf(-118).byteValue()));
    addToMap(SortedMapWritable.class,
        Byte.valueOf(Integer.valueOf(-117).byteValue()));
    addToMap(Text.class,
        Byte.valueOf(Integer.valueOf(-116).byteValue()));
    addToMap(TwoDArrayWritable.class,
        Byte.valueOf(Integer.valueOf(-115).byteValue()));
    
    // UTF8 is deprecated so we don't support it

    addToMap(VIntWritable.class,
        Byte.valueOf(Integer.valueOf(-114).byteValue()));
    addToMap(VLongWritable.class,
        Byte.valueOf(Integer.valueOf(-113).byteValue()));

  }

  /** @return the conf */
  @Override
  public @Tainted Configuration getConf(@Tainted AbstractMapWritable this) {
    return conf.get();
  }

  /** @param conf the conf to set */
  @Override
  public void setConf(@Tainted AbstractMapWritable this, @Tainted Configuration conf) {
    this.conf.set(conf);
  }
  
  @Override
  public void write(@Tainted AbstractMapWritable this, @Tainted DataOutput out) throws IOException {
    
    // First write out the size of the class table and any classes that are
    // "unknown" classes
    
    out.writeByte(newClasses);

    for (@Tainted byte i = 1; i <= newClasses; i++) {
      out.writeByte(i);
      out.writeUTF(getClass(i).getName());
    }
  }
  
  @Override
  public void readFields(@Tainted AbstractMapWritable this, @Tainted DataInput in) throws IOException {
    
    // Get the number of "unknown" classes
    
    newClasses = in.readByte();
    
    // Then read in the class names and add them to our tables
    
    for (@Tainted int i = 0; i < newClasses; i++) {
      @Tainted
      byte id = in.readByte();
      @Tainted
      String className = in.readUTF();
      try {
        addToMap(Class.forName(className), id);
        
      } catch (@Tainted ClassNotFoundException e) {
        throw new @Tainted IOException("can't find class: " + className + " because "+
            e.getMessage());
      }
    }
  }    
}
