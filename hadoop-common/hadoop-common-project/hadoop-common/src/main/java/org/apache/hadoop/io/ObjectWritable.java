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
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.io.*;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ProtoUtil;

import com.google.protobuf.Message;

/** A polymorphic Writable that writes an instance with it's class name.
 * Handles arrays, strings and primitive types without a Writable wrapper.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ObjectWritable implements @Tainted Writable, @Tainted Configurable {

  private @Tainted Class declaredClass;
  private @Tainted Object instance;
  private @Tainted Configuration conf;

  public @Tainted ObjectWritable() {}
  
  public @Tainted ObjectWritable(@Tainted Object instance) {
    set(instance);
  }

  public @Tainted ObjectWritable(@Tainted Class declaredClass, @Tainted Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** Return the instance, or null if none. */
  public @Tainted Object get(@Tainted ObjectWritable this) { return instance; }
  
  /** Return the class this is meant to be. */
  public @Tainted Class getDeclaredClass(@Tainted ObjectWritable this) { return declaredClass; }
  
  /** Reset the instance. */
  public void set(@Tainted ObjectWritable this, @Tainted Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }
  
  @Override
  public @Tainted String toString(@Tainted ObjectWritable this) {
    return "OW[class=" + declaredClass + ",value=" + instance + "]";
  }

  
  @Override
  public void readFields(@Tainted ObjectWritable this, @Tainted DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }
  
  @Override
  public void write(@Tainted ObjectWritable this, @Tainted DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass, conf);
  }

  private static final @Tainted Map<@Tainted String, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object>> PRIMITIVE_NAMES = new @Tainted HashMap<@Tainted String, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>();
  static {
    PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
    PRIMITIVE_NAMES.put("byte", Byte.TYPE);
    PRIMITIVE_NAMES.put("char", Character.TYPE);
    PRIMITIVE_NAMES.put("short", Short.TYPE);
    PRIMITIVE_NAMES.put("int", Integer.TYPE);
    PRIMITIVE_NAMES.put("long", Long.TYPE);
    PRIMITIVE_NAMES.put("float", Float.TYPE);
    PRIMITIVE_NAMES.put("double", Double.TYPE);
    PRIMITIVE_NAMES.put("void", Void.TYPE);
  }

  private static class NullInstance extends @Tainted Configured implements @Tainted Writable {
    private @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> declaredClass;
    public @Tainted NullInstance() { super(null); }
    public @Tainted NullInstance(@Tainted Class declaredClass, @Tainted Configuration conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }
    @Override
    public void readFields(ObjectWritable.@Tainted NullInstance this, @Tainted DataInput in) throws IOException {
      @Tainted
      String className = UTF8.readString(in);
      declaredClass = PRIMITIVE_NAMES.get(className);
      if (declaredClass == null) {
        try {
          declaredClass = getConf().getClassByName(className);
        } catch (@Tainted ClassNotFoundException e) {
          throw new @Tainted RuntimeException(e.toString());
        }
      }
    }
    @Override
    public void write(ObjectWritable.@Tainted NullInstance this, @Tainted DataOutput out) throws IOException {
      UTF8.writeString(out, declaredClass.getName());
    }
  }

  /** Write a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static void writeObject(@Tainted DataOutput out, @Tainted Object instance,
                                 @Tainted
                                 Class declaredClass, 
                                 @Tainted
                                 Configuration conf) throws IOException {
    writeObject(out, instance, declaredClass, conf, false);
  }
  
    /** 
     * Write a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding.  
     * 
     * @param allowCompactArrays - set true for RPC and internal or intra-cluster
     * usages.  Set false for inter-cluster, File, and other persisted output 
     * usages, to preserve the ability to interchange files with other clusters 
     * that may not be running the same version of software.  Sometime in ~2013 
     * we can consider removing this parameter and always using the compact format.
     */
    public static void writeObject(@Tainted DataOutput out, @Tainted Object instance,
        @Tainted
        Class declaredClass, @Tainted Configuration conf, @Tainted boolean allowCompactArrays) 
    throws IOException {

    if (instance == null) {                       // null
      instance = new @Tainted NullInstance(declaredClass, conf);
      declaredClass = Writable.class;
    }
    
    // Special case: must come before writing out the declaredClass.
    // If this is an eligible array of primitives,
    // wrap it in an ArrayPrimitiveWritable$Internal wrapper class.
    if (allowCompactArrays && declaredClass.isArray()
        && instance.getClass().getName().equals(declaredClass.getName())
        && instance.getClass().getComponentType().isPrimitive()) {
      instance = new ArrayPrimitiveWritable.@Tainted Internal(instance);
      declaredClass = ArrayPrimitiveWritable.Internal.class;
    }

    UTF8.writeString(out, declaredClass.getName()); // always write declared

    if (declaredClass.isArray()) {     // non-primitive or non-compact array
      @Tainted
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (@Tainted int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i),
            declaredClass.getComponentType(), conf, allowCompactArrays);
      }
      
    } else if (declaredClass == ArrayPrimitiveWritable.Internal.class) {
      ((ArrayPrimitiveWritable.@Tainted Internal) instance).write(out);
      
    } else if (declaredClass == String.class) {   // String
      UTF8.writeString(out, (@Tainted String)instance);
      
    } else if (declaredClass.isPrimitive()) {     // primitive type

      if (declaredClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((@Tainted Boolean)instance).booleanValue());
      } else if (declaredClass == Character.TYPE) { // char
        out.writeChar(((@Tainted Character)instance).charValue());
      } else if (declaredClass == Byte.TYPE) {    // byte
        out.writeByte(((@Tainted Byte)instance).byteValue());
      } else if (declaredClass == Short.TYPE) {   // short
        out.writeShort(((@Tainted Short)instance).shortValue());
      } else if (declaredClass == Integer.TYPE) { // int
        out.writeInt(((@Tainted Integer)instance).intValue());
      } else if (declaredClass == Long.TYPE) {    // long
        out.writeLong(((@Tainted Long)instance).longValue());
      } else if (declaredClass == Float.TYPE) {   // float
        out.writeFloat(((@Tainted Float)instance).floatValue());
      } else if (declaredClass == Double.TYPE) {  // double
        out.writeDouble(((@Tainted Double)instance).doubleValue());
      } else if (declaredClass == Void.TYPE) {    // void
      } else {
        throw new @Tainted IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isEnum()) {         // enum
      UTF8.writeString(out, ((@Tainted Enum)instance).name());
    } else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
      UTF8.writeString(out, instance.getClass().getName());
      ((@Tainted Writable)instance).write(out);

    } else if (Message.class.isAssignableFrom(declaredClass)) {
      ((@Tainted Message)instance).writeDelimitedTo(
          DataOutputOutputStream.constructOutputStream(out));
    } else {
      throw new @Tainted IOException("Can't write: "+instance+" as "+declaredClass);
    }
  }
  
  
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static @Tainted Object readObject(@Tainted DataInput in, @Tainted Configuration conf)
    throws IOException {
    return readObject(in, null, conf);
  }
    
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  @SuppressWarnings("unchecked")
  public static @Tainted Object readObject(@Tainted DataInput in, @Tainted ObjectWritable objectWritable, @Tainted Configuration conf)
    throws IOException {
    @Tainted
    String className = UTF8.readString(in);
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> declaredClass = PRIMITIVE_NAMES.get(className);
    if (declaredClass == null) {
      declaredClass = loadClass(conf, className);
    }
    
    @Tainted
    Object instance;
    
    if (declaredClass.isPrimitive()) {            // primitive types

      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = Character.valueOf(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = Byte.valueOf(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = Short.valueOf(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = Integer.valueOf(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = Long.valueOf(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = Float.valueOf(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = Double.valueOf(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new @Tainted IllegalArgumentException("Not a primitive: "+declaredClass);
      }

    } else if (declaredClass.isArray()) {              // array
      @Tainted
      int length = in.readInt();
      instance = Array.newInstance(declaredClass.getComponentType(), length);
      for (@Tainted int i = 0; i < length; i++) {
        Array.set(instance, i, readObject(in, conf));
      }
      
    } else if (declaredClass == ArrayPrimitiveWritable.Internal.class) {
      // Read and unwrap ArrayPrimitiveWritable$Internal array.
      // Always allow the read, even if write is disabled by allowCompactArrays.
      ArrayPrimitiveWritable.@Tainted Internal temp = 
          new ArrayPrimitiveWritable.@Tainted Internal();
      temp.readFields(in);
      instance = temp.get();
      declaredClass = instance.getClass();

    } else if (declaredClass == String.class) {        // String
      instance = UTF8.readString(in);
    } else if (declaredClass.isEnum()) {         // enum
      instance = Enum.valueOf((@Tainted Class<@Tainted ? extends @Tainted Enum>) declaredClass, UTF8.readString(in));
    } else if (Message.class.isAssignableFrom(declaredClass)) {
      instance = tryInstantiateProtobuf(declaredClass, in);
    } else {                                      // Writable
      @Tainted
      Class instanceClass = null;
      @Tainted
      String str = UTF8.readString(in);
      instanceClass = loadClass(conf, str);
      
      @Tainted
      Writable writable = WritableFactories.newInstance(instanceClass, conf);
      writable.readFields(in);
      instance = writable;

      if (instanceClass == NullInstance.class) {  // null
        declaredClass = ((@Tainted NullInstance)instance).declaredClass;
        instance = null;
      }
    }

    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }

    return instance;
      
  }

  /**
   * Try to instantiate a protocol buffer of the given message class
   * from the given input stream.
   * 
   * @param protoClass the class of the generated protocol buffer
   * @param dataIn the input stream to read from
   * @return the instantiated Message instance
   * @throws IOException if an IO problem occurs
   */
  private static @Tainted Message tryInstantiateProtobuf(
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> protoClass,
      @Tainted
      DataInput dataIn) throws IOException {

    try {
      if (dataIn instanceof @Tainted InputStream) {
        // We can use the built-in parseDelimitedFrom and not have to re-copy
        // the data
        @Tainted
        Method parseMethod = getStaticProtobufMethod(protoClass,
            "parseDelimitedFrom", InputStream.class);
        return (@Tainted Message)parseMethod.invoke(null, (@Tainted InputStream)dataIn);
      } else {
        // Have to read it into a buffer first, since protobuf doesn't deal
        // with the DataInput interface directly.
        
        // Read the size delimiter that writeDelimitedTo writes
        @Tainted
        int size = ProtoUtil.readRawVarint32(dataIn);
        if (size < 0) {
          throw new @Tainted IOException("Invalid size: " + size);
        }
      
        @Tainted
        byte @Tainted [] data = new @Tainted byte @Tainted [size];
        dataIn.readFully(data);
        @Tainted
        Method parseMethod = getStaticProtobufMethod(protoClass,
            "parseFrom", byte[].class);
        return (@Tainted Message)parseMethod.invoke(null, data);
      }
    } catch (@Tainted InvocationTargetException e) {
      
      if (e.getCause() instanceof @Tainted IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new @Tainted IOException(e.getCause());
      }
    } catch (@Tainted IllegalAccessException iae) {
      throw new @Tainted AssertionError("Could not access parse method in " +
          protoClass);
    }
  }

  static @Tainted Method getStaticProtobufMethod(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> declaredClass, @Tainted String method,
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted ... args) {

    try {
      return declaredClass.getMethod(method, args);
    } catch (@Tainted Exception e) {
      // This is a bug in Hadoop - protobufs should all have this static method
      throw new @Tainted AssertionError("Protocol buffer class " + declaredClass +
          " does not have an accessible parseFrom(InputStream) method!");
    }
  }

  /**
   * Find and load the class with given name <tt>className</tt> by first finding
   * it in the specified <tt>conf</tt>. If the specified <tt>conf</tt> is null,
   * try load it directly.
   */
  public static @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> loadClass(@Tainted Configuration conf, @Tainted String className) {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> declaredClass = null;
    try {
      if (conf != null)
        declaredClass = conf.getClassByName(className);
      else
        declaredClass = Class.forName(className);
    } catch (@Tainted ClassNotFoundException e) {
      throw new @Tainted RuntimeException("readObject can't find class " + className,
          e);
    }
    return declaredClass;
  }

  @Override
  public void setConf(@Tainted ObjectWritable this, @Tainted Configuration conf) {
    this.conf = conf;
  }

  @Override
  public @Tainted Configuration getConf(@Tainted ObjectWritable this) {
    return this.conf;
  }
  
}
