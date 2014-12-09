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
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a wrapper class.  It wraps a Writable implementation around
 * an array of primitives (e.g., int[], long[], etc.), with optimized 
 * wire format, and without creating new objects per element.
 * 
 * This is a wrapper class only; it does not make a copy of the 
 * underlying array.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayPrimitiveWritable implements @Tainted Writable {
  
  //componentType is determined from the component type of the value array 
  //during a "set" operation.  It must be primitive.
  private @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> componentType = null; 
  //declaredComponentType need not be declared, but if you do (by using the
  //ArrayPrimitiveWritable(Class<?>) constructor), it will provide typechecking
  //for all "set" operations.
  private @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> declaredComponentType = null;
  private @Tainted int length;
  private @Tainted Object value; //must be an array of <componentType>[length]
  
  private static final @Tainted Map<@Tainted String, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object>> PRIMITIVE_NAMES = 
    new @Tainted HashMap<@Tainted String, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>(16);
  static {
    PRIMITIVE_NAMES.put(boolean.class.getName(), boolean.class);
    PRIMITIVE_NAMES.put(byte.class.getName(), byte.class);
    PRIMITIVE_NAMES.put(char.class.getName(), char.class);
    PRIMITIVE_NAMES.put(short.class.getName(), short.class);
    PRIMITIVE_NAMES.put(int.class.getName(), int.class);
    PRIMITIVE_NAMES.put(long.class.getName(), long.class);
    PRIMITIVE_NAMES.put(float.class.getName(), float.class);
    PRIMITIVE_NAMES.put(double.class.getName(), double.class);
  }
  
  private static @Tainted Class<@Tainted ?> getPrimitiveClass(@Tainted String className) {
    return PRIMITIVE_NAMES.get(className);
  }
  
  private static void checkPrimitive(@Tainted Class<@Tainted ?> componentType) {
    if (componentType == null) { 
      throw new @Tainted HadoopIllegalArgumentException("null component type not allowed"); 
    }
    if (! PRIMITIVE_NAMES.containsKey(componentType.getName())) {
      throw new @Tainted HadoopIllegalArgumentException("input array component type "
          + componentType.getName() + " is not a candidate primitive type");
    }
  }
  
  private void checkDeclaredComponentType(@Tainted ArrayPrimitiveWritable this, @Tainted Class<@Tainted ?> componentType) {
    if ((declaredComponentType != null) 
        && (componentType != declaredComponentType)) {
      throw new @Tainted HadoopIllegalArgumentException("input array component type "
          + componentType.getName() + " does not match declared type "
          + declaredComponentType.getName());     
    }
  }
  
  private static void checkArray(@Tainted Object value) {
    if (value == null) { 
      throw new @Tainted HadoopIllegalArgumentException("null value not allowed"); 
    }
    if (! value.getClass().isArray()) {
      throw new @Tainted HadoopIllegalArgumentException("non-array value of class "
          + value.getClass() + " not allowed");             
    }
  }
  
  /**
   * Construct an empty instance, for use during Writable read
   */
  public @Tainted ArrayPrimitiveWritable() {
    //empty constructor
  }
  
  /**
   * Construct an instance of known type but no value yet
   * for use with type-specific wrapper classes
   */
  public @Tainted ArrayPrimitiveWritable(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> componentType) {
    checkPrimitive(componentType);
    this.declaredComponentType = componentType;
  }
  
  /**
   * Wrap an existing array of primitives
   * @param value - array of primitives
   */
  public @Tainted ArrayPrimitiveWritable(@Tainted Object value) {
    set(value);
  }
  
  /**
   * Get the original array.  
   * Client must cast it back to type componentType[]
   * (or may use type-specific wrapper classes).
   * @return - original array as Object
   */
  public @Tainted Object get(@Tainted ArrayPrimitiveWritable this) { return value; }
  
  public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getComponentType(@Tainted ArrayPrimitiveWritable this) { return componentType; }
  
  public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getDeclaredComponentType(@Tainted ArrayPrimitiveWritable this) { return declaredComponentType; }
  
  public @Tainted boolean isDeclaredComponentType(@Tainted ArrayPrimitiveWritable this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> componentType) {
    return componentType == declaredComponentType;
  }
  
  public void set(@Tainted ArrayPrimitiveWritable this, @Tainted Object value) {
    checkArray(value);
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> componentType = value.getClass().getComponentType();
    checkPrimitive(componentType);
    checkDeclaredComponentType(componentType);
    this.componentType = componentType;
    this.value = value;
    this.length = Array.getLength(value);
  }
  
  /**
   * Do not use this class.
   * This is an internal class, purely for ObjectWritable to use as
   * a label class for transparent conversions of arrays of primitives
   * during wire protocol reads and writes.
   */
  static class Internal extends @Tainted ArrayPrimitiveWritable {
    @Tainted
    Internal() {             //use for reads
      super(); 
    }
    
    @Tainted
    Internal(@Tainted Object value) { //use for writes
      super(value);
    }
  } //end Internal subclass declaration

  /* 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  @SuppressWarnings("deprecation")
  public void write(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    // write componentType 
    UTF8.writeString(out, componentType.getName());      
    // write length
    out.writeInt(length);

    // do the inner loop.  Walk the decision tree only once.
    if (componentType == Boolean.TYPE) {          // boolean
      writeBooleanArray(out);
    } else if (componentType == Character.TYPE) { // char
      writeCharArray(out);
    } else if (componentType == Byte.TYPE) {      // byte
      writeByteArray(out);
    } else if (componentType == Short.TYPE) {     // short
      writeShortArray(out);
    } else if (componentType == Integer.TYPE) {   // int
      writeIntArray(out);
    } else if (componentType == Long.TYPE) {      // long
      writeLongArray(out);
    } else if (componentType == Float.TYPE) {     // float
      writeFloatArray(out);
    } else if (componentType == Double.TYPE) {    // double
      writeDoubleArray(out);
    } else {
      throw new @Tainted IOException("Component type " + componentType.toString()
          + " is set as the output type, but no encoding is implemented for this type.");
    }
  }

  /* 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    
    // read and set the component type of the array
    @SuppressWarnings("deprecation")
    @Tainted
    String className = UTF8.readString(in);
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> componentType = getPrimitiveClass(className);
    if (componentType == null) {
      throw new @Tainted IOException("encoded array component type "
          + className + " is not a candidate primitive type");
    }
    checkDeclaredComponentType(componentType);
    this.componentType = componentType;
  
    // read and set the length of the array
    @Tainted
    int length = in.readInt();
    if (length < 0) {
      throw new @Tainted IOException("encoded array length is negative " + length);
    }
    this.length = length;
    
    // construct and read in the array
    value = Array.newInstance(componentType, length);

    // do the inner loop.  Walk the decision tree only once.
    if (componentType == Boolean.TYPE) {             // boolean
      readBooleanArray(in);
    } else if (componentType == Character.TYPE) {    // char
      readCharArray(in);
    } else if (componentType == Byte.TYPE) {         // byte
      readByteArray(in);
    } else if (componentType == Short.TYPE) {        // short
      readShortArray(in);
    } else if (componentType == Integer.TYPE) {      // int
      readIntArray(in);
    } else if (componentType == Long.TYPE) {         // long
      readLongArray(in);
    } else if (componentType == Float.TYPE) {        // float
      readFloatArray(in);
    } else if (componentType == Double.TYPE) {       // double
      readDoubleArray(in);
    } else {
      throw new @Tainted IOException("Encoded type " + className
          + " converted to valid component type " + componentType.toString()
          + " but no encoding is implemented for this type.");
    }
  }
  
  //For efficient implementation, there's no way around
  //the following massive code duplication.
  
  private void writeBooleanArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    @Tainted
    boolean @Tainted [] v = (@Tainted boolean @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      out.writeBoolean(v[i]);
  }
  
  private void writeCharArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    @Tainted
    char @Tainted [] v = (@Tainted char @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      out.writeChar(v[i]);
  }
  
  private void writeByteArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    out.write((@Tainted byte @Tainted []) value, 0, length);
  }
  
  private void writeShortArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    @Tainted
    short @Tainted [] v = (@Tainted short @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      out.writeShort(v[i]);
  }
  
  private void writeIntArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    @Tainted
    int @Tainted [] v = (@Tainted int @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      out.writeInt(v[i]);
  }
  
  private void writeLongArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    @Tainted
    long @Tainted [] v = (@Tainted long @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      out.writeLong(v[i]);
  }
  
  private void writeFloatArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    @Tainted
    float @Tainted [] v = (@Tainted float @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      out.writeFloat(v[i]);
  }
  
  private void writeDoubleArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataOutput out) throws IOException {
    @Tainted
    double @Tainted [] v = (@Tainted double @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      out.writeDouble(v[i]);
  }

  private void readBooleanArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    @Tainted
    boolean @Tainted [] v = (@Tainted boolean @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      v[i] = in.readBoolean(); 
  }
  
  private void readCharArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    @Tainted
    char @Tainted [] v = (@Tainted char @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      v[i] = in.readChar(); 
  }
  
  private void readByteArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    in.readFully((@Tainted byte @Tainted []) value, 0, length);
  }
  
  private void readShortArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    @Tainted
    short @Tainted [] v = (@Tainted short @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      v[i] = in.readShort(); 
  }
  
  private void readIntArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    @Tainted
    int @Tainted [] v = (@Tainted int @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      v[i] = in.readInt(); 
  }
  
  private void readLongArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    @Tainted
    long @Tainted [] v = (@Tainted long @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      v[i] = in.readLong(); 
  }
  
  private void readFloatArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    @Tainted
    float @Tainted [] v = (@Tainted float @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      v[i] = in.readFloat(); 
  }
  
  private void readDoubleArray(@Tainted ArrayPrimitiveWritable this, @Tainted DataInput in) throws IOException {
    @Tainted
    double @Tainted [] v = (@Tainted double @Tainted []) value;
    for (@Tainted int i = 0; i < length; i++)
      v[i] = in.readDouble(); 
  }
}

