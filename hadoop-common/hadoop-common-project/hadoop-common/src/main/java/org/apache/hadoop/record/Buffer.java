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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A byte sequence that is used as a Java native type for buffer.
 * It is resizable and distinguishes between the count of the seqeunce and
 * the current capacity.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Buffer implements @Tainted Comparable, @Tainted Cloneable {
  /** Number of valid bytes in this.bytes. */
  private @Tainted int count;
  /** Backing store for Buffer. */
  private @Tainted byte @Tainted [] bytes = null;

  /**
   * Create a zero-count sequence.
   */
  public @Tainted Buffer() {
    this.count = 0;
  }

  /**
   * Create a Buffer using the byte array as the initial value.
   *
   * @param bytes This array becomes the backing storage for the object.
   */
  public @Tainted Buffer(@Tainted byte @Tainted [] bytes) {
    this.bytes = bytes;
    this.count = (bytes == null) ? 0 : bytes.length;
  }
  
  /**
   * Create a Buffer using the byte range as the initial value.
   *
   * @param bytes Copy of this array becomes the backing storage for the object.
   * @param offset offset into byte array
   * @param length length of data
   */
  public @Tainted Buffer(@Tainted byte @Tainted [] bytes, @Tainted int offset, @Tainted int length) {
    copy(bytes, offset, length);
  }
  
  
  /**
   * Use the specified bytes array as underlying sequence.
   *
   * @param bytes byte sequence
   */
  public void set(@Tainted Buffer this, @Tainted byte @Tainted [] bytes) {
    this.count = (bytes == null) ? 0 : bytes.length;
    this.bytes = bytes;
  }
  
  /**
   * Copy the specified byte array to the Buffer. Replaces the current buffer.
   *
   * @param bytes byte array to be assigned
   * @param offset offset into byte array
   * @param length length of data
   */
  public final void copy(@Tainted Buffer this, @Tainted byte @Tainted [] bytes, @Tainted int offset, @Tainted int length) {
    if (this.bytes == null || this.bytes.length < length) {
      this.bytes = new @Tainted byte @Tainted [length];
    }
    System.arraycopy(bytes, offset, this.bytes, 0, length);
    this.count = length;
  }
  
  /**
   * Get the data from the Buffer.
   * 
   * @return The data is only valid between 0 and getCount() - 1.
   */
  public @Tainted byte @Tainted [] get(@Tainted Buffer this) {
    if (bytes == null) {
      bytes = new @Tainted byte @Tainted [0];
    }
    return bytes;
  }
  
  /**
   * Get the current count of the buffer.
   */
  public @Tainted int getCount(@Tainted Buffer this) {
    return count;
  }
  
  /**
   * Get the capacity, which is the maximum count that could handled without
   * resizing the backing storage.
   * 
   * @return The number of bytes
   */
  public @Tainted int getCapacity(@Tainted Buffer this) {
    return this.get().length;
  }
  
  /**
   * Change the capacity of the backing storage.
   * The data is preserved if newCapacity >= getCount().
   * @param newCapacity The new capacity in bytes.
   */
  public void setCapacity(@Tainted Buffer this, @Tainted int newCapacity) {
    if (newCapacity < 0) {
      throw new @Tainted IllegalArgumentException("Invalid capacity argument "+newCapacity); 
    }
    if (newCapacity == 0) {
      this.bytes = null;
      this.count = 0;
      return;
    }
    if (newCapacity != getCapacity()) {
      @Tainted
      byte @Tainted [] data = new @Tainted byte @Tainted [newCapacity];
      if (newCapacity < count) {
        count = newCapacity;
      }
      if (count != 0) {
        System.arraycopy(this.get(), 0, data, 0, count);
      }
      bytes = data;
    }
  }
  
  /**
   * Reset the buffer to 0 size
   */
  public void reset(@Tainted Buffer this) {
    setCapacity(0);
  }
  
  /**
   * Change the capacity of the backing store to be the same as the current 
   * count of buffer.
   */
  public void truncate(@Tainted Buffer this) {
    setCapacity(count);
  }
  
  /**
   * Append specified bytes to the buffer.
   *
   * @param bytes byte array to be appended
   * @param offset offset into byte array
   * @param length length of data

  */
  public void append(@Tainted Buffer this, @Tainted byte @Tainted [] bytes, @Tainted int offset, @Tainted int length) {
    setCapacity(count+length);
    System.arraycopy(bytes, offset, this.get(), count, length);
    count = count + length;
  }
  
  /**
   * Append specified bytes to the buffer
   *
   * @param bytes byte array to be appended
   */
  public void append(@Tainted Buffer this, @Tainted byte @Tainted [] bytes) {
    append(bytes, 0, bytes.length);
  }
  
  // inherit javadoc
  @Override
  public @Tainted int hashCode(@Tainted Buffer this) {
    @Tainted
    int hash = 1;
    @Tainted
    byte @Tainted [] b = this.get();
    for (@Tainted int i = 0; i < count; i++)
      hash = (31 * hash) + b[i];
    return hash;
  }
  
  /**
   * Define the sort order of the Buffer.
   * 
   * @param other The other buffer
   * @return Positive if this is bigger than other, 0 if they are equal, and
   *         negative if this is smaller than other.
   */
  @Override
  public @Tainted int compareTo(@Tainted Buffer this, @Tainted Object other) {
    @Tainted
    Buffer right = ((@Tainted Buffer) other);
    @Tainted
    byte @Tainted [] lb = this.get();
    @Tainted
    byte @Tainted [] rb = right.get();
    for (@Tainted int i = 0; i < count && i < right.count; i++) {
      @Tainted
      int a = (lb[i] & 0xff);
      @Tainted
      int b = (rb[i] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return count - right.count;
  }
  
  // inherit javadoc
  @Override
  public @Tainted boolean equals(@Tainted Buffer this, @Tainted Object other) {
    if (other instanceof @Tainted Buffer && this != other) {
      return compareTo(other) == 0;
    }
    return (this == other);
  }
  
  // inheric javadoc
  @Override
  public @Tainted String toString(@Tainted Buffer this) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder(2*count);
    for(@Tainted int idx = 0; idx < count; idx++) {
      sb.append(Character.forDigit((bytes[idx] & 0xF0) >> 4, 16));
      sb.append(Character.forDigit(bytes[idx] & 0x0F, 16));
    }
    return sb.toString();
  }
  
  /**
   * Convert the byte buffer to a string an specific character encoding
   *
   * @param charsetName Valid Java Character Set Name
   */
  public @Tainted String toString(@Tainted Buffer this, @Tainted String charsetName)
    throws UnsupportedEncodingException {
    return new @Tainted String(this.get(), 0, this.getCount(), charsetName);
  }
  
  // inherit javadoc
  @Override
  public @Tainted Object clone(@Tainted Buffer this) throws CloneNotSupportedException {
    @Tainted
    Buffer result = (@Tainted Buffer) super.clone();
    result.copy(this.get(), 0, this.getCount());
    return result;
  }
}
