/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.BytesWritable;

/**
 * Adaptor class to wrap byte-array backed objects (including java byte array)
 * as RawComparable objects.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ByteArray implements @Tainted RawComparable {
  private final @Tainted byte @Tainted [] buffer;
  private final @Tainted int offset;
  private final @Tainted int len;

  /**
   * Constructing a ByteArray from a {@link BytesWritable}.
   * 
   * @param other
   */
  public @Tainted ByteArray(@Tainted BytesWritable other) {
    this(other.getBytes(), 0, other.getLength());
  }

  /**
   * Wrap a whole byte array as a RawComparable.
   * 
   * @param buffer
   *          the byte array buffer.
   */
  public @Tainted ByteArray(@Tainted byte @Tainted [] buffer) {
    this(buffer, 0, buffer.length);
  }

  /**
   * Wrap a partial byte array as a RawComparable.
   * 
   * @param buffer
   *          the byte array buffer.
   * @param offset
   *          the starting offset
   * @param len
   *          the length of the consecutive bytes to be wrapped.
   */
  public @Tainted ByteArray(@Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int len) {
    if ((offset | len | (buffer.length - offset - len)) < 0) {
      throw new @Tainted IndexOutOfBoundsException();
    }
    this.buffer = buffer;
    this.offset = offset;
    this.len = len;
  }

  /**
   * @return the underlying buffer.
   */
  @Override
  public @Tainted byte @Tainted [] buffer(@Tainted ByteArray this) {
    return buffer;
  }

  /**
   * @return the offset in the buffer.
   */
  @Override
  public @Tainted int offset(@Tainted ByteArray this) {
    return offset;
  }

  /**
   * @return the size of the byte array.
   */
  @Override
  public @Tainted int size(@Tainted ByteArray this) {
    return len;
  }
}
