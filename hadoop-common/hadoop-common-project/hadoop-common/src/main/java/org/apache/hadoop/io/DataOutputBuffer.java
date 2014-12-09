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
import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A reusable {@link DataOutput} implementation that writes to an in-memory
 * buffer.
 *
 * <p>This saves memory over creating a new DataOutputStream and
 * ByteArrayOutputStream each time data is written.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * DataOutputBuffer buffer = new DataOutputBuffer();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write buffer using DataOutput methods ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 *  
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DataOutputBuffer extends @Tainted DataOutputStream {

  private static class Buffer extends @Tainted ByteArrayOutputStream {
    public @Tainted byte @Tainted [] getData(DataOutputBuffer.@Tainted Buffer this) { return buf; }
    public @Tainted int getLength(DataOutputBuffer.@Tainted Buffer this) { return count; }

    public @Tainted Buffer() {
      super();
    }
    
    public @Tainted Buffer(@Tainted int size) {
      super(size);
    }
    
    public void write(DataOutputBuffer.@Tainted Buffer this, @Tainted DataInput in, @Tainted int len) throws IOException {
      @Tainted
      int newcount = count + len;
      if (newcount > buf.length) {
        @Tainted
        byte newbuf @Tainted [] = new @Tainted byte @Tainted [Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      in.readFully(buf, count, len);
      count = newcount;
    }
  }

  private @Tainted Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public @Tainted DataOutputBuffer() {
    this(new @Tainted Buffer());
  }
  
  public @Tainted DataOutputBuffer(@Tainted int size) {
    this(new @Tainted Buffer(size));
  }
  
  private @Tainted DataOutputBuffer(@Tainted Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Returns the current contents of the buffer.
   *  Data is only valid to {@link #getLength()}.
   */
  public @Tainted byte @Tainted [] getData(@Tainted DataOutputBuffer this) { return buffer.getData(); }

  /** Returns the length of the valid data currently in the buffer. */
  public @Tainted int getLength(@Tainted DataOutputBuffer this) { return buffer.getLength(); }

  /** Resets the buffer to empty. */
  public @Tainted DataOutputBuffer reset(@Tainted DataOutputBuffer this) {
    this.written = 0;
    buffer.reset();
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. */
  public void write(@Tainted DataOutputBuffer this, @Tainted DataInput in, @Tainted int length) throws IOException {
    buffer.write(in, length);
  }

  /** Write to a file stream */
  public void writeTo(@Tainted DataOutputBuffer this, @Tainted OutputStream out) throws IOException {
    buffer.writeTo(out);
  }
}
