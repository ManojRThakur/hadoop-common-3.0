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

/** A reusable {@link DataInput} implementation that reads from an in-memory
 * buffer.
 *
 * <p>This saves memory over creating a new DataInputStream and
 * ByteArrayInputStream each time data is read.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * DataInputBuffer buffer = new DataInputBuffer();
 * while (... loop condition ...) {
 *   byte[] data = ... get data ...;
 *   int dataLength = ... get data length ...;
 *   buffer.reset(data, dataLength);
 *   ... read buffer using DataInput methods ...
 * }
 * </pre>
 *  
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DataInputBuffer extends @Tainted DataInputStream {
  private static class Buffer extends @Tainted ByteArrayInputStream {
    public @Tainted Buffer() {
      super(new @Tainted byte @Tainted [] {});
    }

    public void reset(DataInputBuffer.@Tainted Buffer this, @Tainted byte @Tainted [] input, @Tainted int start, @Tainted int length) {
      this.buf = input;
      this.count = start+length;
      this.mark = start;
      this.pos = start;
    }

    public @Tainted byte @Tainted [] getData(DataInputBuffer.@Tainted Buffer this) { return buf; }
    public @Tainted int getPosition(DataInputBuffer.@Tainted Buffer this) { return pos; }
    public @Tainted int getLength(DataInputBuffer.@Tainted Buffer this) { return count; }
  }

  private @Tainted Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public @Tainted DataInputBuffer() {
    this(new @Tainted Buffer());
  }

  private @Tainted DataInputBuffer(@Tainted Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Resets the data that the buffer reads. */
  public void reset(@Tainted DataInputBuffer this, @Tainted byte @Tainted [] input, @Tainted int length) {
    buffer.reset(input, 0, length);
  }

  /** Resets the data that the buffer reads. */
  public void reset(@Tainted DataInputBuffer this, @Tainted byte @Tainted [] input, @Tainted int start, @Tainted int length) {
    buffer.reset(input, start, length);
  }
  
  public @Tainted byte @Tainted [] getData(@Tainted DataInputBuffer this) {
    return buffer.getData();
  }

  /** Returns the current position in the input. */
  public @Tainted int getPosition(@Tainted DataInputBuffer this) { return buffer.getPosition(); }

  /** Returns the length of the input. */
  public @Tainted int getLength(@Tainted DataInputBuffer this) { return buffer.getLength(); }

}
