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
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataInputByteBuffer extends @Tainted DataInputStream {

  private static class Buffer extends @Tainted InputStream {
    private final @Tainted byte @Tainted [] scratch = new @Tainted byte @Tainted [1];
    @Tainted
    ByteBuffer @Tainted [] buffers = new @Tainted ByteBuffer @Tainted [0];
    @Tainted
    int bidx;
    @Tainted
    int pos;
    @Tainted
    int length;
    @Override
    public @Tainted int read(DataInputByteBuffer.@Tainted Buffer this) {
      if (-1 == read(scratch, 0, 1)) {
        return -1;
      }
      return scratch[0] & 0xFF;
    }
    @Override
    public @Tainted int read(DataInputByteBuffer.@Tainted Buffer this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
      if (bidx >= buffers.length) {
        return -1;
      }
      @Tainted
      int cur = 0;
      do {
        @Tainted
        int rem = Math.min(len, buffers[bidx].remaining());
        buffers[bidx].get(b, off, rem);
        cur += rem;
        off += rem;
        len -= rem;
      } while (len > 0 && ++bidx < buffers.length);
      pos += cur;
      return cur;
    }
    public void reset(DataInputByteBuffer.@Tainted Buffer this, @Tainted ByteBuffer @Tainted [] buffers) {
      bidx = pos = length = 0;
      this.buffers = buffers;
      for (@Tainted ByteBuffer b : buffers) {
        length += b.remaining();
      }
    }
    public @Tainted int getPosition(DataInputByteBuffer.@Tainted Buffer this) {
      return pos;
    }
    public @Tainted int getLength(DataInputByteBuffer.@Tainted Buffer this) {
      return length;
    }
    public @Tainted ByteBuffer @Tainted [] getData(DataInputByteBuffer.@Tainted Buffer this) {
      return buffers;
    }
  }

  private @Tainted Buffer buffers;

  public @Tainted DataInputByteBuffer() {
    this(new @Tainted Buffer());
  }

  private @Tainted DataInputByteBuffer(@Tainted Buffer buffers) {
    super(buffers);
    this.buffers = buffers;
  }

  public void reset(@Tainted DataInputByteBuffer this, @Tainted ByteBuffer @Tainted ... input) {
    buffers.reset(input);
  }

  public @Tainted ByteBuffer @Tainted [] getData(@Tainted DataInputByteBuffer this) {
    return buffers.getData();
  }

  public @Tainted int getPosition(@Tainted DataInputByteBuffer this) {
    return buffers.getPosition();
  }

  public @Tainted int getLength(@Tainted DataInputByteBuffer this) {
    return buffers.getLength();
  }
}
