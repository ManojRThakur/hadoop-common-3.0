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
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

public class DataOutputByteBuffer extends @Tainted DataOutputStream {

   static class Buffer extends @Tainted OutputStream {

    final @Tainted byte @Tainted [] b = new @Tainted byte @Tainted [1];
    final @Tainted boolean direct;
    final @Tainted List<@Tainted ByteBuffer> active = new @Tainted ArrayList<@Tainted ByteBuffer>();
    final @Tainted List<@Tainted ByteBuffer> inactive = new @Tainted LinkedList<@Tainted ByteBuffer>();
    @Tainted
    int size;
    @Tainted
    int length;
    @Tainted
    ByteBuffer current;

    @Tainted
    Buffer(@Tainted int size, @Tainted boolean direct) {
      this.direct = direct;
      this.size = size;
      current = direct
          ? ByteBuffer.allocateDirect(size)
          : ByteBuffer.allocate(size);
    }
    @Override
    public void write(DataOutputByteBuffer.@Tainted Buffer this, @Tainted int b) {
      this.b[0] = (@Tainted byte)(b & 0xFF);
      write(this.b);
    }
    @Override
    public void write(DataOutputByteBuffer.@Tainted Buffer this, @Tainted byte @Tainted [] b) {
      write(b, 0, b.length);
    }
    @Override
    public void write(DataOutputByteBuffer.@Tainted Buffer this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
      @Tainted
      int rem = current.remaining();
      while (len > rem) {
        current.put(b, off, rem);
        length += rem;
        current.flip();
        active.add(current);
        off += rem;
        len -= rem;
        rem = getBuffer(len);
      }
      current.put(b, off, len);
      length += len;
    }
    @Tainted
    int getBuffer(DataOutputByteBuffer.@Tainted Buffer this, @Tainted int newsize) {
      if (inactive.isEmpty()) {
        size = Math.max(size << 1, newsize);
        current = direct
            ? ByteBuffer.allocateDirect(size)
            : ByteBuffer.allocate(size);
      } else {
        current = inactive.remove(0);
      }
      return current.remaining();
    }
    @Tainted
    ByteBuffer @Tainted [] getData(DataOutputByteBuffer.@Tainted Buffer this) {
      @Tainted
      ByteBuffer @Tainted [] ret = active.toArray(new @Tainted ByteBuffer @Tainted [active.size() + 1]);
      @Tainted
      ByteBuffer tmp = current.duplicate();
      tmp.flip();
      ret[ret.length - 1] = tmp.slice();
      return ret;
    }
    @Tainted
    int getLength(DataOutputByteBuffer.@Tainted Buffer this) {
      return length;
    }
    void reset(DataOutputByteBuffer.@Tainted Buffer this) {
      length = 0;
      current.rewind();
      inactive.add(0, current);
      for (@Tainted int i = active.size() - 1; i >= 0; --i) {
        @Tainted
        ByteBuffer b = active.remove(i);
        b.rewind();
        inactive.add(0, b);
      }
      current = inactive.remove(0);
    }
  }

  private final @Tainted Buffer buffers;

  public @Tainted DataOutputByteBuffer() {
    this(32);
  }

  public @Tainted DataOutputByteBuffer(@Tainted int size) {
    this(size, false);
  }

  public @Tainted DataOutputByteBuffer(@Tainted int size, @Tainted boolean direct) {
    this(new @Tainted Buffer(size, direct));
  }

  private @Tainted DataOutputByteBuffer(@Tainted Buffer buffers) {
    super(buffers);
    this.buffers = buffers;
  }

  public @Tainted ByteBuffer @Tainted [] getData(@Tainted DataOutputByteBuffer this) {
    return buffers.getData();
  }

  public @Tainted int getLength(@Tainted DataOutputByteBuffer this) {
    return buffers.getLength();
  }

  public void reset(@Tainted DataOutputByteBuffer this) {
    this.written = 0;
    buffers.reset();
  }

}
