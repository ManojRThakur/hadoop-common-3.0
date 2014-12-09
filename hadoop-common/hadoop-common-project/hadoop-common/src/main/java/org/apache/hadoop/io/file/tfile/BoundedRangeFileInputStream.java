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
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * BoundedRangeFIleInputStream abstracts a contiguous region of a Hadoop
 * FSDataInputStream as a regular input stream. One can create multiple
 * BoundedRangeFileInputStream on top of the same FSDataInputStream and they
 * would not interfere with each other.
 */
class BoundedRangeFileInputStream extends @Tainted InputStream {

  private @Tainted FSDataInputStream in;
  private @Tainted long pos;
  private @Tainted long end;
  private @Tainted long mark;
  private final @Tainted byte @Tainted [] oneByte = new @Tainted byte @Tainted [1];

  /**
   * Constructor
   * 
   * @param in
   *          The FSDataInputStream we connect to.
   * @param offset
   *          Begining offset of the region.
   * @param length
   *          Length of the region.
   * 
   *          The actual length of the region may be smaller if (off_begin +
   *          length) goes beyond the end of FS input stream.
   */
  public @Tainted BoundedRangeFileInputStream(@Tainted FSDataInputStream in, @Tainted long offset,
      @Tainted
      long length) {
    if (offset < 0 || length < 0) {
      throw new @Tainted IndexOutOfBoundsException("Invalid offset/length: " + offset
          + "/" + length);
    }

    this.in = in;
    this.pos = offset;
    this.end = offset + length;
    this.mark = -1;
  }

  @Override
  public @Tainted int available(@Tainted BoundedRangeFileInputStream this) throws IOException {
    @Tainted
    int avail = in.available();
    if (pos + avail > end) {
      avail = (@Tainted int) (end - pos);
    }

    return avail;
  }

  @Override
  public @Tainted int read(@Tainted BoundedRangeFileInputStream this) throws IOException {
    @Tainted
    int ret = read(oneByte);
    if (ret == 1) return oneByte[0] & 0xff;
    return -1;
  }

  @Override
  public @Tainted int read(@Tainted BoundedRangeFileInputStream this, @Tainted byte @Tainted [] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public @Tainted int read(@Tainted BoundedRangeFileInputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new @Tainted IndexOutOfBoundsException();
    }

    @Tainted
    int n = (@Tainted int) Math.min(Integer.MAX_VALUE, Math.min(len, (end - pos)));
    if (n == 0) return -1;
    @Tainted
    int ret = 0;
    synchronized (in) {
      in.seek(pos);
      ret = in.read(b, off, n);
    }
    if (ret < 0) {
      end = pos;
      return -1;
    }
    pos += ret;
    return ret;
  }

  @Override
  /*
   * We may skip beyond the end of the file.
   */
  public @Tainted long skip(@Tainted BoundedRangeFileInputStream this, @Tainted long n) throws IOException {
    @Tainted
    long len = Math.min(n, end - pos);
    pos += len;
    return len;
  }

  @Override
  public synchronized void mark(@Tainted BoundedRangeFileInputStream this, @Tainted int readlimit) {
    mark = pos;
  }

  @Override
  public synchronized void reset(@Tainted BoundedRangeFileInputStream this) throws IOException {
    if (mark < 0) throw new @Tainted IOException("Resetting to invalid mark");
    pos = mark;
  }

  @Override
  public @Tainted boolean markSupported(@Tainted BoundedRangeFileInputStream this) {
    return true;
  }

  @Override
  public void close(@Tainted BoundedRangeFileInputStream this) {
    // Invalidate the state of the stream.
    in = null;
    pos = end;
    mark = -1;
  }
}
