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

package org.apache.hadoop.fs;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSOutputSummer extends @Tainted OutputStream {
  // data checksum
  private @Tainted Checksum sum;
  // internal buffer for storing data before it is checksumed
  private @Tainted byte buf @Tainted [];
  // internal buffer for storing checksum
  private @Tainted byte checksum @Tainted [];
  // The number of valid bytes in the buffer.
  private @Tainted int count;
  
  protected @Tainted FSOutputSummer(@Tainted Checksum sum, @Tainted int maxChunkSize, @Tainted int checksumSize) {
    this.sum = sum;
    this.buf = new @Tainted byte @Tainted [maxChunkSize];
    this.checksum = new @Tainted byte @Tainted [checksumSize];
    this.count = 0;
  }
  
  /* write the data chunk in <code>b</code> staring at <code>offset</code> with
   * a length of <code>len</code>, and its checksum
   */
  protected abstract void writeChunk(@Tainted FSOutputSummer this, @Tainted byte @Tainted [] b, @Tainted int offset, @Tainted int len, @Tainted byte @Tainted [] checksum)
  throws IOException;
  
  /**
   * Check if the implementing OutputStream is closed and should no longer
   * accept writes. Implementations should do nothing if this stream is not
   * closed, and should throw an {@link IOException} if it is closed.
   * 
   * @throws IOException if this stream is already closed.
   */
  protected abstract void checkClosed(@Tainted FSOutputSummer this) throws IOException;

  /** Write one byte */
  @Override
  public synchronized void write(@Tainted FSOutputSummer this, @Tainted int b) throws IOException {
    sum.update(b);
    buf[count++] = (@Tainted byte)b;
    if(count == buf.length) {
      flushBuffer();
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array 
   * starting at offset <code>off</code> and generate a checksum for
   * each data chunk.
   *
   * <p> This method stores bytes from the given array into this
   * stream's buffer before it gets checksumed. The buffer gets checksumed 
   * and flushed to the underlying output stream when all data 
   * in a checksum chunk are in the buffer.  If the buffer is empty and
   * requested length is at least as large as the size of next checksum chunk
   * size, this method will checksum and write the chunk directly 
   * to the underlying output stream.  Thus it avoids uneccessary data copy.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public synchronized void write(@Tainted FSOutputSummer this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len)
      throws IOException {
    
    checkClosed();
    
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }

    for (@Tainted int n=0;n<len;n+=write1(b, off+n, len-n)) {
    }
  }
  
  /**
   * Write a portion of an array, flushing to the underlying
   * stream at most once if necessary.
   */
  private @Tainted int write1(@Tainted FSOutputSummer this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
    if(count==0 && len>=buf.length) {
      // local buffer is empty and user data has one chunk
      // checksum and output data
      final @Tainted int length = buf.length;
      sum.update(b, off, length);
      writeChecksumChunk(b, off, length, false);
      return length;
    }
    
    // copy user data to local buffer
    @Tainted
    int bytesToCopy = buf.length-count;
    bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
    sum.update(b, off, bytesToCopy);
    System.arraycopy(b, off, buf, count, bytesToCopy);
    count += bytesToCopy;
    if (count == buf.length) {
      // local buffer is full
      flushBuffer();
    } 
    return bytesToCopy;
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream. 
   */
  protected synchronized void flushBuffer(@Tainted FSOutputSummer this) throws IOException {
    flushBuffer(false);
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream.  If keep is true, then the state of 
   * this object remains intact.
   */
  protected synchronized void flushBuffer(@Tainted FSOutputSummer this, @Tainted boolean keep) throws IOException {
    if (count != 0) {
      @Tainted
      int chunkLen = count;
      count = 0;
      writeChecksumChunk(buf, 0, chunkLen, keep);
      if (keep) {
        count = chunkLen;
      }
    }
  }

  /**
   * Return the number of valid bytes currently in the buffer.
   */
  protected synchronized @Tainted int getBufferedDataSize(@Tainted FSOutputSummer this) {
    return count;
  }
  
  /** Generate checksum for the data chunk and output data chunk & checksum
   * to the underlying output stream. If keep is true then keep the
   * current checksum intact, do not reset it.
   */
  private void writeChecksumChunk(@Tainted FSOutputSummer this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len, @Tainted boolean keep)
  throws IOException {
    @Tainted
    int tempChecksum = (@Tainted int)sum.getValue();
    if (!keep) {
      sum.reset();
    }
    int2byte(tempChecksum, checksum);
    writeChunk(b, off, len, checksum);
  }

  /**
   * Converts a checksum integer value to a byte stream
   */
  static public @Tainted byte @Tainted [] convertToByteStream(@Tainted Checksum sum, @Tainted int checksumSize) {
    return int2byte((@Tainted int)sum.getValue(), new @Tainted byte @Tainted [checksumSize]);
  }

  static @Tainted byte @Tainted [] int2byte(@Tainted int integer, @Tainted byte @Tainted [] bytes) {
    bytes[0] = (@Tainted byte)((integer >>> 24) & 0xFF);
    bytes[1] = (@Tainted byte)((integer >>> 16) & 0xFF);
    bytes[2] = (@Tainted byte)((integer >>>  8) & 0xFF);
    bytes[3] = (@Tainted byte)((integer >>>  0) & 0xFF);
    return bytes;
  }

  /**
   * Resets existing buffer with a new one of the specified size.
   */
  protected synchronized void resetChecksumChunk(@Tainted FSOutputSummer this, @Tainted int size) {
    sum.reset();
    this.buf = new @Tainted byte @Tainted [size];
    this.count = 0;
  }
}
