/*
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

package org.apache.hadoop.io.compress.bzip2;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link Decompressor} based on the popular 
 * bzip2 compression algorithm.
 * http://www.bzip2.org/
 * 
 */
public class Bzip2Decompressor implements @Tainted Decompressor {
  private static final @Tainted int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;
  
  private static final @Tainted Log LOG = LogFactory.getLog(Bzip2Decompressor.class);

  // HACK - Use this as a global lock in the JNI layer.
  private static @Tainted Class<@Tainted Bzip2Decompressor> clazz = Bzip2Decompressor.class;
  
  private @Tainted long stream;
  private @Tainted boolean conserveMemory;
  private @Tainted int directBufferSize;
  private @Tainted Buffer compressedDirectBuf = null;
  private @Tainted int compressedDirectBufOff;
  private @Tainted int compressedDirectBufLen;
  private @Tainted Buffer uncompressedDirectBuf = null;
  private @Tainted byte @Tainted [] userBuf = null;
  private @Tainted int userBufOff = 0;
  private @Tainted int userBufLen = 0;
  private @Tainted boolean finished;

  /**
   * Creates a new decompressor.
   */
  public @Tainted Bzip2Decompressor(@Tainted boolean conserveMemory, @Tainted int directBufferSize) {
    this.conserveMemory = conserveMemory;
    this.directBufferSize = directBufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    
    stream = init(conserveMemory ? 1 : 0);
  }
  
  public @Tainted Bzip2Decompressor() {
    this(false, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  @Override
  public synchronized void setInput(@Tainted Bzip2Decompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    if (b == null) {
      throw new @Tainted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }
  
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    
    setInputFromSavedData();
    
    // Reinitialize bzip2's output direct buffer.
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }
  
  synchronized void setInputFromSavedData(@Tainted Bzip2Decompressor this) {
    compressedDirectBufOff = 0;
    compressedDirectBufLen = userBufLen;
    if (compressedDirectBufLen > directBufferSize) {
      compressedDirectBufLen = directBufferSize;
    }

    // Reinitialize bzip2's input direct buffer.
    compressedDirectBuf.rewind();
    ((@Tainted ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, 
                                          compressedDirectBufLen);
    
    // Note how much data is being fed to bzip2.
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  @Override
  public synchronized void setDictionary(@Tainted Bzip2Decompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public synchronized @Tainted boolean needsInput(@Tainted Bzip2Decompressor this) {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }
    
    // Check if bzip2 has consumed all input.
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input.
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }

  @Override
  public synchronized @Tainted boolean needsDictionary(@Tainted Bzip2Decompressor this) {
    return false;
  }

  @Override
  public synchronized @Tainted boolean finished(@Tainted Bzip2Decompressor this) {
    // Check if bzip2 says it has finished and
    // all compressed data has been consumed.
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  @Override
  public synchronized @Tainted int decompress(@Tainted Bzip2Decompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) 
    throws IOException {
    if (b == null) {
      throw new @Tainted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }
    
    // Check if there is uncompressed data.
    @Tainted
    int n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@Tainted ByteBuffer)uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    
    // Re-initialize bzip2's output direct buffer.
    uncompressedDirectBuf.rewind();
    uncompressedDirectBuf.limit(directBufferSize);

    // Decompress the data.
    n = finished ? 0 : inflateBytesDirect();
    uncompressedDirectBuf.limit(n);

    // Get at most 'len' bytes.
    n = Math.min(n, len);
    ((@Tainted ByteBuffer)uncompressedDirectBuf).get(b, off, n);

    return n;
  }
  
  /**
   * Returns the total number of uncompressed bytes output so far.
   *
   * @return the total (non-negative) number of uncompressed bytes output so far
   */
  public synchronized @Tainted long getBytesWritten(@Tainted Bzip2Decompressor this) {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of compressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of compressed bytes input so far
   */
  public synchronized @Tainted long getBytesRead(@Tainted Bzip2Decompressor this) {
    checkStream();
    return getBytesRead(stream);
  }

  /**
   * Returns the number of bytes remaining in the input buffers; normally
   * called when finished() is true to determine amount of post-gzip-stream
   * data.</p>
   *
   * @return the total (non-negative) number of unprocessed bytes in input
   */
  @Override
  public synchronized @Tainted int getRemaining(@Tainted Bzip2Decompressor this) {
    checkStream();
    return userBufLen + getRemaining(stream);  // userBuf + compressedDirectBuf
  }

  /**
   * Resets everything including the input buffers (user and direct).</p>
   */
  @Override
  public synchronized void reset(@Tainted Bzip2Decompressor this) {
    checkStream();
    end(stream);
    stream = init(conserveMemory ? 1 : 0);
    finished = false;
    compressedDirectBufOff = compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  @Override
  public synchronized void end(@Tainted Bzip2Decompressor this) {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  static void initSymbols(@Tainted String libname) {
    initIDs(libname);
  }

  private void checkStream(@Tainted Bzip2Decompressor this) {
    if (stream == 0)
      throw new @Tainted NullPointerException();
  }
  
  private native static void initIDs(@Tainted String libname);
  private native static @Tainted long init(@Tainted int conserveMemory);
  private native @Tainted int inflateBytesDirect(@Tainted Bzip2Decompressor this);
  private native static @Tainted long getBytesRead(@Tainted long strm);
  private native static @Tainted long getBytesWritten(@Tainted long strm);
  private native static @Tainted int getRemaining(@Tainted long strm);
  private native static void end(@Tainted long strm);
}
