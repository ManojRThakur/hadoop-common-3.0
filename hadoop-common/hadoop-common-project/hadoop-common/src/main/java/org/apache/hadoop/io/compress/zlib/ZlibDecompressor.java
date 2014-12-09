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

package org.apache.hadoop.io.compress.zlib;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Decompressor} based on the popular 
 * zlib compression algorithm.
 * http://www.zlib.net/
 * 
 */
public class ZlibDecompressor implements @Tainted Decompressor {
  private static final @Tainted int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;
  
  // HACK - Use this as a global lock in the JNI layer
  private static @Tainted Class clazz = ZlibDecompressor.class;
  
  private @Tainted long stream;
  private @Tainted CompressionHeader header;
  private @Tainted int directBufferSize;
  private @Tainted Buffer compressedDirectBuf = null;
  private @Tainted int compressedDirectBufOff;
  private @Tainted int compressedDirectBufLen;
  private @Tainted Buffer uncompressedDirectBuf = null;
  private @Tainted byte @Tainted [] userBuf = null;
  private @Tainted int userBufOff = 0; 
  private @Tainted int userBufLen = 0;
  private @Tainted boolean finished;
  private @Tainted boolean needDict;

  /**
   * The headers to detect from compressed data.
   */
  public static enum CompressionHeader {
    /**
     * No headers/trailers/checksums.
     */

@Tainted  NO_HEADER (-15),
    
    /**
     * Default headers/trailers/checksums.
     */

@Tainted  DEFAULT_HEADER (15),
    
    /**
     * Simple gzip headers/trailers.
     */

@Tainted  GZIP_FORMAT (31),
    
    /**
     * Autodetect gzip/zlib headers/trailers.
     */

@Tainted  AUTODETECT_GZIP_ZLIB (47);

    private final @Tainted int windowBits;
    
    @Tainted
    CompressionHeader(@Tainted int windowBits) {
      this.windowBits = windowBits;
    }
    
    public @Tainted int windowBits(ZlibDecompressor.@Tainted CompressionHeader this) {
      return windowBits;
    }
  }

  private static @Tainted boolean nativeZlibLoaded = false;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        // Initialize the native library
        initIDs();
        nativeZlibLoaded = true;
      } catch (@Tainted Throwable t) {
        // Ignore failure to load/initialize native-zlib
      }
    }
  }
  
  static @Tainted boolean isNativeZlibLoaded() {
    return nativeZlibLoaded;
  }

  /**
   * Creates a new decompressor.
   */
  public @Tainted ZlibDecompressor(@Tainted CompressionHeader header, @Tainted int directBufferSize) {
    this.header = header;
    this.directBufferSize = directBufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    
    stream = init(this.header.windowBits());
  }
  
  public @Tainted ZlibDecompressor() {
    this(CompressionHeader.DEFAULT_HEADER, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  @Override
  public synchronized void setInput(@Tainted ZlibDecompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
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
    
    // Reinitialize zlib's output direct buffer 
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }
  
  synchronized void setInputFromSavedData(@Tainted ZlibDecompressor this) {
    compressedDirectBufOff = 0;
    compressedDirectBufLen = userBufLen;
    if (compressedDirectBufLen > directBufferSize) {
      compressedDirectBufLen = directBufferSize;
    }

    // Reinitialize zlib's input direct buffer
    compressedDirectBuf.rewind();
    ((@Tainted ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, 
                                          compressedDirectBufLen);
    
    // Note how much data is being fed to zlib
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  @Override
  public synchronized void setDictionary(@Tainted ZlibDecompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    if (stream == 0 || b == null) {
      throw new @Tainted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }
    setDictionary(stream, b, off, len);
    needDict = false;
  }

  @Override
  public synchronized @Tainted boolean needsInput(@Tainted ZlibDecompressor this) {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }
    
    // Check if zlib has consumed all input
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }

  @Override
  public synchronized @Tainted boolean needsDictionary(@Tainted ZlibDecompressor this) {
    return needDict;
  }

  @Override
  public synchronized @Tainted boolean finished(@Tainted ZlibDecompressor this) {
    // Check if 'zlib' says it's 'finished' and
    // all compressed data has been consumed
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  @Override
  public synchronized @Tainted int decompress(@Tainted ZlibDecompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) 
    throws IOException {
    if (b == null) {
      throw new @Tainted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }
    
    @Tainted
    int n = 0;
    
    // Check if there is uncompressed data
    n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@Tainted ByteBuffer)uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    
    // Re-initialize the zlib's output direct buffer
    uncompressedDirectBuf.rewind();
    uncompressedDirectBuf.limit(directBufferSize);

    // Decompress data
    n = inflateBytesDirect();
    uncompressedDirectBuf.limit(n);

    // Get at most 'len' bytes
    n = Math.min(n, len);
    ((@Tainted ByteBuffer)uncompressedDirectBuf).get(b, off, n);

    return n;
  }
  
  /**
   * Returns the total number of uncompressed bytes output so far.
   *
   * @return the total (non-negative) number of uncompressed bytes output so far
   */
  public synchronized @Tainted long getBytesWritten(@Tainted ZlibDecompressor this) {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of compressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of compressed bytes input so far
   */
  public synchronized @Tainted long getBytesRead(@Tainted ZlibDecompressor this) {
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
  public synchronized @Tainted int getRemaining(@Tainted ZlibDecompressor this) {
    checkStream();
    return userBufLen + getRemaining(stream);  // userBuf + compressedDirectBuf
  }

  /**
   * Resets everything including the input buffers (user and direct).</p>
   */
  @Override
  public synchronized void reset(@Tainted ZlibDecompressor this) {
    checkStream();
    reset(stream);
    finished = false;
    needDict = false;
    compressedDirectBufOff = compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  @Override
  public synchronized void end(@Tainted ZlibDecompressor this) {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  @Override
  protected void finalize(@Tainted ZlibDecompressor this) {
    end();
  }
  
  private void checkStream(@Tainted ZlibDecompressor this) {
    if (stream == 0)
      throw new @Tainted NullPointerException();
  }
  
  private native static void initIDs();
  private native static @Tainted long init(@Tainted int windowBits);
  private native static void setDictionary(@Tainted long strm, @Tainted byte @Tainted [] b, @Tainted int off,
                                           @Tainted
                                           int len);
  private native @Tainted int inflateBytesDirect(@Tainted ZlibDecompressor this);
  private native static @Tainted long getBytesRead(@Tainted long strm);
  private native static @Tainted long getBytesWritten(@Tainted long strm);
  private native static @Tainted int getRemaining(@Tainted long strm);
  private native static void reset(@Tainted long strm);
  private native static void end(@Tainted long strm);
}
