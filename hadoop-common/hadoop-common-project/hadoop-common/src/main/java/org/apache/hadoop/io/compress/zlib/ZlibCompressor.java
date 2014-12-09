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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link Compressor} based on the popular 
 * zlib compression algorithm.
 * http://www.zlib.net/
 * 
 */
public class ZlibCompressor implements @Tainted Compressor {

  private static final @Tainted Log LOG = LogFactory.getLog(ZlibCompressor.class);

  private static final @Tainted int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

  // HACK - Use this as a global lock in the JNI layer
  private static @Tainted Class clazz = ZlibCompressor.class;

  private @Tainted long stream;
  private @Tainted CompressionLevel level;
  private @Tainted CompressionStrategy strategy;
  private final @Tainted CompressionHeader windowBits;
  private @Tainted int directBufferSize;
  private @Tainted byte @Tainted [] userBuf = null;
  private @Tainted int userBufOff = 0;
  private @Tainted int userBufLen = 0;
  private @Tainted Buffer uncompressedDirectBuf = null;
  private @Tainted int uncompressedDirectBufOff = 0;
  private @Tainted int uncompressedDirectBufLen = 0;
  private @Tainted boolean keepUncompressedBuf = false;
  private @Tainted Buffer compressedDirectBuf = null;
  private @Tainted boolean finish;
  private @Tainted boolean finished;

  /**
   * The compression level for zlib library.
   */
  public static enum CompressionLevel {
    /**
     * Compression level for no compression.
     */

@Tainted  NO_COMPRESSION (0),
    
    /**
     * Compression level for fastest compression.
     */

@Tainted  BEST_SPEED (1),
    
    /**
     * Compression level for best compression.
     */

@Tainted  BEST_COMPRESSION (9),
    
    /**
     * Default compression level.
     */

@Tainted  DEFAULT_COMPRESSION (-1);
    
    
    private final @Tainted int compressionLevel;
    
    @Tainted
    CompressionLevel(@Tainted int level) {
      compressionLevel = level;
    }
    
    @Tainted
    int compressionLevel(ZlibCompressor.@Tainted CompressionLevel this) {
      return compressionLevel;
    }
  };
  
  /**
   * The compression level for zlib library.
   */
  public static enum CompressionStrategy {
    /**
     * Compression strategy best used for data consisting mostly of small
     * values with a somewhat random distribution. Forces more Huffman coding
     * and less string matching.
     */

@Untainted  FILTERED (1),
    
    /**
     * Compression strategy for Huffman coding only.
     */

@Untainted  HUFFMAN_ONLY (2),
    
    /**
     * Compression strategy to limit match distances to one
     * (run-length encoding).
     */

@Untainted  RLE (3),

    /**
     * Compression strategy to prevent the use of dynamic Huffman codes, 
     * allowing for a simpler decoder for special applications.
     */

@Untainted  FIXED (4),

    /**
     * Default compression strategy.
     */

@Untainted  DEFAULT_STRATEGY (0);
    
    
    private final @Tainted int compressionStrategy;
    
    @Tainted
    CompressionStrategy(@Tainted int strategy) {
      compressionStrategy = strategy;
    }
    
    @Tainted
    int compressionStrategy(ZlibCompressor.@Tainted CompressionStrategy this) {
      return compressionStrategy;
    }
  };

  /**
   * The type of header for compressed data.
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

@Tainted  GZIP_FORMAT (31);

    private final @Tainted int windowBits;
    
    @Tainted
    CompressionHeader(@Tainted int windowBits) {
      this.windowBits = windowBits;
    }
    
    public @Tainted int windowBits(ZlibCompressor.@Tainted CompressionHeader this) {
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

  protected final void construct(@Tainted ZlibCompressor this, @Tainted CompressionLevel level, @Tainted CompressionStrategy strategy,
      @Tainted
      CompressionHeader header, @Tainted int directBufferSize) {
  }

  /**
   * Creates a new compressor with the default compression level.
   * Compressed data will be generated in ZLIB format.
   */
  public @Tainted ZlibCompressor() {
    this(CompressionLevel.DEFAULT_COMPRESSION,
         CompressionStrategy.DEFAULT_STRATEGY,
         CompressionHeader.DEFAULT_HEADER,
         DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Creates a new compressor, taking settings from the configuration.
   */
  public @Tainted ZlibCompressor(@Tainted Configuration conf) {
    this(ZlibFactory.getCompressionLevel(conf),
         ZlibFactory.getCompressionStrategy(conf),
         CompressionHeader.DEFAULT_HEADER,
         DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /** 
   * Creates a new compressor using the specified compression level.
   * Compressed data will be generated in ZLIB format.
   * 
   * @param level Compression level #CompressionLevel
   * @param strategy Compression strategy #CompressionStrategy
   * @param header Compression header #CompressionHeader
   * @param directBufferSize Size of the direct buffer to be used.
   */
  public @Tainted ZlibCompressor(@Tainted CompressionLevel level, @Tainted CompressionStrategy strategy, 
                        @Tainted
                        CompressionHeader header, @Tainted int directBufferSize) {
    this.level = level;
    this.strategy = strategy;
    this.windowBits = header;
    stream = init(this.level.compressionLevel(), 
                  this.strategy.compressionStrategy(), 
                  this.windowBits.windowBits());

    this.directBufferSize = directBufferSize;
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration. It will reset the compressor's compression level
   * and compression strategy.
   * 
   * @param conf Configuration storing new settings
   */
  @Override
  public synchronized void reinit(@Tainted ZlibCompressor this, @Tainted Configuration conf) {
    reset();
    if (conf == null) {
      return;
    }
    end(stream);
    level = ZlibFactory.getCompressionLevel(conf);
    strategy = ZlibFactory.getCompressionStrategy(conf);
    stream = init(level.compressionLevel(), 
                  strategy.compressionStrategy(), 
                  windowBits.windowBits());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Reinit compressor with new compression configuration");
    }
  }

  @Override
  public synchronized void setInput(@Tainted ZlibCompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    if (b== null) {
      throw new @Tainted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }
    
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    uncompressedDirectBufOff = 0;
    setInputFromSavedData();
    
    // Reinitialize zlib's output direct buffer 
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }
  
  //copy enough data from userBuf to uncompressedDirectBuf
  synchronized void setInputFromSavedData(@Tainted ZlibCompressor this) {
    @Tainted
    int len = Math.min(userBufLen, uncompressedDirectBuf.remaining());
    ((@Tainted ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
    userBufLen -= len;
    userBufOff += len;
    uncompressedDirectBufLen = uncompressedDirectBuf.position();
  }

  @Override
  public synchronized void setDictionary(@Tainted ZlibCompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    if (stream == 0 || b == null) {
      throw new @Tainted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }
    setDictionary(stream, b, off, len);
  }

  @Override
  public synchronized @Tainted boolean needsInput(@Tainted ZlibCompressor this) {
    // Consume remaining compressed data?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if zlib has consumed all input
    // compress should be invoked if keepUncompressedBuf true
    if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
      return false;
    
    if (uncompressedDirectBuf.remaining() > 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        // copy enough data from userBuf to uncompressedDirectBuf
        setInputFromSavedData();
        if (uncompressedDirectBuf.remaining() > 0) // uncompressedDirectBuf is not full
          return true;
        else 
          return false;
      }
    }
    
    return false;
  }
  
  @Override
  public synchronized void finish(@Tainted ZlibCompressor this) {
    finish = true;
  }
  
  @Override
  public synchronized @Tainted boolean finished(@Tainted ZlibCompressor this) {
    // Check if 'zlib' says its 'finished' and
    // all compressed data has been consumed
    return (finished && compressedDirectBuf.remaining() == 0);
  }

  @Override
  public synchronized @Tainted int compress(@Tainted ZlibCompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) 
    throws IOException {
    if (b == null) {
      throw new @Tainted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @Tainted ArrayIndexOutOfBoundsException();
    }
    
    @Tainted
    int n = 0;
    
    // Check if there is compressed data
    n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@Tainted ByteBuffer)compressedDirectBuf).get(b, off, n);
      return n;
    }

    // Re-initialize the zlib's output direct buffer
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress data
    n = deflateBytesDirect();
    compressedDirectBuf.limit(n);
    
    // Check if zlib consumed all input buffer
    // set keepUncompressedBuf properly
    if (uncompressedDirectBufLen <= 0) { // zlib consumed all input buffer
      keepUncompressedBuf = false;
      uncompressedDirectBuf.clear();
      uncompressedDirectBufOff = 0;
      uncompressedDirectBufLen = 0;
    } else { // zlib did not consume all input buffer
      keepUncompressedBuf = true;
    }
    
    // Get atmost 'len' bytes
    n = Math.min(n, len);
    ((@Tainted ByteBuffer)compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Returns the total number of compressed bytes output so far.
   *
   * @return the total (non-negative) number of compressed bytes output so far
   */
  @Override
  public synchronized @Tainted long getBytesWritten(@Tainted ZlibCompressor this) {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of uncompressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of uncompressed bytes input so far
   */
  @Override
  public synchronized @Tainted long getBytesRead(@Tainted ZlibCompressor this) {
    checkStream();
    return getBytesRead(stream);
  }

  @Override
  public synchronized void reset(@Tainted ZlibCompressor this) {
    checkStream();
    reset(stream);
    finish = false;
    finished = false;
    uncompressedDirectBuf.rewind();
    uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
    keepUncompressedBuf = false;
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }
  
  @Override
  public synchronized void end(@Tainted ZlibCompressor this) {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }
  
  private void checkStream(@Tainted ZlibCompressor this) {
    if (stream == 0)
      throw new @Tainted NullPointerException();
  }
  
  private native static void initIDs();
  private native static @Tainted long init(@Tainted int level, @Tainted int strategy, @Tainted int windowBits);
  private native static void setDictionary(@Tainted long strm, @Tainted byte @Tainted [] b, @Tainted int off,
                                           @Tainted
                                           int len);
  private native @Tainted int deflateBytesDirect(@Tainted ZlibCompressor this);
  private native static @Tainted long getBytesRead(@Tainted long strm);
  private native static @Tainted long getBytesWritten(@Tainted long strm);
  private native static void reset(@Tainted long strm);
  private native static void end(@Tainted long strm);

  public native static @Tainted String getLibraryName();
}
