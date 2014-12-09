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

package org.apache.hadoop.io.compress.lz4;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Decompressor} based on the lz4 compression algorithm.
 * http://code.google.com/p/lz4/
 */
public class Lz4Decompressor implements @Tainted Decompressor {
  private static final @Tainted Log LOG =
      LogFactory.getLog(Lz4Compressor.class.getName());
  private static final @Tainted int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  // HACK - Use this as a global lock in the JNI layer
  @SuppressWarnings({"unchecked", "unused"})
  private static @Tainted Class clazz = Lz4Decompressor.class;

  private @Tainted int directBufferSize;
  private @Tainted Buffer compressedDirectBuf = null;
  private @Tainted int compressedDirectBufLen;
  private @Tainted Buffer uncompressedDirectBuf = null;
  private @Tainted byte @Tainted [] userBuf = null;
  private @Tainted int userBufOff = 0;
  private @Tainted int userBufLen = 0;
  private @Tainted boolean finished;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      // Initialize the native library
      try {
        initIDs();
      } catch (@Tainted Throwable t) {
        // Ignore failure to load/initialize lz4
        LOG.warn(t.toString());
      }
    } else {
      LOG.error("Cannot load " + Lz4Compressor.class.getName() +
          " without native hadoop library!");
    }
  }

  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public @Tainted Lz4Decompressor(@Tainted int directBufferSize) {
    this.directBufferSize = directBufferSize;

    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);

  }

  /**
   * Creates a new decompressor with the default buffer size.
   */
  public @Tainted Lz4Decompressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Sets input data for decompression.
   * This should be called if and only if {@link #needsInput()} returns
   * <code>true</code> indicating that more input data is required.
   * (Both native and non-native versions of various Decompressors require
   * that the data passed in via <code>b[]</code> remain unmodified until
   * the caller is explicitly notified--via {@link #needsInput()}--that the
   * buffer may be safely modified.  With this requirement, an extra
   * buffer-copy can be avoided.)
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public synchronized void setInput(@Tainted Lz4Decompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
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

    // Reinitialize lz4's output direct-buffer
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  synchronized void setInputFromSavedData(@Tainted Lz4Decompressor this) {
    compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

    // Reinitialize lz4's input direct buffer
    compressedDirectBuf.rewind();
    ((@Tainted ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff,
        compressedDirectBufLen);

    // Note how much data is being fed to lz4
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  /**
   * Does nothing.
   */
  @Override
  public synchronized void setDictionary(@Tainted Lz4Decompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * {@link #setInput(byte[], int, int)} should be called to
   * provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         {@link #setInput(byte[], int, int)} should be called in
   *         order to provide more input.
   */
  @Override
  public synchronized @Tainted boolean needsInput(@Tainted Lz4Decompressor this) {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if lz4 has consumed all input
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

  /**
   * Returns <code>false</code>.
   *
   * @return <code>false</code>.
   */
  @Override
  public synchronized @Tainted boolean needsDictionary(@Tainted Lz4Decompressor this) {
    return false;
  }

  /**
   * Returns true if the end of the decompressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the decompressed
   *         data output stream has been reached.
   */
  @Override
  public synchronized @Tainted boolean finished(@Tainted Lz4Decompressor this) {
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  /**
   * Fills specified buffer with uncompressed data. Returns actual number
   * of bytes of uncompressed data. A return value of 0 indicates that
   * {@link #needsInput()} should be called in order to determine if more
   * input data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   * @throws IOException
   */
  @Override
  public synchronized @Tainted int decompress(@Tainted Lz4Decompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len)
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
      ((@Tainted ByteBuffer) uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    if (compressedDirectBufLen > 0) {
      // Re-initialize the lz4's output direct buffer
      uncompressedDirectBuf.rewind();
      uncompressedDirectBuf.limit(directBufferSize);

      // Decompress data
      n = decompressBytesDirect();
      uncompressedDirectBuf.limit(n);

      if (userBufLen <= 0) {
        finished = true;
      }

      // Get atmost 'len' bytes
      n = Math.min(n, len);
      ((@Tainted ByteBuffer) uncompressedDirectBuf).get(b, off, n);
    }

    return n;
  }

  /**
   * Returns <code>0</code>.
   *
   * @return <code>0</code>.
   */
  @Override
  public synchronized @Tainted int getRemaining(@Tainted Lz4Decompressor this) {
    // Never use this function in BlockDecompressorStream.
    return 0;
  }

  @Override
  public synchronized void reset(@Tainted Lz4Decompressor this) {
    finished = false;
    compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  /**
   * Resets decompressor and input and output buffers so that a new set of
   * input data can be processed.
   */
  @Override
  public synchronized void end(@Tainted Lz4Decompressor this) {
    // do nothing
  }

  private native static void initIDs();

  private native @Tainted int decompressBytesDirect(@Tainted Lz4Decompressor this);
}
