/**
 * 
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
import java.io.*;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.fs.ByteBufferUtil;
import org.apache.hadoop.util.IdentityHashStore;

/** Utility that wraps a {@link FSInputStream} in a {@link DataInputStream}
 * and buffers input through a {@link BufferedInputStream}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataInputStream extends @Tainted DataInputStream
    implements @Tainted Seekable, @Tainted PositionedReadable, @Tainted Closeable, 
      @Tainted
      ByteBufferReadable, @Tainted HasFileDescriptor, @Tainted CanSetDropBehind, @Tainted CanSetReadahead,
      @Tainted
      HasEnhancedByteBufferAccess {
  /**
   * Map ByteBuffers that we have handed out to readers to ByteBufferPool 
   * objects
   */
  private final @Tainted IdentityHashStore<@Tainted ByteBuffer, @Tainted ByteBufferPool>
    extendedReadBuffers
      = new @Tainted IdentityHashStore<@Tainted ByteBuffer, @Tainted ByteBufferPool>(0);

  public @Tainted FSDataInputStream(@Tainted InputStream in)
    throws IOException {
    super(in);
    if( !(in instanceof @Tainted Seekable) || !(in instanceof @Tainted PositionedReadable) ) {
      throw new @Tainted IllegalArgumentException(
          "In is not an instance of Seekable or PositionedReadable");
    }
  }
  
  /**
   * Seek to the given offset.
   *
   * @param desired offset to seek to
   */
  @Override
  public synchronized void seek(@Tainted FSDataInputStream this, @Tainted long desired) throws IOException {
    ((@Tainted Seekable)in).seek(desired);
  }

  /**
   * Get the current position in the input stream.
   *
   * @return current position in the input stream
   */
  @Override
  public @Tainted long getPos(@Tainted FSDataInputStream this) throws IOException {
    return ((@Tainted Seekable)in).getPos();
  }
  
  /**
   * Read bytes from the given position in the stream to the given buffer.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    maximum number of bytes to read
   * @return total number of bytes read into the buffer, or <code>-1</code>
   *         if there is no more data because the end of the stream has been
   *         reached
   */
  @Override
  public @Tainted int read(@Tainted FSDataInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length)
    throws IOException {
    return ((@Tainted PositionedReadable)in).read(position, buffer, offset, length);
  }

  /**
   * Read bytes from the given position in the stream to the given buffer.
   * Continues to read until <code>length</code> bytes have been read.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    the number of bytes to read
   * @throws EOFException If the end of stream is reached while reading.
   *                      If an exception is thrown an undetermined number
   *                      of bytes in the buffer may have been written. 
   */
  @Override
  public void readFully(@Tainted FSDataInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length)
    throws IOException {
    ((@Tainted PositionedReadable)in).readFully(position, buffer, offset, length);
  }
  
  /**
   * See {@link #readFully(long, byte[], int, int)}.
   */
  @Override
  public void readFully(@Tainted FSDataInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer)
    throws IOException {
    ((@Tainted PositionedReadable)in).readFully(position, buffer, 0, buffer.length);
  }
  
  /**
   * Seek to the given position on an alternate copy of the data.
   *
   * @param  targetPos  position to seek to
   * @return true if a new source is found, false otherwise
   */
  @Override
  public @Tainted boolean seekToNewSource(@Tainted FSDataInputStream this, @Tainted long targetPos) throws IOException {
    return ((@Tainted Seekable)in).seekToNewSource(targetPos); 
  }
  
  /**
   * Get a reference to the wrapped input stream. Used by unit tests.
   *
   * @return the underlying input stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public @Tainted InputStream getWrappedStream(@Tainted FSDataInputStream this) {
    return in;
  }

  @Override
  public @Tainted int read(@Tainted FSDataInputStream this, @Tainted ByteBuffer buf) throws IOException {
    if (in instanceof @Tainted ByteBufferReadable) {
      return ((@Tainted ByteBufferReadable)in).read(buf);
    }

    throw new @Tainted UnsupportedOperationException("Byte-buffer read unsupported by input stream");
  }

  @Override
  public @Tainted FileDescriptor getFileDescriptor(@Tainted FSDataInputStream this) throws IOException {
    if (in instanceof @Tainted HasFileDescriptor) {
      return ((@Tainted HasFileDescriptor) in).getFileDescriptor();
    } else if (in instanceof @Tainted FileInputStream) {
      return ((@Tainted FileInputStream) in).getFD();
    } else {
      return null;
    }
  }

  @Override
  public void setReadahead(@Tainted FSDataInputStream this, @Tainted Long readahead)
      throws IOException, UnsupportedOperationException {
    try {
      ((@Tainted CanSetReadahead)in).setReadahead(readahead);
    } catch (@Tainted ClassCastException e) {
      throw new @Tainted UnsupportedOperationException(
          "this stream does not support setting the readahead " +
          "caching strategy.");
    }
  }

  @Override
  public void setDropBehind(@Tainted FSDataInputStream this, @Tainted Boolean dropBehind)
      throws IOException, UnsupportedOperationException {
    try {
      ((@Tainted CanSetDropBehind)in).setDropBehind(dropBehind);
    } catch (@Tainted ClassCastException e) {
      throw new @Tainted UnsupportedOperationException("this stream does not " +
          "support setting the drop-behind caching setting.");
    }
  }

  @Override
  public @Tainted ByteBuffer read(@Tainted FSDataInputStream this, @Tainted ByteBufferPool bufferPool, @Tainted int maxLength,
      @Tainted
      EnumSet<@Tainted ReadOption> opts) 
          throws IOException, UnsupportedOperationException {
    try {
      return ((@Tainted HasEnhancedByteBufferAccess)in).read(bufferPool,
          maxLength, opts);
    }
    catch (@Tainted ClassCastException e) {
      @Tainted
      ByteBuffer buffer = ByteBufferUtil.
          fallbackRead(this, bufferPool, maxLength);
      if (buffer != null) {
        extendedReadBuffers.put(buffer, bufferPool);
      }
      return buffer;
    }
  }

  private static final @Tainted EnumSet<@Tainted ReadOption> EMPTY_READ_OPTIONS_SET =
      EnumSet.noneOf(ReadOption.class);

  final public @Tainted ByteBuffer read(@Tainted FSDataInputStream this, @Tainted ByteBufferPool bufferPool, @Tainted int maxLength)
          throws IOException, UnsupportedOperationException {
    return read(bufferPool, maxLength, EMPTY_READ_OPTIONS_SET);
  }
  
  @Override
  public void releaseBuffer(@Tainted FSDataInputStream this, @Tainted ByteBuffer buffer) {
    try {
      ((@Tainted HasEnhancedByteBufferAccess)in).releaseBuffer(buffer);
    }
    catch (@Tainted ClassCastException e) {
      @Tainted
      ByteBufferPool bufferPool = extendedReadBuffers.remove( buffer);
      if (bufferPool == null) {
        throw new @Tainted IllegalArgumentException("tried to release a buffer " +
            "that was not created by this stream.");
      }
      bufferPool.putBuffer(buffer);
    }
  }
}
