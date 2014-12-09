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
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Utility that wraps a {@link OutputStream} in a {@link DataOutputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataOutputStream extends @Tainted DataOutputStream
    implements @Tainted Syncable, @Tainted CanSetDropBehind {
  private final @Tainted OutputStream wrappedStream;

  private static class PositionCache extends @Tainted FilterOutputStream {
    private final FileSystem.@Tainted Statistics statistics;
    private @Tainted long position;

    @Tainted
    PositionCache(@Tainted OutputStream out, FileSystem.@Tainted Statistics stats, @Tainted long pos) {
      super(out);
      statistics = stats;
      position = pos;
    }

    @Override
    public void write(FSDataOutputStream.@Tainted PositionCache this, @Tainted int b) throws IOException {
      out.write(b);
      position++;
      if (statistics != null) {
        statistics.incrementBytesWritten(1);
      }
    }
    
    @Override
    public void write(FSDataOutputStream.@Tainted PositionCache this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
      if (statistics != null) {
        statistics.incrementBytesWritten(len);
      }
    }
      
    @Tainted
    long getPos(FSDataOutputStream.@Tainted PositionCache this) {
      return position;                            // return cached position
    }

    @Override
    public void close(FSDataOutputStream.@Tainted PositionCache this) throws IOException {
      out.close();
    }
  }

  public @Tainted FSDataOutputStream(@Tainted OutputStream out, FileSystem.@Tainted Statistics stats) {
    this(out, stats, 0);
  }

  public @Tainted FSDataOutputStream(@Tainted OutputStream out, FileSystem.@Tainted Statistics stats,
                            @Tainted
                            long startPosition) {
    super(new @Tainted PositionCache(out, stats, startPosition));
    wrappedStream = out;
  }
  
  /**
   * Get the current position in the output stream.
   *
   * @return the current position in the output stream
   */
  public @Tainted long getPos(@Tainted FSDataOutputStream this) {
    return ((@Tainted PositionCache)out).getPos();
  }

  /**
   * Close the underlying output stream.
   */
  @Override
  public void close(@Tainted FSDataOutputStream this) throws IOException {
    out.close(); // This invokes PositionCache.close()
  }

  /**
   * Get a reference to the wrapped output stream. Used by unit tests.
   *
   * @return the underlying output stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public @Tainted OutputStream getWrappedStream(@Tainted FSDataOutputStream this) {
    return wrappedStream;
  }

  @Override  // Syncable
  public void hflush(@Tainted FSDataOutputStream this) throws IOException {
    if (wrappedStream instanceof @Tainted Syncable) {
      ((@Tainted Syncable)wrappedStream).hflush();
    } else {
      wrappedStream.flush();
    }
  }
  
  @Override  // Syncable
  public void hsync(@Tainted FSDataOutputStream this) throws IOException {
    if (wrappedStream instanceof @Tainted Syncable) {
      ((@Tainted Syncable)wrappedStream).hsync();
    } else {
      wrappedStream.flush();
    }
  }

  @Override
  public void setDropBehind(@Tainted FSDataOutputStream this, @Tainted Boolean dropBehind) throws IOException {
    try {
      ((@Tainted CanSetDropBehind)wrappedStream).setDropBehind(dropBehind);
    } catch (@Tainted ClassCastException e) {
      throw new @Tainted UnsupportedOperationException("the wrapped stream does " +
          "not support setting the drop-behind caching setting.");
    }
  }
}
