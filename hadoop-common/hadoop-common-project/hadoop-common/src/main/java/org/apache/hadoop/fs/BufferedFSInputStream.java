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
import java.io.BufferedInputStream;
import java.io.FileDescriptor;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * A class optimizes reading from FSInputStream by bufferring
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BufferedFSInputStream extends @Tainted BufferedInputStream
implements @Tainted Seekable, @Tainted PositionedReadable, @Tainted HasFileDescriptor {
  /**
   * Creates a <code>BufferedFSInputStream</code>
   * with the specified buffer size,
   * and saves its  argument, the input stream
   * <code>in</code>, for later use.  An internal
   * buffer array of length  <code>size</code>
   * is created and stored in <code>buf</code>.
   *
   * @param   in     the underlying input stream.
   * @param   size   the buffer size.
   * @exception IllegalArgumentException if size <= 0.
   */
  public @Tainted BufferedFSInputStream(@Tainted FSInputStream in, @Tainted int size) {
    super(in, size);
  }

  @Override
  public @Tainted long getPos(@Tainted BufferedFSInputStream this) throws IOException {
    return ((@Tainted FSInputStream)in).getPos()-(count-pos);
  }

  @Override
  public @Tainted long skip(@Tainted BufferedFSInputStream this, @Tainted long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos()+n);
    return n;
  }

  @Override
  public void seek(@Tainted BufferedFSInputStream this, @Tainted long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    if (this.pos != this.count) {
      // optimize: check if the pos is in the buffer
      // This optimization only works if pos != count -- if they are
      // equal, it's possible that the previous reads were just
      // longer than the total buffer size, and hence skipped the buffer.
      @Tainted
      long end = ((@Tainted FSInputStream)in).getPos();
      @Tainted
      long start = end - count;
      if( pos>=start && pos<end) {
        this.pos = (@Tainted int)(pos-start);
        return;
      }
    }

    // invalidate buffer
    this.pos = 0;
    this.count = 0;

    ((@Tainted FSInputStream)in).seek(pos);
  }

  @Override
  public @Tainted boolean seekToNewSource(@Tainted BufferedFSInputStream this, @Tainted long targetPos) throws IOException {
    pos = 0;
    count = 0;
    return ((@Tainted FSInputStream)in).seekToNewSource(targetPos);
  }

  @Override
  public @Tainted int read(@Tainted BufferedFSInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length) throws IOException {
    return ((@Tainted FSInputStream)in).read(position, buffer, offset, length) ;
  }

  @Override
  public void readFully(@Tainted BufferedFSInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length) throws IOException {
    ((@Tainted FSInputStream)in).readFully(position, buffer, offset, length);
  }

  @Override
  public void readFully(@Tainted BufferedFSInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer) throws IOException {
    ((@Tainted FSInputStream)in).readFully(position, buffer);
  }

  @Override
  public @Tainted FileDescriptor getFileDescriptor(@Tainted BufferedFSInputStream this) throws IOException {
    if (in instanceof @Tainted HasFileDescriptor) {
      return ((@Tainted HasFileDescriptor) in).getFileDescriptor();
    } else {
      return null;
    }
  }
}
