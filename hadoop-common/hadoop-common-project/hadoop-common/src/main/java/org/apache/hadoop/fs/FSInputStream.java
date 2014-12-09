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
import java.io.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ZeroCopyUnavailableException;

/****************************************************************
 * FSInputStream is a generic old InputStream with a little bit
 * of RAF-style seek ability.
 *
 *****************************************************************/
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public abstract class FSInputStream extends @Tainted InputStream
    implements @Tainted Seekable, @Tainted PositionedReadable {
  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   */
  @Override
  public abstract void seek(@Tainted FSInputStream this, @Tainted long pos) throws IOException;

  /**
   * Return the current offset from the start of the file
   */
  @Override
  public abstract @Tainted long getPos(@Tainted FSInputStream this) throws IOException;

  /**
   * Seeks a different copy of the data.  Returns true if 
   * found a new source, false otherwise.
   */
  @Override
  public abstract @Tainted boolean seekToNewSource(@Tainted FSInputStream this, @Tainted long targetPos) throws IOException;

  @Override
  public @Tainted int read(@Tainted FSInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length)
    throws IOException {
    synchronized (this) {
      @Tainted
      long oldPos = getPos();
      @Tainted
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }
  }
    
  @Override
  public void readFully(@Tainted FSInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length)
    throws IOException {
    @Tainted
    int nread = 0;
    while (nread < length) {
      @Tainted
      int nbytes = read(position+nread, buffer, offset+nread, length-nread);
      if (nbytes < 0) {
        throw new @Tainted EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }
  }
    
  @Override
  public void readFully(@Tainted FSInputStream this, @Tainted long position, @Tainted byte @Tainted [] buffer)
    throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
