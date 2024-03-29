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

package org.apache.hadoop.io.compress;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
/**
 * A compression input stream.
 *
 * <p>Implementations are assumed to be buffered.  This permits clients to
 * reposition the underlying input stream then call {@link #resetState()},
 * without having to also synchronize client buffers.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class CompressionInputStream extends @Tainted InputStream implements @Tainted Seekable {
  /**
   * The input stream to be compressed. 
   */
  protected final @Tainted InputStream in;
  protected @Tainted long maxAvailableData = 0L;

  /**
   * Create a compression input stream that reads
   * the decompressed bytes from the given stream.
   * 
   * @param in The input stream to be compressed.
   * @throws IOException
   */
  protected @Tainted CompressionInputStream(@Tainted InputStream in) throws IOException {
    if (!(in instanceof @Tainted Seekable) || !(in instanceof @Tainted PositionedReadable)) {
        this.maxAvailableData = in.available();
    }
    this.in = in;
  }

  @Override
  public void close(@Tainted CompressionInputStream this) throws IOException {
    in.close();
  }
  
  /**
   * Read bytes from the stream.
   * Made abstract to prevent leakage to underlying stream.
   */
  @Override
  public abstract @Tainted int read(@Tainted CompressionInputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException;

  /**
   * Reset the decompressor to its initial state and discard any buffered data,
   * as the underlying stream may have been repositioned.
   */
  public abstract void resetState(@Tainted CompressionInputStream this) throws IOException;
  
  /**
   * This method returns the current position in the stream.
   *
   * @return Current position in stream as a long
   */
  @Override
  public @Tainted long getPos(@Tainted CompressionInputStream this) throws IOException {
    if (!(in instanceof @Tainted Seekable) || !(in instanceof @Tainted PositionedReadable)){
      //This way of getting the current position will not work for file
      //size which can be fit in an int and hence can not be returned by
      //available method.
      return (this.maxAvailableData - this.in.available());
    }
    else{
      return ((@Tainted Seekable)this.in).getPos();
    }

  }

  /**
   * This method is current not supported.
   *
   * @throws UnsupportedOperationException
   */

  @Override
  public void seek(@Tainted CompressionInputStream this, @Tainted long pos) throws UnsupportedOperationException {
    throw new @Tainted UnsupportedOperationException();
  }

  /**
   * This method is current not supported.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public @Tainted boolean seekToNewSource(@Tainted CompressionInputStream this, @Tainted long targetPos) throws UnsupportedOperationException {
    throw new @Tainted UnsupportedOperationException();
  }
}
