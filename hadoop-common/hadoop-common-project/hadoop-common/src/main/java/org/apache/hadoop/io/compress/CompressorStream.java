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
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompressorStream extends @Tainted CompressionOutputStream {
  protected @Tainted Compressor compressor;
  protected @Tainted byte @Tainted [] buffer;
  protected @Tainted boolean closed = false;
  
  public @Tainted CompressorStream(@Tainted OutputStream out, @Tainted Compressor compressor, @Tainted int bufferSize) {
    super(out);

    if (out == null || compressor == null) {
      throw new @Tainted NullPointerException();
    } else if (bufferSize <= 0) {
      throw new @Tainted IllegalArgumentException("Illegal bufferSize");
    }

    this.compressor = compressor;
    buffer = new @Tainted byte @Tainted [bufferSize];
  }

  public @Tainted CompressorStream(@Tainted OutputStream out, @Tainted Compressor compressor) {
    this(out, compressor, 512);
  }
  
  /**
   * Allow derived classes to directly set the underlying stream.
   * 
   * @param out Underlying output stream.
   */
  protected @Tainted CompressorStream(@Tainted OutputStream out) {
    super(out);
  }

  @Override
  public void write(@Tainted CompressorStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    // Sanity checks
    if (compressor.finished()) {
      throw new @Tainted IOException("write beyond end of stream");
    }
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new @Tainted IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    compressor.setInput(b, off, len);
    while (!compressor.needsInput()) {
      compress();
    }
  }

  protected void compress(@Tainted CompressorStream this) throws IOException {
    @Tainted
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      out.write(buffer, 0, len);
    }
  }

  @Override
  public void finish(@Tainted CompressorStream this) throws IOException {
    if (!compressor.finished()) {
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  @Override
  public void resetState(@Tainted CompressorStream this) throws IOException {
    compressor.reset();
  }
  
  @Override
  public void close(@Tainted CompressorStream this) throws IOException {
    if (!closed) {
      finish();
      out.close();
      closed = true;
    }
  }

  private @Tainted byte @Tainted [] oneByte = new @Tainted byte @Tainted [1];
  @Override
  public void write(@Tainted CompressorStream this, @Tainted int b) throws IOException {
    oneByte[0] = (@Tainted byte)(b & 0xff);
    write(oneByte, 0, oneByte.length);
  }

}
