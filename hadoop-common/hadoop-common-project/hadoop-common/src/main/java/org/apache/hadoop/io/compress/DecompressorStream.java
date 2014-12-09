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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.compress.Decompressor;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DecompressorStream extends @Tainted CompressionInputStream {
  protected @Tainted Decompressor decompressor = null;
  protected @Tainted byte @Tainted [] buffer;
  protected @Tainted boolean eof = false;
  protected @Tainted boolean closed = false;
  private @Tainted int lastBytesSent = 0;

  public @Tainted DecompressorStream(@Tainted InputStream in, @Tainted Decompressor decompressor,
                            @Tainted
                            int bufferSize)
  throws IOException {
    super(in);

    if (in == null || decompressor == null) {
      throw new @Tainted NullPointerException();
    } else if (bufferSize <= 0) {
      throw new @Tainted IllegalArgumentException("Illegal bufferSize");
    }

    this.decompressor = decompressor;
    buffer = new @Tainted byte @Tainted [bufferSize];
  }

  public @Tainted DecompressorStream(@Tainted InputStream in, @Tainted Decompressor decompressor)
  throws IOException {
    this(in, decompressor, 512);
  }

  /**
   * Allow derived classes to directly set the underlying stream.
   * 
   * @param in Underlying input stream.
   * @throws IOException
   */
  protected @Tainted DecompressorStream(@Tainted InputStream in) throws IOException {
    super(in);
  }
  
  private @Tainted byte @Tainted [] oneByte = new @Tainted byte @Tainted [1];
  @Override
  public @Tainted int read(@Tainted DecompressorStream this) throws IOException {
    checkStream();
    return (read(oneByte, 0, oneByte.length) == -1) ? -1 : (oneByte[0] & 0xff);
  }

  @Override
  public @Tainted int read(@Tainted DecompressorStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    checkStream();
    
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new @Tainted IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    return decompress(b, off, len);
  }

  protected @Tainted int decompress(@Tainted DecompressorStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    @Tainted
    int n = 0;

    while ((n = decompressor.decompress(b, off, len)) == 0) {
      if (decompressor.needsDictionary()) {
        eof = true;
        return -1;
      }

      if (decompressor.finished()) {
        // First see if there was any leftover buffered input from previous
        // stream; if not, attempt to refill buffer.  If refill -> EOF, we're
        // all done; else reset, fix up input buffer, and get ready for next
        // concatenated substream/"member".
        @Tainted
        int nRemaining = decompressor.getRemaining();
        if (nRemaining == 0) {
          @Tainted
          int m = getCompressedData();
          if (m == -1) {
            // apparently the previous end-of-stream was also end-of-file:
            // return success, as if we had never called getCompressedData()
            eof = true;
            return -1;
          }
          decompressor.reset();
          decompressor.setInput(buffer, 0, m);
          lastBytesSent = m;
        } else {
          // looks like it's a concatenated stream:  reset low-level zlib (or
          // other engine) and buffers, then "resend" remaining input data
          decompressor.reset();
          @Tainted
          int leftoverOffset = lastBytesSent - nRemaining;
          assert (leftoverOffset >= 0);
          // this recopies userBuf -> direct buffer if using native libraries:
          decompressor.setInput(buffer, leftoverOffset, nRemaining);
          // NOTE:  this is the one place we do NOT want to save the number
          // of bytes sent (nRemaining here) into lastBytesSent:  since we
          // are resending what we've already sent before, offset is nonzero
          // in general (only way it could be zero is if it already equals
          // nRemaining), which would then screw up the offset calculation
          // _next_ time around.  IOW, getRemaining() is in terms of the
          // original, zero-offset bufferload, so lastBytesSent must be as
          // well.  Cheesy ASCII art:
          //
          //          <------------ m, lastBytesSent ----------->
          //          +===============================================+
          // buffer:  |1111111111|22222222222222222|333333333333|     |
          //          +===============================================+
          //     #1:  <-- off -->|<-------- nRemaining --------->
          //     #2:  <----------- off ----------->|<-- nRem. -->
          //     #3:  (final substream:  nRemaining == 0; eof = true)
          //
          // If lastBytesSent is anything other than m, as shown, then "off"
          // will be calculated incorrectly.
        }
      } else if (decompressor.needsInput()) {
        @Tainted
        int m = getCompressedData();
        if (m == -1) {
          throw new @Tainted EOFException("Unexpected end of input stream");
        }
        decompressor.setInput(buffer, 0, m);
        lastBytesSent = m;
      }
    }

    return n;
  }

  protected @Tainted int getCompressedData(@Tainted DecompressorStream this) throws IOException {
    checkStream();
  
    // note that the _caller_ is now required to call setInput() or throw
    return in.read(buffer, 0, buffer.length);
  }

  protected void checkStream(@Tainted DecompressorStream this) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }
  }
  
  @Override
  public void resetState(@Tainted DecompressorStream this) throws IOException {
    decompressor.reset();
  }

  private @Tainted byte @Tainted [] skipBytes = new @Tainted byte @Tainted [512];
  @Override
  public @Tainted long skip(@Tainted DecompressorStream this, @Tainted long n) throws IOException {
    // Sanity checks
    if (n < 0) {
      throw new @Tainted IllegalArgumentException("negative skip length");
    }
    checkStream();
    
    // Read 'n' bytes
    @Tainted
    int skipped = 0;
    while (skipped < n) {
      @Tainted
      int len = Math.min(((@Tainted int)n - skipped), skipBytes.length);
      len = read(skipBytes, 0, len);
      if (len == -1) {
        eof = true;
        break;
      }
      skipped += len;
    }
    return skipped;
  }

  @Override
  public @Tainted int available(@Tainted DecompressorStream this) throws IOException {
    checkStream();
    return (eof) ? 0 : 1;
  }

  @Override
  public void close(@Tainted DecompressorStream this) throws IOException {
    if (!closed) {
      in.close();
      closed = true;
    }
  }

  @Override
  public @Tainted boolean markSupported(@Tainted DecompressorStream this) {
    return false;
  }

  @Override
  public synchronized void mark(@Tainted DecompressorStream this, @Tainted int readlimit) {
  }

  @Override
  public synchronized void reset(@Tainted DecompressorStream this) throws IOException {
    throw new @Tainted IOException("mark/reset not supported");
  }

}
