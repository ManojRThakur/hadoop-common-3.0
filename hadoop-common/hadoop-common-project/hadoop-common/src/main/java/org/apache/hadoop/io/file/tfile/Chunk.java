/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Several related classes to support chunk-encoded sub-streams on top of a
 * regular stream.
 */
final class Chunk {

  /**
   * Prevent the instantiation of class.
   */
  private @Tainted Chunk() {
    // nothing
  }

  /**
   * Decoding a chain of chunks encoded through ChunkEncoder or
   * SingleChunkEncoder.
   */
  static public class ChunkDecoder extends @Tainted InputStream {
    private @Tainted DataInputStream in = null;
    private @Tainted boolean lastChunk;
    private @Tainted int remain = 0;
    private @Tainted boolean closed;

    public @Tainted ChunkDecoder() {
      lastChunk = true;
      closed = true;
    }

    public void reset(Chunk.@Tainted ChunkDecoder this, @Tainted DataInputStream downStream) {
      // no need to wind forward the old input.
      in = downStream;
      lastChunk = false;
      remain = 0;
      closed = false;
    }

    /**
     * Constructor
     * 
     * @param in
     *          The source input stream which contains chunk-encoded data
     *          stream.
     */
    public @Tainted ChunkDecoder(@Tainted DataInputStream in) {
      this.in = in;
      lastChunk = false;
      closed = false;
    }

    /**
     * Have we reached the last chunk.
     * 
     * @return true if we have reached the last chunk.
     * @throws java.io.IOException
     */
    public @Tainted boolean isLastChunk(Chunk.@Tainted ChunkDecoder this) throws IOException {
      checkEOF();
      return lastChunk;
    }

    /**
     * How many bytes remain in the current chunk?
     * 
     * @return remaining bytes left in the current chunk.
     * @throws java.io.IOException
     */
    public @Tainted int getRemain(Chunk.@Tainted ChunkDecoder this) throws IOException {
      checkEOF();
      return remain;
    }

    /**
     * Reading the length of next chunk.
     * 
     * @throws java.io.IOException
     *           when no more data is available.
     */
    private void readLength(Chunk.@Tainted ChunkDecoder this) throws IOException {
      remain = Utils.readVInt(in);
      if (remain >= 0) {
        lastChunk = true;
      } else {
        remain = -remain;
      }
    }

    /**
     * Check whether we reach the end of the stream.
     * 
     * @return false if the chunk encoded stream has more data to read (in which
     *         case available() will be greater than 0); true otherwise.
     * @throws java.io.IOException
     *           on I/O errors.
     */
    private @Tainted boolean checkEOF(Chunk.@Tainted ChunkDecoder this) throws IOException {
      if (isClosed()) return true;
      while (true) {
        if (remain > 0) return false;
        if (lastChunk) return true;
        readLength();
      }
    }

    @Override
    /*
     * This method never blocks the caller. Returning 0 does not mean we reach
     * the end of the stream.
     */
    public @Tainted int available(Chunk.@Tainted ChunkDecoder this) {
      return remain;
    }

    @Override
    public @Tainted int read(Chunk.@Tainted ChunkDecoder this) throws IOException {
      if (checkEOF()) return -1;
      @Tainted
      int ret = in.read();
      if (ret < 0) throw new @Tainted IOException("Corrupted chunk encoding stream");
      --remain;
      return ret;
    }

    @Override
    public @Tainted int read(Chunk.@Tainted ChunkDecoder this, @Tainted byte @Tainted [] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public @Tainted int read(Chunk.@Tainted ChunkDecoder this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new @Tainted IndexOutOfBoundsException();
      }

      if (!checkEOF()) {
        @Tainted
        int n = Math.min(remain, len);
        @Tainted
        int ret = in.read(b, off, n);
        if (ret < 0) throw new @Tainted IOException("Corrupted chunk encoding stream");
        remain -= ret;
        return ret;
      }
      return -1;
    }

    @Override
    public @Tainted long skip(Chunk.@Tainted ChunkDecoder this, @Tainted long n) throws IOException {
      if (!checkEOF()) {
        @Tainted
        long ret = in.skip(Math.min(remain, n));
        remain -= ret;
        return ret;
      }
      return 0;
    }

    @Override
    public @Tainted boolean markSupported(Chunk.@Tainted ChunkDecoder this) {
      return false;
    }

    public @Tainted boolean isClosed(Chunk.@Tainted ChunkDecoder this) {
      return closed;
    }

    @Override
    public void close(Chunk.@Tainted ChunkDecoder this) throws IOException {
      if (closed == false) {
        try {
          while (!checkEOF()) {
            skip(Integer.MAX_VALUE);
          }
        } finally {
          closed = true;
        }
      }
    }
  }

  /**
   * Chunk Encoder. Encoding the output data into a chain of chunks in the
   * following sequences: -len1, byte[len1], -len2, byte[len2], ... len_n,
   * byte[len_n]. Where len1, len2, ..., len_n are the lengths of the data
   * chunks. Non-terminal chunks have their lengths negated. Non-terminal chunks
   * cannot have length 0. All lengths are in the range of 0 to
   * Integer.MAX_VALUE and are encoded in Utils.VInt format.
   */
  static public class ChunkEncoder extends @Tainted OutputStream {
    /**
     * The data output stream it connects to.
     */
    private @Tainted DataOutputStream out;

    /**
     * The internal buffer that is only used when we do not know the advertised
     * size.
     */
    private @Tainted byte buf @Tainted [];

    /**
     * The number of valid bytes in the buffer. This value is always in the
     * range <tt>0</tt> through <tt>buf.length</tt>; elements <tt>buf[0]</tt>
     * through <tt>buf[count-1]</tt> contain valid byte data.
     */
    private @Tainted int count;

    /**
     * Constructor.
     * 
     * @param out
     *          the underlying output stream.
     * @param buf
     *          user-supplied buffer. The buffer would be used exclusively by
     *          the ChunkEncoder during its life cycle.
     */
    public @Tainted ChunkEncoder(@Tainted DataOutputStream out, @Tainted byte @Tainted [] buf) {
      this.out = out;
      this.buf = buf;
      this.count = 0;
    }

    /**
     * Write out a chunk.
     * 
     * @param chunk
     *          The chunk buffer.
     * @param offset
     *          Offset to chunk buffer for the beginning of chunk.
     * @param len
     * @param last
     *          Is this the last call to flushBuffer?
     */
    private void writeChunk(Chunk.@Tainted ChunkEncoder this, @Tainted byte @Tainted [] chunk, @Tainted int offset, @Tainted int len, @Tainted boolean last)
        throws IOException {
      if (last) { // always write out the length for the last chunk.
        Utils.writeVInt(out, len);
        if (len > 0) {
          out.write(chunk, offset, len);
        }
      } else {
        if (len > 0) {
          Utils.writeVInt(out, -len);
          out.write(chunk, offset, len);
        }
      }
    }

    /**
     * Write out a chunk that is a concatenation of the internal buffer plus
     * user supplied data. This will never be the last block.
     * 
     * @param data
     *          User supplied data buffer.
     * @param offset
     *          Offset to user data buffer.
     * @param len
     *          User data buffer size.
     */
    private void writeBufData(Chunk.@Tainted ChunkEncoder this, @Tainted byte @Tainted [] data, @Tainted int offset, @Tainted int len)
        throws IOException {
      if (count + len > 0) {
        Utils.writeVInt(out, -(count + len));
        out.write(buf, 0, count);
        count = 0;
        out.write(data, offset, len);
      }
    }

    /**
     * Flush the internal buffer.
     * 
     * Is this the last call to flushBuffer?
     * 
     * @throws java.io.IOException
     */
    private void flushBuffer(Chunk.@Tainted ChunkEncoder this) throws IOException {
      if (count > 0) {
        writeChunk(buf, 0, count, false);
        count = 0;
      }
    }

    @Override
    public void write(Chunk.@Tainted ChunkEncoder this, @Tainted int b) throws IOException {
      if (count >= buf.length) {
        flushBuffer();
      }
      buf[count++] = (@Tainted byte) b;
    }

    @Override
    public void write(Chunk.@Tainted ChunkEncoder this, @Tainted byte b @Tainted []) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(Chunk.@Tainted ChunkEncoder this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
      if ((len + count) >= buf.length) {
        /*
         * If the input data do not fit in buffer, flush the output buffer and
         * then write the data directly. In this way buffered streams will
         * cascade harmlessly.
         */
        writeBufData(b, off, len);
        return;
      }

      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    @Override
    public void flush(Chunk.@Tainted ChunkEncoder this) throws IOException {
      flushBuffer();
      out.flush();
    }

    @Override
    public void close(Chunk.@Tainted ChunkEncoder this) throws IOException {
      if (buf != null) {
        try {
          writeChunk(buf, 0, count, true);
        } finally {
          buf = null;
          out = null;
        }
      }
    }
  }

  /**
   * Encode the whole stream as a single chunk. Expecting to know the size of
   * the chunk up-front.
   */
  static public class SingleChunkEncoder extends @Tainted OutputStream {
    /**
     * The data output stream it connects to.
     */
    private final @Tainted DataOutputStream out;

    /**
     * The remaining bytes to be written.
     */
    private @Tainted int remain;
    private @Tainted boolean closed = false;

    /**
     * Constructor.
     * 
     * @param out
     *          the underlying output stream.
     * @param size
     *          The total # of bytes to be written as a single chunk.
     * @throws java.io.IOException
     *           if an I/O error occurs.
     */
    public @Tainted SingleChunkEncoder(@Tainted DataOutputStream out, @Tainted int size)
        throws IOException {
      this.out = out;
      this.remain = size;
      Utils.writeVInt(out, size);
    }

    @Override
    public void write(Chunk.@Tainted SingleChunkEncoder this, @Tainted int b) throws IOException {
      if (remain > 0) {
        out.write(b);
        --remain;
      } else {
        throw new @Tainted IOException("Writing more bytes than advertised size.");
      }
    }

    @Override
    public void write(Chunk.@Tainted SingleChunkEncoder this, @Tainted byte b @Tainted []) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(Chunk.@Tainted SingleChunkEncoder this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
      if (remain >= len) {
        out.write(b, off, len);
        remain -= len;
      } else {
        throw new @Tainted IOException("Writing more bytes than advertised size.");
      }
    }

    @Override
    public void flush(Chunk.@Tainted SingleChunkEncoder this) throws IOException {
      out.flush();
    }

    @Override
    public void close(Chunk.@Tainted SingleChunkEncoder this) throws IOException {
      if (closed == true) {
        return;
      }

      try {
        if (remain > 0) {
          throw new @Tainted IOException("Writing less bytes than advertised size.");
        }
      } finally {
        closed = true;
      }
    }
  }
}
