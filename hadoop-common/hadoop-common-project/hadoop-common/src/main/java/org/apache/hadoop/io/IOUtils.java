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

package org.apache.hadoop.io;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * An utility class for I/O related functionality. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IOUtils {
  public static final @Tainted Log LOG = LogFactory.getLog(IOUtils.class);

  /**
   * Copies from one stream to another.
   *
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.  
   */
  public static void copyBytes(@Tainted InputStream in, @Tainted OutputStream out, @Tainted int buffSize, @Tainted boolean close) 
    throws IOException {
    try {
      copyBytes(in, out, buffSize);
      if(close) {
        out.close();
        out = null;
        in.close();
        in = null;
      }
    } finally {
      if(close) {
        closeStream(out);
        closeStream(in);
      }
    }
  }
  
  /**
   * Copies from one stream to another.
   * 
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   */
  public static void copyBytes(@Tainted InputStream in, @Tainted OutputStream out, @Tainted int buffSize) 
    throws IOException {
    @Tainted
    PrintStream ps = out instanceof @Tainted PrintStream ? (@Tainted PrintStream)out : null;
    @Tainted
    byte buf @Tainted [] = new @Tainted byte @Tainted [buffSize];
    @Tainted
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead);
      if ((ps != null) && ps.checkError()) {
        throw new @Tainted IOException("Unable to write to output stream.");
      }
      bytesRead = in.read(buf);
    }
  }

  /**
   * Copies from one stream to another. <strong>closes the input and output streams 
   * at the end</strong>.
   *
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object 
   */
  public static void copyBytes(@Tainted InputStream in, @Tainted OutputStream out, @Tainted Configuration conf)
    throws IOException {
    copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096), true);
  }
  
  /**
   * Copies from one stream to another.
   *
   * @param in InputStream to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.
   */
  public static void copyBytes(@Tainted InputStream in, @Tainted OutputStream out, @Tainted Configuration conf, @Tainted boolean close)
    throws IOException {
    copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096),  close);
  }

  /**
   * Copies count bytes from one stream to another.
   *
   * @param in InputStream to read from
   * @param out OutputStream to write to
   * @param count number of bytes to copy
   * @param close whether to close the streams
   * @throws IOException if bytes can not be read or written
   */
  public static void copyBytes(@Tainted InputStream in, @Tainted OutputStream out, @Tainted long count,
      @Tainted
      boolean close) throws IOException {
    @Tainted
    byte buf @Tainted [] = new @Tainted byte @Tainted [4096];
    @Tainted
    long bytesRemaining = count;
    @Tainted
    int bytesRead;

    try {
      while (bytesRemaining > 0) {
        @Tainted
        int bytesToRead = (@Tainted int)
          (bytesRemaining < buf.length ? bytesRemaining : buf.length);

        bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1)
          break;

        out.write(buf, 0, bytesRead);
        bytesRemaining -= bytesRead;
      }
      if (close) {
        out.close();
        out = null;
        in.close();
        in = null;
      }
    } finally {
      if (close) {
        closeStream(out);
        closeStream(in);
      }
    }
  }
  
  /**
   * Utility wrapper for reading from {@link InputStream}. It catches any errors
   * thrown by the underlying stream (either IO or decompression-related), and
   * re-throws as an IOException.
   * 
   * @param is - InputStream to be read from
   * @param buf - buffer the data is read into
   * @param off - offset within buf
   * @param len - amount of data to be read
   * @return number of bytes read
   */
  public static @Tainted int wrappedReadForCompressedData(@Tainted InputStream is, @Tainted byte @Tainted [] buf,
      @Tainted
      int off, @Tainted int len) throws IOException {
    try {
      return is.read(buf, off, len);
    } catch (@Tainted IOException ie) {
      throw ie;
    } catch (@Tainted Throwable t) {
      throw new @Tainted IOException("Error while reading compressed data", t);
    }
  }

  /**
   * Reads len bytes in a loop.
   *
   * @param in InputStream to read from
   * @param buf The buffer to fill
   * @param off offset from the buffer
   * @param len the length of bytes to read
   * @throws IOException if it could not read requested number of bytes 
   * for any reason (including EOF)
   */
  public static void readFully(@Tainted InputStream in, @Tainted byte buf @Tainted [],
      @Tainted
      int off, @Tainted int len) throws IOException {
    @Tainted
    int toRead = len;
    while (toRead > 0) {
      @Tainted
      int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        throw new @Tainted IOException( "Premature EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
  }
  
  /**
   * Similar to readFully(). Skips bytes in a loop.
   * @param in The InputStream to skip bytes from
   * @param len number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes 
   * for any reason (including EOF)
   */
  public static void skipFully(@Tainted InputStream in, @Tainted long len) throws IOException {
    @Tainted
    long amt = len;
    while (amt > 0) {
      @Tainted
      long ret = in.skip(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can 
        // use the read() method to figure out if we're at the end.
        @Tainted
        int b = in.read();
        if (b == -1) {
          throw new @Tainted EOFException( "Premature EOF from inputStream after " +
              "skipping " + (len - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }
  
  /**
   * Close the Closeable objects and <b>ignore</b> any {@link IOException} or 
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void cleanup(@Tainted Log log, java.io.Closeable @Tainted ... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(@Tainted Throwable e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }

  /**
   * Closes the stream ignoring {@link IOException}.
   * Must only be called in cleaning up from exception handlers.
   *
   * @param stream the Stream to close
   */
  public static void closeStream(java.io.Closeable stream) {
    cleanup(null, stream);
  }
  
  /**
   * Closes the socket ignoring {@link IOException}
   *
   * @param sock the Socket to close
   */
  public static void closeSocket(@Tainted Socket sock) {
    if (sock != null) {
      try {
        sock.close();
      } catch (@Tainted IOException ignored) {
        LOG.debug("Ignoring exception while closing socket", ignored);
      }
    }
  }
  
  /**
   * The /dev/null of OutputStreams.
   */
  public static class NullOutputStream extends @Tainted OutputStream {
    @Override
    public void write(IOUtils.@Tainted NullOutputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    }

    @Override
    public void write(IOUtils.@Tainted NullOutputStream this, @Tainted int b) throws IOException {
    }
  }  
  
  /**
   * Write a ByteBuffer to a WritableByteChannel, handling short writes.
   * 
   * @param bc               The WritableByteChannel to write to
   * @param buf              The input buffer
   * @throws IOException     On I/O error
   */
  public static void writeFully(@Tainted WritableByteChannel bc, @Tainted ByteBuffer buf)
      throws IOException {
    do {
      bc.write(buf);
    } while (buf.remaining() > 0);
  }

  /**
   * Write a ByteBuffer to a FileChannel at a given offset, 
   * handling short writes.
   * 
   * @param fc               The FileChannel to write to
   * @param buf              The input buffer
   * @param offset           The offset in the file to start writing at
   * @throws IOException     On I/O error
   */
  public static void writeFully(@Tainted FileChannel fc, @Tainted ByteBuffer buf,
      @Tainted
      long offset) throws IOException {
    do {
      offset += fc.write(buf, offset);
    } while (buf.remaining() > 0);
  }
}
