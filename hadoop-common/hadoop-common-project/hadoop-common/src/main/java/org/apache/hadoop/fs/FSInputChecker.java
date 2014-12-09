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
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * This is a generic input stream for verifying checksums for
 * data before it is read by a user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSInputChecker extends @Tainted FSInputStream {
  public static final @Tainted Log LOG 
  = LogFactory.getLog(FSInputChecker.class);
  
  /** The file name from which data is read from */
  protected @Tainted Path file;
  private @Tainted Checksum sum;
  private @Tainted boolean verifyChecksum = true;
  private @Tainted int maxChunkSize; // data bytes for checksum (eg 512)
  private @Tainted byte @Tainted [] buf; // buffer for non-chunk-aligned reading
  private @Tainted byte @Tainted [] checksum;
  private @Tainted IntBuffer checksumInts; // wrapper on checksum buffer
  private @Tainted int pos; // the position of the reader inside buf
  private @Tainted int count; // the number of bytes currently in buf
  
  private @Tainted int numOfRetries;
  
  // cached file position
  // this should always be a multiple of maxChunkSize
  private @Tainted long chunkPos = 0;

  // Number of checksum chunks that can be read at once into a user
  // buffer. Chosen by benchmarks - higher values do not reduce
  // CPU usage. The size of the data reads made to the underlying stream
  // will be CHUNKS_PER_READ * maxChunkSize.
  private static final @Tainted int CHUNKS_PER_READ = 32;
  protected static final @Tainted int CHECKSUM_SIZE = 4; // 32-bit checksum

  /** Constructor
   * 
   * @param file The name of the file to be read
   * @param numOfRetries Number of read retries when ChecksumError occurs
   */
  protected @Tainted FSInputChecker( @Tainted Path file, @Tainted int numOfRetries) {
    this.file = file;
    this.numOfRetries = numOfRetries;
  }
  
  /** Constructor
   * 
   * @param file The name of the file to be read
   * @param numOfRetries Number of read retries when ChecksumError occurs
   * @param sum the type of Checksum engine
   * @param chunkSize maximun chunk size
   * @param checksumSize the number byte of each checksum
   */
  protected @Tainted FSInputChecker( @Tainted Path file, @Tainted int numOfRetries,
      @Tainted
      boolean verifyChecksum, @Tainted Checksum sum, @Tainted int chunkSize, @Tainted int checksumSize ) {
    this(file, numOfRetries);
    set(verifyChecksum, sum, chunkSize, checksumSize);
  }
  
  /**
   * Reads in checksum chunks into <code>buf</code> at <code>offset</code>
   * and checksum into <code>checksum</code>.
   * Since checksums can be disabled, there are two cases implementors need
   * to worry about:
   *
   *  (a) needChecksum() will return false:
   *     - len can be any positive value
   *     - checksum will be null
   *     Implementors should simply pass through to the underlying data stream.
   * or
   *  (b) needChecksum() will return true:
   *    - len >= maxChunkSize
   *    - checksum.length is a multiple of CHECKSUM_SIZE
   *    Implementors should read an integer number of data chunks into
   *    buf. The amount read should be bounded by len or by 
   *    checksum.length / CHECKSUM_SIZE * maxChunkSize. Note that len may
   *    be a value that is not a multiple of maxChunkSize, in which case
   *    the implementation may return less than len.
   *
   * The method is used for implementing read, therefore, it should be optimized
   * for sequential reading.
   *
   * @param pos chunkPos
   * @param buf desitination buffer
   * @param offset offset in buf at which to store data
   * @param len maximum number of bytes to read
   * @param checksum the data buffer into which to write checksums
   * @return number of bytes read
   */
  abstract protected @Tainted int readChunk(@Tainted FSInputChecker this, @Tainted long pos, @Tainted byte @Tainted [] buf, @Tainted int offset, @Tainted int len,
      @Tainted
      byte @Tainted [] checksum) throws IOException;

  /** Return position of beginning of chunk containing pos. 
   *
   * @param pos a postion in the file
   * @return the starting position of the chunk which contains the byte
   */
  abstract protected @Tainted long getChunkPosition(@Tainted FSInputChecker this, @Tainted long pos);

  /** Return true if there is a need for checksum verification */
  protected synchronized @Tainted boolean needChecksum(@Tainted FSInputChecker this) {
    return verifyChecksum && sum != null;
  }

  /**
   * Read one checksum-verified byte
   * 
   * @return     the next byte of data, or <code>-1</code> if the end of the
   *             stream is reached.
   * @exception  IOException  if an I/O error occurs.
   */

  @Override
  public synchronized @Tainted int read(@Tainted FSInputChecker this) throws IOException {
    if (pos >= count) {
      fill();
      if (pos >= count) {
        return -1;
      }
    }
    return buf[pos++] & 0xff;
  }
  
  /**
   * Read checksum verified bytes from this byte-input stream into 
   * the specified byte array, starting at the given offset.
   *
   * <p> This method implements the general contract of the corresponding
   * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
   * the <code>{@link InputStream}</code> class.  As an additional
   * convenience, it attempts to read as many bytes as possible by repeatedly
   * invoking the <code>read</code> method of the underlying stream.  This
   * iterated <code>read</code> continues until one of the following
   * conditions becomes true: <ul>
   *
   *   <li> The specified number of bytes have been read,
   *
   *   <li> The <code>read</code> method of the underlying stream returns
   *   <code>-1</code>, indicating end-of-file.
   *
   * </ul> If the first <code>read</code> on the underlying stream returns
   * <code>-1</code> to indicate end-of-file then this method returns
   * <code>-1</code>.  Otherwise this method returns the number of bytes
   * actually read.
   *
   * @param      b     destination buffer.
   * @param      off   offset at which to start storing bytes.
   * @param      len   maximum number of bytes to read.
   * @return     the number of bytes read, or <code>-1</code> if the end of
   *             the stream has been reached.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if any checksum error occurs
   */
  @Override
  public synchronized @Tainted int read(@Tainted FSInputChecker this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    // parameter check
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new @Tainted IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    @Tainted
    int n = 0;
    for (;;) {
      @Tainted
      int nread = read1(b, off + n, len - n);
      if (nread <= 0) 
        return (n == 0) ? nread : n;
      n += nread;
      if (n >= len)
        return n;
    }
  }
  
  /**
   * Fills the buffer with a chunk data. 
   * No mark is supported.
   * This method assumes that all data in the buffer has already been read in,
   * hence pos > count.
   */
  private void fill(@Tainted FSInputChecker this  ) throws IOException {
    assert(pos>=count);
    // fill internal buffer
    count = readChecksumChunk(buf, 0, maxChunkSize);
    if (count < 0) count = 0;
  }
  
  /*
   * Read characters into a portion of an array, reading from the underlying
   * stream at most once if necessary.
   */
  private @Tainted int read1(@Tainted FSInputChecker this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len)
  throws IOException {
    @Tainted
    int avail = count-pos;
    if( avail <= 0 ) {
      if(len >= maxChunkSize) {
        // read a chunk to user buffer directly; avoid one copy
        @Tainted
        int nread = readChecksumChunk(b, off, len);
        return nread;
      } else {
        // read a chunk into the local buffer
         fill();
        if( count <= 0 ) {
          return -1;
        } else {
          avail = count;
        }
      }
    }
    
    // copy content of the local buffer to the user buffer
    @Tainted
    int cnt = (avail < len) ? avail : len;
    System.arraycopy(buf, pos, b, off, cnt);
    pos += cnt;
    return cnt;    
  }
  
  /* Read up one or more checksum chunk to array <i>b</i> at pos <i>off</i>
   * It requires at least one checksum chunk boundary
   * in between <cur_pos, cur_pos+len> 
   * and it stops reading at the last boundary or at the end of the stream;
   * Otherwise an IllegalArgumentException is thrown.
   * This makes sure that all data read are checksum verified.
   * 
   * @param b   the buffer into which the data is read.
   * @param off the start offset in array <code>b</code>
   *            at which the data is written.
   * @param len the maximum number of bytes to read.
   * @return    the total number of bytes read into the buffer, or
   *            <code>-1</code> if there is no more data because the end of
   *            the stream has been reached.
   * @throws IOException if an I/O error occurs.
   */ 
  private @Tainted int readChecksumChunk(@Tainted FSInputChecker this, @Tainted byte b @Tainted [], final @Tainted int off, final @Tainted int len)
  throws IOException {
    // invalidate buffer
    count = pos = 0;
          
    @Tainted
    int read = 0;
    @Tainted
    boolean retry = true;
    @Tainted
    int retriesLeft = numOfRetries; 
    do {
      retriesLeft--;

      try {
        read = readChunk(chunkPos, b, off, len, checksum);
        if( read > 0) {
          if( needChecksum() ) {
            verifySums(b, off, read);
          }
          chunkPos += read;
        }
        retry = false;
      } catch (@Tainted ChecksumException ce) {
         LOG.info("Found checksum error: b[" + off + ", " + (off+read) + "]="
              + StringUtils.byteToHexString(b, off, off + read), ce);
          if (retriesLeft == 0) {
            throw ce;
          }
          
          // try a new replica
          if (seekToNewSource(chunkPos)) {
            // Since at least one of the sources is different, 
            // the read might succeed, so we'll retry.
            seek(chunkPos);
          } else {
            // Neither the data stream nor the checksum stream are being read
            // from different sources, meaning we'll still get a checksum error 
            // if we try to do the read again.  We throw an exception instead.
            throw ce;
          }
        }
    } while (retry);
    return read;
  }

  private void verifySums(@Tainted FSInputChecker this, final @Tainted byte b @Tainted [], final @Tainted int off, @Tainted int read)
    throws ChecksumException
  {
    @Tainted
    int leftToVerify = read;
    @Tainted
    int verifyOff = 0;
    checksumInts.rewind();
    checksumInts.limit((read - 1)/maxChunkSize + 1);

    while (leftToVerify > 0) {
      sum.update(b, off + verifyOff, Math.min(leftToVerify, maxChunkSize));
      @Tainted
      int expected = checksumInts.get();
      @Tainted
      int calculated = (@Tainted int)sum.getValue();
      sum.reset();

      if (expected != calculated) {
        @Tainted
        long errPos = chunkPos + verifyOff;
        throw new @Tainted ChecksumException(
         "Checksum error: "+file+" at "+ errPos +
          " exp: " + expected + " got: " + calculated, errPos );
      }
      leftToVerify -= maxChunkSize;
      verifyOff += maxChunkSize;
    }
  }

  /**
   * Convert a checksum byte array to a long
   * This is deprecated since 0.22 since it is no longer in use
   * by this class.
   */
  @Deprecated
  static public @Tainted long checksum2long(@Tainted byte @Tainted [] checksum) {
    @Tainted
    long crc = 0L;
    for(@Tainted int i=0; i<checksum.length; i++) {
      crc |= (0xffL&(@Tainted long)checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  @Override
  public synchronized @Tainted long getPos(@Tainted FSInputChecker this) throws IOException {
    return chunkPos-Math.max(0L, count - pos);
  }

  @Override
  public synchronized @Tainted int available(@Tainted FSInputChecker this) throws IOException {
    return Math.max(0, count - pos);
  }
  
  /**
   * Skips over and discards <code>n</code> bytes of data from the
   * input stream.
   *
   * <p>This method may skip more bytes than are remaining in the backing
   * file. This produces no exception and the number of bytes skipped
   * may include some number of bytes that were beyond the EOF of the
   * backing file. Attempting to read from the stream after skipping past
   * the end will result in -1 indicating the end of the file.
   *
   *<p>If <code>n</code> is negative, no bytes are skipped.
   *
   * @param      n   the number of bytes to be skipped.
   * @return     the actual number of bytes skipped.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if the chunk to skip to is corrupted
   */
  @Override
  public synchronized @Tainted long skip(@Tainted FSInputChecker this, @Tainted long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos()+n);
    return n;
  }

  /**
   * Seek to the given position in the stream.
   * The next read() will be from that position.
   * 
   * <p>This method may seek past the end of the file.
   * This produces no exception and an attempt to read from
   * the stream will result in -1 indicating the end of the file.
   *
   * @param      pos   the postion to seek to.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if the chunk to seek to is corrupted
   */

  @Override
  public synchronized void seek(@Tainted FSInputChecker this, @Tainted long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    // optimize: check if the pos is in the buffer
    @Tainted
    long start = chunkPos - this.count;
    if( pos>=start && pos<chunkPos) {
      this.pos = (@Tainted int)(pos-start);
      return;
    }
    
    // reset the current state
    resetState();
    
    // seek to a checksum boundary
    chunkPos = getChunkPosition(pos);
    
    // scan to the desired position
    @Tainted
    int delta = (@Tainted int)(pos - chunkPos);
    if( delta > 0) {
      readFully(this, new @Tainted byte @Tainted [delta], 0, delta);
    }
  }

  /**
   * A utility function that tries to read up to <code>len</code> bytes from
   * <code>stm</code>
   * 
   * @param stm    an input stream
   * @param buf    destiniation buffer
   * @param offset offset at which to store data
   * @param len    number of bytes to read
   * @return actual number of bytes read
   * @throws IOException if there is any IO error
   */
  protected static @Tainted int readFully(@Tainted InputStream stm, 
      @Tainted
      byte @Tainted [] buf, @Tainted int offset, @Tainted int len) throws IOException {
    @Tainted
    int n = 0;
    for (;;) {
      @Tainted
      int nread = stm.read(buf, offset + n, len - n);
      if (nread <= 0) 
        return (n == 0) ? nread : n;
      n += nread;
      if (n >= len)
        return n;
    }
  }
  
  /**
   * Set the checksum related parameters
   * @param verifyChecksum whether to verify checksum
   * @param sum which type of checksum to use
   * @param maxChunkSize maximun chunk size
   * @param checksumSize checksum size
   */
  final protected synchronized void set(@Tainted FSInputChecker this, @Tainted boolean verifyChecksum,
      @Tainted
      Checksum sum, @Tainted int maxChunkSize, @Tainted int checksumSize) {

    // The code makes assumptions that checksums are always 32-bit.
    assert !verifyChecksum || sum == null || checksumSize == CHECKSUM_SIZE;

    this.maxChunkSize = maxChunkSize;
    this.verifyChecksum = verifyChecksum;
    this.sum = sum;
    this.buf = new @Tainted byte @Tainted [maxChunkSize];
    // The size of the checksum array here determines how much we can
    // read in a single call to readChunk
    this.checksum = new @Tainted byte @Tainted [CHUNKS_PER_READ * checksumSize];
    this.checksumInts = ByteBuffer.wrap(checksum).asIntBuffer();
    this.count = 0;
    this.pos = 0;
  }

  @Override
  final public @Tainted boolean markSupported(@Tainted FSInputChecker this) {
    return false;
  }
  
  @Override
  final public void mark(@Tainted FSInputChecker this, @Tainted int readlimit) {
  }
  
  @Override
  final public void reset(@Tainted FSInputChecker this) throws IOException {
    throw new @Tainted IOException("mark/reset not supported");
  }
  

  /* reset this FSInputChecker's state */
  private void resetState(@Tainted FSInputChecker this) {
    // invalidate buffer
    count = 0;
    pos = 0;
    // reset Checksum
    if (sum != null) {
      sum.reset();
    }
  }
}
