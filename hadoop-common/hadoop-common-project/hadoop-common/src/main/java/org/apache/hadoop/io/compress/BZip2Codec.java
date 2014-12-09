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

package org.apache.hadoop.io.compress;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.bzip2.BZip2Constants;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;

/**
 * This class provides output and input streams for bzip2 compression
 * and decompression.  It uses the native bzip2 library on the system
 * if possible, else it uses a pure-Java implementation of the bzip2
 * algorithm.  The configuration parameter
 * io.compression.codec.bzip2.library can be used to control this
 * behavior.
 *
 * In the pure-Java mode, the Compressor and Decompressor interfaces
 * are not implemented.  Therefore, in that mode, those methods of
 * CompressionCodec which have a Compressor or Decompressor type
 * argument, throw UnsupportedOperationException.
 *
 * Currently, support for splittability is available only in the
 * pure-Java mode; therefore, if a SplitCompressionInputStream is
 * requested, the pure-Java implementation is used, regardless of the
 * setting of the configuration parameter mentioned above.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BZip2Codec implements @Tainted Configurable, @Tainted SplittableCompressionCodec {

  private static final @Tainted String HEADER = "BZ";
  private static final @Tainted int HEADER_LEN = HEADER.length();
  private static final @Tainted String SUB_HEADER = "h9";
  private static final @Tainted int SUB_HEADER_LEN = SUB_HEADER.length();

  private @Tainted Configuration conf;
  
  /**
   * Set the configuration to be used by this object.
   *
   * @param conf the configuration object.
   */
  @Override
  public void setConf(@Tainted BZip2Codec this, @Tainted Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this objec.
   */
  @Override
  public @Tainted Configuration getConf(@Tainted BZip2Codec this) {
    return conf;
  }
  
  /**
  * Creates a new instance of BZip2Codec.
  */
  public @Tainted BZip2Codec() { }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out        the location for the final output stream
   * @return a stream the user can write uncompressed data to, to have it 
   *         compressed
   * @throws IOException
   */
  @Override
  public @Tainted CompressionOutputStream createOutputStream(@Tainted BZip2Codec this, @Tainted OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out        the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to, to have it 
   *         compressed
   * @throws IOException
   */
  @Override
  public @Tainted CompressionOutputStream createOutputStream(@Tainted BZip2Codec this, @Tainted OutputStream out,
      @Tainted
      Compressor compressor) throws IOException {
    return Bzip2Factory.isNativeBzip2Loaded(conf) ?
      new @Tainted CompressorStream(out, compressor, 
                           conf.getInt("io.file.buffer.size", 4*1024)) :
      new @Tainted BZip2CompressionOutputStream(out);
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public @Tainted Class<@Tainted ? extends @Tainted Compressor> getCompressorType(@Tainted BZip2Codec this) {
    return Bzip2Factory.getBzip2CompressorType(conf);
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public @Tainted Compressor createCompressor(@Tainted BZip2Codec this) {
    return Bzip2Factory.getBzip2Compressor(conf);
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * input stream and return a stream for uncompressed data.
   *
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public @Tainted CompressionInputStream createInputStream(@Tainted BZip2Codec this, @Tainted InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}, and return a 
   * stream for uncompressed data.
   *
   * @param in           the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public @Tainted CompressionInputStream createInputStream(@Tainted BZip2Codec this, @Tainted InputStream in,
      @Tainted
      Decompressor decompressor) throws IOException {
    return Bzip2Factory.isNativeBzip2Loaded(conf) ? 
      new @Tainted DecompressorStream(in, decompressor,
                             conf.getInt("io.file.buffer.size", 4*1024)) :
      new @Tainted BZip2CompressionInputStream(in);
  }

  /**
   * Creates CompressionInputStream to be used to read off uncompressed data
   * in one of the two reading modes. i.e. Continuous or Blocked reading modes
   *
   * @param seekableIn The InputStream
   * @param start The start offset into the compressed stream
   * @param end The end offset into the compressed stream
   * @param readMode Controls whether progress is reported continuously or
   *                 only at block boundaries.
   *
   * @return CompressionInputStream for BZip2 aligned at block boundaries
   */
  public @Tainted SplitCompressionInputStream createInputStream(@Tainted BZip2Codec this, @Tainted InputStream seekableIn,
      @Tainted
      Decompressor decompressor, @Tainted long start, @Tainted long end, @Tainted READ_MODE readMode)
      throws IOException {

    if (!(seekableIn instanceof @Tainted Seekable)) {
      throw new @Tainted IOException("seekableIn must be an instance of " +
          Seekable.class.getName());
    }

    //find the position of first BZip2 start up marker
    ((@Tainted Seekable)seekableIn).seek(0);

    // BZip2 start of block markers are of 6 bytes.  But the very first block
    // also has "BZh9", making it 10 bytes.  This is the common case.  But at
    // time stream might start without a leading BZ.
    final @Tainted long FIRST_BZIP2_BLOCK_MARKER_POSITION =
      CBZip2InputStream.numberOfBytesTillNextMarker(seekableIn);
    @Tainted
    long adjStart = Math.max(0L, start - FIRST_BZIP2_BLOCK_MARKER_POSITION);

    ((@Tainted Seekable)seekableIn).seek(adjStart);
    @Tainted
    SplitCompressionInputStream in =
      new @Tainted BZip2CompressionInputStream(seekableIn, adjStart, end, readMode);


    // The following if clause handles the following case:
    // Assume the following scenario in BZip2 compressed stream where
    // . represent compressed data.
    // .....[48 bit Block].....[48 bit   Block].....[48 bit Block]...
    // ........................[47 bits][1 bit].....[48 bit Block]...
    // ................................^[Assume a Byte alignment here]
    // ........................................^^[current position of stream]
    // .....................^^[We go back 10 Bytes in stream and find a Block marker]
    // ........................................^^[We align at wrong position!]
    // ...........................................................^^[While this pos is correct]

    if (in.getPos() <= start) {
      ((@Tainted Seekable)seekableIn).seek(start);
      in = new @Tainted BZip2CompressionInputStream(seekableIn, start, end, readMode);
    }

    return in;
  }

  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public @Tainted Class<@Tainted ? extends @Tainted Decompressor> getDecompressorType(@Tainted BZip2Codec this) {
    return Bzip2Factory.getBzip2DecompressorType(conf);
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public @Tainted Decompressor createDecompressor(@Tainted BZip2Codec this) {
    return Bzip2Factory.getBzip2Decompressor(conf);
  }

  /**
  * .bz2 is recognized as the default extension for compressed BZip2 files
  *
  * @return A String telling the default bzip2 file extension
  */
  @Override
  public @Tainted String getDefaultExtension(@Tainted BZip2Codec this) {
    return ".bz2";
  }

  private static class BZip2CompressionOutputStream extends
      @Tainted
      CompressionOutputStream {

    // class data starts here//
    private @Tainted CBZip2OutputStream output;
    private @Tainted boolean needsReset; 
    // class data ends here//

    public @Tainted BZip2CompressionOutputStream(@Tainted OutputStream out)
        throws IOException {
      super(out);
      needsReset = true;
    }

    private void writeStreamHeader(BZip2Codec.@Tainted BZip2CompressionOutputStream this) throws IOException {
      if (super.out != null) {
        // The compressed bzip2 stream should start with the
        // identifying characters BZ. Caller of CBZip2OutputStream
        // i.e. this class must write these characters.
        out.write(HEADER.getBytes());
      }
    }

    public void finish(BZip2Codec.@Tainted BZip2CompressionOutputStream this) throws IOException {
      if (needsReset) {
        // In the case that nothing is written to this stream, we still need to
        // write out the header before closing, otherwise the stream won't be
        // recognized by BZip2CompressionInputStream.
        internalReset();
      }
      this.output.finish();
      needsReset = true;
    }

    private void internalReset(BZip2Codec.@Tainted BZip2CompressionOutputStream this) throws IOException {
      if (needsReset) {
        needsReset = false;
        writeStreamHeader();
        this.output = new @Tainted CBZip2OutputStream(out);
      }
    }    
    
    public void resetState(BZip2Codec.@Tainted BZip2CompressionOutputStream this) throws IOException {
      // Cannot write to out at this point because out might not be ready
      // yet, as in SequenceFile.Writer implementation.
      needsReset = true;
    }

    public void write(BZip2Codec.@Tainted BZip2CompressionOutputStream this, @Tainted int b) throws IOException {
      if (needsReset) {
        internalReset();
      }
      this.output.write(b);
    }

    public void write(BZip2Codec.@Tainted BZip2CompressionOutputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
      if (needsReset) {
        internalReset();
      }
      this.output.write(b, off, len);
    }

    public void close(BZip2Codec.@Tainted BZip2CompressionOutputStream this) throws IOException {
      if (needsReset) {
        // In the case that nothing is written to this stream, we still need to
        // write out the header before closing, otherwise the stream won't be
        // recognized by BZip2CompressionInputStream.
        internalReset();
      }
      this.output.flush();
      this.output.close();
      needsReset = true;
    }

  }// end of class BZip2CompressionOutputStream

  /**
   * This class is capable to de-compress BZip2 data in two modes;
   * CONTINOUS and BYBLOCK.  BYBLOCK mode makes it possible to
   * do decompression starting any arbitrary position in the stream.
   *
   * So this facility can easily be used to parallelize decompression
   * of a large BZip2 file for performance reasons.  (It is exactly
   * done so for Hadoop framework.  See LineRecordReader for an
   * example).  So one can break the file (of course logically) into
   * chunks for parallel processing.  These "splits" should be like
   * default Hadoop splits (e.g as in FileInputFormat getSplit metod).
   * So this code is designed and tested for FileInputFormat's way
   * of splitting only.
   */

  private static class BZip2CompressionInputStream extends
      @Tainted
      SplitCompressionInputStream {

    // class data starts here//
    private @Tainted CBZip2InputStream input;
    @Tainted
    boolean needsReset;
    private @Tainted BufferedInputStream bufferedIn;
    private @Tainted boolean isHeaderStripped = false;
    private @Tainted boolean isSubHeaderStripped = false;
    private @Tainted READ_MODE readMode = READ_MODE.CONTINUOUS;
    private @Tainted long startingPos = 0L;

    // Following state machine handles different states of compressed stream
    // position
    // HOLD : Don't advertise compressed stream position
    // ADVERTISE : Read 1 more character and advertise stream position
    // See more comments about it before updatePos method.
    private enum POS_ADVERTISEMENT_STATE_MACHINE {

@Tainted  HOLD,  @Tainted  ADVERTISE
    };

    @Tainted
    POS_ADVERTISEMENT_STATE_MACHINE posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
    @Tainted
    long compressedStreamPosition = 0;

    // class data ends here//

    public @Tainted BZip2CompressionInputStream(@Tainted InputStream in) throws IOException {
      this(in, 0L, Long.MAX_VALUE, READ_MODE.CONTINUOUS);
    }

    public @Tainted BZip2CompressionInputStream(@Tainted InputStream in, @Tainted long start, @Tainted long end,
        @Tainted
        READ_MODE readMode) throws IOException {
      super(in, start, end);
      needsReset = false;
      bufferedIn = new @Tainted BufferedInputStream(super.in);
      this.startingPos = super.getPos();
      this.readMode = readMode;
      if (this.startingPos == 0) {
        // We only strip header if it is start of file
        bufferedIn = readStreamHeader();
      }
      input = new @Tainted CBZip2InputStream(bufferedIn, readMode);
      if (this.isHeaderStripped) {
        input.updateReportedByteCount(HEADER_LEN);
      }

      if (this.isSubHeaderStripped) {
        input.updateReportedByteCount(SUB_HEADER_LEN);
      }

      this.updatePos(false);
    }

    private @Tainted BufferedInputStream readStreamHeader(BZip2Codec.@Tainted BZip2CompressionInputStream this) throws IOException {
      // We are flexible enough to allow the compressed stream not to
      // start with the header of BZ. So it works fine either we have
      // the header or not.
      if (super.in != null) {
        bufferedIn.mark(HEADER_LEN);
        @Tainted
        byte @Tainted [] headerBytes = new @Tainted byte @Tainted [HEADER_LEN];
        @Tainted
        int actualRead = bufferedIn.read(headerBytes, 0, HEADER_LEN);
        if (actualRead != -1) {
          @Tainted
          String header = new @Tainted String(headerBytes);
          if (header.compareTo(HEADER) != 0) {
            bufferedIn.reset();
          } else {
            this.isHeaderStripped = true;
            // In case of BYBLOCK mode, we also want to strip off
            // remaining two character of the header.
            if (this.readMode == READ_MODE.BYBLOCK) {
              actualRead = bufferedIn.read(headerBytes, 0,
                  SUB_HEADER_LEN);
              if (actualRead != -1) {
                this.isSubHeaderStripped = true;
              }
            }
          }
        }
      }

      if (bufferedIn == null) {
        throw new @Tainted IOException("Failed to read bzip2 stream.");
      }

      return bufferedIn;

    }// end of method

    public void close(BZip2Codec.@Tainted BZip2CompressionInputStream this) throws IOException {
      if (!needsReset) {
        input.close();
        needsReset = true;
      }
    }

    /**
    * This method updates compressed stream position exactly when the
    * client of this code has read off at least one byte passed any BZip2
    * end of block marker.
    *
    * This mechanism is very helpful to deal with data level record
    * boundaries. Please see constructor and next methods of
    * org.apache.hadoop.mapred.LineRecordReader as an example usage of this
    * feature.  We elaborate it with an example in the following:
    *
    * Assume two different scenarios of the BZip2 compressed stream, where
    * [m] represent end of block, \n is line delimiter and . represent compressed
    * data.
    *
    * ............[m]......\n.......
    *
    * ..........\n[m]......\n.......
    *
    * Assume that end is right after [m].  In the first case the reading
    * will stop at \n and there is no need to read one more line.  (To see the
    * reason of reading one more line in the next() method is explained in LineRecordReader.)
    * While in the second example LineRecordReader needs to read one more line
    * (till the second \n).  Now since BZip2Codecs only update position
    * at least one byte passed a maker, so it is straight forward to differentiate
    * between the two cases mentioned.
    *
    */

    public @Tainted int read(BZip2Codec.@Tainted BZip2CompressionInputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
      if (needsReset) {
        internalReset();
      }

      @Tainted
      int result = 0;
      result = this.input.read(b, off, len);
      if (result == BZip2Constants.END_OF_BLOCK) {
        this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE;
      }

      if (this.posSM == POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE) {
        result = this.input.read(b, off, off + 1);
        // This is the precise time to update compressed stream position
        // to the client of this code.
        this.updatePos(true);
        this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
      }

      return result;

    }

    public @Tainted int read(BZip2Codec.@Tainted BZip2CompressionInputStream this) throws IOException {
      @Tainted
      byte b @Tainted [] = new @Tainted byte @Tainted [1];
      @Tainted
      int result = this.read(b, 0, 1);
      return (result < 0) ? result : (b[0] & 0xff);
    }

    private void internalReset(BZip2Codec.@Tainted BZip2CompressionInputStream this) throws IOException {
      if (needsReset) {
        needsReset = false;
        @Tainted
        BufferedInputStream bufferedIn = readStreamHeader();
        input = new @Tainted CBZip2InputStream(bufferedIn, this.readMode);
      }
    }    
    
    public void resetState(BZip2Codec.@Tainted BZip2CompressionInputStream this) throws IOException {
      // Cannot read from bufferedIn at this point because bufferedIn
      // might not be ready
      // yet, as in SequenceFile.Reader implementation.
      needsReset = true;
    }

    public @Tainted long getPos(BZip2Codec.@Tainted BZip2CompressionInputStream this) {
      return this.compressedStreamPosition;
      }

    /*
     * As the comments before read method tell that
     * compressed stream is advertised when at least
     * one byte passed EOB have been read off.  But
     * there is an exception to this rule.  When we
     * construct the stream we advertise the position
     * exactly at EOB.  In the following method
     * shouldAddOn boolean captures this exception.
     *
     */
    private void updatePos(BZip2Codec.@Tainted BZip2CompressionInputStream this, @Tainted boolean shouldAddOn) {
      @Tainted
      int addOn = shouldAddOn ? 1 : 0;
      this.compressedStreamPosition = this.startingPos
          + this.input.getProcessedByteCount() + addOn;
    }

  }// end of BZip2CompressionInputStream

}
