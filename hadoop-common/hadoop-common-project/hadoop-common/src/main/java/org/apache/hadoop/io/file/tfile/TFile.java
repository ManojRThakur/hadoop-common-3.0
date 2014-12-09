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
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader;
import org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender;
import org.apache.hadoop.io.file.tfile.Chunk.ChunkDecoder;
import org.apache.hadoop.io.file.tfile.Chunk.ChunkEncoder;
import org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator;
import org.apache.hadoop.io.file.tfile.CompareUtils.MemcmpRawComparator;
import org.apache.hadoop.io.file.tfile.Utils.Version;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;

/**
 * A TFile is a container of key-value pairs. Both keys and values are type-less
 * bytes. Keys are restricted to 64KB, value length is not restricted
 * (practically limited to the available disk storage). TFile further provides
 * the following features:
 * <ul>
 * <li>Block Compression.
 * <li>Named meta data blocks.
 * <li>Sorted or unsorted keys.
 * <li>Seek by key or by file offset.
 * </ul>
 * The memory footprint of a TFile includes the following:
 * <ul>
 * <li>Some constant overhead of reading or writing a compressed block.
 * <ul>
 * <li>Each compressed block requires one compression/decompression codec for
 * I/O.
 * <li>Temporary space to buffer the key.
 * <li>Temporary space to buffer the value (for TFile.Writer only). Values are
 * chunk encoded, so that we buffer at most one chunk of user data. By default,
 * the chunk buffer is 1MB. Reading chunked value does not require additional
 * memory.
 * </ul>
 * <li>TFile index, which is proportional to the total number of Data Blocks.
 * The total amount of memory needed to hold the index can be estimated as
 * (56+AvgKeySize)*NumBlocks.
 * <li>MetaBlock index, which is proportional to the total number of Meta
 * Blocks.The total amount of memory needed to hold the index for Meta Blocks
 * can be estimated as (40+AvgMetaBlockName)*NumMetaBlock.
 * </ul>
 * <p>
 * The behavior of TFile can be customized by the following variables through
 * Configuration:
 * <ul>
 * <li><b>tfile.io.chunk.size</b>: Value chunk size. Integer (in bytes). Default
 * to 1MB. Values of the length less than the chunk size is guaranteed to have
 * known value length in read time (See
 * {@link TFile.Reader.Scanner.Entry#isValueLengthKnown()}).
 * <li><b>tfile.fs.output.buffer.size</b>: Buffer size used for
 * FSDataOutputStream. Integer (in bytes). Default to 256KB.
 * <li><b>tfile.fs.input.buffer.size</b>: Buffer size used for
 * FSDataInputStream. Integer (in bytes). Default to 256KB.
 * </ul>
 * <p>
 * Suggestions on performance optimization.
 * <ul>
 * <li>Minimum block size. We recommend a setting of minimum block size between
 * 256KB to 1MB for general usage. Larger block size is preferred if files are
 * primarily for sequential access. However, it would lead to inefficient random
 * access (because there are more data to decompress). Smaller blocks are good
 * for random access, but require more memory to hold the block index, and may
 * be slower to create (because we must flush the compressor stream at the
 * conclusion of each data block, which leads to an FS I/O flush). Further, due
 * to the internal caching in Compression codec, the smallest possible block
 * size would be around 20KB-30KB.
 * <li>The current implementation does not offer true multi-threading for
 * reading. The implementation uses FSDataInputStream seek()+read(), which is
 * shown to be much faster than positioned-read call in single thread mode.
 * However, it also means that if multiple threads attempt to access the same
 * TFile (using multiple scanners) simultaneously, the actual I/O is carried out
 * sequentially even if they access different DFS blocks.
 * <li>Compression codec. Use "none" if the data is not very compressable (by
 * compressable, I mean a compression ratio at least 2:1). Generally, use "lzo"
 * as the starting point for experimenting. "gz" overs slightly better
 * compression ratio over "lzo" but requires 4x CPU to compress and 2x CPU to
 * decompress, comparing to "lzo".
 * <li>File system buffering, if the underlying FSDataInputStream and
 * FSDataOutputStream is already adequately buffered; or if applications
 * reads/writes keys and values in large buffers, we can reduce the sizes of
 * input/output buffering in TFile layer by setting the configuration parameters
 * "tfile.fs.input.buffer.size" and "tfile.fs.output.buffer.size".
 * </ul>
 * 
 * Some design rationale behind TFile can be found at <a
 * href=https://issues.apache.org/jira/browse/HADOOP-3315>Hadoop-3315</a>.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TFile {
  static final @Tainted Log LOG = LogFactory.getLog(TFile.class);

  private static final @Tainted String CHUNK_BUF_SIZE_ATTR = "tfile.io.chunk.size";
  private static final @Tainted String FS_INPUT_BUF_SIZE_ATTR =
      "tfile.fs.input.buffer.size";
  private static final @Tainted String FS_OUTPUT_BUF_SIZE_ATTR =
      "tfile.fs.output.buffer.size";

  static @Tainted int getChunkBufferSize(@Tainted Configuration conf) {
    @Tainted
    int ret = conf.getInt(CHUNK_BUF_SIZE_ATTR, 1024 * 1024);
    return (ret > 0) ? ret : 1024 * 1024;
  }

  static @Tainted int getFSInputBufferSize(@Tainted Configuration conf) {
    return conf.getInt(FS_INPUT_BUF_SIZE_ATTR, 256 * 1024);
  }

  static @Tainted int getFSOutputBufferSize(@Tainted Configuration conf) {
    return conf.getInt(FS_OUTPUT_BUF_SIZE_ATTR, 256 * 1024);
  }

  private static final @Tainted int MAX_KEY_SIZE = 64 * 1024; // 64KB
  static final @Tainted Version API_VERSION = new @Tainted Version((@Tainted short) 1, (@Tainted short) 0);

  /** compression: gzip */
  public static final @Tainted String COMPRESSION_GZ = "gz";
  /** compression: lzo */
  public static final @Tainted String COMPRESSION_LZO = "lzo";
  /** compression: none */
  public static final @Tainted String COMPRESSION_NONE = "none";
  /** comparator: memcmp */
  public static final @Tainted String COMPARATOR_MEMCMP = "memcmp";
  /** comparator prefix: java class */
  public static final @Tainted String COMPARATOR_JCLASS = "jclass:";

  /**
   * Make a raw comparator from a string name.
   * 
   * @param name
   *          Comparator name
   * @return A RawComparable comparator.
   */
  static public @Tainted Comparator<@Tainted RawComparable> makeComparator(@Tainted String name) {
    return TFileMeta.makeComparator(name);
  }

  // Prevent the instantiation of TFiles
  private @Tainted TFile() {
    // nothing
  }

  /**
   * Get names of supported compression algorithms. The names are acceptable by
   * TFile.Writer.
   * 
   * @return Array of strings, each represents a supported compression
   *         algorithm. Currently, the following compression algorithms are
   *         supported.
   *         <ul>
   *         <li>"none" - No compression.
   *         <li>"lzo" - LZO compression.
   *         <li>"gz" - GZIP compression.
   *         </ul>
   */
  public static @Tainted String @Tainted [] getSupportedCompressionAlgorithms() {
    return Compression.getSupportedAlgorithms();
  }

  /**
   * TFile Writer.
   */
  @InterfaceStability.Evolving
  public static class Writer implements @Tainted Closeable {
    // minimum compressed size for a block.
    private final @Tainted int sizeMinBlock;

    // Meta blocks.
    final @Tainted TFileIndex tfileIndex;
    final @Tainted TFileMeta tfileMeta;

    // reference to the underlying BCFile.
    private BCFile.@Tainted Writer writerBCF;

    // current data block appender.
    @Tainted
    BlockAppender blkAppender;
    @Tainted
    long blkRecordCount;

    // buffers for caching the key.
    @Tainted
    BoundedByteArrayOutputStream currentKeyBufferOS;
    @Tainted
    BoundedByteArrayOutputStream lastKeyBufferOS;

    // buffer used by chunk codec
    private @Tainted byte @Tainted [] valueBuffer;

    /**
     * Writer states. The state always transits in circles: READY -> IN_KEY ->
     * END_KEY -> IN_VALUE -> READY.
     */
    private enum State {

@Tainted  READY, // Ready to start a new key-value pair insertion.

@Tainted  IN_KEY, // In the middle of key insertion.

@Tainted  END_KEY, // Key insertion complete, ready to insert value.

@Tainted  IN_VALUE, // In value insertion.
      // ERROR, // Error encountered, cannot continue.

@Tainted  CLOSED, // TFile already closed.
    };

    // current state of Writer.
    @Tainted
    State state = State.READY;
    @Tainted
    Configuration conf;
    @Tainted
    long errorCount = 0;

    /**
     * Constructor
     * 
     * @param fsdos
     *          output stream for writing. Must be at position 0.
     * @param minBlockSize
     *          Minimum compressed block size in bytes. A compression block will
     *          not be closed until it reaches this size except for the last
     *          block.
     * @param compressName
     *          Name of the compression algorithm. Must be one of the strings
     *          returned by {@link TFile#getSupportedCompressionAlgorithms()}.
     * @param comparator
     *          Leave comparator as null or empty string if TFile is not sorted.
     *          Otherwise, provide the string name for the comparison algorithm
     *          for keys. Two kinds of comparators are supported.
     *          <ul>
     *          <li>Algorithmic comparator: binary comparators that is language
     *          independent. Currently, only "memcmp" is supported.
     *          <li>Language-specific comparator: binary comparators that can
     *          only be constructed in specific language. For Java, the syntax
     *          is "jclass:", followed by the class name of the RawComparator.
     *          Currently, we only support RawComparators that can be
     *          constructed through the default constructor (with no
     *          parameters). Parameterized RawComparators such as
     *          {@link WritableComparator} or
     *          {@link JavaSerializationComparator} may not be directly used.
     *          One should write a wrapper class that inherits from such classes
     *          and use its default constructor to perform proper
     *          initialization.
     *          </ul>
     * @param conf
     *          The configuration object.
     * @throws IOException
     */
    public @Tainted Writer(@Tainted FSDataOutputStream fsdos, @Tainted int minBlockSize,
        @Tainted
        String compressName, @Tainted String comparator, @Tainted Configuration conf)
        throws IOException {
      sizeMinBlock = minBlockSize;
      tfileMeta = new @Tainted TFileMeta(comparator);
      tfileIndex = new @Tainted TFileIndex(tfileMeta.getComparator());

      writerBCF = new BCFile.@Tainted Writer(fsdos, compressName, conf);
      currentKeyBufferOS = new @Tainted BoundedByteArrayOutputStream(MAX_KEY_SIZE);
      lastKeyBufferOS = new @Tainted BoundedByteArrayOutputStream(MAX_KEY_SIZE);
      this.conf = conf;
    }

    /**
     * Close the Writer. Resources will be released regardless of the exceptions
     * being thrown. Future close calls will have no effect.
     * 
     * The underlying FSDataOutputStream is not closed.
     */
    @Override
    public void close(TFile.@Tainted Writer this) throws IOException {
      if ((state == State.CLOSED)) {
        return;
      }
      try {
        // First try the normal finish.
        // Terminate upon the first Exception.
        if (errorCount == 0) {
          if (state != State.READY) {
            throw new @Tainted IllegalStateException(
                "Cannot close TFile in the middle of key-value insertion.");
          }

          finishDataBlock(true);

          // first, write out data:TFile.meta
          @Tainted
          BlockAppender outMeta =
              writerBCF
                  .prepareMetaBlock(TFileMeta.BLOCK_NAME, COMPRESSION_NONE);
          try {
            tfileMeta.write(outMeta);
          } finally {
            outMeta.close();
          }

          // second, write out data:TFile.index
          @Tainted
          BlockAppender outIndex =
              writerBCF.prepareMetaBlock(TFileIndex.BLOCK_NAME);
          try {
            tfileIndex.write(outIndex);
          } finally {
            outIndex.close();
          }

          writerBCF.close();
        }
      } finally {
        IOUtils.cleanup(LOG, blkAppender, writerBCF);
        blkAppender = null;
        writerBCF = null;
        state = State.CLOSED;
      }
    }

    /**
     * Adding a new key-value pair to the TFile. This is synonymous to
     * append(key, 0, key.length, value, 0, value.length)
     * 
     * @param key
     *          Buffer for key.
     * @param value
     *          Buffer for value.
     * @throws IOException
     */
    public void append(TFile.@Tainted Writer this, @Tainted byte @Tainted [] key, @Tainted byte @Tainted [] value) throws IOException {
      append(key, 0, key.length, value, 0, value.length);
    }

    /**
     * Adding a new key-value pair to TFile.
     * 
     * @param key
     *          buffer for key.
     * @param koff
     *          offset in key buffer.
     * @param klen
     *          length of key.
     * @param value
     *          buffer for value.
     * @param voff
     *          offset in value buffer.
     * @param vlen
     *          length of value.
     * @throws IOException
     *           Upon IO errors.
     *           <p>
     *           If an exception is thrown, the TFile will be in an inconsistent
     *           state. The only legitimate call after that would be close
     */
    public void append(TFile.@Tainted Writer this, @Tainted byte @Tainted [] key, @Tainted int koff, @Tainted int klen, @Tainted byte @Tainted [] value, @Tainted int voff,
        @Tainted
        int vlen) throws IOException {
      if ((koff | klen | (koff + klen) | (key.length - (koff + klen))) < 0) {
        throw new @Tainted IndexOutOfBoundsException(
            "Bad key buffer offset-length combination.");
      }

      if ((voff | vlen | (voff + vlen) | (value.length - (voff + vlen))) < 0) {
        throw new @Tainted IndexOutOfBoundsException(
            "Bad value buffer offset-length combination.");
      }

      try {
        @Tainted
        DataOutputStream dosKey = prepareAppendKey(klen);
        try {
          ++errorCount;
          dosKey.write(key, koff, klen);
          --errorCount;
        } finally {
          dosKey.close();
        }

        @Tainted
        DataOutputStream dosValue = prepareAppendValue(vlen);
        try {
          ++errorCount;
          dosValue.write(value, voff, vlen);
          --errorCount;
        } finally {
          dosValue.close();
        }
      } finally {
        state = State.READY;
      }
    }

    /**
     * Helper class to register key after close call on key append stream.
     */
    private class KeyRegister extends @Tainted DataOutputStream {
      private final @Tainted int expectedLength;
      private @Tainted boolean closed = false;

      public @Tainted KeyRegister(@Tainted int len) {
        super(currentKeyBufferOS);
        if (len >= 0) {
          currentKeyBufferOS.reset(len);
        } else {
          currentKeyBufferOS.reset();
        }
        expectedLength = len;
      }

      @Override
      public void close(TFile.@Tainted Writer.KeyRegister this) throws IOException {
        if (closed == true) {
          return;
        }

        try {
          ++errorCount;
          @Tainted
          byte @Tainted [] key = currentKeyBufferOS.getBuffer();
          @Tainted
          int len = currentKeyBufferOS.size();
          /**
           * verify length.
           */
          if (expectedLength >= 0 && expectedLength != len) {
            throw new @Tainted IOException("Incorrect key length: expected="
                + expectedLength + " actual=" + len);
          }

          Utils.writeVInt(blkAppender, len);
          blkAppender.write(key, 0, len);
          if (tfileIndex.getFirstKey() == null) {
            tfileIndex.setFirstKey(key, 0, len);
          }

          if (tfileMeta.isSorted() && tfileMeta.getRecordCount()>0) {
            @Tainted
            byte @Tainted [] lastKey = lastKeyBufferOS.getBuffer();
            @Tainted
            int lastLen = lastKeyBufferOS.size();
            if (tfileMeta.getComparator().compare(key, 0, len, lastKey, 0,
                lastLen) < 0) {
              throw new @Tainted IOException("Keys are not added in sorted order");
            }
          }

          @Tainted
          BoundedByteArrayOutputStream tmp = currentKeyBufferOS;
          currentKeyBufferOS = lastKeyBufferOS;
          lastKeyBufferOS = tmp;
          --errorCount;
        } finally {
          closed = true;
          state = State.END_KEY;
        }
      }
    }

    /**
     * Helper class to register value after close call on value append stream.
     */
    private class ValueRegister extends @Tainted DataOutputStream {
      private @Tainted boolean closed = false;

      public @Tainted ValueRegister(@Tainted OutputStream os) {
        super(os);
      }

      // Avoiding flushing call to down stream.
      @Override
      public void flush(TFile.@Tainted Writer.ValueRegister this) {
        // do nothing
      }

      @Override
      public void close(TFile.@Tainted Writer.ValueRegister this) throws IOException {
        if (closed == true) {
          return;
        }

        try {
          ++errorCount;
          super.close();
          blkRecordCount++;
          // bump up the total record count in the whole file
          tfileMeta.incRecordCount();
          finishDataBlock(false);
          --errorCount;
        } finally {
          closed = true;
          state = State.READY;
        }
      }
    }

    /**
     * Obtain an output stream for writing a key into TFile. This may only be
     * called when there is no active Key appending stream or value appending
     * stream.
     * 
     * @param length
     *          The expected length of the key. If length of the key is not
     *          known, set length = -1. Otherwise, the application must write
     *          exactly as many bytes as specified here before calling close on
     *          the returned output stream.
     * @return The key appending output stream.
     * @throws IOException
     * 
     */
    public @Tainted DataOutputStream prepareAppendKey(TFile.@Tainted Writer this, @Tainted int length) throws IOException {
      if (state != State.READY) {
        throw new @Tainted IllegalStateException("Incorrect state to start a new key: "
            + state.name());
      }

      initDataBlock();
      @Tainted
      DataOutputStream ret = new @Tainted KeyRegister(length);
      state = State.IN_KEY;
      return ret;
    }

    /**
     * Obtain an output stream for writing a value into TFile. This may only be
     * called right after a key appending operation (the key append stream must
     * be closed).
     * 
     * @param length
     *          The expected length of the value. If length of the value is not
     *          known, set length = -1. Otherwise, the application must write
     *          exactly as many bytes as specified here before calling close on
     *          the returned output stream. Advertising the value size up-front
     *          guarantees that the value is encoded in one chunk, and avoids
     *          intermediate chunk buffering.
     * @throws IOException
     * 
     */
    public @Tainted DataOutputStream prepareAppendValue(TFile.@Tainted Writer this, @Tainted int length) throws IOException {
      if (state != State.END_KEY) {
        throw new @Tainted IllegalStateException(
            "Incorrect state to start a new value: " + state.name());
      }

      @Tainted
      DataOutputStream ret;

      // unknown length
      if (length < 0) {
        if (valueBuffer == null) {
          valueBuffer = new @Tainted byte @Tainted [getChunkBufferSize(conf)];
        }
        ret = new @Tainted ValueRegister(new @Tainted ChunkEncoder(blkAppender, valueBuffer));
      } else {
        ret =
            new @Tainted ValueRegister(new Chunk.@Tainted SingleChunkEncoder(blkAppender, length));
      }

      state = State.IN_VALUE;
      return ret;
    }

    /**
     * Obtain an output stream for creating a meta block. This function may not
     * be called when there is a key append stream or value append stream
     * active. No more key-value insertion is allowed after a meta data block
     * has been added to TFile.
     * 
     * @param name
     *          Name of the meta block.
     * @param compressName
     *          Name of the compression algorithm to be used. Must be one of the
     *          strings returned by
     *          {@link TFile#getSupportedCompressionAlgorithms()}.
     * @return A DataOutputStream that can be used to write Meta Block data.
     *         Closing the stream would signal the ending of the block.
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     *           the Meta Block with the same name already exists.
     */
    public @Tainted DataOutputStream prepareMetaBlock(TFile.@Tainted Writer this, @Tainted String name, @Tainted String compressName)
        throws IOException, MetaBlockAlreadyExists {
      if (state != State.READY) {
        throw new @Tainted IllegalStateException(
            "Incorrect state to start a Meta Block: " + state.name());
      }

      finishDataBlock(true);
      @Tainted
      DataOutputStream outputStream =
          writerBCF.prepareMetaBlock(name, compressName);
      return outputStream;
    }

    /**
     * Obtain an output stream for creating a meta block. This function may not
     * be called when there is a key append stream or value append stream
     * active. No more key-value insertion is allowed after a meta data block
     * has been added to TFile. Data will be compressed using the default
     * compressor as defined in Writer's constructor.
     * 
     * @param name
     *          Name of the meta block.
     * @return A DataOutputStream that can be used to write Meta Block data.
     *         Closing the stream would signal the ending of the block.
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     *           the Meta Block with the same name already exists.
     */
    public @Tainted DataOutputStream prepareMetaBlock(TFile.@Tainted Writer this, @Tainted String name) throws IOException,
        MetaBlockAlreadyExists {
      if (state != State.READY) {
        throw new @Tainted IllegalStateException(
            "Incorrect state to start a Meta Block: " + state.name());
      }

      finishDataBlock(true);
      return writerBCF.prepareMetaBlock(name);
    }

    /**
     * Check if we need to start a new data block.
     * 
     * @throws IOException
     */
    private void initDataBlock(TFile.@Tainted Writer this) throws IOException {
      // for each new block, get a new appender
      if (blkAppender == null) {
        blkAppender = writerBCF.prepareDataBlock();
      }
    }

    /**
     * Close the current data block if necessary.
     * 
     * @param bForceFinish
     *          Force the closure regardless of the block size.
     * @throws IOException
     */
    void finishDataBlock(TFile.@Tainted Writer this, @Tainted boolean bForceFinish) throws IOException {
      if (blkAppender == null) {
        return;
      }

      // exceeded the size limit, do the compression and finish the block
      if (bForceFinish || blkAppender.getCompressedSize() >= sizeMinBlock) {
        // keep tracks of the last key of each data block, no padding
        // for now
        @Tainted
        TFileIndexEntry keyLast =
            new @Tainted TFileIndexEntry(lastKeyBufferOS.getBuffer(), 0, lastKeyBufferOS
                .size(), blkRecordCount);
        tfileIndex.addEntry(keyLast);
        // close the appender
        blkAppender.close();
        blkAppender = null;
        blkRecordCount = 0;
      }
    }
  }

  /**
   * TFile Reader. Users may only read TFiles by creating TFile.Reader.Scanner.
   * objects. A scanner may scan the whole TFile ({@link Reader#createScanner()}
   * ) , a portion of TFile based on byte offsets (
   * {@link Reader#createScannerByByteRange(long, long)}), or a portion of TFile with keys
   * fall in a certain key range (for sorted TFile only,
   * {@link Reader#createScannerByKey(byte[], byte[])} or
   * {@link Reader#createScannerByKey(RawComparable, RawComparable)}).
   */
  @InterfaceStability.Evolving
  public static class Reader implements @Tainted Closeable {
    // The underlying BCFile reader.
    final BCFile.@Tainted Reader readerBCF;

    // TFile index, it is loaded lazily.
    @Tainted
    TFileIndex tfileIndex = null;
    final @Tainted TFileMeta tfileMeta;
    final @Tainted BytesComparator comparator;

    // global begin and end locations.
    private final @Tainted Location begin;
    private final @Tainted Location end;

    /**
     * Location representing a virtual position in the TFile.
     */
    static final class Location implements @Tainted Comparable<@Tainted Location>, @Tainted Cloneable {
      private @Tainted int blockIndex;
      // distance/offset from the beginning of the block
      private @Tainted long recordIndex;

      @Tainted
      Location(@Tainted int blockIndex, @Tainted long recordIndex) {
        set(blockIndex, recordIndex);
      }

      void incRecordIndex(TFile.Reader.@Tainted Location this) {
        ++recordIndex;
      }

      @Tainted
      Location(@Tainted Location other) {
        set(other);
      }

      @Tainted
      int getBlockIndex(TFile.Reader.@Tainted Location this) {
        return blockIndex;
      }

      @Tainted
      long getRecordIndex(TFile.Reader.@Tainted Location this) {
        return recordIndex;
      }

      void set(TFile.Reader.@Tainted Location this, @Tainted int blockIndex, @Tainted long recordIndex) {
        if ((blockIndex | recordIndex) < 0) {
          throw new @Tainted IllegalArgumentException(
              "Illegal parameter for BlockLocation.");
        }
        this.blockIndex = blockIndex;
        this.recordIndex = recordIndex;
      }

      void set(TFile.Reader.@Tainted Location this, @Tainted Location other) {
        set(other.blockIndex, other.recordIndex);
      }

      /**
       * @see java.lang.Comparable#compareTo(java.lang.Object)
       */
      @Override
      public @Tainted int compareTo(TFile.Reader.@Tainted Location this, @Tainted Location other) {
        return compareTo(other.blockIndex, other.recordIndex);
      }

      @Tainted
      int compareTo(TFile.Reader.@Tainted Location this, @Tainted int bid, @Tainted long rid) {
        if (this.blockIndex == bid) {
          @Tainted
          long ret = this.recordIndex - rid;
          if (ret > 0) return 1;
          if (ret < 0) return -1;
          return 0;
        }
        return this.blockIndex - bid;
      }

      /**
       * @see java.lang.Object#clone()
       */
      @Override
      protected @Tainted Location clone(TFile.Reader.@Tainted Location this) {
        return new @Tainted Location(blockIndex, recordIndex);
      }

      /**
       * @see java.lang.Object#hashCode()
       */
      @Override
      public @Tainted int hashCode(TFile.Reader.@Tainted Location this) {
        final @Tainted int prime = 31;
        @Tainted
        int result = prime + blockIndex;
        result = (@Tainted int) (prime * result + recordIndex);
        return result;
      }

      /**
       * @see java.lang.Object#equals(java.lang.Object)
       */
      @Override
      public @Tainted boolean equals(TFile.Reader.@Tainted Location this, @Tainted Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        @Tainted
        Location other = (@Tainted Location) obj;
        if (blockIndex != other.blockIndex) return false;
        if (recordIndex != other.recordIndex) return false;
        return true;
      }
    }

    /**
     * Constructor
     * 
     * @param fsdis
     *          FS input stream of the TFile.
     * @param fileLength
     *          The length of TFile. This is required because we have no easy
     *          way of knowing the actual size of the input file through the
     *          File input stream.
     * @param conf
     * @throws IOException
     */
    public @Tainted Reader(@Tainted FSDataInputStream fsdis, @Tainted long fileLength, @Tainted Configuration conf)
        throws IOException {
      readerBCF = new BCFile.@Tainted Reader(fsdis, fileLength, conf);

      // first, read TFile meta
      @Tainted
      BlockReader brMeta = readerBCF.getMetaBlock(TFileMeta.BLOCK_NAME);
      try {
        tfileMeta = new @Tainted TFileMeta(brMeta);
      } finally {
        brMeta.close();
      }

      comparator = tfileMeta.getComparator();
      // Set begin and end locations.
      begin = new @Tainted Location(0, 0);
      end = new @Tainted Location(readerBCF.getBlockCount(), 0);
    }

    /**
     * Close the reader. The state of the Reader object is undefined after
     * close. Calling close() for multiple times has no effect.
     */
    @Override
    public void close(TFile.@Tainted Reader this) throws IOException {
      readerBCF.close();
    }

    /**
     * Get the begin location of the TFile.
     * 
     * @return If TFile is not empty, the location of the first key-value pair.
     *         Otherwise, it returns end().
     */
    @Tainted
    Location begin(TFile.@Tainted Reader this) {
      return begin;
    }

    /**
     * Get the end location of the TFile.
     * 
     * @return The location right after the last key-value pair in TFile.
     */
    @Tainted
    Location end(TFile.@Tainted Reader this) {
      return end;
    }

    /**
     * Get the string representation of the comparator.
     * 
     * @return If the TFile is not sorted by keys, an empty string will be
     *         returned. Otherwise, the actual comparator string that is
     *         provided during the TFile creation time will be returned.
     */
    public @Tainted String getComparatorName(TFile.@Tainted Reader this) {
      return tfileMeta.getComparatorString();
    }

    /**
     * Is the TFile sorted?
     * 
     * @return true if TFile is sorted.
     */
    public @Tainted boolean isSorted(TFile.@Tainted Reader this) {
      return tfileMeta.isSorted();
    }

    /**
     * Get the number of key-value pair entries in TFile.
     * 
     * @return the number of key-value pairs in TFile
     */
    public @Tainted long getEntryCount(TFile.@Tainted Reader this) {
      return tfileMeta.getRecordCount();
    }

    /**
     * Lazily loading the TFile index.
     * 
     * @throws IOException
     */
    synchronized void checkTFileDataIndex(TFile.@Tainted Reader this) throws IOException {
      if (tfileIndex == null) {
        @Tainted
        BlockReader brIndex = readerBCF.getMetaBlock(TFileIndex.BLOCK_NAME);
        try {
          tfileIndex =
              new @Tainted TFileIndex(readerBCF.getBlockCount(), brIndex, tfileMeta
                  .getComparator());
        } finally {
          brIndex.close();
        }
      }
    }

    /**
     * Get the first key in the TFile.
     * 
     * @return The first key in the TFile.
     * @throws IOException
     */
    public @Tainted RawComparable getFirstKey(TFile.@Tainted Reader this) throws IOException {
      checkTFileDataIndex();
      return tfileIndex.getFirstKey();
    }

    /**
     * Get the last key in the TFile.
     * 
     * @return The last key in the TFile.
     * @throws IOException
     */
    public @Tainted RawComparable getLastKey(TFile.@Tainted Reader this) throws IOException {
      checkTFileDataIndex();
      return tfileIndex.getLastKey();
    }

    /**
     * Get a Comparator object to compare Entries. It is useful when you want
     * stores the entries in a collection (such as PriorityQueue) and perform
     * sorting or comparison among entries based on the keys without copying out
     * the key.
     * 
     * @return An Entry Comparator..
     */
    public @Tainted Comparator<@Tainted Scanner.Entry> getEntryComparator(TFile.@Tainted Reader this) {
      if (!isSorted()) {
        throw new @Tainted RuntimeException(
            "Entries are not comparable for unsorted TFiles");
      }

      return new @Tainted Comparator<@Tainted Scanner.Entry>() {
        /**
         * Provide a customized comparator for Entries. This is useful if we
         * have a collection of Entry objects. However, if the Entry objects
         * come from different TFiles, users must ensure that those TFiles share
         * the same RawComparator.
         */
        @Override
        public @Tainted int compare(@Tainted Scanner.@Tainted Entry o1, @Tainted Scanner.@Tainted Entry o2) {
          return comparator.compare(o1.getKeyBuffer(), 0, o1.getKeyLength(), o2
              .getKeyBuffer(), 0, o2.getKeyLength());
        }
      };
    }

    /**
     * Get an instance of the RawComparator that is constructed based on the
     * string comparator representation.
     * 
     * @return a Comparator that can compare RawComparable's.
     */
    public @Tainted Comparator<@Tainted RawComparable> getComparator(TFile.@Tainted Reader this) {
      return comparator;
    }

    /**
     * Stream access to a meta block.``
     * 
     * @param name
     *          The name of the meta block.
     * @return The input stream.
     * @throws IOException
     *           on I/O error.
     * @throws MetaBlockDoesNotExist
     *           If the meta block with the name does not exist.
     */
    public @Tainted DataInputStream getMetaBlock(TFile.@Tainted Reader this, @Tainted String name) throws IOException,
        MetaBlockDoesNotExist {
      return readerBCF.getMetaBlock(name);
    }

    /**
     * if greater is true then returns the beginning location of the block
     * containing the key strictly greater than input key. if greater is false
     * then returns the beginning location of the block greater than equal to
     * the input key
     * 
     * @param key
     *          the input key
     * @param greater
     *          boolean flag
     * @return
     * @throws IOException
     */
    @Tainted
    Location getBlockContainsKey(TFile.@Tainted Reader this, @Tainted RawComparable key, @Tainted boolean greater)
        throws IOException {
      if (!isSorted()) {
        throw new @Tainted RuntimeException("Seeking in unsorted TFile");
      }
      checkTFileDataIndex();
      @Tainted
      int blkIndex =
          (greater) ? tfileIndex.upperBound(key) : tfileIndex.lowerBound(key);
      if (blkIndex < 0) return end;
      return new @Tainted Location(blkIndex, 0);
    }

    @Tainted
    Location getLocationByRecordNum(TFile.@Tainted Reader this, @Tainted long recNum) throws IOException {
      checkTFileDataIndex();
      return tfileIndex.getLocationByRecordNum(recNum);
    }

    @Tainted
    long getRecordNumByLocation(TFile.@Tainted Reader this, @Tainted Location location) throws IOException {
      checkTFileDataIndex();
      return tfileIndex.getRecordNumByLocation(location);      
    }
    
    @Tainted
    int compareKeys(TFile.@Tainted Reader this, @Tainted byte @Tainted [] a, @Tainted int o1, @Tainted int l1, @Tainted byte @Tainted [] b, @Tainted int o2, @Tainted int l2) {
      if (!isSorted()) {
        throw new @Tainted RuntimeException("Cannot compare keys for unsorted TFiles.");
      }
      return comparator.compare(a, o1, l1, b, o2, l2);
    }

    @Tainted
    int compareKeys(TFile.@Tainted Reader this, @Tainted RawComparable a, @Tainted RawComparable b) {
      if (!isSorted()) {
        throw new @Tainted RuntimeException("Cannot compare keys for unsorted TFiles.");
      }
      return comparator.compare(a, b);
    }

    /**
     * Get the location pointing to the beginning of the first key-value pair in
     * a compressed block whose byte offset in the TFile is greater than or
     * equal to the specified offset.
     * 
     * @param offset
     *          the user supplied offset.
     * @return the location to the corresponding entry; or end() if no such
     *         entry exists.
     */
    @Tainted
    Location getLocationNear(TFile.@Tainted Reader this, @Tainted long offset) {
      @Tainted
      int blockIndex = readerBCF.getBlockIndexNear(offset);
      if (blockIndex == -1) return end;
      return new @Tainted Location(blockIndex, 0);
    }

    /**
     * Get the RecordNum for the first key-value pair in a compressed block
     * whose byte offset in the TFile is greater than or equal to the specified
     * offset.
     * 
     * @param offset
     *          the user supplied offset.
     * @return the RecordNum to the corresponding entry. If no such entry
     *         exists, it returns the total entry count.
     * @throws IOException
     */
    public @Tainted long getRecordNumNear(TFile.@Tainted Reader this, @Tainted long offset) throws IOException {
      return getRecordNumByLocation(getLocationNear(offset));
    }
    
    /**
     * Get a sample key that is within a block whose starting offset is greater
     * than or equal to the specified offset.
     * 
     * @param offset
     *          The file offset.
     * @return the key that fits the requirement; or null if no such key exists
     *         (which could happen if the offset is close to the end of the
     *         TFile).
     * @throws IOException
     */
    public @Tainted RawComparable getKeyNear(TFile.@Tainted Reader this, @Tainted long offset) throws IOException {
      @Tainted
      int blockIndex = readerBCF.getBlockIndexNear(offset);
      if (blockIndex == -1) return null;
      checkTFileDataIndex();
      return new @Tainted ByteArray(tfileIndex.getEntry(blockIndex).key);
    }

    /**
     * Get a scanner than can scan the whole TFile.
     * 
     * @return The scanner object. A valid Scanner is always returned even if
     *         the TFile is empty.
     * @throws IOException
     */
    public @Tainted Scanner createScanner(TFile.@Tainted Reader this) throws IOException {
      return new @Tainted Scanner(this, begin, end);
    }

    /**
     * Get a scanner that covers a portion of TFile based on byte offsets.
     * 
     * @param offset
     *          The beginning byte offset in the TFile.
     * @param length
     *          The length of the region.
     * @return The actual coverage of the returned scanner tries to match the
     *         specified byte-region but always round up to the compression
     *         block boundaries. It is possible that the returned scanner
     *         contains zero key-value pairs even if length is positive.
     * @throws IOException
     */
    public @Tainted Scanner createScannerByByteRange(TFile.@Tainted Reader this, @Tainted long offset, @Tainted long length) throws IOException {
      return new @Tainted Scanner(this, offset, offset + length);
    }

    /**
     * Get a scanner that covers a portion of TFile based on keys.
     * 
     * @param beginKey
     *          Begin key of the scan (inclusive). If null, scan from the first
     *          key-value entry of the TFile.
     * @param endKey
     *          End key of the scan (exclusive). If null, scan up to the last
     *          key-value entry of the TFile.
     * @return The actual coverage of the returned scanner will cover all keys
     *         greater than or equal to the beginKey and less than the endKey.
     * @throws IOException
     * 
     * @deprecated Use {@link #createScannerByKey(byte[], byte[])} instead.
     */
    @Deprecated
    public @Tainted Scanner createScanner(TFile.@Tainted Reader this, @Tainted byte @Tainted [] beginKey, @Tainted byte @Tainted [] endKey)
      throws IOException {
      return createScannerByKey(beginKey, endKey);
    }
    
    /**
     * Get a scanner that covers a portion of TFile based on keys.
     * 
     * @param beginKey
     *          Begin key of the scan (inclusive). If null, scan from the first
     *          key-value entry of the TFile.
     * @param endKey
     *          End key of the scan (exclusive). If null, scan up to the last
     *          key-value entry of the TFile.
     * @return The actual coverage of the returned scanner will cover all keys
     *         greater than or equal to the beginKey and less than the endKey.
     * @throws IOException
     */
    public @Tainted Scanner createScannerByKey(TFile.@Tainted Reader this, @Tainted byte @Tainted [] beginKey, @Tainted byte @Tainted [] endKey)
        throws IOException {
      return createScannerByKey((beginKey == null) ? null : new @Tainted ByteArray(beginKey,
          0, beginKey.length), (endKey == null) ? null : new @Tainted ByteArray(endKey,
          0, endKey.length));
    }

    /**
     * Get a scanner that covers a specific key range.
     * 
     * @param beginKey
     *          Begin key of the scan (inclusive). If null, scan from the first
     *          key-value entry of the TFile.
     * @param endKey
     *          End key of the scan (exclusive). If null, scan up to the last
     *          key-value entry of the TFile.
     * @return The actual coverage of the returned scanner will cover all keys
     *         greater than or equal to the beginKey and less than the endKey.
     * @throws IOException
     * 
     * @deprecated Use {@link #createScannerByKey(RawComparable, RawComparable)}
     *             instead.
     */
    @Deprecated
    public @Tainted Scanner createScanner(TFile.@Tainted Reader this, @Tainted RawComparable beginKey, @Tainted RawComparable endKey)
        throws IOException {
      return createScannerByKey(beginKey, endKey);
    }

    /**
     * Get a scanner that covers a specific key range.
     * 
     * @param beginKey
     *          Begin key of the scan (inclusive). If null, scan from the first
     *          key-value entry of the TFile.
     * @param endKey
     *          End key of the scan (exclusive). If null, scan up to the last
     *          key-value entry of the TFile.
     * @return The actual coverage of the returned scanner will cover all keys
     *         greater than or equal to the beginKey and less than the endKey.
     * @throws IOException
     */
    public @Tainted Scanner createScannerByKey(TFile.@Tainted Reader this, @Tainted RawComparable beginKey, @Tainted RawComparable endKey)
        throws IOException {
      if ((beginKey != null) && (endKey != null)
          && (compareKeys(beginKey, endKey) >= 0)) {
        return new @Tainted Scanner(this, beginKey, beginKey);
      }
      return new @Tainted Scanner(this, beginKey, endKey);
    }

    /**
     * Create a scanner that covers a range of records.
     * 
     * @param beginRecNum
     *          The RecordNum for the first record (inclusive).
     * @param endRecNum
     *          The RecordNum for the last record (exclusive). To scan the whole
     *          file, either specify endRecNum==-1 or endRecNum==getEntryCount().
     * @return The TFile scanner that covers the specified range of records.
     * @throws IOException
     */
    public @Tainted Scanner createScannerByRecordNum(TFile.@Tainted Reader this, @Tainted long beginRecNum, @Tainted long endRecNum)
        throws IOException {
      if (beginRecNum < 0) beginRecNum = 0;
      if (endRecNum < 0 || endRecNum > getEntryCount()) {
        endRecNum = getEntryCount();
      }
      return new @Tainted Scanner(this, getLocationByRecordNum(beginRecNum),
          getLocationByRecordNum(endRecNum));
    }

    /**
     * The TFile Scanner. The Scanner has an implicit cursor, which, upon
     * creation, points to the first key-value pair in the scan range. If the
     * scan range is empty, the cursor will point to the end of the scan range.
     * <p>
     * Use {@link Scanner#atEnd()} to test whether the cursor is at the end
     * location of the scanner.
     * <p>
     * Use {@link Scanner#advance()} to move the cursor to the next key-value
     * pair (or end if none exists). Use seekTo methods (
     * {@link Scanner#seekTo(byte[])} or
     * {@link Scanner#seekTo(byte[], int, int)}) to seek to any arbitrary
     * location in the covered range (including backward seeking). Use
     * {@link Scanner#rewind()} to seek back to the beginning of the scanner.
     * Use {@link Scanner#seekToEnd()} to seek to the end of the scanner.
     * <p>
     * Actual keys and values may be obtained through {@link Scanner.Entry}
     * object, which is obtained through {@link Scanner#entry()}.
     */
    public static class Scanner implements @Tainted Closeable {
      // The underlying TFile reader.
      final @Tainted Reader reader;
      // current block (null if reaching end)
      private @Tainted BlockReader blkReader;

      @Tainted
      Location beginLocation;
      @Tainted
      Location endLocation;
      @Tainted
      Location currentLocation;

      // flag to ensure value is only examined once.
      @Tainted
      boolean valueChecked = false;
      // reusable buffer for keys.
      final @Tainted byte @Tainted [] keyBuffer;
      // length of key, -1 means key is invalid.
      @Tainted
      int klen = -1;

      static final @Tainted int MAX_VAL_TRANSFER_BUF_SIZE = 128 * 1024;
      @Tainted
      BytesWritable valTransferBuffer;

      @Tainted
      DataInputBuffer keyDataInputStream;
      @Tainted
      ChunkDecoder valueBufferInputStream;
      @Tainted
      DataInputStream valueDataInputStream;
      // vlen == -1 if unknown.
      @Tainted
      int vlen;

      /**
       * Constructor
       * 
       * @param reader
       *          The TFile reader object.
       * @param offBegin
       *          Begin byte-offset of the scan.
       * @param offEnd
       *          End byte-offset of the scan.
       * @throws IOException
       * 
       *           The offsets will be rounded to the beginning of a compressed
       *           block whose offset is greater than or equal to the specified
       *           offset.
       */
      protected @Tainted Scanner(@Tainted Reader reader, @Tainted long offBegin, @Tainted long offEnd)
          throws IOException {
        this(reader, reader.getLocationNear(offBegin), reader
            .getLocationNear(offEnd));
      }

      /**
       * Constructor
       * 
       * @param reader
       *          The TFile reader object.
       * @param begin
       *          Begin location of the scan.
       * @param end
       *          End location of the scan.
       * @throws IOException
       */
      @Tainted
      Scanner(@Tainted Reader reader, @Tainted Location begin, @Tainted Location end) throws IOException {
        this.reader = reader;
        // ensure the TFile index is loaded throughout the life of scanner.
        reader.checkTFileDataIndex();
        beginLocation = begin;
        endLocation = end;

        valTransferBuffer = new @Tainted BytesWritable();
        // TODO: remember the longest key in a TFile, and use it to replace
        // MAX_KEY_SIZE.
        keyBuffer = new @Tainted byte @Tainted [MAX_KEY_SIZE];
        keyDataInputStream = new @Tainted DataInputBuffer();
        valueBufferInputStream = new @Tainted ChunkDecoder();
        valueDataInputStream = new @Tainted DataInputStream(valueBufferInputStream);

        if (beginLocation.compareTo(endLocation) >= 0) {
          currentLocation = new @Tainted Location(endLocation);
        } else {
          currentLocation = new @Tainted Location(0, 0);
          initBlock(beginLocation.getBlockIndex());
          inBlockAdvance(beginLocation.getRecordIndex());
        }
      }

      /**
       * Constructor
       * 
       * @param reader
       *          The TFile reader object.
       * @param beginKey
       *          Begin key of the scan. If null, scan from the first <K,V>
       *          entry of the TFile.
       * @param endKey
       *          End key of the scan. If null, scan up to the last <K, V> entry
       *          of the TFile.
       * @throws IOException
       */
      protected @Tainted Scanner(@Tainted Reader reader, @Tainted RawComparable beginKey,
          @Tainted
          RawComparable endKey) throws IOException {
        this(reader, (beginKey == null) ? reader.begin() : reader
            .getBlockContainsKey(beginKey, false), reader.end());
        if (beginKey != null) {
          inBlockAdvance(beginKey, false);
          beginLocation.set(currentLocation);
        }
        if (endKey != null) {
          seekTo(endKey, false);
          endLocation.set(currentLocation);
          seekTo(beginLocation);
        }
      }

      /**
       * Move the cursor to the first entry whose key is greater than or equal
       * to the input key. Synonymous to seekTo(key, 0, key.length). The entry
       * returned by the previous entry() call will be invalid.
       * 
       * @param key
       *          The input key
       * @return true if we find an equal key.
       * @throws IOException
       */
      public @Tainted boolean seekTo(TFile.Reader.@Tainted Scanner this, @Tainted byte @Tainted [] key) throws IOException {
        return seekTo(key, 0, key.length);
      }

      /**
       * Move the cursor to the first entry whose key is greater than or equal
       * to the input key. The entry returned by the previous entry() call will
       * be invalid.
       * 
       * @param key
       *          The input key
       * @param keyOffset
       *          offset in the key buffer.
       * @param keyLen
       *          key buffer length.
       * @return true if we find an equal key; false otherwise.
       * @throws IOException
       */
      public @Tainted boolean seekTo(TFile.Reader.@Tainted Scanner this, @Tainted byte @Tainted [] key, @Tainted int keyOffset, @Tainted int keyLen)
          throws IOException {
        return seekTo(new @Tainted ByteArray(key, keyOffset, keyLen), false);
      }

      private @Tainted boolean seekTo(TFile.Reader.@Tainted Scanner this, @Tainted RawComparable key, @Tainted boolean beyond)
          throws IOException {
        @Tainted
        Location l = reader.getBlockContainsKey(key, beyond);
        if (l.compareTo(beginLocation) < 0) {
          l = beginLocation;
        } else if (l.compareTo(endLocation) >= 0) {
          seekTo(endLocation);
          return false;
        }

        // check if what we are seeking is in the later part of the current
        // block.
        if (atEnd() || (l.getBlockIndex() != currentLocation.getBlockIndex())
            || (compareCursorKeyTo(key) >= 0)) {
          // sorry, we must seek to a different location first.
          seekTo(l);
        }

        return inBlockAdvance(key, beyond);
      }

      /**
       * Move the cursor to the new location. The entry returned by the previous
       * entry() call will be invalid.
       * 
       * @param l
       *          new cursor location. It must fall between the begin and end
       *          location of the scanner.
       * @throws IOException
       */
      private void seekTo(TFile.Reader.@Tainted Scanner this, @Tainted Location l) throws IOException {
        if (l.compareTo(beginLocation) < 0) {
          throw new @Tainted IllegalArgumentException(
              "Attempt to seek before the begin location.");
        }

        if (l.compareTo(endLocation) > 0) {
          throw new @Tainted IllegalArgumentException(
              "Attempt to seek after the end location.");
        }

        if (l.compareTo(endLocation) == 0) {
          parkCursorAtEnd();
          return;
        }

        if (l.getBlockIndex() != currentLocation.getBlockIndex()) {
          // going to a totally different block
          initBlock(l.getBlockIndex());
        } else {
          if (valueChecked) {
            // may temporarily go beyond the last record in the block (in which
            // case the next if loop will always be true).
            inBlockAdvance(1);
          }
          if (l.getRecordIndex() < currentLocation.getRecordIndex()) {
            initBlock(l.getBlockIndex());
          }
        }

        inBlockAdvance(l.getRecordIndex() - currentLocation.getRecordIndex());

        return;
      }

      /**
       * Rewind to the first entry in the scanner. The entry returned by the
       * previous entry() call will be invalid.
       * 
       * @throws IOException
       */
      public void rewind(TFile.Reader.@Tainted Scanner this) throws IOException {
        seekTo(beginLocation);
      }

      /**
       * Seek to the end of the scanner. The entry returned by the previous
       * entry() call will be invalid.
       * 
       * @throws IOException
       */
      public void seekToEnd(TFile.Reader.@Tainted Scanner this) throws IOException {
        parkCursorAtEnd();
      }

      /**
       * Move the cursor to the first entry whose key is greater than or equal
       * to the input key. Synonymous to lowerBound(key, 0, key.length). The
       * entry returned by the previous entry() call will be invalid.
       * 
       * @param key
       *          The input key
       * @throws IOException
       */
      public void lowerBound(TFile.Reader.@Tainted Scanner this, @Tainted byte @Tainted [] key) throws IOException {
        lowerBound(key, 0, key.length);
      }

      /**
       * Move the cursor to the first entry whose key is greater than or equal
       * to the input key. The entry returned by the previous entry() call will
       * be invalid.
       * 
       * @param key
       *          The input key
       * @param keyOffset
       *          offset in the key buffer.
       * @param keyLen
       *          key buffer length.
       * @throws IOException
       */
      public void lowerBound(TFile.Reader.@Tainted Scanner this, @Tainted byte @Tainted [] key, @Tainted int keyOffset, @Tainted int keyLen)
          throws IOException {
        seekTo(new @Tainted ByteArray(key, keyOffset, keyLen), false);
      }

      /**
       * Move the cursor to the first entry whose key is strictly greater than
       * the input key. Synonymous to upperBound(key, 0, key.length). The entry
       * returned by the previous entry() call will be invalid.
       * 
       * @param key
       *          The input key
       * @throws IOException
       */
      public void upperBound(TFile.Reader.@Tainted Scanner this, @Tainted byte @Tainted [] key) throws IOException {
        upperBound(key, 0, key.length);
      }

      /**
       * Move the cursor to the first entry whose key is strictly greater than
       * the input key. The entry returned by the previous entry() call will be
       * invalid.
       * 
       * @param key
       *          The input key
       * @param keyOffset
       *          offset in the key buffer.
       * @param keyLen
       *          key buffer length.
       * @throws IOException
       */
      public void upperBound(TFile.Reader.@Tainted Scanner this, @Tainted byte @Tainted [] key, @Tainted int keyOffset, @Tainted int keyLen)
          throws IOException {
        seekTo(new @Tainted ByteArray(key, keyOffset, keyLen), true);
      }

      /**
       * Move the cursor to the next key-value pair. The entry returned by the
       * previous entry() call will be invalid.
       * 
       * @return true if the cursor successfully moves. False when cursor is
       *         already at the end location and cannot be advanced.
       * @throws IOException
       */
      public @Tainted boolean advance(TFile.Reader.@Tainted Scanner this) throws IOException {
        if (atEnd()) {
          return false;
        }

        @Tainted
        int curBid = currentLocation.getBlockIndex();
        @Tainted
        long curRid = currentLocation.getRecordIndex();
        @Tainted
        long entriesInBlock = reader.getBlockEntryCount(curBid);
        if (curRid + 1 >= entriesInBlock) {
          if (endLocation.compareTo(curBid + 1, 0) <= 0) {
            // last entry in TFile.
            parkCursorAtEnd();
          } else {
            // last entry in Block.
            initBlock(curBid + 1);
          }
        } else {
          inBlockAdvance(1);
        }
        return true;
      }

      /**
       * Load a compressed block for reading. Expecting blockIndex is valid.
       * 
       * @throws IOException
       */
      private void initBlock(TFile.Reader.@Tainted Scanner this, @Tainted int blockIndex) throws IOException {
        klen = -1;
        if (blkReader != null) {
          try {
            blkReader.close();
          } finally {
            blkReader = null;
          }
        }
        blkReader = reader.getBlockReader(blockIndex);
        currentLocation.set(blockIndex, 0);
      }

      private void parkCursorAtEnd(TFile.Reader.@Tainted Scanner this) throws IOException {
        klen = -1;
        currentLocation.set(endLocation);
        if (blkReader != null) {
          try {
            blkReader.close();
          } finally {
            blkReader = null;
          }
        }
      }

      /**
       * Close the scanner. Release all resources. The behavior of using the
       * scanner after calling close is not defined. The entry returned by the
       * previous entry() call will be invalid.
       */
      @Override
      public void close(TFile.Reader.@Tainted Scanner this) throws IOException {
        parkCursorAtEnd();
      }

      /**
       * Is cursor at the end location?
       * 
       * @return true if the cursor is at the end location.
       */
      public @Tainted boolean atEnd(TFile.Reader.@Tainted Scanner this) {
        return (currentLocation.compareTo(endLocation) >= 0);
      }

      /**
       * check whether we have already successfully obtained the key. It also
       * initializes the valueInputStream.
       */
      void checkKey(TFile.Reader.@Tainted Scanner this) throws IOException {
        if (klen >= 0) return;
        if (atEnd()) {
          throw new @Tainted EOFException("No key-value to read");
        }
        klen = -1;
        vlen = -1;
        valueChecked = false;

        klen = Utils.readVInt(blkReader);
        blkReader.readFully(keyBuffer, 0, klen);
        valueBufferInputStream.reset(blkReader);
        if (valueBufferInputStream.isLastChunk()) {
          vlen = valueBufferInputStream.getRemain();
        }
      }

      /**
       * Get an entry to access the key and value.
       * 
       * @return The Entry object to access the key and value.
       * @throws IOException
       */
      public @Tainted Entry entry(TFile.Reader.@Tainted Scanner this) throws IOException {
        checkKey();
        return new @Tainted Entry();
      }

      /**
       * Get the RecordNum corresponding to the entry pointed by the cursor.
       * @return The RecordNum corresponding to the entry pointed by the cursor.
       * @throws IOException
       */
      public @Tainted long getRecordNum(TFile.Reader.@Tainted Scanner this) throws IOException {
        return reader.getRecordNumByLocation(currentLocation);
      }
      
      /**
       * Internal API. Comparing the key at cursor to user-specified key.
       * 
       * @param other
       *          user-specified key.
       * @return negative if key at cursor is smaller than user key; 0 if equal;
       *         and positive if key at cursor greater than user key.
       * @throws IOException
       */
      @Tainted
      int compareCursorKeyTo(TFile.Reader.@Tainted Scanner this, @Tainted RawComparable other) throws IOException {
        checkKey();
        return reader.compareKeys(keyBuffer, 0, klen, other.buffer(), other
            .offset(), other.size());
      }

      /**
       * Entry to a &lt;Key, Value&gt; pair.
       */
      public class Entry implements @Tainted Comparable<@Tainted RawComparable> {
        /**
         * Get the length of the key.
         * 
         * @return the length of the key.
         */
        public @Tainted int getKeyLength(TFile.Reader.@Tainted Scanner.Entry this) {
          return klen;
        }

        @Tainted
        byte @Tainted [] getKeyBuffer(TFile.Reader.@Tainted Scanner.Entry this) {
          return keyBuffer;
        }

        /**
         * Copy the key and value in one shot into BytesWritables. This is
         * equivalent to getKey(key); getValue(value);
         * 
         * @param key
         *          BytesWritable to hold key.
         * @param value
         *          BytesWritable to hold value
         * @throws IOException
         */
        public void get(TFile.Reader.@Tainted Scanner.Entry this, @Tainted BytesWritable key, @Tainted BytesWritable value)
            throws IOException {
          getKey(key);
          getValue(value);
        }

        /**
         * Copy the key into BytesWritable. The input BytesWritable will be
         * automatically resized to the actual key size.
         * 
         * @param key
         *          BytesWritable to hold the key.
         * @throws IOException
         */
        public @Tainted int getKey(TFile.Reader.@Tainted Scanner.Entry this, @Tainted BytesWritable key) throws IOException {
          key.setSize(getKeyLength());
          getKey(key.getBytes());
          return key.getLength();
        }

        /**
         * Copy the value into BytesWritable. The input BytesWritable will be
         * automatically resized to the actual value size. The implementation
         * directly uses the buffer inside BytesWritable for storing the value.
         * The call does not require the value length to be known.
         * 
         * @param value
         * @throws IOException
         */
        public @Tainted long getValue(TFile.Reader.@Tainted Scanner.Entry this, @Tainted BytesWritable value) throws IOException {
          @Tainted
          DataInputStream dis = getValueStream();
          @Tainted
          int size = 0;
          try {
            @Tainted
            int remain;
            while ((remain = valueBufferInputStream.getRemain()) > 0) {
              value.setSize(size + remain);
              dis.readFully(value.getBytes(), size, remain);
              size += remain;
            }
            return value.getLength();
          } finally {
            dis.close();
          }
        }

        /**
         * Writing the key to the output stream. This method avoids copying key
         * buffer from Scanner into user buffer, then writing to the output
         * stream.
         * 
         * @param out
         *          The output stream
         * @return the length of the key.
         * @throws IOException
         */
        public @Tainted int writeKey(TFile.Reader.@Tainted Scanner.Entry this, @Tainted OutputStream out) throws IOException {
          out.write(keyBuffer, 0, klen);
          return klen;
        }

        /**
         * Writing the value to the output stream. This method avoids copying
         * value data from Scanner into user buffer, then writing to the output
         * stream. It does not require the value length to be known.
         * 
         * @param out
         *          The output stream
         * @return the length of the value
         * @throws IOException
         */
        public @Tainted long writeValue(TFile.Reader.@Tainted Scanner.Entry this, @Tainted OutputStream out) throws IOException {
          @Tainted
          DataInputStream dis = getValueStream();
          @Tainted
          long size = 0;
          try {
            @Tainted
            int chunkSize;
            while ((chunkSize = valueBufferInputStream.getRemain()) > 0) {
              chunkSize = Math.min(chunkSize, MAX_VAL_TRANSFER_BUF_SIZE);
              valTransferBuffer.setSize(chunkSize);
              dis.readFully(valTransferBuffer.getBytes(), 0, chunkSize);
              out.write(valTransferBuffer.getBytes(), 0, chunkSize);
              size += chunkSize;
            }
            return size;
          } finally {
            dis.close();
          }
        }

        /**
         * Copy the key into user supplied buffer.
         * 
         * @param buf
         *          The buffer supplied by user. The length of the buffer must
         *          not be shorter than the key length.
         * @return The length of the key.
         * 
         * @throws IOException
         */
        public @Tainted int getKey(TFile.Reader.@Tainted Scanner.Entry this, @Tainted byte @Tainted [] buf) throws IOException {
          return getKey(buf, 0);
        }

        /**
         * Copy the key into user supplied buffer.
         * 
         * @param buf
         *          The buffer supplied by user.
         * @param offset
         *          The starting offset of the user buffer where we should copy
         *          the key into. Requiring the key-length + offset no greater
         *          than the buffer length.
         * @return The length of the key.
         * @throws IOException
         */
        public @Tainted int getKey(TFile.Reader.@Tainted Scanner.Entry this, @Tainted byte @Tainted [] buf, @Tainted int offset) throws IOException {
          if ((offset | (buf.length - offset - klen)) < 0) {
            throw new @Tainted IndexOutOfBoundsException(
                "Bufer not enough to store the key");
          }
          System.arraycopy(keyBuffer, 0, buf, offset, klen);
          return klen;
        }

        /**
         * Streaming access to the key. Useful for desrializing the key into
         * user objects.
         * 
         * @return The input stream.
         */
        public @Tainted DataInputStream getKeyStream(TFile.Reader.@Tainted Scanner.Entry this) {
          keyDataInputStream.reset(keyBuffer, klen);
          return keyDataInputStream;
        }

        /**
         * Get the length of the value. isValueLengthKnown() must be tested
         * true.
         * 
         * @return the length of the value.
         */
        public @Tainted int getValueLength(TFile.Reader.@Tainted Scanner.Entry this) {
          if (vlen >= 0) {
            return vlen;
          }

          throw new @Tainted RuntimeException("Value length unknown.");
        }

        /**
         * Copy value into user-supplied buffer. User supplied buffer must be
         * large enough to hold the whole value. The value part of the key-value
         * pair pointed by the current cursor is not cached and can only be
         * examined once. Calling any of the following functions more than once
         * without moving the cursor will result in exception:
         * {@link #getValue(byte[])}, {@link #getValue(byte[], int)},
         * {@link #getValueStream}.
         * 
         * @return the length of the value. Does not require
         *         isValueLengthKnown() to be true.
         * @throws IOException
         * 
         */
        public @Tainted int getValue(TFile.Reader.@Tainted Scanner.Entry this, @Tainted byte @Tainted [] buf) throws IOException {
          return getValue(buf, 0);
        }

        /**
         * Copy value into user-supplied buffer. User supplied buffer must be
         * large enough to hold the whole value (starting from the offset). The
         * value part of the key-value pair pointed by the current cursor is not
         * cached and can only be examined once. Calling any of the following
         * functions more than once without moving the cursor will result in
         * exception: {@link #getValue(byte[])}, {@link #getValue(byte[], int)},
         * {@link #getValueStream}.
         * 
         * @return the length of the value. Does not require
         *         isValueLengthKnown() to be true.
         * @throws IOException
         */
        public @Tainted int getValue(TFile.Reader.@Tainted Scanner.Entry this, @Tainted byte @Tainted [] buf, @Tainted int offset) throws IOException {
          @Tainted
          DataInputStream dis = getValueStream();
          try {
            if (isValueLengthKnown()) {
              if ((offset | (buf.length - offset - vlen)) < 0) {
                throw new @Tainted IndexOutOfBoundsException(
                    "Buffer too small to hold value");
              }
              dis.readFully(buf, offset, vlen);
              return vlen;
            }

            @Tainted
            int nextOffset = offset;
            while (nextOffset < buf.length) {
              @Tainted
              int n = dis.read(buf, nextOffset, buf.length - nextOffset);
              if (n < 0) {
                break;
              }
              nextOffset += n;
            }
            if (dis.read() >= 0) {
              // attempt to read one more byte to determine whether we reached
              // the
              // end or not.
              throw new @Tainted IndexOutOfBoundsException(
                  "Buffer too small to hold value");
            }
            return nextOffset - offset;
          } finally {
            dis.close();
          }
        }

        /**
         * Stream access to value. The value part of the key-value pair pointed
         * by the current cursor is not cached and can only be examined once.
         * Calling any of the following functions more than once without moving
         * the cursor will result in exception: {@link #getValue(byte[])},
         * {@link #getValue(byte[], int)}, {@link #getValueStream}.
         * 
         * @return The input stream for reading the value.
         * @throws IOException
         */
        public @Tainted DataInputStream getValueStream(TFile.Reader.@Tainted Scanner.Entry this) throws IOException {
          if (valueChecked == true) {
            throw new @Tainted IllegalStateException(
                "Attempt to examine value multiple times.");
          }
          valueChecked = true;
          return valueDataInputStream;
        }

        /**
         * Check whether it is safe to call getValueLength().
         * 
         * @return true if value length is known before hand. Values less than
         *         the chunk size will always have their lengths known before
         *         hand. Values that are written out as a whole (with advertised
         *         length up-front) will always have their lengths known in
         *         read.
         */
        public @Tainted boolean isValueLengthKnown(TFile.Reader.@Tainted Scanner.Entry this) {
          return (vlen >= 0);
        }

        /**
         * Compare the entry key to another key. Synonymous to compareTo(key, 0,
         * key.length).
         * 
         * @param buf
         *          The key buffer.
         * @return comparison result between the entry key with the input key.
         */
        public @Tainted int compareTo(TFile.Reader.@Tainted Scanner.Entry this, @Tainted byte @Tainted [] buf) {
          return compareTo(buf, 0, buf.length);
        }

        /**
         * Compare the entry key to another key. Synonymous to compareTo(new
         * ByteArray(buf, offset, length)
         * 
         * @param buf
         *          The key buffer
         * @param offset
         *          offset into the key buffer.
         * @param length
         *          the length of the key.
         * @return comparison result between the entry key with the input key.
         */
        public @Tainted int compareTo(TFile.Reader.@Tainted Scanner.Entry this, @Tainted byte @Tainted [] buf, @Tainted int offset, @Tainted int length) {
          return compareTo(new @Tainted ByteArray(buf, offset, length));
        }

        /**
         * Compare an entry with a RawComparable object. This is useful when
         * Entries are stored in a collection, and we want to compare a user
         * supplied key.
         */
        @Override
        public @Tainted int compareTo(TFile.Reader.@Tainted Scanner.Entry this, @Tainted RawComparable key) {
          return reader.compareKeys(keyBuffer, 0, getKeyLength(), key.buffer(),
              key.offset(), key.size());
        }

        /**
         * Compare whether this and other points to the same key value.
         */
        @Override
        public @Tainted boolean equals(TFile.Reader.@Tainted Scanner.Entry this, @Tainted Object other) {
          if (this == other) return true;
          if (!(other instanceof @Tainted Entry)) return false;
          return ((@Tainted Entry) other).compareTo(keyBuffer, 0, getKeyLength()) == 0;
        }

        @Override
        public @Tainted int hashCode(TFile.Reader.@Tainted Scanner.Entry this) {
          return WritableComparator.hashBytes(keyBuffer, 0, getKeyLength());
        }
      }

      /**
       * Advance cursor by n positions within the block.
       * 
       * @param n
       *          Number of key-value pairs to skip in block.
       * @throws IOException
       */
      private void inBlockAdvance(TFile.Reader.@Tainted Scanner this, @Tainted long n) throws IOException {
        for (@Tainted long i = 0; i < n; ++i) {
          checkKey();
          if (!valueBufferInputStream.isClosed()) {
            valueBufferInputStream.close();
          }
          klen = -1;
          currentLocation.incRecordIndex();
        }
      }

      /**
       * Advance cursor in block until we find a key that is greater than or
       * equal to the input key.
       * 
       * @param key
       *          Key to compare.
       * @param greater
       *          advance until we find a key greater than the input key.
       * @return true if we find a equal key.
       * @throws IOException
       */
      private @Tainted boolean inBlockAdvance(TFile.Reader.@Tainted Scanner this, @Tainted RawComparable key, @Tainted boolean greater)
          throws IOException {
        @Tainted
        int curBid = currentLocation.getBlockIndex();
        @Tainted
        long entryInBlock = reader.getBlockEntryCount(curBid);
        if (curBid == endLocation.getBlockIndex()) {
          entryInBlock = endLocation.getRecordIndex();
        }

        while (currentLocation.getRecordIndex() < entryInBlock) {
          @Tainted
          int cmp = compareCursorKeyTo(key);
          if (cmp > 0) return false;
          if (cmp == 0 && !greater) return true;
          if (!valueBufferInputStream.isClosed()) {
            valueBufferInputStream.close();
          }
          klen = -1;
          currentLocation.incRecordIndex();
        }

        throw new @Tainted RuntimeException("Cannot find matching key in block.");
      }
    }

    @Tainted
    long getBlockEntryCount(TFile.@Tainted Reader this, @Tainted int curBid) {
      return tfileIndex.getEntry(curBid).entries();
    }

    @Tainted
    BlockReader getBlockReader(TFile.@Tainted Reader this, @Tainted int blockIndex) throws IOException {
      return readerBCF.getDataBlock(blockIndex);
    }
  }

  /**
   * Data structure representing "TFile.meta" meta block.
   */
  static final class TFileMeta {
    final static @Tainted String BLOCK_NAME = "TFile.meta";
    final @Tainted Version version;
    private @Tainted long recordCount;
    private final @Tainted String strComparator;
    private final @Tainted BytesComparator comparator;

    // ctor for writes
    public @Tainted TFileMeta(@Tainted String comparator) {
      // set fileVersion to API version when we create it.
      version = TFile.API_VERSION;
      recordCount = 0;
      strComparator = (comparator == null) ? "" : comparator;
      this.comparator = makeComparator(strComparator);
    }

    // ctor for reads
    public @Tainted TFileMeta(@Tainted DataInput in) throws IOException {
      version = new @Tainted Version(in);
      if (!version.compatibleWith(TFile.API_VERSION)) {
        throw new @Tainted RuntimeException("Incompatible TFile fileVersion.");
      }
      recordCount = Utils.readVLong(in);
      strComparator = Utils.readString(in);
      comparator = makeComparator(strComparator);
    }

    @SuppressWarnings("unchecked")
    static @Tainted BytesComparator makeComparator(@Tainted String comparator) {
      if (comparator.length() == 0) {
        // unsorted keys
        return null;
      }
      if (comparator.equals(COMPARATOR_MEMCMP)) {
        // default comparator
        return new @Tainted BytesComparator(new @Tainted MemcmpRawComparator());
      } else if (comparator.startsWith(COMPARATOR_JCLASS)) {
        @Tainted
        String compClassName =
            comparator.substring(COMPARATOR_JCLASS.length()).trim();
        try {
          @Tainted
          Class compClass = Class.forName(compClassName);
          // use its default ctor to create an instance
          return new @Tainted BytesComparator((@Tainted RawComparator<@Tainted Object>) compClass
              .newInstance());
        } catch (@Tainted Exception e) {
          throw new @Tainted IllegalArgumentException(
              "Failed to instantiate comparator: " + comparator + "("
                  + e.toString() + ")");
        }
      } else {
        throw new @Tainted IllegalArgumentException("Unsupported comparator: "
            + comparator);
      }
    }

    public void write(TFile.@Tainted TFileMeta this, @Tainted DataOutput out) throws IOException {
      TFile.API_VERSION.write(out);
      Utils.writeVLong(out, recordCount);
      Utils.writeString(out, strComparator);
    }

    public @Tainted long getRecordCount(TFile.@Tainted TFileMeta this) {
      return recordCount;
    }

    public void incRecordCount(TFile.@Tainted TFileMeta this) {
      ++recordCount;
    }

    public @Tainted boolean isSorted(TFile.@Tainted TFileMeta this) {
      return !strComparator.isEmpty();
    }

    public @Tainted String getComparatorString(TFile.@Tainted TFileMeta this) {
      return strComparator;
    }

    public @Tainted BytesComparator getComparator(TFile.@Tainted TFileMeta this) {
      return comparator;
    }

    public @Tainted Version getVersion(TFile.@Tainted TFileMeta this) {
      return version;
    }
  } // END: class MetaTFileMeta

  /**
   * Data structure representing "TFile.index" meta block.
   */
  static class TFileIndex {
    final static @Tainted String BLOCK_NAME = "TFile.index";
    private @Tainted ByteArray firstKey;
    private final @Tainted ArrayList<@Tainted TFileIndexEntry> index;
    private final @Tainted ArrayList<@Tainted Long> recordNumIndex;
    private final @Tainted BytesComparator comparator;
    private @Tainted long sum = 0;
    
    /**
     * For reading from file.
     * 
     * @throws IOException
     */
    public @Tainted TFileIndex(@Tainted int entryCount, @Tainted DataInput in, @Tainted BytesComparator comparator)
        throws IOException {
      index = new @Tainted ArrayList<@Tainted TFileIndexEntry>(entryCount);
      recordNumIndex = new @Tainted ArrayList<@Tainted Long>(entryCount);
      @Tainted
      int size = Utils.readVInt(in); // size for the first key entry.
      if (size > 0) {
        @Tainted
        byte @Tainted [] buffer = new @Tainted byte @Tainted [size];
        in.readFully(buffer);
        @Tainted
        DataInputStream firstKeyInputStream =
            new @Tainted DataInputStream(new @Tainted ByteArrayInputStream(buffer, 0, size));

        @Tainted
        int firstKeyLength = Utils.readVInt(firstKeyInputStream);
        firstKey = new @Tainted ByteArray(new @Tainted byte @Tainted [firstKeyLength]);
        firstKeyInputStream.readFully(firstKey.buffer());

        for (@Tainted int i = 0; i < entryCount; i++) {
          size = Utils.readVInt(in);
          if (buffer.length < size) {
            buffer = new @Tainted byte @Tainted [size];
          }
          in.readFully(buffer, 0, size);
          @Tainted
          TFileIndexEntry idx =
              new @Tainted TFileIndexEntry(new @Tainted DataInputStream(new @Tainted ByteArrayInputStream(
                  buffer, 0, size)));
          index.add(idx);
          sum += idx.entries();
          recordNumIndex.add(sum);
        }
      } else {
        if (entryCount != 0) {
          throw new @Tainted RuntimeException("Internal error");
        }
      }
      this.comparator = comparator;
    }

    /**
     * @param key
     *          input key.
     * @return the ID of the first block that contains key >= input key. Or -1
     *         if no such block exists.
     */
    public @Tainted int lowerBound(TFile.@Tainted TFileIndex this, @Tainted RawComparable key) {
      if (comparator == null) {
        throw new @Tainted RuntimeException("Cannot search in unsorted TFile");
      }

      if (firstKey == null) {
        return -1; // not found
      }

      @Tainted
      int ret = Utils.lowerBound(index, key, comparator);
      if (ret == index.size()) {
        return -1;
      }
      return ret;
    }

    /**
     * @param key
     *          input key.
     * @return the ID of the first block that contains key > input key. Or -1
     *         if no such block exists.
     */
    public @Tainted int upperBound(TFile.@Tainted TFileIndex this, @Tainted RawComparable key) {
      if (comparator == null) {
        throw new @Tainted RuntimeException("Cannot search in unsorted TFile");
      }

      if (firstKey == null) {
        return -1; // not found
      }

      @Tainted
      int ret = Utils.upperBound(index, key, comparator);
      if (ret == index.size()) {
        return -1;
      }
      return ret;
    }

    /**
     * For writing to file.
     */
    public @Tainted TFileIndex(@Tainted BytesComparator comparator) {
      index = new @Tainted ArrayList<@Tainted TFileIndexEntry>();
      recordNumIndex = new @Tainted ArrayList<@Tainted Long>();
      this.comparator = comparator;
    }

    public @Tainted RawComparable getFirstKey(TFile.@Tainted TFileIndex this) {
      return firstKey;
    }
    
    public Reader.@Tainted Location getLocationByRecordNum(TFile.@Tainted TFileIndex this, @Tainted long recNum) {
      @Tainted
      int idx = Utils.upperBound(recordNumIndex, recNum);
      @Tainted
      long lastRecNum = (idx == 0)? 0: recordNumIndex.get(idx-1);
      return new Reader.@Tainted Location(idx, recNum-lastRecNum);
    }

    public @Tainted long getRecordNumByLocation(TFile.@Tainted TFileIndex this, Reader.@Tainted Location location) {
      @Tainted
      int blkIndex = location.getBlockIndex();
      @Tainted
      long lastRecNum = (blkIndex == 0) ? 0: recordNumIndex.get(blkIndex-1);
      return lastRecNum + location.getRecordIndex();
    }
    
    public void setFirstKey(TFile.@Tainted TFileIndex this, @Tainted byte @Tainted [] key, @Tainted int offset, @Tainted int length) {
      firstKey = new @Tainted ByteArray(new @Tainted byte @Tainted [length]);
      System.arraycopy(key, offset, firstKey.buffer(), 0, length);
    }

    public @Tainted RawComparable getLastKey(TFile.@Tainted TFileIndex this) {
      if (index.size() == 0) {
        return null;
      }
      return new @Tainted ByteArray(index.get(index.size() - 1).buffer());
    }

    public void addEntry(TFile.@Tainted TFileIndex this, @Tainted TFileIndexEntry keyEntry) {
      index.add(keyEntry);
      sum += keyEntry.entries();
      recordNumIndex.add(sum);
    }

    public @Tainted TFileIndexEntry getEntry(TFile.@Tainted TFileIndex this, @Tainted int bid) {
      return index.get(bid);
    }

    public void write(TFile.@Tainted TFileIndex this, @Tainted DataOutput out) throws IOException {
      if (firstKey == null) {
        Utils.writeVInt(out, 0);
        return;
      }

      @Tainted
      DataOutputBuffer dob = new @Tainted DataOutputBuffer();
      Utils.writeVInt(dob, firstKey.size());
      dob.write(firstKey.buffer());
      Utils.writeVInt(out, dob.size());
      out.write(dob.getData(), 0, dob.getLength());

      for (@Tainted TFileIndexEntry entry : index) {
        dob.reset();
        entry.write(dob);
        Utils.writeVInt(out, dob.getLength());
        out.write(dob.getData(), 0, dob.getLength());
      }
    }
  }

  /**
   * TFile Data Index entry. We should try to make the memory footprint of each
   * index entry as small as possible.
   */
  static final class TFileIndexEntry implements @Tainted RawComparable {
    final @Tainted byte @Tainted [] key;
    // count of <key, value> entries in the block.
    final @Tainted long kvEntries;

    public @Tainted TFileIndexEntry(@Tainted DataInput in) throws IOException {
      @Tainted
      int len = Utils.readVInt(in);
      key = new @Tainted byte @Tainted [len];
      in.readFully(key, 0, len);
      kvEntries = Utils.readVLong(in);
    }

    // default entry, without any padding
    public @Tainted TFileIndexEntry(@Tainted byte @Tainted [] newkey, @Tainted int offset, @Tainted int len, @Tainted long entries) {
      key = new @Tainted byte @Tainted [len];
      System.arraycopy(newkey, offset, key, 0, len);
      this.kvEntries = entries;
    }

    @Override
    public @Tainted byte @Tainted [] buffer(TFile.@Tainted TFileIndexEntry this) {
      return key;
    }

    @Override
    public @Tainted int offset(TFile.@Tainted TFileIndexEntry this) {
      return 0;
    }

    @Override
    public @Tainted int size(TFile.@Tainted TFileIndexEntry this) {
      return key.length;
    }

    @Tainted
    long entries(TFile.@Tainted TFileIndexEntry this) {
      return kvEntries;
    }

    public void write(TFile.@Tainted TFileIndexEntry this, @Tainted DataOutput out) throws IOException {
      Utils.writeVInt(out, key.length);
      out.write(key, 0, key.length);
      Utils.writeVLong(out, kvEntries);
    }
  }

  /**
   * Dumping the TFile information.
   * 
   * @param args
   *          A list of TFile paths.
   */
  public static void main(@Tainted String @Tainted [] args) {
    System.out.printf("TFile Dumper (TFile %s, BCFile %s)\n", TFile.API_VERSION
        .toString(), BCFile.API_VERSION.toString());
    if (args.length == 0) {
      System.out
          .println("Usage: java ... org.apache.hadoop.io.file.tfile.TFile tfile-path [tfile-path ...]");
      System.exit(0);
    }
    @Tainted
    Configuration conf = new @Tainted Configuration();

    for (@Tainted String file : args) {
      System.out.println("===" + file + "===");
      try {
        TFileDumper.dumpInfo(file, System.out, conf);
      } catch (@Tainted IOException e) {
        e.printStackTrace(System.err);
      }
    }
  }
}
