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
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.CompareUtils.Scalar;
import org.apache.hadoop.io.file.tfile.CompareUtils.ScalarComparator;
import org.apache.hadoop.io.file.tfile.CompareUtils.ScalarLong;
import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.io.file.tfile.Utils.Version;

/**
 * Block Compressed file, the underlying physical storage layer for TFile.
 * BCFile provides the basic block level compression for the data block and meta
 * blocks. It is separated from TFile as it may be used for other
 * block-compressed file implementation.
 */
final class BCFile {
  // the current version of BCFile impl, increment them (major or minor) made
  // enough changes
  static final @Tainted Version API_VERSION = new @Tainted Version((@Tainted short) 1, (@Tainted short) 0);
  static final @Tainted Log LOG = LogFactory.getLog(BCFile.class);

  /**
   * Prevent the instantiation of BCFile objects.
   */
  private @Tainted BCFile() {
    // nothing
  }

  /**
   * BCFile writer, the entry point for creating a new BCFile.
   */
  static public class Writer implements @Tainted Closeable {
    private final @Tainted FSDataOutputStream out;
    private final @Tainted Configuration conf;
    // the single meta block containing index of compressed data blocks
    final @Tainted DataIndex dataIndex;
    // index for meta blocks
    final @Tainted MetaIndex metaIndex;
    @Tainted
    boolean blkInProgress = false;
    private @Tainted boolean metaBlkSeen = false;
    private @Tainted boolean closed = false;
    @Tainted
    long errorCount = 0;
    // reusable buffers.
    private @Tainted BytesWritable fsOutputBuffer;

    /**
     * Call-back interface to register a block after a block is closed.
     */
    private static interface BlockRegister {
      /**
       * Register a block that is fully closed.
       * 
       * @param raw
       *          The size of block in terms of uncompressed bytes.
       * @param offsetStart
       *          The start offset of the block.
       * @param offsetEnd
       *          One byte after the end of the block. Compressed block size is
       *          offsetEnd - offsetStart.
       */
      public void register(BCFile.Writer.@Tainted BlockRegister this, @Tainted long raw, @Tainted long offsetStart, @Tainted long offsetEnd);
    }

    /**
     * Intermediate class that maintain the state of a Writable Compression
     * Block.
     */
    private static final class WBlockState {
      private final @Tainted Algorithm compressAlgo;
      private @Tainted Compressor compressor; // !null only if using native
      // Hadoop compression
      private final @Tainted FSDataOutputStream fsOut;
      private final @Tainted long posStart;
      private final @Tainted SimpleBufferedOutputStream fsBufferedOutput;
      private @Tainted OutputStream out;

      /**
       * @param compressionAlgo
       *          The compression algorithm to be used to for compression.
       * @throws IOException
       */
      public @Tainted WBlockState(@Tainted Algorithm compressionAlgo, @Tainted FSDataOutputStream fsOut,
          @Tainted
          BytesWritable fsOutputBuffer, @Tainted Configuration conf) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.fsOut = fsOut;
        this.posStart = fsOut.getPos();

        fsOutputBuffer.setCapacity(TFile.getFSOutputBufferSize(conf));

        this.fsBufferedOutput =
            new @Tainted SimpleBufferedOutputStream(this.fsOut, fsOutputBuffer.getBytes());
        this.compressor = compressAlgo.getCompressor();

        try {
          this.out =
              compressionAlgo.createCompressionStream(fsBufferedOutput,
                  compressor, 0);
        } catch (@Tainted IOException e) {
          compressAlgo.returnCompressor(compressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       * 
       * @return the output stream suitable for writing block data.
       */
      @Tainted
      OutputStream getOutputStream(BCFile.Writer.@Tainted WBlockState this) {
        return out;
      }

      /**
       * Get the current position in file.
       * 
       * @return The current byte offset in underlying file.
       * @throws IOException
       */
      @Tainted
      long getCurrentPos(BCFile.Writer.@Tainted WBlockState this) throws IOException {
        return fsOut.getPos() + fsBufferedOutput.size();
      }

      @Tainted
      long getStartPos(BCFile.Writer.@Tainted WBlockState this) {
        return posStart;
      }

      /**
       * Current size of compressed data.
       * 
       * @return
       * @throws IOException
       */
      @Tainted
      long getCompressedSize(BCFile.Writer.@Tainted WBlockState this) throws IOException {
        @Tainted
        long ret = getCurrentPos() - posStart;
        return ret;
      }

      /**
       * Finishing up the current block.
       */
      public void finish(BCFile.Writer.@Tainted WBlockState this) throws IOException {
        try {
          if (out != null) {
            out.flush();
            out = null;
          }
        } finally {
          compressAlgo.returnCompressor(compressor);
          compressor = null;
        }
      }
    }

    /**
     * Access point to stuff data into a block.
     * 
     * TODO: Change DataOutputStream to something else that tracks the size as
     * long instead of int. Currently, we will wrap around if the row block size
     * is greater than 4GB.
     */
    public class BlockAppender extends @Tainted DataOutputStream {
      private final @Tainted BlockRegister blockRegister;
      private final @Tainted WBlockState wBlkState;
      @SuppressWarnings("hiding")
      private @Tainted boolean closed = false;

      /**
       * Constructor
       * 
       * @param register
       *          the block register, which is called when the block is closed.
       * @param wbs
       *          The writable compression block state.
       */
      @Tainted
      BlockAppender(@Tainted BlockRegister register, @Tainted WBlockState wbs) {
        super(wbs.getOutputStream());
        this.blockRegister = register;
        this.wBlkState = wbs;
      }

      /**
       * Get the raw size of the block.
       * 
       * @return the number of uncompressed bytes written through the
       *         BlockAppender so far.
       * @throws IOException
       */
      public @Tainted long getRawSize(BCFile.@Tainted Writer.BlockAppender this) throws IOException {
        /**
         * Expecting the size() of a block not exceeding 4GB. Assuming the
         * size() will wrap to negative integer if it exceeds 2GB.
         */
        return size() & 0x00000000ffffffffL;
      }

      /**
       * Get the compressed size of the block in progress.
       * 
       * @return the number of compressed bytes written to the underlying FS
       *         file. The size may be smaller than actual need to compress the
       *         all data written due to internal buffering inside the
       *         compressor.
       * @throws IOException
       */
      public @Tainted long getCompressedSize(BCFile.@Tainted Writer.BlockAppender this) throws IOException {
        return wBlkState.getCompressedSize();
      }

      @Override
      public void flush(BCFile.@Tainted Writer.BlockAppender this) {
        // The down stream is a special kind of stream that finishes a
        // compression block upon flush. So we disable flush() here.
      }

      /**
       * Signaling the end of write to the block. The block register will be
       * called for registering the finished block.
       */
      @Override
      public void close(BCFile.@Tainted Writer.BlockAppender this) throws IOException {
        if (closed == true) {
          return;
        }
        try {
          ++errorCount;
          wBlkState.finish();
          blockRegister.register(getRawSize(), wBlkState.getStartPos(),
              wBlkState.getCurrentPos());
          --errorCount;
        } finally {
          closed = true;
          blkInProgress = false;
        }
      }
    }

    /**
     * Constructor
     * 
     * @param fout
     *          FS output stream.
     * @param compressionName
     *          Name of the compression algorithm, which will be used for all
     *          data blocks.
     * @throws IOException
     * @see Compression#getSupportedAlgorithms
     */
    public @Tainted Writer(@Tainted FSDataOutputStream fout, @Tainted String compressionName,
        @Tainted
        Configuration conf) throws IOException {
      if (fout.getPos() != 0) {
        throw new @Tainted IOException("Output file not at zero offset.");
      }

      this.out = fout;
      this.conf = conf;
      dataIndex = new @Tainted DataIndex(compressionName);
      metaIndex = new @Tainted MetaIndex();
      fsOutputBuffer = new @Tainted BytesWritable();
      Magic.write(fout);
    }

    /**
     * Close the BCFile Writer. Attempting to use the Writer after calling
     * <code>close</code> is not allowed and may lead to undetermined results.
     */
    @Override
    public void close(BCFile.@Tainted Writer this) throws IOException {
      if (closed == true) {
        return;
      }

      try {
        if (errorCount == 0) {
          if (blkInProgress == true) {
            throw new @Tainted IllegalStateException(
                "Close() called with active block appender.");
          }

          // add metaBCFileIndex to metaIndex as the last meta block
          @Tainted
          BlockAppender appender =
              prepareMetaBlock(DataIndex.BLOCK_NAME,
                  getDefaultCompressionAlgorithm());
          try {
            dataIndex.write(appender);
          } finally {
            appender.close();
          }

          @Tainted
          long offsetIndexMeta = out.getPos();
          metaIndex.write(out);

          // Meta Index and the trailing section are written out directly.
          out.writeLong(offsetIndexMeta);

          API_VERSION.write(out);
          Magic.write(out);
          out.flush();
        }
      } finally {
        closed = true;
      }
    }

    private @Tainted Algorithm getDefaultCompressionAlgorithm(BCFile.@Tainted Writer this) {
      return dataIndex.getDefaultCompressionAlgorithm();
    }

    private @Tainted BlockAppender prepareMetaBlock(BCFile.@Tainted Writer this, @Tainted String name, @Tainted Algorithm compressAlgo)
        throws IOException, MetaBlockAlreadyExists {
      if (blkInProgress == true) {
        throw new @Tainted IllegalStateException(
            "Cannot create Meta Block until previous block is closed.");
      }

      if (metaIndex.getMetaByName(name) != null) {
        throw new @Tainted MetaBlockAlreadyExists("name=" + name);
      }

      @Tainted
      MetaBlockRegister mbr = new @Tainted MetaBlockRegister(name, compressAlgo);
      @Tainted
      WBlockState wbs =
          new @Tainted WBlockState(compressAlgo, out, fsOutputBuffer, conf);
      @Tainted
      BlockAppender ba = new @Tainted BlockAppender(mbr, wbs);
      blkInProgress = true;
      metaBlkSeen = true;
      return ba;
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the
     * block. There can only be one BlockAppender stream active at any time.
     * Regular Blocks may not be created after the first Meta Blocks. The caller
     * must call BlockAppender.close() to conclude the block creation.
     * 
     * @param name
     *          The name of the Meta Block. The name must not conflict with
     *          existing Meta Blocks.
     * @param compressionName
     *          The name of the compression algorithm to be used.
     * @return The BlockAppender stream
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     */
    public @Tainted BlockAppender prepareMetaBlock(BCFile.@Tainted Writer this, @Tainted String name, @Tainted String compressionName)
        throws IOException, MetaBlockAlreadyExists {
      return prepareMetaBlock(name, Compression
          .getCompressionAlgorithmByName(compressionName));
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the
     * block. The Meta Block will be compressed with the same compression
     * algorithm as data blocks. There can only be one BlockAppender stream
     * active at any time. Regular Blocks may not be created after the first
     * Meta Blocks. The caller must call BlockAppender.close() to conclude the
     * block creation.
     * 
     * @param name
     *          The name of the Meta Block. The name must not conflict with
     *          existing Meta Blocks.
     * @return The BlockAppender stream
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     * @throws IOException
     */
    public @Tainted BlockAppender prepareMetaBlock(BCFile.@Tainted Writer this, @Tainted String name) throws IOException,
        MetaBlockAlreadyExists {
      return prepareMetaBlock(name, getDefaultCompressionAlgorithm());
    }

    /**
     * Create a Data Block and obtain an output stream for adding data into the
     * block. There can only be one BlockAppender stream active at any time.
     * Data Blocks may not be created after the first Meta Blocks. The caller
     * must call BlockAppender.close() to conclude the block creation.
     * 
     * @return The BlockAppender stream
     * @throws IOException
     */
    public @Tainted BlockAppender prepareDataBlock(BCFile.@Tainted Writer this) throws IOException {
      if (blkInProgress == true) {
        throw new @Tainted IllegalStateException(
            "Cannot create Data Block until previous block is closed.");
      }

      if (metaBlkSeen == true) {
        throw new @Tainted IllegalStateException(
            "Cannot create Data Block after Meta Blocks.");
      }

      @Tainted
      DataBlockRegister dbr = new @Tainted DataBlockRegister();

      @Tainted
      WBlockState wbs =
          new @Tainted WBlockState(getDefaultCompressionAlgorithm(), out,
              fsOutputBuffer, conf);
      @Tainted
      BlockAppender ba = new @Tainted BlockAppender(dbr, wbs);
      blkInProgress = true;
      return ba;
    }

    /**
     * Callback to make sure a meta block is added to the internal list when its
     * stream is closed.
     */
    private class MetaBlockRegister implements @Tainted BlockRegister {
      private final @Tainted String name;
      private final @Tainted Algorithm compressAlgo;

      @Tainted
      MetaBlockRegister(@Tainted String name, @Tainted Algorithm compressAlgo) {
        this.name = name;
        this.compressAlgo = compressAlgo;
      }

      @Override
      public void register(BCFile.@Tainted Writer.MetaBlockRegister this, @Tainted long raw, @Tainted long begin, @Tainted long end) {
        metaIndex.addEntry(new @Tainted MetaIndexEntry(name, compressAlgo,
            new @Tainted BlockRegion(begin, end - begin, raw)));
      }
    }

    /**
     * Callback to make sure a data block is added to the internal list when
     * it's being closed.
     * 
     */
    private class DataBlockRegister implements @Tainted BlockRegister {
      @Tainted
      DataBlockRegister() {
        // do nothing
      }

      @Override
      public void register(BCFile.@Tainted Writer.DataBlockRegister this, @Tainted long raw, @Tainted long begin, @Tainted long end) {
        dataIndex.addBlockRegion(new @Tainted BlockRegion(begin, end - begin, raw));
      }
    }
  }

  /**
   * BCFile Reader, interface to read the file's data and meta blocks.
   */
  static public class Reader implements @Tainted Closeable {
    private final @Tainted FSDataInputStream in;
    private final @Tainted Configuration conf;
    final @Tainted DataIndex dataIndex;
    // Index for meta blocks
    final @Tainted MetaIndex metaIndex;
    final @Tainted Version version;

    /**
     * Intermediate class that maintain the state of a Readable Compression
     * Block.
     */
    static private final class RBlockState {
      private final @Tainted Algorithm compressAlgo;
      private @Tainted Decompressor decompressor;
      private final @Tainted BlockRegion region;
      private final @Tainted InputStream in;

      public @Tainted RBlockState(@Tainted Algorithm compressionAlgo, @Tainted FSDataInputStream fsin,
          @Tainted
          BlockRegion region, @Tainted Configuration conf) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.region = region;
        this.decompressor = compressionAlgo.getDecompressor();

        try {
          this.in =
              compressAlgo
                  .createDecompressionStream(new @Tainted BoundedRangeFileInputStream(
                      fsin, this.region.getOffset(), this.region
                          .getCompressedSize()), decompressor, TFile
                      .getFSInputBufferSize(conf));
        } catch (@Tainted IOException e) {
          compressAlgo.returnDecompressor(decompressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       * 
       * @return the output stream suitable for writing block data.
       */
      public @Tainted InputStream getInputStream(BCFile.Reader.@Tainted RBlockState this) {
        return in;
      }

      public @Tainted String getCompressionName(BCFile.Reader.@Tainted RBlockState this) {
        return compressAlgo.getName();
      }

      public @Tainted BlockRegion getBlockRegion(BCFile.Reader.@Tainted RBlockState this) {
        return region;
      }

      public void finish(BCFile.Reader.@Tainted RBlockState this) throws IOException {
        try {
          in.close();
        } finally {
          compressAlgo.returnDecompressor(decompressor);
          decompressor = null;
        }
      }
    }

    /**
     * Access point to read a block.
     */
    public static class BlockReader extends @Tainted DataInputStream {
      private final @Tainted RBlockState rBlkState;
      private @Tainted boolean closed = false;

      @Tainted
      BlockReader(@Tainted RBlockState rbs) {
        super(rbs.getInputStream());
        rBlkState = rbs;
      }

      /**
       * Finishing reading the block. Release all resources.
       */
      @Override
      public void close(BCFile.Reader.@Tainted BlockReader this) throws IOException {
        if (closed == true) {
          return;
        }
        try {
          // Do not set rBlkState to null. People may access stats after calling
          // close().
          rBlkState.finish();
        } finally {
          closed = true;
        }
      }

      /**
       * Get the name of the compression algorithm used to compress the block.
       * 
       * @return name of the compression algorithm.
       */
      public @Tainted String getCompressionName(BCFile.Reader.@Tainted BlockReader this) {
        return rBlkState.getCompressionName();
      }

      /**
       * Get the uncompressed size of the block.
       * 
       * @return uncompressed size of the block.
       */
      public @Tainted long getRawSize(BCFile.Reader.@Tainted BlockReader this) {
        return rBlkState.getBlockRegion().getRawSize();
      }

      /**
       * Get the compressed size of the block.
       * 
       * @return compressed size of the block.
       */
      public @Tainted long getCompressedSize(BCFile.Reader.@Tainted BlockReader this) {
        return rBlkState.getBlockRegion().getCompressedSize();
      }

      /**
       * Get the starting position of the block in the file.
       * 
       * @return the starting position of the block in the file.
       */
      public @Tainted long getStartPos(BCFile.Reader.@Tainted BlockReader this) {
        return rBlkState.getBlockRegion().getOffset();
      }
    }

    /**
     * Constructor
     * 
     * @param fin
     *          FS input stream.
     * @param fileLength
     *          Length of the corresponding file
     * @throws IOException
     */
    public @Tainted Reader(@Tainted FSDataInputStream fin, @Tainted long fileLength, @Tainted Configuration conf)
        throws IOException {
      this.in = fin;
      this.conf = conf;

      // move the cursor to the beginning of the tail, containing: offset to the
      // meta block index, version and magic
      fin.seek(fileLength - Magic.size() - Version.size() - Long.SIZE
          / Byte.SIZE);
      @Tainted
      long offsetIndexMeta = fin.readLong();
      version = new @Tainted Version(fin);
      Magic.readAndVerify(fin);

      if (!version.compatibleWith(BCFile.API_VERSION)) {
        throw new @Tainted RuntimeException("Incompatible BCFile fileBCFileVersion.");
      }

      // read meta index
      fin.seek(offsetIndexMeta);
      metaIndex = new @Tainted MetaIndex(fin);

      // read data:BCFile.index, the data block index
      @Tainted
      BlockReader blockR = getMetaBlock(DataIndex.BLOCK_NAME);
      try {
        dataIndex = new @Tainted DataIndex(blockR);
      } finally {
        blockR.close();
      }
    }

    /**
     * Get the name of the default compression algorithm.
     * 
     * @return the name of the default compression algorithm.
     */
    public @Tainted String getDefaultCompressionName(BCFile.@Tainted Reader this) {
      return dataIndex.getDefaultCompressionAlgorithm().getName();
    }

    /**
     * Get version of BCFile file being read.
     * 
     * @return version of BCFile file being read.
     */
    public @Tainted Version getBCFileVersion(BCFile.@Tainted Reader this) {
      return version;
    }

    /**
     * Get version of BCFile API.
     * 
     * @return version of BCFile API.
     */
    public @Tainted Version getAPIVersion(BCFile.@Tainted Reader this) {
      return API_VERSION;
    }

    /**
     * Finishing reading the BCFile. Release all resources.
     */
    @Override
    public void close(BCFile.@Tainted Reader this) {
      // nothing to be done now
    }

    /**
     * Get the number of data blocks.
     * 
     * @return the number of data blocks.
     */
    public @Tainted int getBlockCount(BCFile.@Tainted Reader this) {
      return dataIndex.getBlockRegionList().size();
    }

    /**
     * Stream access to a Meta Block.
     * 
     * @param name
     *          meta block name
     * @return BlockReader input stream for reading the meta block.
     * @throws IOException
     * @throws MetaBlockDoesNotExist
     *           The Meta Block with the given name does not exist.
     */
    public @Tainted BlockReader getMetaBlock(BCFile.@Tainted Reader this, @Tainted String name) throws IOException,
        MetaBlockDoesNotExist {
      @Tainted
      MetaIndexEntry imeBCIndex = metaIndex.getMetaByName(name);
      if (imeBCIndex == null) {
        throw new @Tainted MetaBlockDoesNotExist("name=" + name);
      }

      @Tainted
      BlockRegion region = imeBCIndex.getRegion();
      return createReader(imeBCIndex.getCompressionAlgorithm(), region);
    }

    /**
     * Stream access to a Data Block.
     * 
     * @param blockIndex
     *          0-based data block index.
     * @return BlockReader input stream for reading the data block.
     * @throws IOException
     */
    public @Tainted BlockReader getDataBlock(BCFile.@Tainted Reader this, @Tainted int blockIndex) throws IOException {
      if (blockIndex < 0 || blockIndex >= getBlockCount()) {
        throw new @Tainted IndexOutOfBoundsException(String.format(
            "blockIndex=%d, numBlocks=%d", blockIndex, getBlockCount()));
      }

      @Tainted
      BlockRegion region = dataIndex.getBlockRegionList().get(blockIndex);
      return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
    }

    private @Tainted BlockReader createReader(BCFile.@Tainted Reader this, @Tainted Algorithm compressAlgo, @Tainted BlockRegion region)
        throws IOException {
      @Tainted
      RBlockState rbs = new @Tainted RBlockState(compressAlgo, in, region, conf);
      return new @Tainted BlockReader(rbs);
    }

    /**
     * Find the smallest Block index whose starting offset is greater than or
     * equal to the specified offset.
     * 
     * @param offset
     *          User-specific offset.
     * @return the index to the data Block if such block exists; or -1
     *         otherwise.
     */
    public @Tainted int getBlockIndexNear(BCFile.@Tainted Reader this, @Tainted long offset) {
      @Tainted
      ArrayList<@Tainted BlockRegion> list = dataIndex.getBlockRegionList();
      @Tainted
      int idx =
          Utils
              .lowerBound(list, new @Tainted ScalarLong(offset), new @Tainted ScalarComparator());

      if (idx == list.size()) {
        return -1;
      }

      return idx;
    }
  }

  /**
   * Index for all Meta blocks.
   */
  static class MetaIndex {
    // use a tree map, for getting a meta block entry by name
    final @Tainted Map<@Tainted String, @Tainted MetaIndexEntry> index;

    // for write
    public @Tainted MetaIndex() {
      index = new @Tainted TreeMap<@Tainted String, @Tainted MetaIndexEntry>();
    }

    // for read, construct the map from the file
    public @Tainted MetaIndex(@Tainted DataInput in) throws IOException {
      @Tainted
      int count = Utils.readVInt(in);
      index = new @Tainted TreeMap<@Tainted String, @Tainted MetaIndexEntry>();

      for (@Tainted int nx = 0; nx < count; nx++) {
        @Tainted
        MetaIndexEntry indexEntry = new @Tainted MetaIndexEntry(in);
        index.put(indexEntry.getMetaName(), indexEntry);
      }
    }

    public void addEntry(BCFile.@Tainted MetaIndex this, @Tainted MetaIndexEntry indexEntry) {
      index.put(indexEntry.getMetaName(), indexEntry);
    }

    public @Tainted MetaIndexEntry getMetaByName(BCFile.@Tainted MetaIndex this, @Tainted String name) {
      return index.get(name);
    }

    public void write(BCFile.@Tainted MetaIndex this, @Tainted DataOutput out) throws IOException {
      Utils.writeVInt(out, index.size());

      for (@Tainted MetaIndexEntry indexEntry : index.values()) {
        indexEntry.write(out);
      }
    }
  }

  /**
   * An entry describes a meta block in the MetaIndex.
   */
  static final class MetaIndexEntry {
    private final @Tainted String metaName;
    private final @Tainted Algorithm compressionAlgorithm;
    private final static @Tainted String defaultPrefix = "data:";

    private final @Tainted BlockRegion region;

    public @Tainted MetaIndexEntry(@Tainted DataInput in) throws IOException {
      @Tainted
      String fullMetaName = Utils.readString(in);
      if (fullMetaName.startsWith(defaultPrefix)) {
        metaName =
            fullMetaName.substring(defaultPrefix.length(), fullMetaName
                .length());
      } else {
        throw new @Tainted IOException("Corrupted Meta region Index");
      }

      compressionAlgorithm =
          Compression.getCompressionAlgorithmByName(Utils.readString(in));
      region = new @Tainted BlockRegion(in);
    }

    public @Tainted MetaIndexEntry(@Tainted String metaName, @Tainted Algorithm compressionAlgorithm,
        @Tainted
        BlockRegion region) {
      this.metaName = metaName;
      this.compressionAlgorithm = compressionAlgorithm;
      this.region = region;
    }

    public @Tainted String getMetaName(BCFile.@Tainted MetaIndexEntry this) {
      return metaName;
    }

    public @Tainted Algorithm getCompressionAlgorithm(BCFile.@Tainted MetaIndexEntry this) {
      return compressionAlgorithm;
    }

    public @Tainted BlockRegion getRegion(BCFile.@Tainted MetaIndexEntry this) {
      return region;
    }

    public void write(BCFile.@Tainted MetaIndexEntry this, @Tainted DataOutput out) throws IOException {
      Utils.writeString(out, defaultPrefix + metaName);
      Utils.writeString(out, compressionAlgorithm.getName());

      region.write(out);
    }
  }

  /**
   * Index of all compressed data blocks.
   */
  static class DataIndex {
    final static @Tainted String BLOCK_NAME = "BCFile.index";

    private final @Tainted Algorithm defaultCompressionAlgorithm;

    // for data blocks, each entry specifies a block's offset, compressed size
    // and raw size
    private final @Tainted ArrayList<@Tainted BlockRegion> listRegions;

    // for read, deserialized from a file
    public @Tainted DataIndex(@Tainted DataInput in) throws IOException {
      defaultCompressionAlgorithm =
          Compression.getCompressionAlgorithmByName(Utils.readString(in));

      @Tainted
      int n = Utils.readVInt(in);
      listRegions = new @Tainted ArrayList<@Tainted BlockRegion>(n);

      for (@Tainted int i = 0; i < n; i++) {
        @Tainted
        BlockRegion region = new @Tainted BlockRegion(in);
        listRegions.add(region);
      }
    }

    // for write
    public @Tainted DataIndex(@Tainted String defaultCompressionAlgorithmName) {
      this.defaultCompressionAlgorithm =
          Compression
              .getCompressionAlgorithmByName(defaultCompressionAlgorithmName);
      listRegions = new @Tainted ArrayList<@Tainted BlockRegion>();
    }

    public @Tainted Algorithm getDefaultCompressionAlgorithm(BCFile.@Tainted DataIndex this) {
      return defaultCompressionAlgorithm;
    }

    public @Tainted ArrayList<@Tainted BlockRegion> getBlockRegionList(BCFile.@Tainted DataIndex this) {
      return listRegions;
    }

    public void addBlockRegion(BCFile.@Tainted DataIndex this, @Tainted BlockRegion region) {
      listRegions.add(region);
    }

    public void write(BCFile.@Tainted DataIndex this, @Tainted DataOutput out) throws IOException {
      Utils.writeString(out, defaultCompressionAlgorithm.getName());

      Utils.writeVInt(out, listRegions.size());

      for (@Tainted BlockRegion region : listRegions) {
        region.write(out);
      }
    }
  }

  /**
   * Magic number uniquely identifying a BCFile in the header/footer.
   */
  static final class Magic {
    private final static @Tainted byte @Tainted [] AB_MAGIC_BCFILE =
        new byte @Tainted [] {
            // ... total of 16 bytes
            (@Tainted byte) 0xd1, (@Tainted byte) 0x11, (@Tainted byte) 0xd3, (@Tainted byte) 0x68, (@Tainted byte) 0x91,
            (@Tainted byte) 0xb5, (@Tainted byte) 0xd7, (@Tainted byte) 0xb6, (@Tainted byte) 0x39, (@Tainted byte) 0xdf,
            (@Tainted byte) 0x41, (@Tainted byte) 0x40, (@Tainted byte) 0x92, (@Tainted byte) 0xba, (@Tainted byte) 0xe1,
            (@Tainted byte) 0x50 };

    public static void readAndVerify(@Tainted DataInput in) throws IOException {
      @Tainted
      byte @Tainted [] abMagic = new @Tainted byte @Tainted [size()];
      in.readFully(abMagic);

      // check against AB_MAGIC_BCFILE, if not matching, throw an
      // Exception
      if (!Arrays.equals(abMagic, AB_MAGIC_BCFILE)) {
        throw new @Tainted IOException("Not a valid BCFile.");
      }
    }

    public static void write(@Tainted DataOutput out) throws IOException {
      out.write(AB_MAGIC_BCFILE);
    }

    public static @Tainted int size() {
      return AB_MAGIC_BCFILE.length;
    }
  }

  /**
   * Block region.
   */
  static final class BlockRegion implements @Tainted Scalar {
    private final @Tainted long offset;
    private final @Tainted long compressedSize;
    private final @Tainted long rawSize;

    public @Tainted BlockRegion(@Tainted DataInput in) throws IOException {
      offset = Utils.readVLong(in);
      compressedSize = Utils.readVLong(in);
      rawSize = Utils.readVLong(in);
    }

    public @Tainted BlockRegion(@Tainted long offset, @Tainted long compressedSize, @Tainted long rawSize) {
      this.offset = offset;
      this.compressedSize = compressedSize;
      this.rawSize = rawSize;
    }

    public void write(BCFile.@Tainted BlockRegion this, @Tainted DataOutput out) throws IOException {
      Utils.writeVLong(out, offset);
      Utils.writeVLong(out, compressedSize);
      Utils.writeVLong(out, rawSize);
    }

    public @Tainted long getOffset(BCFile.@Tainted BlockRegion this) {
      return offset;
    }

    public @Tainted long getCompressedSize(BCFile.@Tainted BlockRegion this) {
      return compressedSize;
    }

    public @Tainted long getRawSize(BCFile.@Tainted BlockRegion this) {
      return rawSize;
    }

    @Override
    public @Tainted long magnitude(BCFile.@Tainted BlockRegion this) {
      return offset;
    }
  }
}
