/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/*
 * This package is based on the work done by Keiron Liddle, Aftex Software
 * <keiron@aftexsw.com> to whom the Ant project is very grateful for his
 * great code.
 */

package org.apache.hadoop.io.compress.bzip2;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.OutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IOUtils;

/**
 * An output stream that compresses into the BZip2 format (without the file
 * header chars) into another stream.
 *
 * <p>
 * The compression requires large amounts of memory. Thus you should call the
 * {@link #close() close()} method as soon as possible, to force
 * <tt>CBZip2OutputStream</tt> to release the allocated memory.
 * </p>
 *
 * <p>
 * You can shrink the amount of allocated memory and maybe raise the compression
 * speed by choosing a lower blocksize, which in turn may cause a lower
 * compression ratio. You can avoid unnecessary memory allocation by avoiding
 * using a blocksize which is bigger than the size of the input.
 * </p>
 *
 * <p>
 * You can compute the memory usage for compressing by the following formula:
 * </p>
 *
 * <pre>
 * &lt;code&gt;400k + (9 * blocksize)&lt;/code&gt;.
 * </pre>
 *
 * <p>
 * To get the memory required for decompression by {@link CBZip2InputStream
 * CBZip2InputStream} use
 * </p>
 *
 * <pre>
 * &lt;code&gt;65k + (5 * blocksize)&lt;/code&gt;.
 * </pre>
 *
 * <table width="100%" border="1">
 * <colgroup> <col width="33%" /> <col width="33%" /> <col width="33%" />
 * </colgroup>
 * <tr>
 * <th colspan="3">Memory usage by blocksize</th>
 * </tr>
 * <tr>
 * <th align="right">Blocksize</th> <th align="right">Compression<br>
 * memory usage</th> <th align="right">Decompression<br>
 * memory usage</th>
 * </tr>
 * <tr>
 * <td align="right">100k</td>
 * <td align="right">1300k</td>
 * <td align="right">565k</td>
 * </tr>
 * <tr>
 * <td align="right">200k</td>
 * <td align="right">2200k</td>
 * <td align="right">1065k</td>
 * </tr>
 * <tr>
 * <td align="right">300k</td>
 * <td align="right">3100k</td>
 * <td align="right">1565k</td>
 * </tr>
 * <tr>
 * <td align="right">400k</td>
 * <td align="right">4000k</td>
 * <td align="right">2065k</td>
 * </tr>
 * <tr>
 * <td align="right">500k</td>
 * <td align="right">4900k</td>
 * <td align="right">2565k</td>
 * </tr>
 * <tr>
 * <td align="right">600k</td>
 * <td align="right">5800k</td>
 * <td align="right">3065k</td>
 * </tr>
 * <tr>
 * <td align="right">700k</td>
 * <td align="right">6700k</td>
 * <td align="right">3565k</td>
 * </tr>
 * <tr>
 * <td align="right">800k</td>
 * <td align="right">7600k</td>
 * <td align="right">4065k</td>
 * </tr>
 * <tr>
 * <td align="right">900k</td>
 * <td align="right">8500k</td>
 * <td align="right">4565k</td>
 * </tr>
 * </table>
 *
 * <p>
 * For decompression <tt>CBZip2InputStream</tt> allocates less memory if the
 * bzipped input is smaller than one block.
 * </p>
 *
 * <p>
 * Instances of this class are not threadsafe.
 * </p>
 *
 * <p>
 * TODO: Update to BZip2 1.0.1
 * </p>
 *
 */
public class CBZip2OutputStream extends @Tainted OutputStream implements @Tainted BZip2Constants {

  /**
  * The minimum supported blocksize <tt> == 1</tt>.
  */
  public static final @Tainted int MIN_BLOCKSIZE = 1;

  /**
  * The maximum supported blocksize <tt> == 9</tt>.
  */
  public static final @Tainted int MAX_BLOCKSIZE = 9;

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  */
  protected static final @Tainted int SETMASK = (1 << 21);

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  */
  protected static final @Tainted int CLEARMASK = (~SETMASK);

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  */
  protected static final @Tainted int GREATER_ICOST = 15;

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  */
  protected static final @Tainted int LESSER_ICOST = 0;

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  */
  protected static final @Tainted int SMALL_THRESH = 20;

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  */
  protected static final @Tainted int DEPTH_THRESH = 10;

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  */
  protected static final @Tainted int WORK_FACTOR = 30;

  /**
  * This constant is accessible by subclasses for historical purposes. If you
  * don't know what it means then you don't need it.
  * <p>
  * If you are ever unlucky/improbable enough to get a stack overflow whilst
  * sorting, increase the following constant and try again. In practice I
  * have never seen the stack go above 27 elems, so the following limit seems
  * very generous.
  * </p>
  */
  protected static final @Tainted int QSORT_STACK_SIZE = 1000;

  /**
  * Knuth's increments seem to work better than Incerpi-Sedgewick here.
  * Possibly because the number of elems to sort is usually small, typically
  * &lt;= 20.
  */
  private static final @Tainted int @Tainted [] INCS = new int @Tainted [] { 1, 4, 13, 40, 121, 364, 1093, 3280,
      9841, 29524, 88573, 265720, 797161, 2391484 };

  /**
  * This method is accessible by subclasses for historical purposes. If you
  * don't know what it does then you don't need it.
  */
  protected static void hbMakeCodeLengths(@Tainted char @Tainted [] len, @Tainted int @Tainted [] freq,
      @Tainted
      int alphaSize, @Tainted int maxLen) {
    /*
    * Nodes and heap entries run from 1. Entry 0 for both the heap and
    * nodes is a sentinel.
    */
    final @Tainted int @Tainted [] heap = new @Tainted int @Tainted [MAX_ALPHA_SIZE * 2];
    final @Tainted int @Tainted [] weight = new @Tainted int @Tainted [MAX_ALPHA_SIZE * 2];
    final @Tainted int @Tainted [] parent = new @Tainted int @Tainted [MAX_ALPHA_SIZE * 2];

    for (@Tainted int i = alphaSize; --i >= 0;) {
      weight[i + 1] = (freq[i] == 0 ? 1 : freq[i]) << 8;
    }

    for (@Tainted boolean tooLong = true; tooLong;) {
      tooLong = false;

      @Tainted
      int nNodes = alphaSize;
      @Untainted
      int nHeap = 0;
      heap[0] = 0;
      weight[0] = 0;
      parent[0] = -2;

      for (@Tainted int i = 1; i <= alphaSize; i++) {
        parent[i] = -1;
        nHeap++;
        heap[nHeap] = i;

        @Tainted
        int zz = nHeap;
        @Tainted
        int tmp = heap[zz];
        while (weight[tmp] < weight[heap[zz >> 1]]) {
          heap[zz] = heap[zz >> 1];
          zz >>= 1;
        }
        heap[zz] = tmp;
      }

      // assert (nHeap < (MAX_ALPHA_SIZE + 2)) : nHeap;

      while (nHeap > 1) {
        @Tainted
        int n1 = heap[1];
        heap[1] = heap[nHeap];
        nHeap--;

        @Tainted
        int yy = 0;
        @Tainted
        int zz = 1;
        @Tainted
        int tmp = heap[1];

        while (true) {
          yy = zz << 1;

          if (yy > nHeap) {
            break;
          }

          if ((yy < nHeap)
              && (weight[heap[yy + 1]] < weight[heap[yy]])) {
            yy++;
          }

          if (weight[tmp] < weight[heap[yy]]) {
            break;
          }

          heap[zz] = heap[yy];
          zz = yy;
        }

        heap[zz] = tmp;

        @Tainted
        int n2 = heap[1];
        heap[1] = heap[nHeap];
        nHeap--;

        yy = 0;
        zz = 1;
        tmp = heap[1];

        while (true) {
          yy = zz << 1;

          if (yy > nHeap) {
            break;
          }

          if ((yy < nHeap)
              && (weight[heap[yy + 1]] < weight[heap[yy]])) {
            yy++;
          }

          if (weight[tmp] < weight[heap[yy]]) {
            break;
          }

          heap[zz] = heap[yy];
          zz = yy;
        }

        heap[zz] = tmp;
        nNodes++;
        parent[n1] = parent[n2] = nNodes;

        final @Tainted int weight_n1 = weight[n1];
        final @Tainted int weight_n2 = weight[n2];
        weight[nNodes] = (((weight_n1 & 0xffffff00) + (weight_n2 & 0xffffff00)) | (1 + (((weight_n1 & 0x000000ff) > (weight_n2 & 0x000000ff)) ? (weight_n1 & 0x000000ff)
            : (weight_n2 & 0x000000ff))));

        parent[nNodes] = -1;
        nHeap++;
        heap[nHeap] = nNodes;

        tmp = 0;
        zz = nHeap;
        tmp = heap[zz];
        final @Tainted int weight_tmp = weight[tmp];
        while (weight_tmp < weight[heap[zz >> 1]]) {
          heap[zz] = heap[zz >> 1];
          zz >>= 1;
        }
        heap[zz] = tmp;

      }

      // assert (nNodes < (MAX_ALPHA_SIZE * 2)) : nNodes;

      for (@Tainted int i = 1; i <= alphaSize; i++) {
        @Tainted
        int j = 0;
        @Tainted
        int k = i;

        for (@Tainted int parent_k; (parent_k = parent[k]) >= 0;) {
          k = parent_k;
          j++;
        }

        len[i - 1] = (@Tainted char) j;
        if (j > maxLen) {
          tooLong = true;
        }
      }

      if (tooLong) {
        for (@Tainted int i = 1; i < alphaSize; i++) {
          @Tainted
          int j = weight[i] >> 8;
          j = 1 + (j >> 1);
          weight[i] = j << 8;
        }
      }
    }
  }

  private static void hbMakeCodeLengths(final @Tainted byte @Tainted [] len, final @Tainted int @Tainted [] freq,
      final @Tainted Data dat, final @Tainted int alphaSize, final @Tainted int maxLen) {
    /*
    * Nodes and heap entries run from 1. Entry 0 for both the heap and
    * nodes is a sentinel.
    */
    final @Tainted int @Tainted [] heap = dat.heap;
    final @Tainted int @Tainted [] weight = dat.weight;
    final @Tainted int @Tainted [] parent = dat.parent;

    for (@Tainted int i = alphaSize; --i >= 0;) {
      weight[i + 1] = (freq[i] == 0 ? 1 : freq[i]) << 8;
    }

    for (@Tainted boolean tooLong = true; tooLong;) {
      tooLong = false;

      @Tainted
      int nNodes = alphaSize;
      @Untainted
      int nHeap = 0;
      heap[0] = 0;
      weight[0] = 0;
      parent[0] = -2;

      for (@Tainted int i = 1; i <= alphaSize; i++) {
        parent[i] = -1;
        nHeap++;
        heap[nHeap] = i;

        @Tainted
        int zz = nHeap;
        @Tainted
        int tmp = heap[zz];
        while (weight[tmp] < weight[heap[zz >> 1]]) {
          heap[zz] = heap[zz >> 1];
          zz >>= 1;
        }
        heap[zz] = tmp;
      }

      while (nHeap > 1) {
        @Tainted
        int n1 = heap[1];
        heap[1] = heap[nHeap];
        nHeap--;

        @Tainted
        int yy = 0;
        @Tainted
        int zz = 1;
        @Tainted
        int tmp = heap[1];

        while (true) {
          yy = zz << 1;

          if (yy > nHeap) {
            break;
          }

          if ((yy < nHeap)
              && (weight[heap[yy + 1]] < weight[heap[yy]])) {
            yy++;
          }

          if (weight[tmp] < weight[heap[yy]]) {
            break;
          }

          heap[zz] = heap[yy];
          zz = yy;
        }

        heap[zz] = tmp;

        @Tainted
        int n2 = heap[1];
        heap[1] = heap[nHeap];
        nHeap--;

        yy = 0;
        zz = 1;
        tmp = heap[1];

        while (true) {
          yy = zz << 1;

          if (yy > nHeap) {
            break;
          }

          if ((yy < nHeap)
              && (weight[heap[yy + 1]] < weight[heap[yy]])) {
            yy++;
          }

          if (weight[tmp] < weight[heap[yy]]) {
            break;
          }

          heap[zz] = heap[yy];
          zz = yy;
        }

        heap[zz] = tmp;
        nNodes++;
        parent[n1] = parent[n2] = nNodes;

        final @Tainted int weight_n1 = weight[n1];
        final @Tainted int weight_n2 = weight[n2];
        weight[nNodes] = ((weight_n1 & 0xffffff00) + (weight_n2 & 0xffffff00))
            | (1 + (((weight_n1 & 0x000000ff) > (weight_n2 & 0x000000ff)) ? (weight_n1 & 0x000000ff)
                : (weight_n2 & 0x000000ff)));

        parent[nNodes] = -1;
        nHeap++;
        heap[nHeap] = nNodes;

        tmp = 0;
        zz = nHeap;
        tmp = heap[zz];
        final @Tainted int weight_tmp = weight[tmp];
        while (weight_tmp < weight[heap[zz >> 1]]) {
          heap[zz] = heap[zz >> 1];
          zz >>= 1;
        }
        heap[zz] = tmp;

      }

      for (@Tainted int i = 1; i <= alphaSize; i++) {
        @Tainted
        int j = 0;
        @Tainted
        int k = i;

        for (@Tainted int parent_k; (parent_k = parent[k]) >= 0;) {
          k = parent_k;
          j++;
        }

        len[i - 1] = (@Tainted byte) j;
        if (j > maxLen) {
          tooLong = true;
        }
      }

      if (tooLong) {
        for (@Tainted int i = 1; i < alphaSize; i++) {
          @Tainted
          int j = weight[i] >> 8;
          j = 1 + (j >> 1);
          weight[i] = j << 8;
        }
      }
    }
  }

  /**
  * Index of the last char in the block, so the block size == last + 1.
  */
  private @Tainted int last;

  /**
  * Index in fmap[] of original string after sorting.
  */
  private @Tainted int origPtr;

  /**
  * Always: in the range 0 .. 9. The current block size is 100000 * this
  * number.
  */
  private final @Tainted int blockSize100k;

  private @Tainted boolean blockRandomised;

  private @Tainted int bsBuff;
  private @Tainted int bsLive;
  private final @Tainted CRC crc = new @Tainted CRC();

  private @Tainted int nInUse;

  private @Tainted int nMTF;

  /*
  * Used when sorting. If too many long comparisons happen, we stop sorting,
  * randomise the block slightly, and try again.
  */
  private @Tainted int workDone;
  private @Tainted int workLimit;
  private @Tainted boolean firstAttempt;

  private @Tainted int currentChar = -1;
  private @Tainted int runLength = 0;

  private @Tainted int blockCRC;
  private @Tainted int combinedCRC;
  private @Tainted int allowableBlockSize;

  /**
  * All memory intensive stuff.
  */
  private CBZip2OutputStream.@Tainted Data data;

  private @Tainted OutputStream out;

  /**
  * Chooses a blocksize based on the given length of the data to compress.
  *
  * @return The blocksize, between {@link #MIN_BLOCKSIZE} and
  *         {@link #MAX_BLOCKSIZE} both inclusive. For a negative
  *         <tt>inputLength</tt> this method returns <tt>MAX_BLOCKSIZE</tt>
  *         always.
  *
  * @param inputLength
  *            The length of the data which will be compressed by
  *            <tt>CBZip2OutputStream</tt>.
  */
  public static @Tainted int chooseBlockSize(@Tainted long inputLength) {
    return (inputLength > 0) ? (@Tainted int) Math
        .min((inputLength / 132000) + 1, 9) : MAX_BLOCKSIZE;
  }

  /**
  * Constructs a new <tt>CBZip2OutputStream</tt> with a blocksize of 900k.
  *
  * <p>
  * <b>Attention: </b>The caller is resonsible to write the two BZip2 magic
  * bytes <tt>"BZ"</tt> to the specified stream prior to calling this
  * constructor.
  * </p>
  *
  * @param out *
  *            the destination stream.
  *
  * @throws IOException
  *             if an I/O error occurs in the specified stream.
  * @throws NullPointerException
  *             if <code>out == null</code>.
  */
  public @Tainted CBZip2OutputStream(final @Tainted OutputStream out) throws IOException {
    this(out, MAX_BLOCKSIZE);
  }

  /**
  * Constructs a new <tt>CBZip2OutputStream</tt> with specified blocksize.
  *
  * <p>
  * <b>Attention: </b>The caller is resonsible to write the two BZip2 magic
  * bytes <tt>"BZ"</tt> to the specified stream prior to calling this
  * constructor.
  * </p>
  *
  *
  * @param out
  *            the destination stream.
  * @param blockSize
  *            the blockSize as 100k units.
  *
  * @throws IOException
  *             if an I/O error occurs in the specified stream.
  * @throws IllegalArgumentException
  *             if <code>(blockSize < 1) || (blockSize > 9)</code>.
  * @throws NullPointerException
  *             if <code>out == null</code>.
  *
  * @see #MIN_BLOCKSIZE
  * @see #MAX_BLOCKSIZE
  */
  public @Tainted CBZip2OutputStream(final @Tainted OutputStream out, final @Tainted int blockSize)
      throws IOException {
    super();

    if (blockSize < 1) {
      throw new @Tainted IllegalArgumentException("blockSize(" + blockSize
          + ") < 1");
    }
    if (blockSize > 9) {
      throw new @Tainted IllegalArgumentException("blockSize(" + blockSize
          + ") > 9");
    }

    this.blockSize100k = blockSize;
    this.out = out;
    init();
  }

  @Override
  public void write(@Tainted CBZip2OutputStream this, final @Tainted int b) throws IOException {
    if (this.out != null) {
      write0(b);
    } else {
      throw new @Tainted IOException("closed");
    }
  }

  private void writeRun(@Tainted CBZip2OutputStream this) throws IOException {
    final @Tainted int lastShadow = this.last;

    if (lastShadow < this.allowableBlockSize) {
      final @Tainted int currentCharShadow = this.currentChar;
      final @Tainted Data dataShadow = this.data;
      dataShadow.inUse[currentCharShadow] = true;
      final @Tainted byte ch = (@Tainted byte) currentCharShadow;

      @Tainted
      int runLengthShadow = this.runLength;
      this.crc.updateCRC(currentCharShadow, runLengthShadow);

      switch (runLengthShadow) {
      case 1:
        dataShadow.block[lastShadow + 2] = ch;
        this.last = lastShadow + 1;
        break;

      case 2:
        dataShadow.block[lastShadow + 2] = ch;
        dataShadow.block[lastShadow + 3] = ch;
        this.last = lastShadow + 2;
        break;

      case 3: {
        final @Tainted byte @Tainted [] block = dataShadow.block;
        block[lastShadow + 2] = ch;
        block[lastShadow + 3] = ch;
        block[lastShadow + 4] = ch;
        this.last = lastShadow + 3;
      }
        break;

      default: {
        runLengthShadow -= 4;
        dataShadow.inUse[runLengthShadow] = true;
        final @Tainted byte @Tainted [] block = dataShadow.block;
        block[lastShadow + 2] = ch;
        block[lastShadow + 3] = ch;
        block[lastShadow + 4] = ch;
        block[lastShadow + 5] = ch;
        block[lastShadow + 6] = (@Tainted byte) runLengthShadow;
        this.last = lastShadow + 5;
      }
        break;

      }
    } else {
      endBlock();
      initBlock();
      writeRun();
    }
  }

  /**
  * Overriden to close the stream.
  */
  @Override
  protected void finalize(@Tainted CBZip2OutputStream this) throws Throwable {
    finish();
    super.finalize();
  }

  
  public void finish(@Tainted CBZip2OutputStream this) throws IOException {
    if (out != null) {
      try {
        if (this.runLength > 0) {
          writeRun();
        }
        this.currentChar = -1;
        endBlock();
        endCompression();
      } finally {
        this.out = null;
        this.data = null;
      }
    }
  }

  @Override
  public void close(@Tainted CBZip2OutputStream this) throws IOException {
    if (out != null) {
      @Tainted
      OutputStream outShadow = this.out;
      try {
        finish();
        outShadow.close();
        outShadow = null;
      } finally {
        IOUtils.closeStream(outShadow);
      }
    }
  }
  
  @Override
  public void flush(@Tainted CBZip2OutputStream this) throws IOException {
    @Tainted
    OutputStream outShadow = this.out;
    if (outShadow != null) {
      outShadow.flush();
    }
  }

  private void init(@Tainted CBZip2OutputStream this) throws IOException {
    // write magic: done by caller who created this stream
    // this.out.write('B');
    // this.out.write('Z');

    this.data = new @Tainted Data(this.blockSize100k);

    /*
    * Write `magic' bytes h indicating file-format == huffmanised, followed
    * by a digit indicating blockSize100k.
    */
    bsPutUByte('h');
    bsPutUByte('0' + this.blockSize100k);

    this.combinedCRC = 0;
    initBlock();
  }

  private void initBlock(@Tainted CBZip2OutputStream this) {
    // blockNo++;
    this.crc.initialiseCRC();
    this.last = -1;
    // ch = 0;

    @Tainted
    boolean @Tainted [] inUse = this.data.inUse;
    for (@Tainted int i = 256; --i >= 0;) {
      inUse[i] = false;
    }

    /* 20 is just a paranoia constant */
    this.allowableBlockSize = (this.blockSize100k * BZip2Constants.baseBlockSize) - 20;
  }

  private void endBlock(@Tainted CBZip2OutputStream this) throws IOException {
    this.blockCRC = this.crc.getFinalCRC();
    this.combinedCRC = (this.combinedCRC << 1) | (this.combinedCRC >>> 31);
    this.combinedCRC ^= this.blockCRC;

    // empty block at end of file
    if (this.last == -1) {
      return;
    }

    /* sort the block and establish posn of original string */
    blockSort();

    /*
    * A 6-byte block header, the value chosen arbitrarily as 0x314159265359
    * :-). A 32 bit value does not really give a strong enough guarantee
    * that the value will not appear by chance in the compressed
    * datastream. Worst-case probability of this event, for a 900k block,
    * is about 2.0e-3 for 32 bits, 1.0e-5 for 40 bits and 4.0e-8 for 48
    * bits. For a compressed file of size 100Gb -- about 100000 blocks --
    * only a 48-bit marker will do. NB: normal compression/ decompression
    * donot rely on these statistical properties. They are only important
    * when trying to recover blocks from damaged files.
    */
    bsPutUByte(0x31);
    bsPutUByte(0x41);
    bsPutUByte(0x59);
    bsPutUByte(0x26);
    bsPutUByte(0x53);
    bsPutUByte(0x59);

    /* Now the block's CRC, so it is in a known place. */
    bsPutInt(this.blockCRC);

    /* Now a single bit indicating randomisation. */
    if (this.blockRandomised) {
      bsW(1, 1);
    } else {
      bsW(1, 0);
    }

    /* Finally, block's contents proper. */
    moveToFrontCodeAndSend();
  }

  private void endCompression(@Tainted CBZip2OutputStream this) throws IOException {
    /*
    * Now another magic 48-bit number, 0x177245385090, to indicate the end
    * of the last block. (sqrt(pi), if you want to know. I did want to use
    * e, but it contains too much repetition -- 27 18 28 18 28 46 -- for me
    * to feel statistically comfortable. Call me paranoid.)
    */
    bsPutUByte(0x17);
    bsPutUByte(0x72);
    bsPutUByte(0x45);
    bsPutUByte(0x38);
    bsPutUByte(0x50);
    bsPutUByte(0x90);

    bsPutInt(this.combinedCRC);
    bsFinishedWithStream();
  }

  /**
  * Returns the blocksize parameter specified at construction time.
  */
  public final @Tainted int getBlockSize(@Tainted CBZip2OutputStream this) {
    return this.blockSize100k;
  }

  @Override
  public void write(@Tainted CBZip2OutputStream this, final @Tainted byte @Tainted [] buf, @Tainted int offs, final @Tainted int len)
      throws IOException {
    if (offs < 0) {
      throw new @Tainted IndexOutOfBoundsException("offs(" + offs + ") < 0.");
    }
    if (len < 0) {
      throw new @Tainted IndexOutOfBoundsException("len(" + len + ") < 0.");
    }
    if (offs + len > buf.length) {
      throw new @Tainted IndexOutOfBoundsException("offs(" + offs + ") + len("
          + len + ") > buf.length(" + buf.length + ").");
    }
    if (this.out == null) {
      throw new @Tainted IOException("stream closed");
    }

    for (@Tainted int hi = offs + len; offs < hi;) {
      write0(buf[offs++]);
    }
  }

  private void write0(@Tainted CBZip2OutputStream this, @Tainted int b) throws IOException {
    if (this.currentChar != -1) {
      b &= 0xff;
      if (this.currentChar == b) {
        if (++this.runLength > 254) {
          writeRun();
          this.currentChar = -1;
          this.runLength = 0;
        }
        // else nothing to do
      } else {
        writeRun();
        this.runLength = 1;
        this.currentChar = b;
      }
    } else {
      this.currentChar = b & 0xff;
      this.runLength++;
    }
  }

  private static void hbAssignCodes(final @Tainted int @Tainted [] code, final @Tainted byte @Tainted [] length,
      final @Tainted int minLen, final @Tainted int maxLen, final @Tainted int alphaSize) {
    @Tainted
    int vec = 0;
    for (@Tainted int n = minLen; n <= maxLen; n++) {
      for (@Tainted int i = 0; i < alphaSize; i++) {
        if ((length[i] & 0xff) == n) {
          code[i] = vec;
          vec++;
        }
      }
      vec <<= 1;
    }
  }

  private void bsFinishedWithStream(@Tainted CBZip2OutputStream this) throws IOException {
    while (this.bsLive > 0) {
      @Tainted
      int ch = this.bsBuff >> 24;
      this.out.write(ch); // write 8-bit
      this.bsBuff <<= 8;
      this.bsLive -= 8;
    }
  }

  private void bsW(@Tainted CBZip2OutputStream this, final @Tainted int n, final @Tainted int v) throws IOException {
    final @Tainted OutputStream outShadow = this.out;
    @Tainted
    int bsLiveShadow = this.bsLive;
    @Tainted
    int bsBuffShadow = this.bsBuff;

    while (bsLiveShadow >= 8) {
      outShadow.write(bsBuffShadow >> 24); // write 8-bit
      bsBuffShadow <<= 8;
      bsLiveShadow -= 8;
    }

    this.bsBuff = bsBuffShadow | (v << (32 - bsLiveShadow - n));
    this.bsLive = bsLiveShadow + n;
  }

  private void bsPutUByte(@Tainted CBZip2OutputStream this, final @Tainted int c) throws IOException {
    bsW(8, c);
  }

  private void bsPutInt(@Tainted CBZip2OutputStream this, final @Tainted int u) throws IOException {
    bsW(8, (u >> 24) & 0xff);
    bsW(8, (u >> 16) & 0xff);
    bsW(8, (u >> 8) & 0xff);
    bsW(8, u & 0xff);
  }

  private void sendMTFValues(@Tainted CBZip2OutputStream this) throws IOException {
    final @Tainted byte @Tainted [] @Tainted [] len = this.data.sendMTFValues_len;
    final @Tainted int alphaSize = this.nInUse + 2;

    for (@Tainted int t = N_GROUPS; --t >= 0;) {
      @Tainted
      byte @Tainted [] len_t = len[t];
      for (@Tainted int v = alphaSize; --v >= 0;) {
        len_t[v] = GREATER_ICOST;
      }
    }

    /* Decide how many coding tables to use */
    // assert (this.nMTF > 0) : this.nMTF;
    final @Tainted int nGroups = (this.nMTF < 200) ? 2 : (this.nMTF < 600) ? 3
        : (this.nMTF < 1200) ? 4 : (this.nMTF < 2400) ? 5 : 6;

    /* Generate an initial set of coding tables */
    sendMTFValues0(nGroups, alphaSize);

    /*
    * Iterate up to N_ITERS times to improve the tables.
    */
    final @Tainted int nSelectors = sendMTFValues1(nGroups, alphaSize);

    /* Compute MTF values for the selectors. */
    sendMTFValues2(nGroups, nSelectors);

    /* Assign actual codes for the tables. */
    sendMTFValues3(nGroups, alphaSize);

    /* Transmit the mapping table. */
    sendMTFValues4();

    /* Now the selectors. */
    sendMTFValues5(nGroups, nSelectors);

    /* Now the coding tables. */
    sendMTFValues6(nGroups, alphaSize);

    /* And finally, the block data proper */
    sendMTFValues7(nSelectors);
  }

  private void sendMTFValues0(@Tainted CBZip2OutputStream this, final @Tainted int nGroups, final @Tainted int alphaSize) {
    final @Tainted byte @Tainted [] @Tainted [] len = this.data.sendMTFValues_len;
    final @Tainted int @Tainted [] mtfFreq = this.data.mtfFreq;

    @Tainted
    int remF = this.nMTF;
    @Tainted
    int gs = 0;

    for (@Tainted int nPart = nGroups; nPart > 0; nPart--) {
      final @Tainted int tFreq = remF / nPart;
      @Tainted
      int ge = gs - 1;
      @Tainted
      int aFreq = 0;

      for (final @Tainted int a = alphaSize - 1; (aFreq < tFreq) && (ge < a);) {
        aFreq += mtfFreq[++ge];
      }

      if ((ge > gs) && (nPart != nGroups) && (nPart != 1)
          && (((nGroups - nPart) & 1) != 0)) {
        aFreq -= mtfFreq[ge--];
      }

      final @Tainted byte @Tainted [] len_np = len[nPart - 1];
      for (@Tainted int v = alphaSize; --v >= 0;) {
        if ((v >= gs) && (v <= ge)) {
          len_np[v] = LESSER_ICOST;
        } else {
          len_np[v] = GREATER_ICOST;
        }
      }

      gs = ge + 1;
      remF -= aFreq;
    }
  }

  private @Tainted int sendMTFValues1(@Tainted CBZip2OutputStream this, final @Tainted int nGroups, final @Tainted int alphaSize) {
    final @Tainted Data dataShadow = this.data;
    final @Tainted int @Tainted [] @Tainted [] rfreq = dataShadow.sendMTFValues_rfreq;
    final @Tainted int @Tainted [] fave = dataShadow.sendMTFValues_fave;
    final @Tainted short @Tainted [] cost = dataShadow.sendMTFValues_cost;
    final @Tainted char @Tainted [] sfmap = dataShadow.sfmap;
    final @Tainted byte @Tainted [] selector = dataShadow.selector;
    final @Tainted byte @Tainted [] @Tainted [] len = dataShadow.sendMTFValues_len;
    final @Tainted byte @Tainted [] len_0 = len[0];
    final @Tainted byte @Tainted [] len_1 = len[1];
    final @Tainted byte @Tainted [] len_2 = len[2];
    final @Tainted byte @Tainted [] len_3 = len[3];
    final @Tainted byte @Tainted [] len_4 = len[4];
    final @Tainted byte @Tainted [] len_5 = len[5];
    final @Tainted int nMTFShadow = this.nMTF;

    @Tainted
    int nSelectors = 0;

    for (@Tainted int iter = 0; iter < N_ITERS; iter++) {
      for (@Tainted int t = nGroups; --t >= 0;) {
        fave[t] = 0;
        @Tainted
        int @Tainted [] rfreqt = rfreq[t];
        for (@Tainted int i = alphaSize; --i >= 0;) {
          rfreqt[i] = 0;
        }
      }

      nSelectors = 0;

      for (@Tainted int gs = 0; gs < this.nMTF;) {
        /* Set group start & end marks. */

        /*
        * Calculate the cost of this group as coded by each of the
        * coding tables.
        */

        final @Tainted int ge = Math.min(gs + G_SIZE - 1, nMTFShadow - 1);

        if (nGroups == N_GROUPS) {
          // unrolled version of the else-block

          @Tainted
          short cost0 = 0;
          @Tainted
          short cost1 = 0;
          @Tainted
          short cost2 = 0;
          @Tainted
          short cost3 = 0;
          @Tainted
          short cost4 = 0;
          @Tainted
          short cost5 = 0;

          for (@Tainted int i = gs; i <= ge; i++) {
            final @Tainted int icv = sfmap[i];
            cost0 += len_0[icv] & 0xff;
            cost1 += len_1[icv] & 0xff;
            cost2 += len_2[icv] & 0xff;
            cost3 += len_3[icv] & 0xff;
            cost4 += len_4[icv] & 0xff;
            cost5 += len_5[icv] & 0xff;
          }

          cost[0] = cost0;
          cost[1] = cost1;
          cost[2] = cost2;
          cost[3] = cost3;
          cost[4] = cost4;
          cost[5] = cost5;

        } else {
          for (@Tainted int t = nGroups; --t >= 0;) {
            cost[t] = 0;
          }

          for (@Tainted int i = gs; i <= ge; i++) {
            final @Tainted int icv = sfmap[i];
            for (@Tainted int t = nGroups; --t >= 0;) {
              cost[t] += len[t][icv] & 0xff;
            }
          }
        }

        /*
        * Find the coding table which is best for this group, and
        * record its identity in the selector table.
        */
        @Tainted
        int bt = -1;
        for (@Tainted int t = nGroups, bc = 999999999; --t >= 0;) {
          final @Tainted int cost_t = cost[t];
          if (cost_t < bc) {
            bc = cost_t;
            bt = t;
          }
        }

        fave[bt]++;
        selector[nSelectors] = (@Tainted byte) bt;
        nSelectors++;

        /*
        * Increment the symbol frequencies for the selected table.
        */
        final @Tainted int @Tainted [] rfreq_bt = rfreq[bt];
        for (@Tainted int i = gs; i <= ge; i++) {
          rfreq_bt[sfmap[i]]++;
        }

        gs = ge + 1;
      }

      /*
      * Recompute the tables based on the accumulated frequencies.
      */
      for (@Tainted int t = 0; t < nGroups; t++) {
        hbMakeCodeLengths(len[t], rfreq[t], this.data, alphaSize, 20);
      }
    }

    return nSelectors;
  }

  private void sendMTFValues2(@Tainted CBZip2OutputStream this, final @Tainted int nGroups, final @Tainted int nSelectors) {
    // assert (nGroups < 8) : nGroups;

    final @Tainted Data dataShadow = this.data;
    @Tainted
    byte @Tainted [] pos = dataShadow.sendMTFValues2_pos;

    for (@Tainted int i = nGroups; --i >= 0;) {
      pos[i] = (@Tainted byte) i;
    }

    for (@Tainted int i = 0; i < nSelectors; i++) {
      final @Tainted byte ll_i = dataShadow.selector[i];
      @Tainted
      byte tmp = pos[0];
      @Tainted
      int j = 0;

      while (ll_i != tmp) {
        j++;
        @Tainted
        byte tmp2 = tmp;
        tmp = pos[j];
        pos[j] = tmp2;
      }

      pos[0] = tmp;
      dataShadow.selectorMtf[i] = (@Tainted byte) j;
    }
  }

  private void sendMTFValues3(@Tainted CBZip2OutputStream this, final @Tainted int nGroups, final @Tainted int alphaSize) {
    @Tainted
    int @Tainted [] @Tainted [] code = this.data.sendMTFValues_code;
    @Tainted
    byte @Tainted [] @Tainted [] len = this.data.sendMTFValues_len;

    for (@Tainted int t = 0; t < nGroups; t++) {
      @Tainted
      int minLen = 32;
      @Tainted
      int maxLen = 0;
      final @Tainted byte @Tainted [] len_t = len[t];
      for (@Tainted int i = alphaSize; --i >= 0;) {
        final @Tainted int l = len_t[i] & 0xff;
        if (l > maxLen) {
          maxLen = l;
        }
        if (l < minLen) {
          minLen = l;
        }
      }

      // assert (maxLen <= 20) : maxLen;
      // assert (minLen >= 1) : minLen;

      hbAssignCodes(code[t], len[t], minLen, maxLen, alphaSize);
    }
  }

  private void sendMTFValues4(@Tainted CBZip2OutputStream this) throws IOException {
    final @Tainted boolean @Tainted [] inUse = this.data.inUse;
    final @Tainted boolean @Tainted [] inUse16 = this.data.sentMTFValues4_inUse16;

    for (@Tainted int i = 16; --i >= 0;) {
      inUse16[i] = false;
      final @Tainted int i16 = i * 16;
      for (@Tainted int j = 16; --j >= 0;) {
        if (inUse[i16 + j]) {
          inUse16[i] = true;
        }
      }
    }

    for (@Tainted int i = 0; i < 16; i++) {
      bsW(1, inUse16[i] ? 1 : 0);
    }

    final @Tainted OutputStream outShadow = this.out;
    @Tainted
    int bsLiveShadow = this.bsLive;
    @Tainted
    int bsBuffShadow = this.bsBuff;

    for (@Tainted int i = 0; i < 16; i++) {
      if (inUse16[i]) {
        final @Tainted int i16 = i * 16;
        for (@Tainted int j = 0; j < 16; j++) {
          // inlined: bsW(1, inUse[i16 + j] ? 1 : 0);
          while (bsLiveShadow >= 8) {
            outShadow.write(bsBuffShadow >> 24); // write 8-bit
            bsBuffShadow <<= 8;
            bsLiveShadow -= 8;
          }
          if (inUse[i16 + j]) {
            bsBuffShadow |= 1 << (32 - bsLiveShadow - 1);
          }
          bsLiveShadow++;
        }
      }
    }

    this.bsBuff = bsBuffShadow;
    this.bsLive = bsLiveShadow;
  }

  private void sendMTFValues5(@Tainted CBZip2OutputStream this, final @Tainted int nGroups, final @Tainted int nSelectors)
      throws IOException {
    bsW(3, nGroups);
    bsW(15, nSelectors);

    final @Tainted OutputStream outShadow = this.out;
    final @Tainted byte @Tainted [] selectorMtf = this.data.selectorMtf;

    @Tainted
    int bsLiveShadow = this.bsLive;
    @Tainted
    int bsBuffShadow = this.bsBuff;

    for (@Tainted int i = 0; i < nSelectors; i++) {
      for (@Tainted int j = 0, hj = selectorMtf[i] & 0xff; j < hj; j++) {
        // inlined: bsW(1, 1);
        while (bsLiveShadow >= 8) {
          outShadow.write(bsBuffShadow >> 24);
          bsBuffShadow <<= 8;
          bsLiveShadow -= 8;
        }
        bsBuffShadow |= 1 << (32 - bsLiveShadow - 1);
        bsLiveShadow++;
      }

      // inlined: bsW(1, 0);
      while (bsLiveShadow >= 8) {
        outShadow.write(bsBuffShadow >> 24);
        bsBuffShadow <<= 8;
        bsLiveShadow -= 8;
      }
      // bsBuffShadow |= 0 << (32 - bsLiveShadow - 1);
      bsLiveShadow++;
    }

    this.bsBuff = bsBuffShadow;
    this.bsLive = bsLiveShadow;
  }

  private void sendMTFValues6(@Tainted CBZip2OutputStream this, final @Tainted int nGroups, final @Tainted int alphaSize)
      throws IOException {
    final @Tainted byte @Tainted [] @Tainted [] len = this.data.sendMTFValues_len;
    final @Tainted OutputStream outShadow = this.out;

    @Tainted
    int bsLiveShadow = this.bsLive;
    @Tainted
    int bsBuffShadow = this.bsBuff;

    for (@Tainted int t = 0; t < nGroups; t++) {
      @Tainted
      byte @Tainted [] len_t = len[t];
      @Tainted
      int curr = len_t[0] & 0xff;

      // inlined: bsW(5, curr);
      while (bsLiveShadow >= 8) {
        outShadow.write(bsBuffShadow >> 24); // write 8-bit
        bsBuffShadow <<= 8;
        bsLiveShadow -= 8;
      }
      bsBuffShadow |= curr << (32 - bsLiveShadow - 5);
      bsLiveShadow += 5;

      for (@Tainted int i = 0; i < alphaSize; i++) {
        @Tainted
        int lti = len_t[i] & 0xff;
        while (curr < lti) {
          // inlined: bsW(2, 2);
          while (bsLiveShadow >= 8) {
            outShadow.write(bsBuffShadow >> 24); // write 8-bit
            bsBuffShadow <<= 8;
            bsLiveShadow -= 8;
          }
          bsBuffShadow |= 2 << (32 - bsLiveShadow - 2);
          bsLiveShadow += 2;

          curr++; /* 10 */
        }

        while (curr > lti) {
          // inlined: bsW(2, 3);
          while (bsLiveShadow >= 8) {
            outShadow.write(bsBuffShadow >> 24); // write 8-bit
            bsBuffShadow <<= 8;
            bsLiveShadow -= 8;
          }
          bsBuffShadow |= 3 << (32 - bsLiveShadow - 2);
          bsLiveShadow += 2;

          curr--; /* 11 */
        }

        // inlined: bsW(1, 0);
        while (bsLiveShadow >= 8) {
          outShadow.write(bsBuffShadow >> 24); // write 8-bit
          bsBuffShadow <<= 8;
          bsLiveShadow -= 8;
        }
        // bsBuffShadow |= 0 << (32 - bsLiveShadow - 1);
        bsLiveShadow++;
      }
    }

    this.bsBuff = bsBuffShadow;
    this.bsLive = bsLiveShadow;
  }

  private void sendMTFValues7(@Tainted CBZip2OutputStream this, final @Tainted int nSelectors) throws IOException {
    final @Tainted Data dataShadow = this.data;
    final @Tainted byte @Tainted [] @Tainted [] len = dataShadow.sendMTFValues_len;
    final @Tainted int @Tainted [] @Tainted [] code = dataShadow.sendMTFValues_code;
    final @Tainted OutputStream outShadow = this.out;
    final @Tainted byte @Tainted [] selector = dataShadow.selector;
    final @Tainted char @Tainted [] sfmap = dataShadow.sfmap;
    final @Tainted int nMTFShadow = this.nMTF;

    @Tainted
    int selCtr = 0;

    @Tainted
    int bsLiveShadow = this.bsLive;
    @Tainted
    int bsBuffShadow = this.bsBuff;

    for (@Tainted int gs = 0; gs < nMTFShadow;) {
      final @Tainted int ge = Math.min(gs + G_SIZE - 1, nMTFShadow - 1);
      final @Tainted int selector_selCtr = selector[selCtr] & 0xff;
      final @Tainted int @Tainted [] code_selCtr = code[selector_selCtr];
      final @Tainted byte @Tainted [] len_selCtr = len[selector_selCtr];

      while (gs <= ge) {
        final @Tainted int sfmap_i = sfmap[gs];

        //
        // inlined: bsW(len_selCtr[sfmap_i] & 0xff,
        // code_selCtr[sfmap_i]);
        //
        while (bsLiveShadow >= 8) {
          outShadow.write(bsBuffShadow >> 24);
          bsBuffShadow <<= 8;
          bsLiveShadow -= 8;
        }
        final @Tainted int n = len_selCtr[sfmap_i] & 0xFF;
        bsBuffShadow |= code_selCtr[sfmap_i] << (32 - bsLiveShadow - n);
        bsLiveShadow += n;

        gs++;
      }

      gs = ge + 1;
      selCtr++;
    }

    this.bsBuff = bsBuffShadow;
    this.bsLive = bsLiveShadow;
  }

  private void moveToFrontCodeAndSend(@Tainted CBZip2OutputStream this) throws IOException {
    bsW(24, this.origPtr);
    generateMTFValues();
    sendMTFValues();
  }

  /**
  * This is the most hammered method of this class.
  *
  * <p>
  * This is the version using unrolled loops. Normally I never use such ones
  * in Java code. The unrolling has shown a noticable performance improvement
  * on JRE 1.4.2 (Linux i586 / HotSpot Client). Of course it depends on the
  * JIT compiler of the vm.
  * </p>
  */
  private @Tainted boolean mainSimpleSort(@Tainted CBZip2OutputStream this, final @Tainted Data dataShadow, final @Tainted int lo,
      final @Tainted int hi, final @Tainted int d) {
    final @Tainted int bigN = hi - lo + 1;
    if (bigN < 2) {
      return this.firstAttempt && (this.workDone > this.workLimit);
    }

    @Tainted
    int hp = 0;
    while (INCS[hp] < bigN) {
      hp++;
    }

    final @Tainted int @Tainted [] fmap = dataShadow.fmap;
    final @Tainted char @Tainted [] quadrant = dataShadow.quadrant;
    final @Tainted byte @Tainted [] block = dataShadow.block;
    final @Tainted int lastShadow = this.last;
    final @Tainted int lastPlus1 = lastShadow + 1;
    final @Tainted boolean firstAttemptShadow = this.firstAttempt;
    final @Tainted int workLimitShadow = this.workLimit;
    @Tainted
    int workDoneShadow = this.workDone;

    // Following block contains unrolled code which could be shortened by
    // coding it in additional loops.

    HP: while (--hp >= 0) {
      final @Tainted int h = INCS[hp];
      final @Tainted int mj = lo + h - 1;

      for (@Tainted int i = lo + h; i <= hi;) {
        // copy
        for (@Tainted int k = 3; (i <= hi) && (--k >= 0); i++) {
          final @Tainted int v = fmap[i];
          final @Tainted int vd = v + d;
          @Tainted
          int j = i;

          // for (int a;
          // (j > mj) && mainGtU((a = fmap[j - h]) + d, vd,
          // block, quadrant, lastShadow);
          // j -= h) {
          // fmap[j] = a;
          // }
          //
          // unrolled version:

          // start inline mainGTU
          @Tainted
          boolean onceRunned = false;
          @Tainted
          int a = 0;

          HAMMER: while (true) {
            if (onceRunned) {
              fmap[j] = a;
              if ((j -= h) <= mj) {
                break HAMMER;
              }
            } else {
              onceRunned = true;
            }

            a = fmap[j - h];
            @Tainted
            int i1 = a + d;
            @Tainted
            int i2 = vd;

            // following could be done in a loop, but
            // unrolled it for performance:
            if (block[i1 + 1] == block[i2 + 1]) {
              if (block[i1 + 2] == block[i2 + 2]) {
                if (block[i1 + 3] == block[i2 + 3]) {
                  if (block[i1 + 4] == block[i2 + 4]) {
                    if (block[i1 + 5] == block[i2 + 5]) {
                      if (block[(i1 += 6)] == block[(i2 += 6)]) {
                        @Tainted
                        int x = lastShadow;
                        X: while (x > 0) {
                          x -= 4;

                          if (block[i1 + 1] == block[i2 + 1]) {
                            if (quadrant[i1] == quadrant[i2]) {
                              if (block[i1 + 2] == block[i2 + 2]) {
                                if (quadrant[i1 + 1] == quadrant[i2 + 1]) {
                                  if (block[i1 + 3] == block[i2 + 3]) {
                                    if (quadrant[i1 + 2] == quadrant[i2 + 2]) {
                                      if (block[i1 + 4] == block[i2 + 4]) {
                                        if (quadrant[i1 + 3] == quadrant[i2 + 3]) {
                                          if ((i1 += 4) >= lastPlus1) {
                                            i1 -= lastPlus1;
                                          }
                                          if ((i2 += 4) >= lastPlus1) {
                                            i2 -= lastPlus1;
                                          }
                                          workDoneShadow++;
                                          continue X;
                                        } else if ((quadrant[i1 + 3] > quadrant[i2 + 3])) {
                                          continue HAMMER;
                                        } else {
                                          break HAMMER;
                                        }
                                      } else if ((block[i1 + 4] & 0xff) > (block[i2 + 4] & 0xff)) {
                                        continue HAMMER;
                                      } else {
                                        break HAMMER;
                                      }
                                    } else if ((quadrant[i1 + 2] > quadrant[i2 + 2])) {
                                      continue HAMMER;
                                    } else {
                                      break HAMMER;
                                    }
                                  } else if ((block[i1 + 3] & 0xff) > (block[i2 + 3] & 0xff)) {
                                    continue HAMMER;
                                  } else {
                                    break HAMMER;
                                  }
                                } else if ((quadrant[i1 + 1] > quadrant[i2 + 1])) {
                                  continue HAMMER;
                                } else {
                                  break HAMMER;
                                }
                              } else if ((block[i1 + 2] & 0xff) > (block[i2 + 2] & 0xff)) {
                                continue HAMMER;
                              } else {
                                break HAMMER;
                              }
                            } else if ((quadrant[i1] > quadrant[i2])) {
                              continue HAMMER;
                            } else {
                              break HAMMER;
                            }
                          } else if ((block[i1 + 1] & 0xff) > (block[i2 + 1] & 0xff)) {
                            continue HAMMER;
                          } else {
                            break HAMMER;
                          }

                        }
                        break HAMMER;
                      } // while x > 0
                      else {
                        if ((block[i1] & 0xff) > (block[i2] & 0xff)) {
                          continue HAMMER;
                        } else {
                          break HAMMER;
                        }
                      }
                    } else if ((block[i1 + 5] & 0xff) > (block[i2 + 5] & 0xff)) {
                      continue HAMMER;
                    } else {
                      break HAMMER;
                    }
                  } else if ((block[i1 + 4] & 0xff) > (block[i2 + 4] & 0xff)) {
                    continue HAMMER;
                  } else {
                    break HAMMER;
                  }
                } else if ((block[i1 + 3] & 0xff) > (block[i2 + 3] & 0xff)) {
                  continue HAMMER;
                } else {
                  break HAMMER;
                }
              } else if ((block[i1 + 2] & 0xff) > (block[i2 + 2] & 0xff)) {
                continue HAMMER;
              } else {
                break HAMMER;
              }
            } else if ((block[i1 + 1] & 0xff) > (block[i2 + 1] & 0xff)) {
              continue HAMMER;
            } else {
              break HAMMER;
            }

          } // HAMMER
          // end inline mainGTU

          fmap[j] = v;
        }

        if (firstAttemptShadow && (i <= hi)
            && (workDoneShadow > workLimitShadow)) {
          break HP;
        }
      }
    }

    this.workDone = workDoneShadow;
    return firstAttemptShadow && (workDoneShadow > workLimitShadow);
  }

  private static void vswap(@Tainted int @Tainted [] fmap, @Tainted int p1, @Tainted int p2, @Tainted int n) {
    n += p1;
    while (p1 < n) {
      @Tainted
      int t = fmap[p1];
      fmap[p1++] = fmap[p2];
      fmap[p2++] = t;
    }
  }

  private static @Tainted byte med3(@Tainted byte a, @Tainted byte b, @Tainted byte c) {
    return (a < b) ? (b < c ? b : a < c ? c : a) : (b > c ? b : a > c ? c
        : a);
  }

  private void blockSort(@Tainted CBZip2OutputStream this) {
    this.workLimit = WORK_FACTOR * this.last;
    this.workDone = 0;
    this.blockRandomised = false;
    this.firstAttempt = true;
    mainSort();

    if (this.firstAttempt && (this.workDone > this.workLimit)) {
      randomiseBlock();
      this.workLimit = this.workDone = 0;
      this.firstAttempt = false;
      mainSort();
    }

    @Tainted
    int @Tainted [] fmap = this.data.fmap;
    this.origPtr = -1;
    for (@Tainted int i = 0, lastShadow = this.last; i <= lastShadow; i++) {
      if (fmap[i] == 0) {
        this.origPtr = i;
        break;
      }
    }

    // assert (this.origPtr != -1) : this.origPtr;
  }

  /**
  * Method "mainQSort3", file "blocksort.c", BZip2 1.0.2
  */
  private void mainQSort3(@Tainted CBZip2OutputStream this, final @Tainted Data dataShadow, final @Tainted int loSt,
      final @Tainted int hiSt, final @Tainted int dSt) {
    final @Tainted int @Tainted [] stack_ll = dataShadow.stack_ll;
    final @Tainted int @Tainted [] stack_hh = dataShadow.stack_hh;
    final @Tainted int @Tainted [] stack_dd = dataShadow.stack_dd;
    final @Tainted int @Tainted [] fmap = dataShadow.fmap;
    final @Tainted byte @Tainted [] block = dataShadow.block;

    stack_ll[0] = loSt;
    stack_hh[0] = hiSt;
    stack_dd[0] = dSt;

    for (@Tainted int sp = 1; --sp >= 0;) {
      final @Tainted int lo = stack_ll[sp];
      final @Tainted int hi = stack_hh[sp];
      final @Tainted int d = stack_dd[sp];

      if ((hi - lo < SMALL_THRESH) || (d > DEPTH_THRESH)) {
        if (mainSimpleSort(dataShadow, lo, hi, d)) {
          return;
        }
      } else {
        final @Tainted int d1 = d + 1;
        final @Tainted int med = med3(block[fmap[lo] + d1],
            block[fmap[hi] + d1], block[fmap[(lo + hi) >>> 1] + d1]) & 0xff;

        @Tainted
        int unLo = lo;
        @Tainted
        int unHi = hi;
        @Tainted
        int ltLo = lo;
        @Tainted
        int gtHi = hi;

        while (true) {
          while (unLo <= unHi) {
            final @Tainted int n = ((@Tainted int) block[fmap[unLo] + d1] & 0xff)
                - med;
            if (n == 0) {
              final @Tainted int temp = fmap[unLo];
              fmap[unLo++] = fmap[ltLo];
              fmap[ltLo++] = temp;
            } else if (n < 0) {
              unLo++;
            } else {
              break;
            }
          }

          while (unLo <= unHi) {
            final @Tainted int n = ((@Tainted int) block[fmap[unHi] + d1] & 0xff)
                - med;
            if (n == 0) {
              final @Tainted int temp = fmap[unHi];
              fmap[unHi--] = fmap[gtHi];
              fmap[gtHi--] = temp;
            } else if (n > 0) {
              unHi--;
            } else {
              break;
            }
          }

          if (unLo <= unHi) {
            final @Tainted int temp = fmap[unLo];
            fmap[unLo++] = fmap[unHi];
            fmap[unHi--] = temp;
          } else {
            break;
          }
        }

        if (gtHi < ltLo) {
          stack_ll[sp] = lo;
          stack_hh[sp] = hi;
          stack_dd[sp] = d1;
          sp++;
        } else {
          @Tainted
          int n = ((ltLo - lo) < (unLo - ltLo)) ? (ltLo - lo)
              : (unLo - ltLo);
          vswap(fmap, lo, unLo - n, n);
          @Tainted
          int m = ((hi - gtHi) < (gtHi - unHi)) ? (hi - gtHi)
              : (gtHi - unHi);
          vswap(fmap, unLo, hi - m + 1, m);

          n = lo + unLo - ltLo - 1;
          m = hi - (gtHi - unHi) + 1;

          stack_ll[sp] = lo;
          stack_hh[sp] = n;
          stack_dd[sp] = d;
          sp++;

          stack_ll[sp] = n + 1;
          stack_hh[sp] = m - 1;
          stack_dd[sp] = d1;
          sp++;

          stack_ll[sp] = m;
          stack_hh[sp] = hi;
          stack_dd[sp] = d;
          sp++;
        }
      }
    }
  }

  private void mainSort(@Tainted CBZip2OutputStream this) {
    final @Tainted Data dataShadow = this.data;
    final @Tainted int @Tainted [] runningOrder = dataShadow.mainSort_runningOrder;
    final @Tainted int @Tainted [] copy = dataShadow.mainSort_copy;
    final @Tainted boolean @Tainted [] bigDone = dataShadow.mainSort_bigDone;
    final @Tainted int @Tainted [] ftab = dataShadow.ftab;
    final @Tainted byte @Tainted [] block = dataShadow.block;
    final @Tainted int @Tainted [] fmap = dataShadow.fmap;
    final @Tainted char @Tainted [] quadrant = dataShadow.quadrant;
    final @Tainted int lastShadow = this.last;
    final @Tainted int workLimitShadow = this.workLimit;
    final @Tainted boolean firstAttemptShadow = this.firstAttempt;

    // Set up the 2-byte frequency table
    for (@Tainted int i = 65537; --i >= 0;) {
      ftab[i] = 0;
    }

    /*
    * In the various block-sized structures, live data runs from 0 to
    * last+NUM_OVERSHOOT_BYTES inclusive. First, set up the overshoot area
    * for block.
    */
    for (@Tainted int i = 0; i < NUM_OVERSHOOT_BYTES; i++) {
      block[lastShadow + i + 2] = block[(i % (lastShadow + 1)) + 1];
    }
    for (@Tainted int i = lastShadow + NUM_OVERSHOOT_BYTES +1; --i >= 0;) {
      quadrant[i] = 0;
    }
    block[0] = block[lastShadow + 1];

    // Complete the initial radix sort:

    @Tainted
    int c1 = block[0] & 0xff;
    for (@Tainted int i = 0; i <= lastShadow; i++) {
      final @Tainted int c2 = block[i + 1] & 0xff;
      ftab[(c1 << 8) + c2]++;
      c1 = c2;
    }

    for (@Tainted int i = 1; i <= 65536; i++)
      ftab[i] += ftab[i - 1];

    c1 = block[1] & 0xff;
    for (@Tainted int i = 0; i < lastShadow; i++) {
      final @Tainted int c2 = block[i + 2] & 0xff;
      fmap[--ftab[(c1 << 8) + c2]] = i;
      c1 = c2;
    }

    fmap[--ftab[((block[lastShadow + 1] & 0xff) << 8) + (block[1] & 0xff)]] = lastShadow;

    /*
    * Now ftab contains the first loc of every small bucket. Calculate the
    * running order, from smallest to largest big bucket.
    */
    for (@Tainted int i = 256; --i >= 0;) {
      bigDone[i] = false;
      runningOrder[i] = i;
    }

    for (@Tainted int h = 364; h != 1;) {
      h /= 3;
      for (@Tainted int i = h; i <= 255; i++) {
        final @Tainted int vv = runningOrder[i];
        final @Tainted int a = ftab[(vv + 1) << 8] - ftab[vv << 8];
        final @Tainted int b = h - 1;
        @Tainted
        int j = i;
        for (@Tainted int ro = runningOrder[j - h]; (ftab[(ro + 1) << 8] - ftab[ro << 8]) > a; ro = runningOrder[j
            - h]) {
          runningOrder[j] = ro;
          j -= h;
          if (j <= b) {
            break;
          }
        }
        runningOrder[j] = vv;
      }
    }

    /*
    * The main sorting loop.
    */
    for (@Tainted int i = 0; i <= 255; i++) {
      /*
      * Process big buckets, starting with the least full.
      */
      final @Tainted int ss = runningOrder[i];

      // Step 1:
      /*
      * Complete the big bucket [ss] by quicksorting any unsorted small
      * buckets [ss, j]. Hopefully previous pointer-scanning phases have
      * already completed many of the small buckets [ss, j], so we don't
      * have to sort them at all.
      */
      for (@Tainted int j = 0; j <= 255; j++) {
        final @Tainted int sb = (ss << 8) + j;
        final @Tainted int ftab_sb = ftab[sb];
        if ((ftab_sb & SETMASK) != SETMASK) {
          final @Tainted int lo = ftab_sb & CLEARMASK;
          final @Tainted int hi = (ftab[sb + 1] & CLEARMASK) - 1;
          if (hi > lo) {
            mainQSort3(dataShadow, lo, hi, 2);
            if (firstAttemptShadow
                && (this.workDone > workLimitShadow)) {
              return;
            }
          }
          ftab[sb] = ftab_sb | SETMASK;
        }
      }

      // Step 2:
      // Now scan this big bucket so as to synthesise the
      // sorted order for small buckets [t, ss] for all t != ss.

      for (@Tainted int j = 0; j <= 255; j++) {
        copy[j] = ftab[(j << 8) + ss] & CLEARMASK;
      }

      for (@Tainted int j = ftab[ss << 8] & CLEARMASK, hj = (ftab[(ss + 1) << 8] & CLEARMASK); j < hj; j++) {
        final @Tainted int fmap_j = fmap[j];
        c1 = block[fmap_j] & 0xff;
        if (!bigDone[c1]) {
          fmap[copy[c1]] = (fmap_j == 0) ? lastShadow : (fmap_j - 1);
          copy[c1]++;
        }
      }

      for (@Tainted int j = 256; --j >= 0;)
        ftab[(j << 8) + ss] |= SETMASK;

      // Step 3:
      /*
      * The ss big bucket is now done. Record this fact, and update the
      * quadrant descriptors. Remember to update quadrants in the
      * overshoot area too, if necessary. The "if (i < 255)" test merely
      * skips this updating for the last bucket processed, since updating
      * for the last bucket is pointless.
      */
      bigDone[ss] = true;

      if (i < 255) {
        final @Tainted int bbStart = ftab[ss << 8] & CLEARMASK;
        final @Tainted int bbSize = (ftab[(ss + 1) << 8] & CLEARMASK) - bbStart;
        @Tainted
        int shifts = 0;

        while ((bbSize >> shifts) > 65534) {
          shifts++;
        }

        for (@Tainted int j = 0; j < bbSize; j++) {
          final @Tainted int a2update = fmap[bbStart + j];
          final @Tainted char qVal = (@Tainted char) (j >> shifts);
          quadrant[a2update] = qVal;
          if (a2update < NUM_OVERSHOOT_BYTES) {
            quadrant[a2update + lastShadow + 1] = qVal;
          }
        }
      }

    }
  }

  private void randomiseBlock(@Tainted CBZip2OutputStream this) {
    final @Tainted boolean @Tainted [] inUse = this.data.inUse;
    final @Tainted byte @Tainted [] block = this.data.block;
    final @Tainted int lastShadow = this.last;

    for (@Tainted int i = 256; --i >= 0;)
      inUse[i] = false;

    @Tainted
    int rNToGo = 0;
    @Tainted
    int rTPos = 0;
    for (@Tainted int i = 0, j = 1; i <= lastShadow; i = j, j++) {
      if (rNToGo == 0) {
        rNToGo = (@Tainted char) BZip2Constants.rNums[rTPos];
        if (++rTPos == 512) {
          rTPos = 0;
        }
      }

      rNToGo--;
      block[j] ^= ((rNToGo == 1) ? 1 : 0);

      // handle 16 bit signed numbers
      inUse[block[j] & 0xff] = true;
    }

    this.blockRandomised = true;
  }

  private void generateMTFValues(@Tainted CBZip2OutputStream this) {
    final @Tainted int lastShadow = this.last;
    final @Tainted Data dataShadow = this.data;
    final @Tainted boolean @Tainted [] inUse = dataShadow.inUse;
    final @Tainted byte @Tainted [] block = dataShadow.block;
    final @Tainted int @Tainted [] fmap = dataShadow.fmap;
    final @Tainted char @Tainted [] sfmap = dataShadow.sfmap;
    final @Tainted int @Tainted [] mtfFreq = dataShadow.mtfFreq;
    final @Tainted byte @Tainted [] unseqToSeq = dataShadow.unseqToSeq;
    final @Tainted byte @Tainted [] yy = dataShadow.generateMTFValues_yy;

    // make maps
    @Tainted
    int nInUseShadow = 0;
    for (@Tainted int i = 0; i < 256; i++) {
      if (inUse[i]) {
        unseqToSeq[i] = (@Tainted byte) nInUseShadow;
        nInUseShadow++;
      }
    }
    this.nInUse = nInUseShadow;

    final @Tainted int eob = nInUseShadow + 1;

    for (@Tainted int i = eob; i >= 0; i--) {
      mtfFreq[i] = 0;
    }

    for (@Tainted int i = nInUseShadow; --i >= 0;) {
      yy[i] = (@Tainted byte) i;
    }

    @Tainted
    int wr = 0;
    @Tainted
    int zPend = 0;

    for (@Tainted int i = 0; i <= lastShadow; i++) {
      final @Tainted byte ll_i = unseqToSeq[block[fmap[i]] & 0xff];
      @Tainted
      byte tmp = yy[0];
      @Tainted
      int j = 0;

      while (ll_i != tmp) {
        j++;
        @Tainted
        byte tmp2 = tmp;
        tmp = yy[j];
        yy[j] = tmp2;
      }
      yy[0] = tmp;

      if (j == 0) {
        zPend++;
      } else {
        if (zPend > 0) {
          zPend--;
          while (true) {
            if ((zPend & 1) == 0) {
              sfmap[wr] = RUNA;
              wr++;
              mtfFreq[RUNA]++;
            } else {
              sfmap[wr] = RUNB;
              wr++;
              mtfFreq[RUNB]++;
            }

            if (zPend >= 2) {
              zPend = (zPend - 2) >> 1;
            } else {
              break;
            }
          }
          zPend = 0;
        }
        sfmap[wr] = (@Tainted char) (j + 1);
        wr++;
        mtfFreq[j + 1]++;
      }
    }

    if (zPend > 0) {
      zPend--;
      while (true) {
        if ((zPend & 1) == 0) {
          sfmap[wr] = RUNA;
          wr++;
          mtfFreq[RUNA]++;
        } else {
          sfmap[wr] = RUNB;
          wr++;
          mtfFreq[RUNB]++;
        }

        if (zPend >= 2) {
          zPend = (zPend - 2) >> 1;
        } else {
          break;
        }
      }
    }

    sfmap[wr] = (@Tainted char) eob;
    mtfFreq[eob]++;
    this.nMTF = wr + 1;
  }

  private static final class Data extends @Tainted Object {

    // with blockSize 900k
    final @Tainted boolean @Tainted [] inUse = new @Tainted boolean @Tainted [256]; // 256 byte
    final @Tainted byte @Tainted [] unseqToSeq = new @Tainted byte @Tainted [256]; // 256 byte
    final @Tainted int @Tainted [] mtfFreq = new @Tainted int @Tainted [MAX_ALPHA_SIZE]; // 1032 byte
    final @Tainted byte @Tainted [] selector = new @Tainted byte @Tainted [MAX_SELECTORS]; // 18002 byte
    final @Tainted byte @Tainted [] selectorMtf = new @Tainted byte @Tainted [MAX_SELECTORS]; // 18002 byte

    final @Tainted byte @Tainted [] generateMTFValues_yy = new @Tainted byte @Tainted [256]; // 256 byte
    final @Tainted byte @Tainted [] @Tainted [] sendMTFValues_len = new byte @Tainted [N_GROUPS] @Tainted [MAX_ALPHA_SIZE]; // 1548
    // byte
    final @Tainted int @Tainted [] @Tainted [] sendMTFValues_rfreq = new int @Tainted [N_GROUPS] @Tainted [MAX_ALPHA_SIZE]; // 6192
    // byte
    final @Tainted int @Tainted [] sendMTFValues_fave = new @Tainted int @Tainted [N_GROUPS]; // 24 byte
    final @Tainted short @Tainted [] sendMTFValues_cost = new @Tainted short @Tainted [N_GROUPS]; // 12 byte
    final @Tainted int @Tainted [] @Tainted [] sendMTFValues_code = new int @Tainted [N_GROUPS] @Tainted [MAX_ALPHA_SIZE]; // 6192
    // byte
    final @Tainted byte @Tainted [] sendMTFValues2_pos = new @Tainted byte @Tainted [N_GROUPS]; // 6 byte
    final @Tainted boolean @Tainted [] sentMTFValues4_inUse16 = new @Tainted boolean @Tainted [16]; // 16 byte

    final @Tainted int @Tainted [] stack_ll = new @Tainted int @Tainted [QSORT_STACK_SIZE]; // 4000 byte
    final @Tainted int @Tainted [] stack_hh = new @Tainted int @Tainted [QSORT_STACK_SIZE]; // 4000 byte
    final @Tainted int @Tainted [] stack_dd = new @Tainted int @Tainted [QSORT_STACK_SIZE]; // 4000 byte

    final @Tainted int @Tainted [] mainSort_runningOrder = new @Tainted int @Tainted [256]; // 1024 byte
    final @Tainted int @Tainted [] mainSort_copy = new @Tainted int @Tainted [256]; // 1024 byte
    final @Tainted boolean @Tainted [] mainSort_bigDone = new @Tainted boolean @Tainted [256]; // 256 byte

    final @Tainted int @Tainted [] heap = new @Tainted int @Tainted [MAX_ALPHA_SIZE + 2]; // 1040 byte
    final @Tainted int @Tainted [] weight = new @Tainted int @Tainted [MAX_ALPHA_SIZE * 2]; // 2064 byte
    final @Tainted int @Tainted [] parent = new @Tainted int @Tainted [MAX_ALPHA_SIZE * 2]; // 2064 byte

    final @Tainted int @Tainted [] ftab = new @Tainted int @Tainted [65537]; // 262148 byte
    // ------------
    // 333408 byte

    final @Tainted byte @Tainted [] block; // 900021 byte
    final @Tainted int @Tainted [] fmap; // 3600000 byte
    final @Tainted char @Tainted [] sfmap; // 3600000 byte
    // ------------
    // 8433529 byte
    // ============

    /**
    * Array instance identical to sfmap, both are used only temporarily and
    * indepently, so we do not need to allocate additional memory.
    */
    final @Tainted char @Tainted [] quadrant;

    @Tainted
    Data(@Tainted int blockSize100k) {
      super();

      final @Tainted int n = blockSize100k * BZip2Constants.baseBlockSize;
      this.block = new @Tainted byte @Tainted [(n + 1 + NUM_OVERSHOOT_BYTES)];
      this.fmap = new @Tainted int @Tainted [n];
      this.sfmap = new @Tainted char @Tainted [2 * n];
      this.quadrant = this.sfmap;
    }

  }

}
