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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * This class extends {@link MapFile} and provides very much the same
 * functionality. However, it uses dynamic Bloom filters to provide
 * quick membership test for keys, and it offers a fast version of 
 * {@link Reader#get(WritableComparable, Writable)} operation, especially in
 * case of sparsely populated MapFile-s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BloomMapFile {
  private static final @Tainted Log LOG = LogFactory.getLog(BloomMapFile.class);
  public static final @Tainted String BLOOM_FILE_NAME = "bloom";
  public static final @Tainted int HASH_COUNT = 5;
  
  public static void delete(@Tainted FileSystem fs, @Tainted String name) throws IOException {
    @Tainted
    Path dir = new @Tainted Path(name);
    @Tainted
    Path data = new @Tainted Path(dir, MapFile.DATA_FILE_NAME);
    @Tainted
    Path index = new @Tainted Path(dir, MapFile.INDEX_FILE_NAME);
    @Tainted
    Path bloom = new @Tainted Path(dir, BLOOM_FILE_NAME);

    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(bloom, true);
    fs.delete(dir, true);
  }

  private static @Tainted byte @Tainted [] byteArrayForBloomKey(@Tainted DataOutputBuffer buf) {
    @Tainted
    int cleanLength = buf.getLength();
    @Tainted
    byte @Tainted [] ba = buf.getData();
    if (cleanLength != ba.length) {
      ba = new @Tainted byte @Tainted [cleanLength];
      System.arraycopy(buf.getData(), 0, ba, 0, cleanLength);
    }
    return ba;
  }
  
  public static class Writer extends MapFile.@Tainted Writer {
    private @Tainted DynamicBloomFilter bloomFilter;
    private @Tainted int numKeys;
    private @Tainted int vectorSize;
    private @Tainted Key bloomKey = new @Tainted Key();
    private @Tainted DataOutputBuffer buf = new @Tainted DataOutputBuffer();
    private @Tainted FileSystem fs;
    private @Tainted Path dir;
    
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
        @Tainted
        Class<@Tainted ? extends @Tainted WritableComparable> keyClass,
        @Tainted
        Class<@Tainted ? extends @Tainted Writable> valClass, @Tainted CompressionType compress,
        @Tainted
        CompressionCodec codec, @Tainted Progressable progress) throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress, codec), progressable(progress));
    }

    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
        @Tainted
        Class<@Tainted ? extends @Tainted WritableComparable> keyClass,
        @Tainted
        Class valClass, @Tainted CompressionType compress,
        @Tainted
        Progressable progress) throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress), progressable(progress));
    }

    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
        @Tainted
        Class<@Tainted ? extends @Tainted WritableComparable> keyClass,
        @Tainted
        Class valClass, @Tainted CompressionType compress)
        throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress));
    }

    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
        @Tainted
        WritableComparator comparator, @Tainted Class valClass,
        @Tainted
        CompressionType compress, @Tainted CompressionCodec codec, @Tainted Progressable progress)
        throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress, codec), 
           progressable(progress));
    }

    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
        @Tainted
        WritableComparator comparator, @Tainted Class valClass,
        @Tainted
        CompressionType compress, @Tainted Progressable progress) throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress),
           progressable(progress));
    }

    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
        @Tainted
        WritableComparator comparator, @Tainted Class valClass, @Tainted CompressionType compress)
        throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress));
    }

    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
        @Tainted
        WritableComparator comparator, @Tainted Class valClass) throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator), 
           valueClass(valClass));
    }

    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  Class<@Tainted ? extends @Tainted WritableComparable> keyClass,
                  @Tainted
                  Class valClass) throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass), valueClass(valClass));
    }

    public @Tainted Writer(@Tainted Configuration conf, @Tainted Path dir, 
                  SequenceFile.Writer.@Tainted Option @Tainted ... options) throws IOException {
      super(conf, dir, options);
      this.fs = dir.getFileSystem(conf);
      this.dir = dir;
      initBloomFilter(conf);
    }

    private synchronized void initBloomFilter(BloomMapFile.@Tainted Writer this, @Tainted Configuration conf) {
      numKeys = conf.getInt("io.mapfile.bloom.size", 1024 * 1024);
      // vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
      // single key, where <code> is the number of hash functions,
      // <code>n</code> is the number of keys and <code>c</code> is the desired
      // max. error rate.
      // Our desired error rate is by default 0.005, i.e. 0.5%
      @Tainted
      float errorRate = conf.getFloat("io.mapfile.bloom.error.rate", 0.005f);
      vectorSize = (@Tainted int)Math.ceil((double)(-HASH_COUNT * numKeys) /
          Math.log(1.0 - Math.pow(errorRate, 1.0/HASH_COUNT)));
      bloomFilter = new @Tainted DynamicBloomFilter(vectorSize, HASH_COUNT,
          Hash.getHashType(conf), numKeys);
    }

    @Override
    public synchronized void append(BloomMapFile.@Tainted Writer this, @Tainted WritableComparable key, @Tainted Writable val)
        throws IOException {
      super.append(key, val);
      buf.reset();
      key.write(buf);
      bloomKey.set(byteArrayForBloomKey(buf), 1.0);
      bloomFilter.add(bloomKey);
    }

    @Override
    public synchronized void close(BloomMapFile.@Tainted Writer this) throws IOException {
      super.close();
      @Tainted
      DataOutputStream out = fs.create(new @Tainted Path(dir, BLOOM_FILE_NAME), true);
      try {
        bloomFilter.write(out);
        out.flush();
        out.close();
        out = null;
      } finally {
        IOUtils.closeStream(out);
      }
    }

  }
  
  public static class Reader extends MapFile.@Tainted Reader {
    private @Tainted DynamicBloomFilter bloomFilter;
    private @Tainted DataOutputBuffer buf = new @Tainted DataOutputBuffer();
    private @Tainted Key bloomKey = new @Tainted Key();

    public @Tainted Reader(@Tainted Path dir, @Tainted Configuration conf,
                  SequenceFile.Reader.@Tainted Option @Tainted ... options) throws IOException {
      super(dir, conf, options);
      initBloomFilter(dir, conf);
    }

    @Deprecated
    public @Tainted Reader(@Tainted FileSystem fs, @Tainted String dirName, @Tainted Configuration conf)
        throws IOException {
      this(new @Tainted Path(dirName), conf);
    }

    @Deprecated
    public @Tainted Reader(@Tainted FileSystem fs, @Tainted String dirName, @Tainted WritableComparator comparator,
        @Tainted
        Configuration conf, @Tainted boolean open) throws IOException {
      this(new @Tainted Path(dirName), conf, comparator(comparator));
    }

    @Deprecated
    public @Tainted Reader(@Tainted FileSystem fs, @Tainted String dirName, @Tainted WritableComparator comparator,
        @Tainted
        Configuration conf) throws IOException {
      this(new @Tainted Path(dirName), conf, comparator(comparator));
    }
    
    private void initBloomFilter(BloomMapFile.@Tainted Reader this, @Tainted Path dirName, 
                                 @Tainted
                                 Configuration conf) {
      
      @Tainted
      DataInputStream in = null;
      try {
        @Tainted
        FileSystem fs = dirName.getFileSystem(conf);
        in = fs.open(new @Tainted Path(dirName, BLOOM_FILE_NAME));
        bloomFilter = new @Tainted DynamicBloomFilter();
        bloomFilter.readFields(in);
        in.close();
        in = null;
      } catch (@Tainted IOException ioe) {
        LOG.warn("Can't open BloomFilter: " + ioe + " - fallback to MapFile.");
        bloomFilter = null;
      } finally {
        IOUtils.closeStream(in);
      }
    }
    
    /**
     * Checks if this MapFile has the indicated key. The membership test is
     * performed using a Bloom filter, so the result has always non-zero
     * probability of false positives.
     * @param key key to check
     * @return  false iff key doesn't exist, true if key probably exists.
     * @throws IOException
     */
    public @Tainted boolean probablyHasKey(BloomMapFile.@Tainted Reader this, @Tainted WritableComparable key) throws IOException {
      if (bloomFilter == null) {
        return true;
      }
      buf.reset();
      key.write(buf);
      bloomKey.set(byteArrayForBloomKey(buf), 1.0);
      return bloomFilter.membershipTest(bloomKey);
    }
    
    /**
     * Fast version of the
     * {@link MapFile.Reader#get(WritableComparable, Writable)} method. First
     * it checks the Bloom filter for the existence of the key, and only if
     * present it performs the real get operation. This yields significant
     * performance improvements for get operations on sparsely populated files.
     */
    @Override
    public synchronized @Tainted Writable get(BloomMapFile.@Tainted Reader this, @Tainted WritableComparable key, @Tainted Writable val)
        throws IOException {
      if (!probablyHasKey(key)) {
        return null;
      }
      return super.get(key, val);
    }
    
    /**
     * Retrieve the Bloom filter used by this instance of the Reader.
     * @return a Bloom filter (see {@link Filter})
     */
    public @Tainted Filter getBloomFilter(BloomMapFile.@Tainted Reader this) {
      return bloomFilter;
    }
  }
}
