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
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/** A file-based map from keys to values.
 * 
 * <p>A map is a directory containing two files, the <code>data</code> file,
 * containing all keys and values in the map, and a smaller <code>index</code>
 * file, containing a fraction of the keys.  The fraction is determined by
 * {@link Writer#getIndexInterval()}.
 *
 * <p>The index file is read entirely into memory.  Thus key implementations
 * should try to keep themselves small.
 *
 * <p>Map files are created by adding entries in-order.  To maintain a large
 * database, perform updates by copying the previous version of a database and
 * merging in a sorted change list, to create a new version of the database in
 * a new file.  Sorting large change lists can be done with {@link
 * SequenceFile.Sorter}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapFile {
  private static final @Tainted Log LOG = LogFactory.getLog(MapFile.class);

  /** The name of the index file. */
  public static final @Tainted String INDEX_FILE_NAME = "index";

  /** The name of the data file. */
  public static final @Tainted String DATA_FILE_NAME = "data";

  protected @Tainted MapFile() {}                          // no public ctor

  /** Writes a new map. */
  public static class Writer implements java.io.Closeable {
    private SequenceFile.@Tainted Writer data;
    private SequenceFile.@Tainted Writer index;

    final private static @Tainted String INDEX_INTERVAL = "io.map.index.interval";
    private @Tainted int indexInterval = 128;

    private @Tainted long size;
    private @Tainted LongWritable position = new @Tainted LongWritable();

    // the following fields are used only for checking key order
    private @Tainted WritableComparator comparator;
    private @Tainted DataInputBuffer inBuf = new @Tainted DataInputBuffer();
    private @Tainted DataOutputBuffer outBuf = new @Tainted DataOutputBuffer();
    private @Tainted WritableComparable lastKey;

    /** What's the position (in bytes) we wrote when we got the last index */
    private @Tainted long lastIndexPos = -1;

    /**
     * What was size when we last wrote an index. Set to MIN_VALUE to ensure that
     * we have an index at position zero -- midKey will throw an exception if this
     * is not the case
     */
    private @Tainted long lastIndexKeyCount = Long.MIN_VALUE;


    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  Class<@Tainted ? extends @Tainted WritableComparable> keyClass, 
                  @Tainted
                  Class valClass) throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass), valueClass(valClass));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  Class<@Tainted ? extends @Tainted WritableComparable> keyClass, @Tainted Class valClass,
                  @Tainted
                  CompressionType compress, 
                  @Tainted
                  Progressable progress) throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass), valueClass(valClass),
           compression(compress), progressable(progress));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  Class<@Tainted ? extends @Tainted WritableComparable> keyClass, @Tainted Class valClass,
                  @Tainted
                  CompressionType compress, @Tainted CompressionCodec codec,
                  @Tainted
                  Progressable progress) throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass), valueClass(valClass),
           compression(compress, codec), progressable(progress));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  Class<@Tainted ? extends @Tainted WritableComparable> keyClass, @Tainted Class valClass,
                  @Tainted
                  CompressionType compress) throws IOException {
      this(conf, new @Tainted Path(dirName), keyClass(keyClass),
           valueClass(valClass), compression(compress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  WritableComparator comparator, @Tainted Class valClass
                  ) throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator), 
           valueClass(valClass));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  WritableComparator comparator, @Tainted Class valClass,
                  SequenceFile.@Tainted CompressionType compress) throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...)} instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  WritableComparator comparator, @Tainted Class valClass,
                  SequenceFile.@Tainted CompressionType compress,
                  @Tainted
                  Progressable progress) throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress),
           progressable(progress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs, @Tainted String dirName,
                  @Tainted
                  WritableComparator comparator, @Tainted Class valClass,
                  SequenceFile.@Tainted CompressionType compress, @Tainted CompressionCodec codec,
                  @Tainted
                  Progressable progress) throws IOException {
      this(conf, new @Tainted Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress, codec),
           progressable(progress));
    }
    
    // our options are a superset of sequence file writer options
    public static interface Option extends SequenceFile.Writer.@Tainted Option { }
    
    private static class KeyClassOption extends Options.@Tainted ClassOption
                                        implements @Tainted Option {
      @Tainted
      KeyClassOption(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> value) {
        super(value);
      }
    }
    
    private static class ComparatorOption implements @Tainted Option {
      private final @Tainted WritableComparator value;
      @Tainted
      ComparatorOption(@Tainted WritableComparator value) {
        this.value = value;
      }
      @Tainted
      WritableComparator getValue(MapFile.Writer.@Tainted ComparatorOption this) {
        return value;
      }
    }

    public static @Tainted Option keyClass(@Tainted Class<@Tainted ? extends @Tainted WritableComparable> value) {
      return new @Tainted KeyClassOption(value);
    }
    
    public static @Tainted Option comparator(@Tainted WritableComparator value) {
      return new @Tainted ComparatorOption(value);
    }

    public static SequenceFile.Writer.@Tainted Option valueClass(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> value) {
      return SequenceFile.Writer.valueClass(value);
    }
    
    public static 
    SequenceFile.Writer.@Tainted Option compression(@Tainted CompressionType type) {
      return SequenceFile.Writer.compression(type);
    }

    public static 
    SequenceFile.Writer.@Tainted Option compression(@Tainted CompressionType type,
        @Tainted
        CompressionCodec codec) {
      return SequenceFile.Writer.compression(type, codec);
    }

    public static SequenceFile.Writer.@Tainted Option progressable(@Tainted Progressable value) {
      return SequenceFile.Writer.progressable(value);
    }

    @SuppressWarnings("unchecked")
    public @Tainted Writer(@Tainted Configuration conf, 
                  @Tainted
                  Path dirName,
                  SequenceFile.Writer.@Tainted Option @Tainted ... opts
                  ) throws IOException {
      @Tainted
      KeyClassOption keyClassOption = 
        Options.getOption(KeyClassOption.class, opts);
      @Tainted
      ComparatorOption comparatorOption =
        Options.getOption(ComparatorOption.class, opts);
      if ((keyClassOption == null) == (comparatorOption == null)) {
        throw new @Tainted IllegalArgumentException("key class or comparator option "
                                           + "must be set");
      }
      this.indexInterval = conf.getInt(INDEX_INTERVAL, this.indexInterval);

      @Tainted
      Class<@Tainted ? extends @Tainted WritableComparable> keyClass;
      if (keyClassOption == null) {
        this.comparator = comparatorOption.getValue();
        keyClass = comparator.getKeyClass();
      } else {
        keyClass= 
          (@Tainted Class<@Tainted ? extends @Tainted WritableComparable>) keyClassOption.getValue();
        this.comparator = WritableComparator.get(keyClass);
      }
      this.lastKey = comparator.newKey();
      @Tainted
      FileSystem fs = dirName.getFileSystem(conf);

      if (!fs.mkdirs(dirName)) {
        throw new @Tainted IOException("Mkdirs failed to create directory " + dirName);
      }
      @Tainted
      Path dataFile = new @Tainted Path(dirName, DATA_FILE_NAME);
      @Tainted
      Path indexFile = new @Tainted Path(dirName, INDEX_FILE_NAME);

      SequenceFile.Writer.@Tainted Option @Tainted [] dataOptions =
        Options.prependOptions(opts, 
                               SequenceFile.Writer.file(dataFile),
                               SequenceFile.Writer.keyClass(keyClass));
      this.data = SequenceFile.createWriter(conf, dataOptions);

      SequenceFile.Writer.@Tainted Option @Tainted [] indexOptions =
        Options.prependOptions(opts, SequenceFile.Writer.file(indexFile),
            SequenceFile.Writer.keyClass(keyClass),
            SequenceFile.Writer.valueClass(LongWritable.class),
            SequenceFile.Writer.compression(CompressionType.BLOCK));
      this.index = SequenceFile.createWriter(conf, indexOptions);      
    }

    /** The number of entries that are added before an index entry is added.*/
    public @Tainted int getIndexInterval(MapFile.@Tainted Writer this) { return indexInterval; }

    /** Sets the index interval.
     * @see #getIndexInterval()
     */
    public void setIndexInterval(MapFile.@Tainted Writer this, @Tainted int interval) { indexInterval = interval; }

    /** Sets the index interval and stores it in conf
     * @see #getIndexInterval()
     */
    public static void setIndexInterval(@Tainted Configuration conf, @Tainted int interval) {
      conf.setInt(INDEX_INTERVAL, interval);
    }

    /** Close the map. */
    @Override
    public synchronized void close(MapFile.@Tainted Writer this) throws IOException {
      data.close();
      index.close();
    }

    /** Append a key/value pair to the map.  The key must be greater or equal
     * to the previous key added to the map. */
    public synchronized void append(MapFile.@Tainted Writer this, @Tainted WritableComparable key, @Tainted Writable val)
      throws IOException {

      checkKey(key);

      @Tainted
      long pos = data.getLength();      
      // Only write an index if we've changed positions. In a block compressed
      // file, this means we write an entry at the start of each block      
      if (size >= lastIndexKeyCount + indexInterval && pos > lastIndexPos) {
        position.set(pos);                        // point to current eof
        index.append(key, position);
        lastIndexPos = pos;
        lastIndexKeyCount = size;
      }

      data.append(key, val);                      // append key/value to data
      size++;
    }

    private void checkKey(MapFile.@Tainted Writer this, @Tainted WritableComparable key) throws IOException {
      // check that keys are well-ordered
      if (size != 0 && comparator.compare(lastKey, key) > 0)
        throw new @Tainted IOException("key out of order: "+key+" after "+lastKey);
          
      // update lastKey with a copy of key by writing and reading
      outBuf.reset();
      key.write(outBuf);                          // write new key

      inBuf.reset(outBuf.getData(), outBuf.getLength());
      lastKey.readFields(inBuf);                  // read into lastKey
    }

  }
  
  /** Provide access to an existing map. */
  public static class Reader implements java.io.Closeable {
      
    /** Number of index entries to skip between each entry.  Zero by default.
     * Setting this to values larger than zero can facilitate opening large map
     * files using less memory. */
    private @Tainted int INDEX_SKIP = 0;
      
    private @Tainted WritableComparator comparator;

    private @Tainted WritableComparable nextKey;
    private @Tainted long seekPosition = -1;
    private @Tainted int seekIndex = -1;
    private @Tainted long firstPosition;

    // the data, on disk
    private SequenceFile.@Tainted Reader data;
    private SequenceFile.@Tainted Reader index;

    // whether the index Reader was closed
    private @Tainted boolean indexClosed = false;

    // the index, in memory
    private @Tainted int count = -1;
    private @Tainted WritableComparable @Tainted [] keys;
    private @Tainted long @Tainted [] positions;

    /** Returns the class of keys in this file. */
    public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getKeyClass(MapFile.@Tainted Reader this) { return data.getKeyClass(); }

    /** Returns the class of values in this file. */
    public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getValueClass(MapFile.@Tainted Reader this) { return data.getValueClass(); }

    public static interface Option extends SequenceFile.Reader.@Tainted Option {}
    
    public static @Tainted Option comparator(@Tainted WritableComparator value) {
      return new @Tainted ComparatorOption(value);
    }

    static class ComparatorOption implements @Tainted Option {
      private final @Tainted WritableComparator value;
      @Tainted
      ComparatorOption(@Tainted WritableComparator value) {
        this.value = value;
      }
      @Tainted
      WritableComparator getValue(MapFile.Reader.@Tainted ComparatorOption this) {
        return value;
      }
    }

    public @Tainted Reader(@Tainted Path dir, @Tainted Configuration conf,
                  SequenceFile.Reader.@Tainted Option @Tainted ... opts) throws IOException {
      @Tainted
      ComparatorOption comparatorOption = 
        Options.getOption(ComparatorOption.class, opts);
      @Tainted
      WritableComparator comparator =
        comparatorOption == null ? null : comparatorOption.getValue();
      INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
      open(dir, comparator, conf, opts);
    }
 
    /** Construct a map reader for the named map.
     * @deprecated
     */
    @Deprecated
    public @Tainted Reader(@Tainted FileSystem fs, @Tainted String dirName, 
                  @Tainted
                  Configuration conf) throws IOException {
      this(new @Tainted Path(dirName), conf);
    }

    /** Construct a map reader for the named map using the named comparator.
     * @deprecated
     */
    @Deprecated
    public @Tainted Reader(@Tainted FileSystem fs, @Tainted String dirName, @Tainted WritableComparator comparator, 
                  @Tainted
                  Configuration conf) throws IOException {
      this(new @Tainted Path(dirName), conf, comparator(comparator));
    }
    
    protected synchronized void open(MapFile.@Tainted Reader this, @Tainted Path dir,
                                     @Tainted
                                     WritableComparator comparator,
                                     @Tainted
                                     Configuration conf, 
                                     SequenceFile.Reader.@Tainted Option @Tainted ... options
                                     ) throws IOException {
      @Tainted
      Path dataFile = new @Tainted Path(dir, DATA_FILE_NAME);
      @Tainted
      Path indexFile = new @Tainted Path(dir, INDEX_FILE_NAME);

      // open the data
      this.data = createDataFileReader(dataFile, conf, options);
      this.firstPosition = data.getPosition();

      if (comparator == null)
        this.comparator = 
          WritableComparator.get(data.getKeyClass().
                                   asSubclass(WritableComparable.class));
      else
        this.comparator = comparator;

      // open the index
      SequenceFile.Reader.@Tainted Option @Tainted [] indexOptions =
        Options.prependOptions(options, SequenceFile.Reader.file(indexFile));
      this.index = new SequenceFile.@Tainted Reader(conf, indexOptions);
    }

    /**
     * Override this method to specialize the type of
     * {@link SequenceFile.Reader} returned.
     */
    protected SequenceFile.@Tainted Reader 
      createDataFileReader(MapFile.@Tainted Reader this, @Tainted Path dataFile, @Tainted Configuration conf,
                           SequenceFile.Reader.@Tainted Option @Tainted ... options
                           ) throws IOException {
      SequenceFile.Reader.@Tainted Option @Tainted [] newOptions =
        Options.prependOptions(options, SequenceFile.Reader.file(dataFile));
      return new SequenceFile.@Tainted Reader(conf, newOptions);
    }

    private void readIndex(MapFile.@Tainted Reader this) throws IOException {
      // read the index entirely into memory
      if (this.keys != null)
        return;
      this.count = 0;
      this.positions = new @Tainted long @Tainted [1024];

      try {
        @Tainted
        int skip = INDEX_SKIP;
        @Tainted
        LongWritable position = new @Tainted LongWritable();
        @Tainted
        WritableComparable lastKey = null;
        @Tainted
        long lastIndex = -1;
        @Tainted
        ArrayList<@Tainted WritableComparable> keyBuilder = new @Tainted ArrayList<@Tainted WritableComparable>(1024);
        while (true) {
          @Tainted
          WritableComparable k = comparator.newKey();

          if (!index.next(k, position))
            break;

          // check order to make sure comparator is compatible
          if (lastKey != null && comparator.compare(lastKey, k) > 0)
            throw new @Tainted IOException("key out of order: "+k+" after "+lastKey);
          lastKey = k;
          if (skip > 0) {
            skip--;
            continue;                             // skip this entry
          } else {
            skip = INDEX_SKIP;                    // reset skip
          }

	  // don't read an index that is the same as the previous one. Block
	  // compressed map files used to do this (multiple entries would point
	  // at the same block)
	  if (position.get() == lastIndex)
	    continue;

          if (count == positions.length) {
	    positions = Arrays.copyOf(positions, positions.length * 2);
          }

          keyBuilder.add(k);
          positions[count] = position.get();
          count++;
        }

        this.keys = keyBuilder.toArray(new @Tainted WritableComparable @Tainted [count]);
        positions = Arrays.copyOf(positions, count);
      } catch (@Tainted EOFException e) {
        LOG.warn("Unexpected EOF reading " + index +
                              " at entry #" + count + ".  Ignoring.");
      } finally {
	indexClosed = true;
        index.close();
      }
    }

    /** Re-positions the reader before its first key. */
    public synchronized void reset(MapFile.@Tainted Reader this) throws IOException {
      data.seek(firstPosition);
    }

    /** Get the key at approximately the middle of the file. Or null if the
     *  file is empty. 
     */
    public synchronized @Tainted WritableComparable midKey(MapFile.@Tainted Reader this) throws IOException {

      readIndex();
      if (count == 0) {
        return null;
      }
    
      return keys[(count - 1) / 2];
    }
    
    /** Reads the final key from the file.
     *
     * @param key key to read into
     */
    public synchronized void finalKey(MapFile.@Tainted Reader this, @Tainted WritableComparable key)
      throws IOException {

      @Tainted
      long originalPosition = data.getPosition(); // save position
      try {
        readIndex();                              // make sure index is valid
        if (count > 0) {
          data.seek(positions[count-1]);          // skip to last indexed entry
        } else {
          reset();                                // start at the beginning
        }
        while (data.next(key)) {}                 // scan to eof

      } finally {
        data.seek(originalPosition);              // restore position
      }
    }

    /** Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.  Returns true iff the named key exists
     * in this map.
     */
    public synchronized @Tainted boolean seek(MapFile.@Tainted Reader this, @Tainted WritableComparable key) throws IOException {
      return seekInternal(key) == 0;
    }

    /** 
     * Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.
     *
     * @return  0   - exact match found
     *          < 0 - positioned at next record
     *          1   - no more records in file
     */
    private synchronized @Tainted int seekInternal(MapFile.@Tainted Reader this, @Tainted WritableComparable key)
      throws IOException {
      return seekInternal(key, false);
    }

    /** 
     * Positions the reader at the named key, or if none such exists, at the
     * key that falls just before or just after dependent on how the
     * <code>before</code> parameter is set.
     * 
     * @param before - IF true, and <code>key</code> does not exist, position
     * file at entry that falls just before <code>key</code>.  Otherwise,
     * position file at record that sorts just after.
     * @return  0   - exact match found
     *          < 0 - positioned at next record
     *          1   - no more records in file
     */
    private synchronized @Tainted int seekInternal(MapFile.@Tainted Reader this, @Tainted WritableComparable key,
        final @Tainted boolean before)
      throws IOException {
      readIndex();                                // make sure index is read

      if (seekIndex != -1                         // seeked before
          && seekIndex+1 < count           
          && comparator.compare(key, keys[seekIndex+1])<0 // before next indexed
          && comparator.compare(key, nextKey)
          >= 0) {                                 // but after last seeked
        // do nothing
      } else {
        seekIndex = binarySearch(key);
        if (seekIndex < 0)                        // decode insertion point
          seekIndex = -seekIndex-2;

        if (seekIndex == -1)                      // belongs before first entry
          seekPosition = firstPosition;           // use beginning of file
        else
          seekPosition = positions[seekIndex];    // else use index
      }
      data.seek(seekPosition);
      
      if (nextKey == null)
        nextKey = comparator.newKey();
     
      // If we're looking for the key before, we need to keep track
      // of the position we got the current key as well as the position
      // of the key before it.
      @Tainted
      long prevPosition = -1;
      @Tainted
      long curPosition = seekPosition;

      while (data.next(nextKey)) {
        @Tainted
        int c = comparator.compare(key, nextKey);
        if (c <= 0) {                             // at or beyond desired
          if (before && c != 0) {
            if (prevPosition == -1) {
              // We're on the first record of this index block
              // and we've already passed the search key. Therefore
              // we must be at the beginning of the file, so seek
              // to the beginning of this block and return c
              data.seek(curPosition);
            } else {
              // We have a previous record to back up to
              data.seek(prevPosition);
              data.next(nextKey);
              // now that we've rewound, the search key must be greater than this key
              return 1;
            }
          }
          return c;
        }
        if (before) {
          prevPosition = curPosition;
          curPosition = data.getPosition();
        }
      }

      return 1;
    }

    private @Tainted int binarySearch(MapFile.@Tainted Reader this, @Tainted WritableComparable key) {
      @Tainted
      int low = 0;
      @Tainted
      int high = count-1;

      while (low <= high) {
        @Tainted
        int mid = (low + high) >>> 1;
        @Tainted
        WritableComparable midVal = keys[mid];
        @Tainted
        int cmp = comparator.compare(midVal, key);

        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return mid;                             // key found
      }
      return -(low + 1);                          // key not found.
    }

    /** Read the next key/value pair in the map into <code>key</code> and
     * <code>val</code>.  Returns true if such a pair exists and false when at
     * the end of the map */
    public synchronized @Tainted boolean next(MapFile.@Tainted Reader this, @Tainted WritableComparable key, @Tainted Writable val)
      throws IOException {
      return data.next(key, val);
    }

    /** Return the value for the named key, or null if none exists. */
    public synchronized @Tainted Writable get(MapFile.@Tainted Reader this, @Tainted WritableComparable key, @Tainted Writable val)
      throws IOException {
      if (seek(key)) {
        data.getCurrentValue(val);
        return val;
      } else
        return null;
    }

    /** 
     * Finds the record that is the closest match to the specified key.
     * Returns <code>key</code> or if it does not exist, at the first entry
     * after the named key.
     * 
-     * @param key       - key that we're trying to find
-     * @param val       - data value if key is found
-     * @return          - the key that was the closest match or null if eof.
     */
    public synchronized @Tainted WritableComparable getClosest(MapFile.@Tainted Reader this, @Tainted WritableComparable key,
      @Tainted
      Writable val)
    throws IOException {
      return getClosest(key, val, false);
    }

    /** 
     * Finds the record that is the closest match to the specified key.
     * 
     * @param key       - key that we're trying to find
     * @param val       - data value if key is found
     * @param before    - IF true, and <code>key</code> does not exist, return
     * the first entry that falls just before the <code>key</code>.  Otherwise,
     * return the record that sorts just after.
     * @return          - the key that was the closest match or null if eof.
     */
    public synchronized @Tainted WritableComparable getClosest(MapFile.@Tainted Reader this, @Tainted WritableComparable key,
        @Tainted
        Writable val, final @Tainted boolean before)
      throws IOException {
     
      @Tainted
      int c = seekInternal(key, before);

      // If we didn't get an exact match, and we ended up in the wrong
      // direction relative to the query key, return null since we
      // must be at the beginning or end of the file.
      if ((!before && c > 0) ||
          (before && c < 0)) {
        return null;
      }

      data.getCurrentValue(val);
      return nextKey;
    }

    /** Close the map. */
    @Override
    public synchronized void close(MapFile.@Tainted Reader this) throws IOException {
      if (!indexClosed) {
        index.close();
      }
      data.close();
    }

  }

  /** Renames an existing map directory. */
  public static void rename(@Tainted FileSystem fs, @Tainted String oldName, @Tainted String newName)
    throws IOException {
    @Tainted
    Path oldDir = new @Tainted Path(oldName);
    @Tainted
    Path newDir = new @Tainted Path(newName);
    if (!fs.rename(oldDir, newDir)) {
      throw new @Tainted IOException("Could not rename " + oldDir + " to " + newDir);
    }
  }

  /** Deletes the named map file. */
  public static void delete(@Tainted FileSystem fs, @Tainted String name) throws IOException {
    @Tainted
    Path dir = new @Tainted Path(name);
    @Tainted
    Path data = new @Tainted Path(dir, DATA_FILE_NAME);
    @Tainted
    Path index = new @Tainted Path(dir, INDEX_FILE_NAME);

    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(dir, true);
  }

  /**
   * This method attempts to fix a corrupt MapFile by re-creating its index.
   * @param fs filesystem
   * @param dir directory containing the MapFile data and index
   * @param keyClass key class (has to be a subclass of Writable)
   * @param valueClass value class (has to be a subclass of Writable)
   * @param dryrun do not perform any changes, just report what needs to be done
   * @return number of valid entries in this MapFile, or -1 if no fixing was needed
   * @throws Exception
   */
  public static @Tainted long fix(@Tainted FileSystem fs, @Tainted Path dir,
                         @Tainted
                         Class<@Tainted ? extends @Tainted Writable> keyClass,
                         @Tainted
                         Class<@Tainted ? extends @Tainted Writable> valueClass, @Tainted boolean dryrun,
                         @Tainted
                         Configuration conf) throws Exception {
    @Tainted
    String dr = (dryrun ? "[DRY RUN ] " : "");
    @Tainted
    Path data = new @Tainted Path(dir, DATA_FILE_NAME);
    @Tainted
    Path index = new @Tainted Path(dir, INDEX_FILE_NAME);
    @Tainted
    int indexInterval = conf.getInt(Writer.INDEX_INTERVAL, 128);
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new @Tainted Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    SequenceFile.@Tainted Reader dataReader = 
      new SequenceFile.@Tainted Reader(conf, SequenceFile.Reader.file(data));
    if (!dataReader.getKeyClass().equals(keyClass)) {
      throw new @Tainted Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() +
                          ", got " + dataReader.getKeyClass().getName());
    }
    if (!dataReader.getValueClass().equals(valueClass)) {
      throw new @Tainted Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() +
                          ", got " + dataReader.getValueClass().getName());
    }
    @Tainted
    long cnt = 0L;
    @Tainted
    Writable key = ReflectionUtils.newInstance(keyClass, conf);
    @Tainted
    Writable value = ReflectionUtils.newInstance(valueClass, conf);
    SequenceFile.@Tainted Writer indexWriter = null;
    if (!dryrun) {
      indexWriter = 
        SequenceFile.createWriter(conf, 
                                  SequenceFile.Writer.file(index), 
                                  SequenceFile.Writer.keyClass(keyClass), 
                                  SequenceFile.Writer.valueClass
                                    (LongWritable.class));
    }
    try {
      @Tainted
      long pos = 0L;
      @Tainted
      LongWritable position = new @Tainted LongWritable();
      while(dataReader.next(key, value)) {
        cnt++;
        if (cnt % indexInterval == 0) {
          position.set(pos);
          if (!dryrun) indexWriter.append(key, position);
        }
        pos = dataReader.getPosition();
      }
    } catch(@Tainted Throwable t) {
      // truncated data file. swallow it.
    }
    dataReader.close();
    if (!dryrun) indexWriter.close();
    return cnt;
  }


  public static void main(@Tainted String @Tainted [] args) throws Exception {
    @Tainted
    String usage = "Usage: MapFile inFile outFile";
      
    if (args.length != 2) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    @Tainted
    String in = args[0];
    @Tainted
    String out = args[1];

    @Tainted
    Configuration conf = new @Tainted Configuration();
    @Tainted
    FileSystem fs = FileSystem.getLocal(conf);
    MapFile.@Tainted Reader reader = new MapFile.@Tainted Reader(fs, in, conf);
    MapFile.@Tainted Writer writer =
      new MapFile.@Tainted Writer(conf, fs, out,
          reader.getKeyClass().asSubclass(WritableComparable.class),
          reader.getValueClass());

    @Tainted
    WritableComparable key =
      ReflectionUtils.newInstance(reader.getKeyClass().asSubclass(WritableComparable.class), conf);
    @Tainted
    Writable value =
      ReflectionUtils.newInstance(reader.getValueClass().asSubclass(Writable.class), conf);

    while (reader.next(key, value))               // copy all entries
      writer.append(key, value);

    writer.close();
  }

}
