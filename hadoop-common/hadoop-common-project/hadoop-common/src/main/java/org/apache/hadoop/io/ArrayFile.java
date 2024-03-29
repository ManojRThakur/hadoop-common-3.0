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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;


/** A dense file-based mapping from integers to values. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayFile extends @Tainted MapFile {

  protected @Tainted ArrayFile() {}                            // no public ctor

  /** Write a new array file. */
  public static class Writer extends MapFile.@Tainted Writer {
    private @Tainted LongWritable count = new @Tainted LongWritable(0);

    /** Create the named file for values of the named class. */
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs,
                  @Tainted
                  String file, @Tainted Class<@Tainted ? extends @Tainted Writable> valClass)
      throws IOException {
      super(conf, new @Tainted Path(file), keyClass(LongWritable.class), 
            valueClass(valClass));
    }

    /** Create the named file for values of the named class. */
    public @Tainted Writer(@Tainted Configuration conf, @Tainted FileSystem fs,
                  @Tainted
                  String file, @Tainted Class<@Tainted ? extends @Tainted Writable> valClass,
                  @Tainted
                  CompressionType compress, @Tainted Progressable progress)
      throws IOException {
      super(conf, new @Tainted Path(file), 
            keyClass(LongWritable.class), 
            valueClass(valClass), 
            compression(compress), 
            progressable(progress));
    }

    /** Append a value to the file. */
    public synchronized void append(ArrayFile.@Tainted Writer this, @Tainted Writable value) throws IOException {
      super.append(count, value);                 // add to map
      count.set(count.get()+1);                   // increment count
    }
  }

  /** Provide access to an existing array file. */
  public static class Reader extends MapFile.@Tainted Reader {
    private @Tainted LongWritable key = new @Tainted LongWritable();

    /** Construct an array reader for the named file.*/
    public @Tainted Reader(@Tainted FileSystem fs, @Tainted String file, 
                  @Tainted
                  Configuration conf) throws IOException {
      super(new @Tainted Path(file), conf);
    }

    /** Positions the reader before its <code>n</code>th value. */
    public synchronized void seek(ArrayFile.@Tainted Reader this, @Tainted long n) throws IOException {
      key.set(n);
      seek(key);
    }

    /** Read and return the next value in the file. */
    public synchronized @Tainted Writable next(ArrayFile.@Tainted Reader this, @Tainted Writable value) throws IOException {
      return next(key, value) ? value : null;
    }

    /** Returns the key associated with the most recent call to {@link
     * #seek(long)}, {@link #next(Writable)}, or {@link
     * #get(long,Writable)}. */
    public synchronized @Tainted long key(ArrayFile.@Tainted Reader this) throws IOException {
      return key.get();
    }

    /** Return the <code>n</code>th value in the file. */
    public synchronized @Tainted Writable get(ArrayFile.@Tainted Reader this, @Tainted long n, @Tainted Writable value)
      throws IOException {
      key.set(n);
      return get(key, value);
    }
  }

}
