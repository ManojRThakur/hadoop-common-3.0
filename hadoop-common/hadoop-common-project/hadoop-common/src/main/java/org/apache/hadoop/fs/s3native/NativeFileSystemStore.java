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

package org.apache.hadoop.fs.s3native;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface NativeFileSystemStore {
  
  void initialize(@Tainted NativeFileSystemStore this, @Tainted URI uri, @Tainted Configuration conf) throws IOException;
  
  void storeFile(@Tainted NativeFileSystemStore this, @Tainted String key, @Tainted File file, @Tainted byte @Tainted [] md5Hash) throws IOException;
  void storeEmptyFile(@Tainted NativeFileSystemStore this, @Tainted String key) throws IOException;
  
  @Tainted
  FileMetadata retrieveMetadata(@Tainted NativeFileSystemStore this, @Tainted String key) throws IOException;
  @Tainted
  InputStream retrieve(@Tainted NativeFileSystemStore this, @Tainted String key) throws IOException;
  @Tainted
  InputStream retrieve(@Tainted NativeFileSystemStore this, @Tainted String key, @Tainted long byteRangeStart) throws IOException;
  
  @Tainted
  PartialListing list(@Tainted NativeFileSystemStore this, @Tainted String prefix, @Tainted int maxListingLength) throws IOException;
  @Tainted
  PartialListing list(@Tainted NativeFileSystemStore this, @Tainted String prefix, @Tainted int maxListingLength, @Tainted String priorLastKey, @Tainted boolean recursive)
    throws IOException;
  
  void delete(@Tainted NativeFileSystemStore this, @Tainted String key) throws IOException;

  void copy(@Tainted NativeFileSystemStore this, @Tainted String srcKey, @Tainted String dstKey) throws IOException;
  
  /**
   * Delete all keys with the given prefix. Used for testing.
   * @throws IOException
   */
  void purge(@Tainted NativeFileSystemStore this, @Tainted String prefix) throws IOException;
  
  /**
   * Diagnostic method to dump state to the console.
   * @throws IOException
   */
  void dump(@Tainted NativeFileSystemStore this) throws IOException;
}
