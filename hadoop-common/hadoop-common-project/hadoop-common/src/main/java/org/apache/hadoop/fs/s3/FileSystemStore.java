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

package org.apache.hadoop.fs.s3;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A facility for storing and retrieving {@link INode}s and {@link Block}s.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface FileSystemStore {
  
  void initialize(@Tainted FileSystemStore this, @Tainted URI uri, @Tainted Configuration conf) throws IOException;
  @Tainted
  String getVersion(@Tainted FileSystemStore this) throws IOException;

  void storeINode(@Tainted FileSystemStore this, @Tainted Path path, @Tainted INode inode) throws IOException;
  void storeBlock(@Tainted FileSystemStore this, @Tainted Block block, @Tainted File file) throws IOException;
  
  @Tainted
  boolean inodeExists(@Tainted FileSystemStore this, @Tainted Path path) throws IOException;
  @Tainted
  boolean blockExists(@Tainted FileSystemStore this, @Tainted long blockId) throws IOException;

  @Tainted
  INode retrieveINode(@Tainted FileSystemStore this, @Tainted Path path) throws IOException;
  @Tainted
  File retrieveBlock(@Tainted FileSystemStore this, @Tainted Block block, @Tainted long byteRangeStart) throws IOException;

  void deleteINode(@Tainted FileSystemStore this, @Tainted Path path) throws IOException;
  void deleteBlock(@Tainted FileSystemStore this, @Tainted Block block) throws IOException;

  @Tainted
  Set<@Tainted Path> listSubPaths(@Tainted FileSystemStore this, @Tainted Path path) throws IOException;
  @Tainted
  Set<@Tainted Path> listDeepSubPaths(@Tainted FileSystemStore this, @Tainted Path path) throws IOException;

  /**
   * Delete everything. Used for testing.
   * @throws IOException
   */
  void purge(@Tainted FileSystemStore this) throws IOException;
  
  /**
   * Diagnostic method to dump all INodes to the console.
   * @throws IOException
   */
  void dump(@Tainted FileSystemStore this) throws IOException;
}
