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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * Holds basic metadata for a file stored in a {@link NativeFileSystemStore}.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FileMetadata {
  private final @Tainted String key;
  private final @Tainted long length;
  private final @Tainted long lastModified;
  
  public @Tainted FileMetadata(@Tainted String key, @Tainted long length, @Tainted long lastModified) {
    this.key = key;
    this.length = length;
    this.lastModified = lastModified;
  }
  
  public @Tainted String getKey(@Tainted FileMetadata this) {
    return key;
  }
  
  public @Tainted long getLength(@Tainted FileMetadata this) {
    return length;
  }

  public @Tainted long getLastModified(@Tainted FileMetadata this) {
    return lastModified;
  }
  
  @Override
  public @Tainted String toString(@Tainted FileMetadata this) {
    return "FileMetadata[" + key + ", " + length + ", " + lastModified + "]";
  }
  
}
