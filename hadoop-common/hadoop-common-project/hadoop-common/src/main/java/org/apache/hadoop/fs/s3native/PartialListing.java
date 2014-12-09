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
 * Holds information on a directory listing for a
 * {@link NativeFileSystemStore}.
 * This includes the {@link FileMetadata files} and directories
 * (their names) contained in a directory.
 * </p>
 * <p>
 * This listing may be returned in chunks, so a <code>priorLastKey</code>
 * is provided so that the next chunk may be requested.
 * </p>
 * @see NativeFileSystemStore#list(String, int, String)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class PartialListing {
  
  private final @Tainted String priorLastKey;
  private final @Tainted FileMetadata @Tainted [] files;
  private final @Tainted String @Tainted [] commonPrefixes;
  
  public @Tainted PartialListing(@Tainted String priorLastKey, @Tainted FileMetadata @Tainted [] files,
      @Tainted
      String @Tainted [] commonPrefixes) {
    this.priorLastKey = priorLastKey;
    this.files = files;
    this.commonPrefixes = commonPrefixes;
  }

  public @Tainted FileMetadata @Tainted [] getFiles(@Tainted PartialListing this) {
    return files;
  }

  public @Tainted String @Tainted [] getCommonPrefixes(@Tainted PartialListing this) {
    return commonPrefixes;
  }

  public @Tainted String getPriorLastKey(@Tainted PartialListing this) {
    return priorLastKey;
  }
  
}
