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
package org.apache.hadoop.fs;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/** An abstract class representing file checksums for files. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileChecksum implements @Tainted Writable {
  /** The checksum algorithm name */ 
  public abstract @Tainted String getAlgorithmName(@Tainted FileChecksum this);

  /** The length of the checksum in bytes */ 
  public abstract @Tainted int getLength(@Tainted FileChecksum this);

  /** The value of the checksum in bytes */ 
  public abstract @Tainted byte @Tainted [] getBytes(@Tainted FileChecksum this);

  /** Return true if both the algorithms and the values are the same. */
  @Override
  public @Tainted boolean equals(@Tainted FileChecksum this, @Tainted Object other) {
    if (other == this) {
      return true;
    }
    if (other == null || !(other instanceof @Tainted FileChecksum)) {
      return false;
    }

    final @Tainted FileChecksum that = (@Tainted FileChecksum)other;
    return this.getAlgorithmName().equals(that.getAlgorithmName())
      && Arrays.equals(this.getBytes(), that.getBytes());
  }
  
  @Override
  public @Tainted int hashCode(@Tainted FileChecksum this) {
    return getAlgorithmName().hashCode() ^ Arrays.hashCode(getBytes());
  }
}