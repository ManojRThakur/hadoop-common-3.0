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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * This class defines a FileStatus that includes a file's block locations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LocatedFileStatus extends @Tainted FileStatus {
  private @Tainted BlockLocation @Tainted [] locations;

  /**
   * Constructor 
   * @param stat a file status
   * @param locations a file's block locations
   */
  public @Tainted LocatedFileStatus(@Tainted FileStatus stat, @Tainted BlockLocation @Tainted [] locations)
  throws IOException {
    this(stat.getLen(), stat.isDirectory(), stat.getReplication(),
        stat.getBlockSize(), stat.getModificationTime(),
        stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
        stat.getGroup(), null, stat.getPath(), locations);
    if (isSymlink()) {
      setSymlink(stat.getSymlink());
    }
  }

  /**
   * Constructor
   * 
   * @param length a file's length
   * @param isdir if the path is a directory
   * @param block_replication the file's replication factor
   * @param blocksize a file's block size
   * @param modification_time a file's modification time
   * @param access_time a file's access time
   * @param permission a file's permission
   * @param owner a file's owner
   * @param group a file's group
   * @param symlink symlink if the path is a symbolic link
   * @param path the path's qualified name
   * @param locations a file's block locations
   */
  public @Tainted LocatedFileStatus(@Tainted long length, @Tainted boolean isdir,
          @Tainted
          int block_replication,
          @Tainted
          long blocksize, @Tainted long modification_time, @Tainted long access_time,
          @Tainted
          FsPermission permission, @Untainted String owner, @Untainted String group, 
          @Tainted
          Path symlink,
          @Tainted
          Path path,
          @Tainted
          BlockLocation @Tainted [] locations) {
	  super(length, isdir, block_replication, blocksize, modification_time,
			  access_time, permission, owner, group, symlink, path);
	  this.locations = locations;
  }
  
  /**
   * Get the file's block locations
   * @return the file's block locations
   */
  public @Tainted BlockLocation @Tainted [] getBlockLocations(@Tainted LocatedFileStatus this) {
	  return locations;
  }
  
  /**
   * Compare this object to another object
   * 
   * @param   o the object to be compared.
   * @return  a negative integer, zero, or a positive integer as this object
   *   is less than, equal to, or greater than the specified object.
   * 
   * @throws ClassCastException if the specified object's is not of 
   *         type FileStatus
   */
  @Override
  public @Tainted int compareTo(@Tainted LocatedFileStatus this, @Tainted Object o) {
    return super.compareTo(o);
  }
  
  /** Compare if this object is equal to another object
   * @param   o the object to be compared.
   * @return  true if two file status has the same path name; false if not.
   */
  @Override
  public @Tainted boolean equals(@Tainted LocatedFileStatus this, @Tainted Object o) {
    return super.equals(o);
  }
  
  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return  a hash code value for the path name.
   */
  @Override
  public @Tainted int hashCode(@Tainted LocatedFileStatus this) {
    return super.hashCode();
  }
}
