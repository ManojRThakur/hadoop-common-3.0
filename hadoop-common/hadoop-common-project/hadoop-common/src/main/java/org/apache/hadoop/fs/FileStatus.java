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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** Interface that represents the client side information for a file.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileStatus implements @Tainted Writable, @Tainted Comparable {

  private @Tainted Path path;
  private @Tainted long length;
  private @Tainted boolean isdir;
  private @Tainted short block_replication;
  private @Tainted long blocksize;
  private @Tainted long modification_time;
  private @Tainted long access_time;
  private @Tainted FsPermission permission;
  private @Untainted String owner;
  private @Untainted String group;
  private @Tainted Path symlink;
  
  public @Tainted FileStatus() { this(0, false, 0, 0, 0, 0, null, null, null, null); }
  
  //We should deprecate this soon?
  public @Tainted FileStatus(@Tainted long length, @Tainted boolean isdir, @Tainted int block_replication,
                    @Tainted
                    long blocksize, @Tainted long modification_time, @Tainted Path path) {

    this(length, isdir, block_replication, blocksize, modification_time,
         0, null, null, null, path);
  }

  /**
   * Constructor for file systems on which symbolic links are not supported
   */
  public @Tainted FileStatus(@Tainted long length, @Tainted boolean isdir,
                    @Tainted
                    int block_replication,
                    @Tainted
                    long blocksize, @Tainted long modification_time, @Tainted long access_time,
                    @Tainted
                    FsPermission permission, @Untainted String owner, @Untainted String group, 
                    @Tainted
                    Path path) {
    this(length, isdir, block_replication, blocksize, modification_time,
         access_time, permission, owner, group, null, path);
  }

  public @Tainted FileStatus(@Tainted long length, @Tainted boolean isdir,
                    @Tainted
                    int block_replication,
                    @Tainted
                    long blocksize, @Tainted long modification_time, @Tainted long access_time,
                    @Tainted
                    FsPermission permission, @Untainted String owner, @Untainted String group, 
                    @Tainted
                    Path symlink,
                    @Tainted
                    Path path) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = (@Tainted short)block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
    if (permission != null) {
      this.permission = permission;
    } else if (isdir) {
      this.permission = FsPermission.getDirDefault();
    } else if (symlink!=null) {
      this.permission = FsPermission.getDefault();
    } else {
      this.permission = FsPermission.getFileDefault();
    }
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.symlink = symlink;
    this.path = path;
    // The variables isdir and symlink indicate the type:
    // 1. isdir implies directory, in which case symlink must be null.
    // 2. !isdir implies a file or symlink, symlink != null implies a
    //    symlink, otherwise it's a file.
    assert (isdir && symlink == null) || !isdir;
  }

  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  public @Tainted long getLen(@Tainted FileStatus this) {
    return length;
  }

  /**
   * Is this a file?
   * @return true if this is a file
   */
  public @Tainted boolean isFile(@Tainted FileStatus this) {
    return !isdir && !isSymlink();
  }

  /**
   * Is this a directory?
   * @return true if this is a directory
   */
  public @Tainted boolean isDirectory(@Tainted FileStatus this) {
    return isdir;
  }
  
  /**
   * Old interface, instead use the explicit {@link FileStatus#isFile()}, 
   * {@link FileStatus#isDirectory()}, and {@link FileStatus#isSymlink()} 
   * @return true if this is a directory.
   * @deprecated Use {@link FileStatus#isFile()},  
   * {@link FileStatus#isDirectory()}, and {@link FileStatus#isSymlink()} 
   * instead.
   */
  @Deprecated
  public @Tainted boolean isDir(@Tainted FileStatus this) {
    return isdir;
  }
  
  /**
   * Is this a symbolic link?
   * @return true if this is a symbolic link
   */
  public @Tainted boolean isSymlink(@Tainted FileStatus this) {
    return symlink != null;
  }

  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  public @Tainted long getBlockSize(@Tainted FileStatus this) {
    return blocksize;
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  public @Tainted short getReplication(@Tainted FileStatus this) {
    return block_replication;
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  public @Tainted long getModificationTime(@Tainted FileStatus this) {
    return modification_time;
  }

  /**
   * Get the access time of the file.
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  public @Tainted long getAccessTime(@Tainted FileStatus this) {
    return access_time;
  }

  /**
   * Get FsPermission associated with the file.
   * @return permssion. If a filesystem does not have a notion of permissions
   *         or if permissions could not be determined, then default 
   *         permissions equivalent of "rwxrwxrwx" is returned.
   */
  public @Tainted FsPermission getPermission(@Tainted FileStatus this) {
    return permission;
  }
  
  /**
   * Get the owner of the file.
   * @return owner of the file. The string could be empty if there is no
   *         notion of owner of a file in a filesystem or if it could not 
   *         be determined (rare).
   */
  public @Untainted String getOwner(@Tainted FileStatus this) {
    return owner;
  }
  
  /**
   * Get the group associated with the file.
   * @return group for the file. The string could be empty if there is no
   *         notion of group of a file in a filesystem or if it could not 
   *         be determined (rare).
   */
  public @Untainted String getGroup(@Tainted FileStatus this) {
    return group;
  }
  
  public @Tainted Path getPath(@Tainted FileStatus this) {
    return path;
  }
  
  public void setPath(@Tainted FileStatus this, final @Tainted Path p) {
    path = p;
  }

  /* These are provided so that these values could be loaded lazily 
   * by a filesystem (e.g. local file system).
   */
  
  /**
   * Sets permission.
   * @param permission if permission is null, default value is set
   */
  protected void setPermission(@Tainted FileStatus this, @Tainted FsPermission permission) {
    this.permission = (permission == null) ? 
                      FsPermission.getFileDefault() : permission;
  }
  
  /**
   * Sets owner.
   * @param owner if it is null, default value is set
   */  
  protected void setOwner(@Tainted FileStatus this, @Untainted String owner) {
    this.owner = (owner == null) ? "" : owner;
  }
  
  /**
   * Sets group.
   * @param group if it is null, default value is set
   */  
  protected void setGroup(@Tainted FileStatus this, @Untainted String group) {
    this.group = (group == null) ? "" :  group;
  }

  /**
   * @return The contents of the symbolic link.
   */
  public @Tainted Path getSymlink(@Tainted FileStatus this) throws IOException {
    if (!isSymlink()) {
      throw new @Tainted IOException("Path " + path + " is not a symbolic link");
    }
    return symlink;
  }

  public void setSymlink(@Tainted FileStatus this, final @Tainted Path p) {
    symlink = p;
  }
  
  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public void write(@Tainted FileStatus this, @Tainted DataOutput out) throws IOException {
    Text.writeString(out, getPath().toString(), Text.DEFAULT_MAX_LEN);
    out.writeLong(getLen());
    out.writeBoolean(isDirectory());
    out.writeShort(getReplication());
    out.writeLong(getBlockSize());
    out.writeLong(getModificationTime());
    out.writeLong(getAccessTime());
    getPermission().write(out);
    Text.writeString(out, getOwner(), Text.DEFAULT_MAX_LEN);
    Text.writeString(out, getGroup(), Text.DEFAULT_MAX_LEN);
    out.writeBoolean(isSymlink());
    if (isSymlink()) {
      Text.writeString(out, getSymlink().toString(), Text.DEFAULT_MAX_LEN);
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  @Override
  public void readFields(@Tainted FileStatus this, @Tainted DataInput in) throws IOException {
    @Tainted
    String strPath = Text.readString(in, Text.DEFAULT_MAX_LEN);
    this.path = new @Tainted Path(strPath);
    this.length = in.readLong();
    this.isdir = in.readBoolean();
    this.block_replication = in.readShort();
    blocksize = in.readLong();
    modification_time = in.readLong();
    access_time = in.readLong();
    permission.readFields(in);
    owner = (@Untainted String) Text.readString(in, Text.DEFAULT_MAX_LEN);
    group = (@Untainted String) Text.readString(in, Text.DEFAULT_MAX_LEN);
    if (in.readBoolean()) {
      this.symlink = new @Tainted Path(Text.readString(in, Text.DEFAULT_MAX_LEN));
    } else {
      this.symlink = null;
    }
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
  public @Tainted int compareTo(@Tainted FileStatus this, @Tainted Object o) {
    @Tainted
    FileStatus other = (@Tainted FileStatus)o;
    return this.getPath().compareTo(other.getPath());
  }
  
  /** Compare if this object is equal to another object
   * @param   o the object to be compared.
   * @return  true if two file status has the same path name; false if not.
   */
  @Override
  public @Tainted boolean equals(@Tainted FileStatus this, @Tainted Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof @Tainted FileStatus)) {
      return false;
    }
    @Tainted
    FileStatus other = (@Tainted FileStatus)o;
    return this.getPath().equals(other.getPath());
  }
  
  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return  a hash code value for the path name.
   */
  @Override
  public @Tainted int hashCode(@Tainted FileStatus this) {
    return getPath().hashCode();
  }
  
  @Override
  public @Tainted String toString(@Tainted FileStatus this) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder();
    sb.append(getClass().getSimpleName()); 
    sb.append("{");
    sb.append("path=" + path);
    sb.append("; isDirectory=" + isdir);
    if(!isDirectory()){
      sb.append("; length=" + length);
      sb.append("; replication=" + block_replication);
      sb.append("; blocksize=" + blocksize);
    }
    sb.append("; modification_time=" + modification_time);
    sb.append("; access_time=" + access_time);
    sb.append("; owner=" + owner);
    sb.append("; group=" + group);
    sb.append("; permission=" + permission);
    sb.append("; isSymlink=" + isSymlink());
    if(isSymlink()) {
      sb.append("; symlink=" + symlink);
    }
    sb.append("}");
    return sb.toString();
  }
}
