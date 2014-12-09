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
package org.apache.hadoop.fs.viewfs;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;


/**
 * This class is needed to address the  problem described in
 * {@link ViewFileSystem#getFileStatus(org.apache.hadoop.fs.Path)} and
 * {@link ViewFs#getFileStatus(org.apache.hadoop.fs.Path)}
 */
class ViewFsFileStatus extends @Tainted FileStatus {
   final @Tainted FileStatus myFs;
   @Tainted
   Path modifiedPath;
   @Tainted
   ViewFsFileStatus(@Tainted FileStatus fs, @Tainted Path newPath) {
     myFs = fs;
     modifiedPath = newPath;
   }
   
   @Override
   public @Tainted boolean equals(@Tainted ViewFsFileStatus this, @Tainted Object o) {
     return super.equals(o);
   }
   
   @Override
  public @Tainted int hashCode(@Tainted ViewFsFileStatus this) {
     return super.hashCode();
   }
   
   @Override
   public @Tainted long getLen(@Tainted ViewFsFileStatus this) {
     return myFs.getLen();
   }

   @Override
   public @Tainted boolean isFile(@Tainted ViewFsFileStatus this) {
     return myFs.isFile();
   }

   @Override
   public @Tainted boolean isDirectory(@Tainted ViewFsFileStatus this) {
     return  myFs.isDirectory();
   }
   
   @Override
   @SuppressWarnings("deprecation")
   public @Tainted boolean isDir(@Tainted ViewFsFileStatus this) {
     return myFs.isDirectory();
   }
   
   @Override
   public @Tainted boolean isSymlink(@Tainted ViewFsFileStatus this) {
     return myFs.isSymlink();
   }

   @Override
   public @Tainted long getBlockSize(@Tainted ViewFsFileStatus this) {
     return myFs.getBlockSize();
   }

   @Override
   public @Tainted short getReplication(@Tainted ViewFsFileStatus this) {
     return myFs.getReplication();
   }

   @Override
   public @Tainted long getModificationTime(@Tainted ViewFsFileStatus this) {
     return myFs.getModificationTime();
   }

   @Override
   public @Tainted long getAccessTime(@Tainted ViewFsFileStatus this) {
     return myFs.getAccessTime();
   }

   @Override
   public @Tainted FsPermission getPermission(@Tainted ViewFsFileStatus this) {
     return myFs.getPermission();
   }
   
   @Override
   public @Untainted String getOwner(@Tainted ViewFsFileStatus this) {
     return myFs.getOwner();
   }
   
   @Override
   public @Untainted String getGroup(@Tainted ViewFsFileStatus this) {
     return myFs.getGroup();
   }
   
   @Override
   public @Tainted Path getPath(@Tainted ViewFsFileStatus this) {
     return modifiedPath;
   }
   
   @Override
   public void setPath(@Tainted ViewFsFileStatus this, final @Tainted Path p) {
     modifiedPath = p;
   }

   @Override
   public @Tainted Path getSymlink(@Tainted ViewFsFileStatus this) throws IOException {
     return myFs.getSymlink();
   }
}

