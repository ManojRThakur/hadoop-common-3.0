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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * Implementation of AbstractFileSystem based on the existing implementation of 
 * {@link FileSystem}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class DelegateToFileSystem extends @Tainted AbstractFileSystem {
  protected final @Tainted FileSystem fsImpl;
  
  protected @Tainted DelegateToFileSystem(@Tainted URI theUri, @Tainted FileSystem theFsImpl,
      @Tainted
      Configuration conf, @Tainted String supportedScheme, @Tainted boolean authorityRequired)
      throws IOException, URISyntaxException {
    super(theUri, supportedScheme, authorityRequired, 
        FileSystem.getDefaultUri(conf).getPort());
    fsImpl = theFsImpl;
    fsImpl.initialize(theUri, conf);
    fsImpl.statistics = getStatistics();
  }

  @Override
  public @Tainted Path getInitialWorkingDirectory(@Tainted DelegateToFileSystem this) {
    return fsImpl.getInitialWorkingDirectory();
  }
  
  @Override
  @SuppressWarnings("deprecation") // call to primitiveCreate
  public @Tainted FSDataOutputStream createInternal (@Tainted DelegateToFileSystem this, @Tainted Path f,
      @Tainted
      EnumSet<@Tainted CreateFlag> flag, @Tainted FsPermission absolutePermission, @Tainted int bufferSize,
      @Tainted
      short replication, @Tainted long blockSize, @Tainted Progressable progress,
      @Tainted
      ChecksumOpt checksumOpt, @Tainted boolean createParent) throws IOException {
    checkPath(f);
    
    // Default impl assumes that permissions do not matter
    // calling the regular create is good enough.
    // FSs that implement permissions should override this.

    if (!createParent) { // parent must exist.
      // since this.create makes parent dirs automatically
      // we must throw exception if parent does not exist.
      final @Tainted FileStatus stat = getFileStatus(f.getParent());
      if (stat == null) {
        throw new @Tainted FileNotFoundException("Missing parent:" + f);
      }
      if (!stat.isDirectory()) {
          throw new @Tainted ParentNotDirectoryException("parent is not a dir:" + f);
      }
      // parent does exist - go ahead with create of file.
    }
    return fsImpl.primitiveCreate(f, absolutePermission, flag,
        bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public @Tainted boolean delete(@Tainted DelegateToFileSystem this, @Tainted Path f, @Tainted boolean recursive) throws IOException {
    checkPath(f);
    return fsImpl.delete(f, recursive);
  }

  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted DelegateToFileSystem this, @Tainted Path f, @Tainted long start, @Tainted long len)
      throws IOException {
    checkPath(f);
    return fsImpl.getFileBlockLocations(f, start, len);
  }

  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted DelegateToFileSystem this, @Tainted Path f) throws IOException {
    checkPath(f);
    return fsImpl.getFileChecksum(f);
  }

  @Override
  public @Tainted FileStatus getFileStatus(@Tainted DelegateToFileSystem this, @Tainted Path f) throws IOException {
    checkPath(f);
    return fsImpl.getFileStatus(f);
  }

  @Override
  public @Tainted FileStatus getFileLinkStatus(@Tainted DelegateToFileSystem this, final @Tainted Path f) throws IOException {
    @Tainted
    FileStatus status = fsImpl.getFileLinkStatus(f);
    // FileSystem#getFileLinkStatus qualifies the link target
    // AbstractFileSystem needs to return it plain since it's qualified
    // in FileContext, so re-get and set the plain target
    if (status.isSymlink()) {
      status.setSymlink(fsImpl.getLinkTarget(f));
    }
    return status;
  }

  @Override
  public @Tainted FsStatus getFsStatus(@Tainted DelegateToFileSystem this) throws IOException {
    return fsImpl.getStatus();
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted DelegateToFileSystem this) throws IOException {
    return fsImpl.getServerDefaults();
  }
  
  @Override
  public @Tainted Path getHomeDirectory(@Tainted DelegateToFileSystem this) {
    return fsImpl.getHomeDirectory();
  }

  @Override
  public @Tainted int getUriDefaultPort(@Tainted DelegateToFileSystem this) {
    return 0;
  }

  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted DelegateToFileSystem this, @Tainted Path f) throws IOException {
    checkPath(f);
    return fsImpl.listStatus(f);
  }

  @Override
  @SuppressWarnings("deprecation") // call to primitiveMkdir
  public void mkdir(@Tainted DelegateToFileSystem this, @Tainted Path dir, @Tainted FsPermission permission, @Tainted boolean createParent)
      throws IOException {
    checkPath(dir);
    fsImpl.primitiveMkdir(dir, permission, createParent);
    
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted DelegateToFileSystem this, @Tainted Path f, @Tainted int bufferSize) throws IOException {
    checkPath(f);
    return fsImpl.open(f, bufferSize);
  }

  @Override
  @SuppressWarnings("deprecation") // call to rename
  public void renameInternal(@Tainted DelegateToFileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    checkPath(src);
    checkPath(dst);
    fsImpl.rename(src, dst, Options.Rename.NONE);
  }

  @Override
  public void setOwner(@Tainted DelegateToFileSystem this, @Tainted Path f, @Untainted String username, @Untainted String groupname)
      throws IOException {
    checkPath(f);
    fsImpl.setOwner(f, username, groupname);
  }

  @Override
  public void setPermission(@Tainted DelegateToFileSystem this, @Tainted Path f, @Tainted FsPermission permission)
      throws IOException {
    checkPath(f);
    fsImpl.setPermission(f, permission);
  }

  @Override
  public @Tainted boolean setReplication(@Tainted DelegateToFileSystem this, @Tainted Path f, @Tainted short replication)
      throws IOException {
    checkPath(f);
    return fsImpl.setReplication(f, replication);
  }

  @Override
  public void setTimes(@Tainted DelegateToFileSystem this, @Tainted Path f, @Tainted long mtime, @Tainted long atime) throws IOException {
    checkPath(f);
    fsImpl.setTimes(f, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(@Tainted DelegateToFileSystem this, @Tainted boolean verifyChecksum) throws IOException {
    fsImpl.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public @Tainted boolean supportsSymlinks(@Tainted DelegateToFileSystem this) {
    return fsImpl.supportsSymlinks();
  }  
  
  @Override
  public void createSymlink(@Tainted DelegateToFileSystem this, @Tainted Path target, @Tainted Path link, @Tainted boolean createParent) 
      throws IOException { 
    fsImpl.createSymlink(target, link, createParent);
  } 
  
  @Override
  public @Tainted Path getLinkTarget(@Tainted DelegateToFileSystem this, final @Tainted Path f) throws IOException {
    return fsImpl.getLinkTarget(f);
  }

  @Override //AbstractFileSystem
  public @Tainted String getCanonicalServiceName(@Tainted DelegateToFileSystem this) {
    return fsImpl.getCanonicalServiceName();
  }
  
  @Override //AbstractFileSystem
  public @Tainted List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> getDelegationTokens(@Tainted DelegateToFileSystem this, @Tainted String renewer) throws IOException {
    return Arrays.asList(fsImpl.addDelegationTokens(renewer, null));
  }
}
