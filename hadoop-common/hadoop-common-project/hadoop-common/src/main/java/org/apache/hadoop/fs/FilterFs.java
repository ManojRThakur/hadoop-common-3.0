package org.apache.hadoop.fs;
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
import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * A <code>FilterFs</code> contains some other file system, which it uses as its
 * basic file system, possibly transforming the data along the way or providing
 * additional functionality. The class <code>FilterFs</code> itself simply
 * overrides all methods of <code>AbstractFileSystem</code> with versions that
 * pass all requests to the contained file system. Subclasses of
 * <code>FilterFs</code> may further override some of these methods and may also
 * provide additional methods and fields.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class FilterFs extends @Tainted AbstractFileSystem {
  private final @Tainted AbstractFileSystem myFs;
  
  protected @Tainted AbstractFileSystem getMyFs(@Tainted FilterFs this) {
    return myFs;
  }
  
  protected @Tainted FilterFs(@Tainted AbstractFileSystem fs) throws IOException,
      URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(),
        fs.getUri().getAuthority() != null, fs.getUriDefaultPort());
    myFs = fs;
  }

  @Override
  public @Tainted Statistics getStatistics(@Tainted FilterFs this) {
    return myFs.getStatistics();
  }
  
  @Override
  public @Tainted Path makeQualified(@Tainted FilterFs this, @Tainted Path path) {
    return myFs.makeQualified(path);
  }

  @Override
  public @Tainted Path getInitialWorkingDirectory(@Tainted FilterFs this) {
    return myFs.getInitialWorkingDirectory();
  }
  
  @Override
  public @Tainted Path getHomeDirectory(@Tainted FilterFs this) {
    return myFs.getHomeDirectory();
  }
  
  @Override
  public @Tainted FSDataOutputStream createInternal(@Tainted FilterFs this, @Tainted Path f,
    @Tainted
    EnumSet<@Tainted CreateFlag> flag, @Tainted FsPermission absolutePermission, @Tainted int bufferSize,
    @Tainted
    short replication, @Tainted long blockSize, @Tainted Progressable progress,
    @Tainted
    ChecksumOpt checksumOpt, @Tainted boolean createParent) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.createInternal(f, flag, absolutePermission, bufferSize,
        replication, blockSize, progress, checksumOpt, createParent);
  }

  @Override
  public @Tainted boolean delete(@Tainted FilterFs this, @Tainted Path f, @Tainted boolean recursive) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.delete(f, recursive);
  }

  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted FilterFs this, @Tainted Path f, @Tainted long start, @Tainted long len)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileBlockLocations(f, start, len);
  }

  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted FilterFs this, @Tainted Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileChecksum(f);
  }

  @Override
  public @Tainted FileStatus getFileStatus(@Tainted FilterFs this, @Tainted Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileStatus(f);
  }

  @Override
  public @Tainted FileStatus getFileLinkStatus(@Tainted FilterFs this, final @Tainted Path f) 
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileLinkStatus(f);
  }
  
  @Override
  public @Tainted FsStatus getFsStatus(@Tainted FilterFs this, final @Tainted Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    return myFs.getFsStatus(f);
  }

  @Override
  public @Tainted FsStatus getFsStatus(@Tainted FilterFs this) throws IOException {
    return myFs.getFsStatus();
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted FilterFs this) throws IOException {
    return myFs.getServerDefaults();
  }
  

  @Override
  public @Tainted Path resolvePath(@Tainted FilterFs this, final @Tainted Path p) throws FileNotFoundException,
        UnresolvedLinkException, AccessControlException, IOException {
    return myFs.resolvePath(p);
  }

  @Override
  public @Tainted int getUriDefaultPort(@Tainted FilterFs this) {
    return myFs.getUriDefaultPort();
  }

  @Override
  public @Tainted URI getUri(@Tainted FilterFs this) {
    return myFs.getUri();
  }
  
  @Override
  public void checkPath(@Tainted FilterFs this, @Tainted Path path) {
    myFs.checkPath(path);
  }
  
  @Override
  public @Tainted String getUriPath(@Tainted FilterFs this, final @Tainted Path p) {
    return myFs.getUriPath(p);
  }
  
  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted FilterFs this, @Tainted Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.listStatus(f);
  }

  @Override
  public @Tainted RemoteIterator<@Tainted Path> listCorruptFileBlocks(@Tainted FilterFs this, @Tainted Path path)
    throws IOException {
    return myFs.listCorruptFileBlocks(path);
  }

  @Override
  public void mkdir(@Tainted FilterFs this, @Tainted Path dir, @Tainted FsPermission permission, @Tainted boolean createParent)
    throws IOException, UnresolvedLinkException {
    checkPath(dir);
    myFs.mkdir(dir, permission, createParent);
    
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted FilterFs this, final @Tainted Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    checkPath(f);
    return myFs.open(f);
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted FilterFs this, @Tainted Path f, @Tainted int bufferSize) 
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.open(f, bufferSize);
  }

  @Override
  public void renameInternal(@Tainted FilterFs this, @Tainted Path src, @Tainted Path dst) 
    throws IOException, UnresolvedLinkException {
    checkPath(src);
    checkPath(dst);
    myFs.rename(src, dst, Options.Rename.NONE);
  }

  @Override
  public void renameInternal(@Tainted FilterFs this, final @Tainted Path src, final @Tainted Path dst,
      @Tainted
      boolean overwrite) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    myFs.renameInternal(src, dst, overwrite);
  }
  
  @Override
  public void setOwner(@Tainted FilterFs this, @Tainted Path f, @Untainted String username, @Untainted String groupname)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setOwner(f, username, groupname);
    
  }

  @Override
  public void setPermission(@Tainted FilterFs this, @Tainted Path f, @Tainted FsPermission permission)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setPermission(f, permission);
  }

  @Override
  public @Tainted boolean setReplication(@Tainted FilterFs this, @Tainted Path f, @Tainted short replication)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.setReplication(f, replication);
  }

  @Override
  public void setTimes(@Tainted FilterFs this, @Tainted Path f, @Tainted long mtime, @Tainted long atime) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setTimes(f, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(@Tainted FilterFs this, @Tainted boolean verifyChecksum) 
      throws IOException, UnresolvedLinkException {
    myFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public @Tainted boolean supportsSymlinks(@Tainted FilterFs this) {
    return myFs.supportsSymlinks();
  }

  @Override
  public void createSymlink(@Tainted FilterFs this, @Tainted Path target, @Tainted Path link, @Tainted boolean createParent) 
    throws IOException, UnresolvedLinkException {
    myFs.createSymlink(target, link, createParent);
  }

  @Override
  public @Tainted Path getLinkTarget(@Tainted FilterFs this, final @Tainted Path f) throws IOException {
    return myFs.getLinkTarget(f);
  }
  
  @Override // AbstractFileSystem
  public @Tainted String getCanonicalServiceName(@Tainted FilterFs this) {
    return myFs.getCanonicalServiceName();
  }
  
  @Override // AbstractFileSystem
  public @Tainted List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> getDelegationTokens(@Tainted FilterFs this, @Tainted String renewer) throws IOException {
    return myFs.getDelegationTokens(renewer);
  }

  @Override
  public @Tainted boolean isValidName(@Tainted FilterFs this, @Tainted String src) {
    return myFs.isValidName(src);
  }
}
