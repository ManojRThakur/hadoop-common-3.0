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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * <code>ChrootedFs</code> is a file system with its root some path
 * below the root of its base file system.
 * Example: For a base file system hdfs://nn1/ with chRoot at /usr/foo, the
 * members will be setup as shown below.
 * <ul>
 * <li>myFs is the base file system and points to hdfs at nn1</li>
 * <li>myURI is hdfs://nn1/user/foo</li>
 * <li>chRootPathPart is /user/foo</li>
 * <li>workingDir is a directory related to chRoot</li>
 * </ul>
 * 
 * The paths are resolved as follows by ChRootedFileSystem:
 * <ul>
 * <li> Absolute path /a/b/c is resolved to /user/foo/a/b/c at myFs</li>
 * <li> Relative path x/y is resolved to /user/foo/<workingDir>/x/y</li>
 * </ul>

 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
class ChRootedFs extends @Tainted AbstractFileSystem {
  private final @Tainted AbstractFileSystem myFs;  // the base file system whose root is changed
  private final @Tainted URI myUri; // the base URI + the chroot
  private final @Tainted Path chRootPathPart; // the root below the root of the base
  private final @Tainted String chRootPathPartString;
  
  protected @Tainted AbstractFileSystem getMyFs(@Tainted ChRootedFs this) {
    return myFs;
  }
  
  /**
   * 
   * @param path
   * @return return full path including the chroot
   */
  protected @Tainted Path fullPath(@Tainted ChRootedFs this, final @Tainted Path path) {
    super.checkPath(path);
    return new @Tainted Path((chRootPathPart.isRoot() ? "" : chRootPathPartString)
        + path.toUri().getPath());
  }

  @Override
  public @Tainted boolean isValidName(@Tainted ChRootedFs this, @Tainted String src) {
    return myFs.isValidName(fullPath(new @Tainted Path(src)).toUri().toString());
  }

  public @Tainted ChRootedFs(final @Tainted AbstractFileSystem fs, final @Tainted Path theRoot)
    throws URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(),
        fs.getUri().getAuthority() != null, fs.getUriDefaultPort());
    myFs = fs;
    myFs.checkPath(theRoot);
    chRootPathPart = new @Tainted Path(myFs.getUriPath(theRoot));
    chRootPathPartString = chRootPathPart.toUri().getPath();
    /*
     * We are making URI include the chrootedPath: e.g. file:///chrootedPath.
     * This is questionable since Path#makeQualified(uri, path) ignores
     * the pathPart of a uri. Since this class is internal we can ignore
     * this issue but if we were to make it external then this needs
     * to be resolved.
     */
    // Handle the two cases:
    //              scheme:/// and scheme://authority/
    myUri = new @Tainted URI(myFs.getUri().toString() + 
        (myFs.getUri().getAuthority() == null ? "" :  Path.SEPARATOR) +
          chRootPathPart.toUri().getPath().substring(1));
    super.checkPath(theRoot);
  }
  
  @Override
  public @Tainted URI getUri(@Tainted ChRootedFs this) {
    return myUri;
  }

  
  /**
   *  
   * Strip out the root from the path.
   * 
   * @param p - fully qualified path p
   * @return -  the remaining path  without the begining /
   */
  public @Tainted String stripOutRoot(@Tainted ChRootedFs this, final @Tainted Path p) {
    try {
     checkPath(p);
    } catch (@Tainted IllegalArgumentException e) {
      throw new @Tainted RuntimeException("Internal Error - path " + p +
          " should have been with URI" + myUri);
    }
    @Tainted
    String pathPart = p.toUri().getPath();
    return  (pathPart.length() == chRootPathPartString.length()) ?
        "" : pathPart.substring(chRootPathPartString.length() +
            (chRootPathPart.isRoot() ? 0 : 1));
  }
  

  @Override
  public @Tainted Path getHomeDirectory(@Tainted ChRootedFs this) {
    return myFs.getHomeDirectory();
  }
  
  @Override
  public @Tainted Path getInitialWorkingDirectory(@Tainted ChRootedFs this) {
    /*
     * 3 choices here: return null or / or strip out the root out of myFs's
     *  inital wd. 
     * Only reasonable choice for initialWd for chrooted fds is null 
     */
    return null;
  }
  
  
  public @Tainted Path getResolvedQualifiedPath(@Tainted ChRootedFs this, final @Tainted Path f)
      throws FileNotFoundException {
    return myFs.makeQualified(
        new @Tainted Path(chRootPathPartString + f.toUri().toString()));
  }
  
  @Override
  public @Tainted FSDataOutputStream createInternal(@Tainted ChRootedFs this, final @Tainted Path f,
      final @Tainted EnumSet<@Tainted CreateFlag> flag, final @Tainted FsPermission absolutePermission,
      final @Tainted int bufferSize, final @Tainted short replication, final @Tainted long blockSize,
      final @Tainted Progressable progress, final @Tainted ChecksumOpt checksumOpt,
      final @Tainted boolean createParent) throws IOException, UnresolvedLinkException {
    return myFs.createInternal(fullPath(f), flag,
        absolutePermission, bufferSize,
        replication, blockSize, progress, checksumOpt, createParent);
  }

  @Override
  public @Tainted boolean delete(@Tainted ChRootedFs this, final @Tainted Path f, final @Tainted boolean recursive) 
      throws IOException, UnresolvedLinkException {
    return myFs.delete(fullPath(f), recursive);
  }

  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted ChRootedFs this, final @Tainted Path f, final @Tainted long start,
      final @Tainted long len) throws IOException, UnresolvedLinkException {
    return myFs.getFileBlockLocations(fullPath(f), start, len);
  }

  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted ChRootedFs this, final @Tainted Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.getFileChecksum(fullPath(f));
  }

  @Override
  public @Tainted FileStatus getFileStatus(@Tainted ChRootedFs this, final @Tainted Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.getFileStatus(fullPath(f));
  }

  @Override
  public @Tainted FileStatus getFileLinkStatus(@Tainted ChRootedFs this, final @Tainted Path f) 
    throws IOException, UnresolvedLinkException {
    return myFs.getFileLinkStatus(fullPath(f));
  }
  
  @Override
  public @Tainted FsStatus getFsStatus(@Tainted ChRootedFs this) throws IOException {
    return myFs.getFsStatus();
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted ChRootedFs this) throws IOException {
    return myFs.getServerDefaults();
  }

  @Override
  public @Tainted int getUriDefaultPort(@Tainted ChRootedFs this) {
    return myFs.getUriDefaultPort();
  }

  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted ChRootedFs this, final @Tainted Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.listStatus(fullPath(f));
  }

  @Override
  public void mkdir(@Tainted ChRootedFs this, final @Tainted Path dir, final @Tainted FsPermission permission,
      final @Tainted boolean createParent) throws IOException, UnresolvedLinkException {
    myFs.mkdir(fullPath(dir), permission, createParent);
    
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted ChRootedFs this, final @Tainted Path f, final @Tainted int bufferSize) 
    throws IOException, UnresolvedLinkException {
    return myFs.open(fullPath(f), bufferSize);
  }

  @Override
  public void renameInternal(@Tainted ChRootedFs this, final @Tainted Path src, final @Tainted Path dst)
    throws IOException, UnresolvedLinkException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    myFs.renameInternal(fullPath(src), fullPath(dst));
  }
  
  @Override
  public void renameInternal(@Tainted ChRootedFs this, final @Tainted Path src, final @Tainted Path dst, 
      final @Tainted boolean overwrite)
    throws IOException, UnresolvedLinkException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    myFs.renameInternal(fullPath(src), fullPath(dst), overwrite);
  }

  @Override
  public void setOwner(@Tainted ChRootedFs this, final @Tainted Path f, final @Untainted String username,
      final @Untainted String groupname)
    throws IOException, UnresolvedLinkException {
    myFs.setOwner(fullPath(f), username, groupname);
    
  }

  @Override
  public void setPermission(@Tainted ChRootedFs this, final @Tainted Path f, final @Tainted FsPermission permission)
    throws IOException, UnresolvedLinkException {
    myFs.setPermission(fullPath(f), permission);
  }

  @Override
  public @Tainted boolean setReplication(@Tainted ChRootedFs this, final @Tainted Path f, final @Tainted short replication)
    throws IOException, UnresolvedLinkException {
    return myFs.setReplication(fullPath(f), replication);
  }

  @Override
  public void setTimes(@Tainted ChRootedFs this, final @Tainted Path f, final @Tainted long mtime, final @Tainted long atime) 
      throws IOException, UnresolvedLinkException {
    myFs.setTimes(fullPath(f), mtime, atime);
  }

  @Override
  public void setVerifyChecksum(@Tainted ChRootedFs this, final @Tainted boolean verifyChecksum) 
      throws IOException, UnresolvedLinkException {
    myFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public @Tainted boolean supportsSymlinks(@Tainted ChRootedFs this) {
    return myFs.supportsSymlinks();
  }

  @Override
  public void createSymlink(@Tainted ChRootedFs this, final @Tainted Path target, final @Tainted Path link,
      final @Tainted boolean createParent) throws IOException, UnresolvedLinkException {
    /*
     * We leave the link alone:
     * If qualified or link relative then of course it is okay.
     * If absolute (ie / relative) then the link has to be resolved
     * relative to the changed root.
     */
    myFs.createSymlink(fullPath(target), link, createParent);
  }

  @Override
  public @Tainted Path getLinkTarget(@Tainted ChRootedFs this, final @Tainted Path f) throws IOException {
    return myFs.getLinkTarget(fullPath(f));
  }
  
  
  @Override
  public @Tainted List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> getDelegationTokens(@Tainted ChRootedFs this, @Tainted String renewer) throws IOException {
    return myFs.getDelegationTokens(renewer);
  }
}
