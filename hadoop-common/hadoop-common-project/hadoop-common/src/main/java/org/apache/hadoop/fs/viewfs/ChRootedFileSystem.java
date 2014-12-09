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
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * <code>ChRootedFileSystem</code> is a file system with its root some path
 * below the root of its base file system. 
 * 
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
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
class ChRootedFileSystem extends @Tainted FilterFileSystem {
  private final @Tainted URI myUri; // the base URI + the chRoot
  private final @Tainted Path chRootPathPart; // the root below the root of the base
  private final @Tainted String chRootPathPartString;
  private @Tainted Path workingDir;
  
  protected @Tainted FileSystem getMyFs(@Tainted ChRootedFileSystem this) {
    return getRawFileSystem();
  }
  
  /**
   * @param path
   * @return  full path including the chroot 
   */
  protected @Tainted Path fullPath(@Tainted ChRootedFileSystem this, final @Tainted Path path) {
    super.checkPath(path);
    return path.isAbsolute() ? 
        new @Tainted Path((chRootPathPart.isRoot() ? "" : chRootPathPartString)
            + path.toUri().getPath()) :
        new @Tainted Path(chRootPathPartString + workingDir.toUri().getPath(), path);
  }
  
  /**
   * Constructor
   * @param uri base file system
   * @param conf configuration
   * @throws IOException 
   */
  public @Tainted ChRootedFileSystem(final @Tainted URI uri, @Tainted Configuration conf)
      throws IOException {
    super(FileSystem.get(uri, conf));
    @Tainted
    String pathString = uri.getPath();
    if (pathString.isEmpty()) {
      pathString = "/";
    }
    chRootPathPart = new @Tainted Path(pathString);
    chRootPathPartString = chRootPathPart.toUri().getPath();
    myUri = uri;
    workingDir = getHomeDirectory();
    // We don't use the wd of the myFs
  }
  
  /** 
   * Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(@Tainted ChRootedFileSystem this, final @Tainted URI name, final @Tainted Configuration conf)
      throws IOException {
    super.initialize(name, conf);
    setConf(conf);
  }

  @Override
  public @Tainted URI getUri(@Tainted ChRootedFileSystem this) {
    return myUri;
  }
  
  /**
   * Strip out the root from the path.
   * @param p - fully qualified path p
   * @return -  the remaining path  without the begining /
   * @throws IOException if the p is not prefixed with root
   */
  @Tainted
  String stripOutRoot(@Tainted ChRootedFileSystem this, final @Tainted Path p) throws IOException {
    try {
     checkPath(p);
    } catch (@Tainted IllegalArgumentException e) {
      throw new @Tainted IOException("Internal Error - path " + p +
          " should have been with URI: " + myUri);
    }
    @Tainted
    String pathPart = p.toUri().getPath();
    return (pathPart.length() == chRootPathPartString.length()) ? "" : pathPart
        .substring(chRootPathPartString.length() + (chRootPathPart.isRoot() ? 0 : 1));
  }
  
  @Override
  protected @Tainted Path getInitialWorkingDirectory(@Tainted ChRootedFileSystem this) {
    /*
     * 3 choices here: 
     *     null or / or /user/<uname> or strip out the root out of myFs's
     *  inital wd. 
     * Only reasonable choice for initialWd for chrooted fds is null 
     * so that the default rule for wd is applied
     */
    return null;
  }
  
  public @Tainted Path getResolvedQualifiedPath(@Tainted ChRootedFileSystem this, final @Tainted Path f)
      throws FileNotFoundException {
    return makeQualified(
        new @Tainted Path(chRootPathPartString + f.toUri().toString()));
  }

  @Override
  public @Tainted Path getWorkingDirectory(@Tainted ChRootedFileSystem this) {
    return workingDir;
  }
  
  @Override
  public void setWorkingDirectory(@Tainted ChRootedFileSystem this, final @Tainted Path new_dir) {
    workingDir = new_dir.isAbsolute() ? new_dir : new @Tainted Path(workingDir, new_dir);
  }

  @Override
  public @Tainted FSDataOutputStream create(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted FsPermission permission,
      final @Tainted boolean overwrite, final @Tainted int bufferSize, final @Tainted short replication,
      final @Tainted long blockSize, final @Tainted Progressable progress) throws IOException {
    return super.create(fullPath(f), permission, overwrite, bufferSize,
        replication, blockSize, progress);
  }
  
  @Override
  @Deprecated
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted ChRootedFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      EnumSet<@Tainted CreateFlag> flags, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    
    return super.createNonRecursive(fullPath(f), permission, flags, bufferSize, replication, blockSize,
        progress);
  }

  @Override
  public @Tainted boolean delete(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted boolean recursive) 
      throws IOException {
    return super.delete(fullPath(f), recursive);
  }
  

  @Override
  @SuppressWarnings("deprecation")
  public @Tainted boolean delete(@Tainted ChRootedFileSystem this, @Tainted Path f) throws IOException {
   return delete(f, true);
  }

  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted ChRootedFileSystem this, final @Tainted FileStatus fs, final @Tainted long start,
      final @Tainted long len) throws IOException {
    return super.getFileBlockLocations(
        new @Tainted ViewFsFileStatus(fs, fullPath(fs.getPath())), start, len);
  }

  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted ChRootedFileSystem this, final @Tainted Path f) 
      throws IOException {
    return super.getFileChecksum(fullPath(f));
  }

  @Override
  public @Tainted FileStatus getFileStatus(@Tainted ChRootedFileSystem this, final @Tainted Path f) 
      throws IOException {
    return super.getFileStatus(fullPath(f));
  }

  @Override
  public @Tainted FsStatus getStatus(@Tainted ChRootedFileSystem this, @Tainted Path p) throws IOException {
    return super.getStatus(fullPath(p));
  }

  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted ChRootedFileSystem this, final @Tainted Path f) 
      throws IOException {
    return super.listStatus(fullPath(f));
  }
  
  @Override
  public @Tainted boolean mkdirs(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted FsPermission permission)
      throws IOException {
    return super.mkdirs(fullPath(f), permission);
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted int bufferSize) 
    throws IOException {
    return super.open(fullPath(f), bufferSize);
  }
  
  @Override
  public @Tainted FSDataOutputStream append(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted int bufferSize,
      final @Tainted Progressable progress) throws IOException {
    return super.append(fullPath(f), bufferSize, progress);
  }

  @Override
  public @Tainted boolean rename(@Tainted ChRootedFileSystem this, final @Tainted Path src, final @Tainted Path dst) throws IOException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    return super.rename(fullPath(src), fullPath(dst)); 
  }
  
  @Override
  public void setOwner(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Untainted String username,
      final @Untainted String groupname)
    throws IOException {
    super.setOwner(fullPath(f), username, groupname);
  }

  @Override
  public void setPermission(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted FsPermission permission)
    throws IOException {
    super.setPermission(fullPath(f), permission);
  }

  @Override
  public @Tainted boolean setReplication(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted short replication)
    throws IOException {
    return super.setReplication(fullPath(f), replication);
  }

  @Override
  public void setTimes(@Tainted ChRootedFileSystem this, final @Tainted Path f, final @Tainted long mtime, final @Tainted long atime) 
      throws IOException {
    super.setTimes(fullPath(f), mtime, atime);
  }
  
  @Override
  public @Tainted Path resolvePath(@Tainted ChRootedFileSystem this, final @Tainted Path p) throws IOException {
    return super.resolvePath(fullPath(p));
  }

  @Override
  public @Tainted ContentSummary getContentSummary(@Tainted ChRootedFileSystem this, @Tainted Path f) throws IOException {
    return super.getContentSummary(fullPath(f));
  }
  

  private static @Tainted Path rootPath = new @Tainted Path(Path.SEPARATOR);

  @Override
  public @Tainted long getDefaultBlockSize(@Tainted ChRootedFileSystem this) {
    return getDefaultBlockSize(fullPath(rootPath));
  }
  
  @Override
  public @Tainted long getDefaultBlockSize(@Tainted ChRootedFileSystem this, @Tainted Path f) {
    return super.getDefaultBlockSize(fullPath(f));
  }  

  @Override
  public @Tainted short getDefaultReplication(@Tainted ChRootedFileSystem this) {
    return getDefaultReplication(fullPath(rootPath));
  }

  @Override
  public @Tainted short getDefaultReplication(@Tainted ChRootedFileSystem this, @Tainted Path f) {
    return super.getDefaultReplication(fullPath(f));
  }
  
  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted ChRootedFileSystem this) throws IOException {
    return getServerDefaults(fullPath(rootPath));
  }  

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted ChRootedFileSystem this, @Tainted Path f) throws IOException {
    return super.getServerDefaults(fullPath(f));
  }  
}
