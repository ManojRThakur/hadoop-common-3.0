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
import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_RRR;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree.INode;
import org.apache.hadoop.fs.viewfs.InodeTree.INodeLink;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

/**
 * ViewFileSystem (extends the FileSystem interface) implements a client-side
 * mount table. Its spec and implementation is identical to {@link ViewFs}.
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class ViewFileSystem extends @Tainted FileSystem {

  private static final @Tainted Path ROOT_PATH = new @Tainted Path(Path.SEPARATOR);

  static @Tainted AccessControlException readOnlyMountTable(final @Tainted String operation,
      final @Tainted String p) {
    return new @Tainted AccessControlException( 
        "InternalDir of ViewFileSystem is readonly; operation=" + operation + 
        "Path=" + p);
  }
  static @Tainted AccessControlException readOnlyMountTable(final @Tainted String operation,
      final @Tainted Path p) {
    return readOnlyMountTable(operation, p.toString());
  }
  
  static public class MountPoint {
    private @Tainted Path src;       // the src of the mount
    private @Tainted URI @Tainted [] targets; //  target of the mount; Multiple targets imply mergeMount
    @Tainted
    MountPoint(@Tainted Path srcPath, @Tainted URI @Tainted [] targetURIs) {
      src = srcPath;
      targets = targetURIs;
    }
    @Tainted
    Path getSrc(ViewFileSystem.@Tainted MountPoint this) {
      return src;
    }
    @Tainted
    URI @Tainted [] getTargets(ViewFileSystem.@Tainted MountPoint this) {
      return targets;
    }
  }
  
  final @Tainted long creationTime; // of the the mount table
  final @Tainted UserGroupInformation ugi; // the user/group of user who created mtable
  @Tainted
  URI myUri;
  private @Tainted Path workingDir;
  @Tainted
  Configuration config;
  @Tainted
  InodeTree<@Tainted FileSystem> fsState;  // the fs state; ie the mount table
  @Tainted
  Path homeDir = null;
  
  /**
   * Make the path Absolute and get the path-part of a pathname.
   * Checks that URI matches this file system 
   * and that the path-part is a valid name.
   * 
   * @param p path
   * @return path-part of the Path p
   */
  private @Tainted String getUriPath(@Tainted ViewFileSystem this, final @Tainted Path p) {
    checkPath(p);
    @Tainted
    String s = makeAbsolute(p).toUri().getPath();
    return s;
  }
  
  private @Tainted Path makeAbsolute(@Tainted ViewFileSystem this, final @Tainted Path f) {
    return f.isAbsolute() ? f : new @Tainted Path(workingDir, f);
  }
  
  /**
   * This is the  constructor with the signature needed by
   * {@link FileSystem#createFileSystem(URI, Configuration)}
   * 
   * After this constructor is called initialize() is called.
   * @throws IOException 
   */
  public @Tainted ViewFileSystem() throws IOException {
    ugi = UserGroupInformation.getCurrentUser();
    creationTime = Time.now();
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>viewfs</code>
   */
  @Override
  public @Tainted String getScheme(@Tainted ViewFileSystem this) {
    return "viewfs";
  }

  /**
   * Called after a new FileSystem instance is constructed.
   * @param theUri a uri whose authority section names the host, port, etc. for
   *          this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(@Tainted ViewFileSystem this, final @Tainted URI theUri, final @Tainted Configuration conf)
      throws IOException {
    super.initialize(theUri, conf);
    setConf(conf);
    config = conf;
    // Now build  client side view (i.e. client side mount table) from config.
    final @Tainted String authority = theUri.getAuthority();
    try {
      myUri = new @Tainted URI(FsConstants.VIEWFS_SCHEME, authority, "/", null, null);
      fsState = new @Tainted InodeTree<@Tainted FileSystem>(conf, authority) {

        @Override
        protected
        @Tainted
        FileSystem getTargetFileSystem(final @Tainted URI uri)
          throws URISyntaxException, IOException {
            return new @Tainted ChRootedFileSystem(uri, config);
        }

        @Override
        protected
        @Tainted
        FileSystem getTargetFileSystem(final @Tainted INodeDir<@Tainted FileSystem> dir)
          throws URISyntaxException {
          return new @Tainted InternalDirOfViewFs(dir, creationTime, ugi, myUri);
        }

        @Override
        protected
        @Tainted
        FileSystem getTargetFileSystem(@Tainted URI @Tainted [] mergeFsURIList)
            throws URISyntaxException, UnsupportedFileSystemException {
          throw new @Tainted UnsupportedFileSystemException("mergefs not implemented");
          // return MergeFs.createMergeFs(mergeFsURIList, config);
        }
      };
      workingDir = this.getHomeDirectory();
    } catch (@Tainted URISyntaxException e) {
      throw new @Tainted IOException("URISyntax exception: " + theUri);
    }

  }
  
  
  /**
   * Convenience Constructor for apps to call directly
   * @param theUri which must be that of ViewFileSystem
   * @param conf
   * @throws IOException
   */
  @Tainted
  ViewFileSystem(final @Tainted URI theUri, final @Tainted Configuration conf)
    throws IOException {
    this();
    initialize(theUri, conf);
  }
  
  /**
   * Convenience Constructor for apps to call directly
   * @param conf
   * @throws IOException
   */
  public @Tainted ViewFileSystem(final @Tainted Configuration conf) throws IOException {
    this(FsConstants.VIEWFS_URI, conf);
  }
  
  public @Tainted Path getTrashCanLocation(@Tainted ViewFileSystem this, final @Tainted Path f) throws FileNotFoundException {
    final InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.isInternalDir() ? null : res.targetFileSystem.getHomeDirectory();
  }
  
  @Override
  public @Tainted URI getUri(@Tainted ViewFileSystem this) {
    return myUri;
  }
  
  @Override
  public @Tainted Path resolvePath(@Tainted ViewFileSystem this, final @Tainted Path f)
      throws IOException {
    final InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res;
      res = fsState.resolve(getUriPath(f), true);
    if (res.isInternalDir()) {
      return f;
    }
    return res.targetFileSystem.resolvePath(res.remainingPath);
  }
  
  @Override
  public @Tainted Path getHomeDirectory(@Tainted ViewFileSystem this) {
    if (homeDir == null) {
      @Tainted
      String base = fsState.getHomeDirPrefixValue();
      if (base == null) {
        base = "/user";
      }
      homeDir = (base.equals("/") ? 
          this.makeQualified(new @Tainted Path(base + ugi.getShortUserName())):
          this.makeQualified(new @Tainted Path(base + "/" + ugi.getShortUserName())));
    }
    return homeDir;
  }
  
  @Override
  public @Tainted Path getWorkingDirectory(@Tainted ViewFileSystem this) {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(@Tainted ViewFileSystem this, final @Tainted Path new_dir) {
    getUriPath(new_dir); // this validates the path
    workingDir = makeAbsolute(new_dir);
  }
  
  @Override
  public @Tainted FSDataOutputStream append(@Tainted ViewFileSystem this, final @Tainted Path f, final @Tainted int bufferSize,
      final @Tainted Progressable progress) throws IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.append(res.remainingPath, bufferSize, progress);
  }
  
  @Override
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted ViewFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      EnumSet<@Tainted CreateFlag> flags, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (@Tainted FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.createNonRecursive(res.remainingPath, permission,
         flags, bufferSize, replication, blockSize, progress);
  }
  
  @Override
  public @Tainted FSDataOutputStream create(@Tainted ViewFileSystem this, final @Tainted Path f, final @Tainted FsPermission permission,
      final @Tainted boolean overwrite, final @Tainted int bufferSize, final @Tainted short replication,
      final @Tainted long blockSize, final @Tainted Progressable progress) throws IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (@Tainted FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.create(res.remainingPath, permission,
         overwrite, bufferSize, replication, blockSize, progress);
  }

  
  @Override
  public @Tainted boolean delete(@Tainted ViewFileSystem this, final @Tainted Path f, final @Tainted boolean recursive)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.remainingPath == InodeTree.SlashPath) {
      throw readOnlyMountTable("delete", f);
    }
    return res.targetFileSystem.delete(res.remainingPath, recursive);
  }
  
  @Override
  @SuppressWarnings("deprecation")
  public @Tainted boolean delete(@Tainted ViewFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
      return delete(f, true);
  }
  
  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted ViewFileSystem this, @Tainted FileStatus fs, 
      @Tainted
      long start, @Tainted long len) throws IOException {
    final InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(fs.getPath()), true);
    return res.targetFileSystem.getFileBlockLocations(
          new @Tainted ViewFsFileStatus(fs, res.remainingPath), start, len);
  }

  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted ViewFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }

  @Override
  public @Tainted FileStatus getFileStatus(@Tainted ViewFileSystem this, final @Tainted Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    
    // FileStatus#getPath is a fully qualified path relative to the root of 
    // target file system.
    // We need to change it to viewfs URI - relative to root of mount table.
    
    // The implementors of RawLocalFileSystem were trying to be very smart.
    // They implement FileStatus#getOwener lazily -- the object
    // returned is really a RawLocalFileSystem that expect the
    // FileStatus#getPath to be unchanged so that it can get owner when needed.
    // Hence we need to interpose a new ViewFileSystemFileStatus that 
    // works around.
    @Tainted
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return new @Tainted ViewFsFileStatus(status, this.makeQualified(f));
  }
  
  
  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted ViewFileSystem this, final @Tainted Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    
    @Tainted
    FileStatus @Tainted [] statusLst = res.targetFileSystem.listStatus(res.remainingPath);
    if (!res.isInternalDir()) {
      // We need to change the name in the FileStatus as described in
      // {@link #getFileStatus }
      @Tainted
      ChRootedFileSystem targetFs;
      targetFs = (@Tainted ChRootedFileSystem) res.targetFileSystem;
      @Tainted
      int i = 0;
      for (@Tainted FileStatus status : statusLst) {
          @Tainted
          String suffix = targetFs.stripOutRoot(status.getPath());
          statusLst[i++] = new @Tainted ViewFsFileStatus(status, this.makeQualified(
              suffix.length() == 0 ? f : new @Tainted Path(res.resolvedPath, suffix)));
      }
    }
    return statusLst;
  }

  @Override
  public @Tainted boolean mkdirs(@Tainted ViewFileSystem this, final @Tainted Path dir, final @Tainted FsPermission permission)
      throws IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(dir), false);
   return  res.targetFileSystem.mkdirs(res.remainingPath, permission);
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted ViewFileSystem this, final @Tainted Path f, final @Tainted int bufferSize)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.open(res.remainingPath, bufferSize);
  }

  
  @Override
  public @Tainted boolean rename(@Tainted ViewFileSystem this, final @Tainted Path src, final @Tainted Path dst) throws IOException {
    // passing resolveLastComponet as false to catch renaming a mount point to 
    // itself. We need to catch this as an internal operation and fail.
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> resSrc = 
      fsState.resolve(getUriPath(src), false); 
  
    if (resSrc.isInternalDir()) {
      throw readOnlyMountTable("rename", src);
    }
      
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> resDst = 
      fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
          throw readOnlyMountTable("rename", dst);
    }
    /**
    // Alternate 1: renames within same file system - valid but we disallow
    // Alternate 2: (as described in next para - valid but we have disallowed it
    //
    // Note we compare the URIs. the URIs include the link targets. 
    // hence we allow renames across mount links as long as the mount links
    // point to the same target.
    if (!resSrc.targetFileSystem.getUri().equals(
              resDst.targetFileSystem.getUri())) {
      throw new IOException("Renames across Mount points not supported");
    }
    */
    
    //
    // Alternate 3 : renames ONLY within the the same mount links.
    //
    if (resSrc.targetFileSystem !=resDst.targetFileSystem) {
      throw new @Tainted IOException("Renames across Mount points not supported");
    }
    return resSrc.targetFileSystem.rename(resSrc.remainingPath,
        resDst.remainingPath);
  }
  
  @Override
  public void setOwner(@Tainted ViewFileSystem this, final @Tainted Path f, final @Untainted String username,
      final @Untainted String groupname) throws AccessControlException,
      FileNotFoundException,
      IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setOwner(res.remainingPath, username, groupname); 
  }

  @Override
  public void setPermission(@Tainted ViewFileSystem this, final @Tainted Path f, final @Tainted FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setPermission(res.remainingPath, permission); 
  }

  @Override
  public @Tainted boolean setReplication(@Tainted ViewFileSystem this, final @Tainted Path f, final @Tainted short replication)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.setReplication(res.remainingPath, replication);
  }

  @Override
  public void setTimes(@Tainted ViewFileSystem this, final @Tainted Path f, final @Tainted long mtime, final @Tainted long atime)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setTimes(res.remainingPath, mtime, atime); 
  }

  @Override
  public void setVerifyChecksum(@Tainted ViewFileSystem this, final @Tainted boolean verifyChecksum) { 
    @Tainted
    List<InodeTree.@Tainted MountPoint<@Tainted FileSystem>> mountPoints = 
        fsState.getMountPoints();
    for (InodeTree.@Tainted MountPoint<@Tainted FileSystem> mount : mountPoints) {
      mount.target.targetFileSystem.setVerifyChecksum(verifyChecksum);
    }
  }
  
  @Override
  public @Tainted long getDefaultBlockSize(@Tainted ViewFileSystem this) {
    throw new @Tainted NotInMountpointException("getDefaultBlockSize");
  }

  @Override
  public @Tainted short getDefaultReplication(@Tainted ViewFileSystem this) {
    throw new @Tainted NotInMountpointException("getDefaultReplication");
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted ViewFileSystem this) throws IOException {
    throw new @Tainted NotInMountpointException("getServerDefaults");
  }

  @Override
  public @Tainted long getDefaultBlockSize(@Tainted ViewFileSystem this, @Tainted Path f) {
    try {
      InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res =
        fsState.resolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultBlockSize(res.remainingPath);
    } catch (@Tainted FileNotFoundException e) {
      throw new @Tainted NotInMountpointException(f, "getDefaultBlockSize"); 
    }
  }

  @Override
  public @Tainted short getDefaultReplication(@Tainted ViewFileSystem this, @Tainted Path f) {
    try {
      InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res =
        fsState.resolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultReplication(res.remainingPath);
    } catch (@Tainted FileNotFoundException e) {
      throw new @Tainted NotInMountpointException(f, "getDefaultReplication"); 
    }
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted ViewFileSystem this, @Tainted Path f) throws IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getServerDefaults(res.remainingPath);    
  }

  @Override
  public @Tainted ContentSummary getContentSummary(@Tainted ViewFileSystem this, @Tainted Path f) throws IOException {
    InodeTree.@Tainted ResolveResult<@Tainted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getContentSummary(res.remainingPath);
  }

  @Override
  public void setWriteChecksum(@Tainted ViewFileSystem this, final @Tainted boolean writeChecksum) { 
    @Tainted
    List<InodeTree.@Tainted MountPoint<@Tainted FileSystem>> mountPoints = 
        fsState.getMountPoints();
    for (InodeTree.@Tainted MountPoint<@Tainted FileSystem> mount : mountPoints) {
      mount.target.targetFileSystem.setWriteChecksum(writeChecksum);
    }
  }

  @Override
  public @Tainted FileSystem @Tainted [] getChildFileSystems(@Tainted ViewFileSystem this) {
    @Tainted
    List<InodeTree.@Tainted MountPoint<@Tainted FileSystem>> mountPoints =
        fsState.getMountPoints();
    @Tainted
    Set<@Tainted FileSystem> children = new @Tainted HashSet<@Tainted FileSystem>();
    for (InodeTree.@Tainted MountPoint<@Tainted FileSystem> mountPoint : mountPoints) {
      @Tainted
      FileSystem targetFs = mountPoint.target.targetFileSystem;
      children.addAll(Arrays.asList(targetFs.getChildFileSystems()));
    }
    return children.toArray(new @Tainted FileSystem @Tainted []{});
  }
  
  public @Tainted MountPoint @Tainted [] getMountPoints(@Tainted ViewFileSystem this) {
    @Tainted
    List<InodeTree.@Tainted MountPoint<@Tainted FileSystem>> mountPoints = 
                  fsState.getMountPoints();
    
    @Tainted
    MountPoint @Tainted [] result = new @Tainted MountPoint @Tainted [mountPoints.size()];
    for ( @Tainted int i = 0; i < mountPoints.size(); ++i ) {
      result[i] = new @Tainted MountPoint(new @Tainted Path(mountPoints.get(i).src), 
                              mountPoints.get(i).target.targetDirLinkList);
    }
    return result;
  }
  
  /*
   * An instance of this class represents an internal dir of the viewFs 
   * that is internal dir of the mount table.
   * It is a read only mount tables and create, mkdir or delete operations
   * are not allowed.
   * If called on create or mkdir then this target is the parent of the
   * directory in which one is trying to create or mkdir; hence
   * in this case the path name passed in is the last component. 
   * Otherwise this target is the end point of the path and hence
   * the path name passed in is null. 
   */
  static class InternalDirOfViewFs extends @Tainted FileSystem {
    final InodeTree.@Tainted INodeDir<@Tainted FileSystem>  theInternalDir;
    final @Tainted long creationTime; // of the the mount table
    final @Tainted UserGroupInformation ugi; // the user/group of user who created mtable
    final @Tainted URI myUri;
    
    public @Tainted InternalDirOfViewFs(final InodeTree.@Tainted INodeDir<@Tainted FileSystem> dir,
        final @Tainted long cTime, final @Tainted UserGroupInformation ugi, @Tainted URI uri)
      throws URISyntaxException {
      myUri = uri;
      try {
        initialize(myUri, new @Tainted Configuration());
      } catch (@Tainted IOException e) {
        throw new @Tainted RuntimeException("Cannot occur");
      }
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
    }

    static private void checkPathIsSlash(final @Tainted Path f) throws IOException {
      if (f != InodeTree.SlashPath) {
        throw new @Tainted IOException (
        "Internal implementation error: expected file name to be /" );
      }
    }
    
    @Override
    public @Tainted URI getUri(ViewFileSystem.@Tainted InternalDirOfViewFs this) {
      return myUri;
    }

    @Override
    public @Tainted Path getWorkingDirectory(ViewFileSystem.@Tainted InternalDirOfViewFs this) {
      throw new @Tainted RuntimeException (
      "Internal impl error: getWorkingDir should not have been called" );
    }

    @Override
    public void setWorkingDirectory(ViewFileSystem.@Tainted InternalDirOfViewFs this, final @Tainted Path new_dir) {
      throw new @Tainted RuntimeException (
      "Internal impl error: getWorkingDir should not have been called" ); 
    }

    @Override
    public @Tainted FSDataOutputStream append(ViewFileSystem.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted int bufferSize,
        final @Tainted Progressable progress) throws IOException {
      throw readOnlyMountTable("append", f);
    }

    @Override
    public @Tainted FSDataOutputStream create(ViewFileSystem.@Tainted InternalDirOfViewFs this, final @Tainted Path f,
        final @Tainted FsPermission permission, final @Tainted boolean overwrite,
        final @Tainted int bufferSize, final @Tainted short replication, final @Tainted long blockSize,
        final @Tainted Progressable progress) throws AccessControlException {
      throw readOnlyMountTable("create", f);
    }

    @Override
    public @Tainted boolean delete(ViewFileSystem.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted boolean recursive)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("delete", f);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public @Tainted boolean delete(ViewFileSystem.@Tainted InternalDirOfViewFs this, final @Tainted Path f)
        throws AccessControlException, IOException {
      return delete(f, true);
    }

    @Override
    public @Tainted BlockLocation @Tainted [] getFileBlockLocations(ViewFileSystem.@Tainted InternalDirOfViewFs this, final @Tainted FileStatus fs,
        final @Tainted long start, final @Tainted long len) throws 
        FileNotFoundException, IOException {
      checkPathIsSlash(fs.getPath());
      throw new @Tainted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @Tainted FileChecksum getFileChecksum(ViewFileSystem.@Tainted InternalDirOfViewFs this, final @Tainted Path f)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @Tainted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @Tainted FileStatus getFileStatus(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f) throws IOException {
      checkPathIsSlash(f);
      return new @Tainted FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],

          new @Tainted Path(theInternalDir.fullPath).makeQualified(
              myUri, ROOT_PATH));
    }
    

    @Override
    public @Tainted FileStatus @Tainted [] listStatus(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f) throws AccessControlException,
        FileNotFoundException, IOException {
      checkPathIsSlash(f);
      @Tainted
      FileStatus @Tainted [] result = new @Tainted FileStatus @Tainted [theInternalDir.children.size()];
      @Tainted
      int i = 0;
      for (@Tainted Entry<@Tainted String, @Tainted INode<@Tainted FileSystem>> iEntry : 
                                          theInternalDir.children.entrySet()) {
        @Tainted
        INode<@Tainted FileSystem> inode = iEntry.getValue();
        if (inode instanceof @Tainted INodeLink ) {
          @Tainted
          INodeLink<@Tainted FileSystem> link = (@Tainted INodeLink<@Tainted FileSystem>) inode;

          result[i++] = new @Tainted FileStatus(0, false, 0, 0,
            creationTime, creationTime, PERMISSION_RRR,
            ugi.getUserName(), ugi.getGroupNames()[0],
            link.getTargetLink(),
            new @Tainted Path(inode.fullPath).makeQualified(
                myUri, null));
        } else {
          result[i++] = new @Tainted FileStatus(0, true, 0, 0,
            creationTime, creationTime, PERMISSION_RRR,
            ugi.getUserName(), ugi.getGroupNames()[0],
            new @Tainted Path(inode.fullPath).makeQualified(
                myUri, null));
        }
      }
      return result;
    }

    @Override
    public @Tainted boolean mkdirs(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path dir, @Tainted FsPermission permission)
        throws AccessControlException, FileAlreadyExistsException {
      if (theInternalDir.isRoot && dir == null) {
        throw new @Tainted FileAlreadyExistsException("/ already exits");
      }
      // Note dir starts with /
      if (theInternalDir.children.containsKey(dir.toString().substring(1))) {
        return true; // this is the stupid semantics of FileSystem
      }
      throw readOnlyMountTable("mkdirs",  dir);
    }

    @Override
    public @Tainted FSDataInputStream open(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f, @Tainted int bufferSize)
        throws AccessControlException, FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @Tainted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @Tainted boolean rename(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path src, @Tainted Path dst) throws AccessControlException,
        IOException {
      checkPathIsSlash(src);
      checkPathIsSlash(dst);
      throw readOnlyMountTable("rename", src);     
    }

    @Override
    public void setOwner(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f, @Tainted String username, @Tainted String groupname)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setOwner", f);
    }

    @Override
    public void setPermission(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f, @Tainted FsPermission permission)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setPermission", f);    
    }

    @Override
    public @Tainted boolean setReplication(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f, @Tainted short replication)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setReplication", f);
    }

    @Override
    public void setTimes(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f, @Tainted long mtime, @Tainted long atime)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setTimes", f);    
    }

    @Override
    public void setVerifyChecksum(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted boolean verifyChecksum) {
      // Noop for viewfs
    }

    @Override
    public @Tainted FsServerDefaults getServerDefaults(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f) throws IOException {
      throw new @Tainted NotInMountpointException(f, "getServerDefaults");
    }
    
    @Override
    public @Tainted long getDefaultBlockSize(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f) {
      throw new @Tainted NotInMountpointException(f, "getDefaultBlockSize");
    }

    @Override
    public @Tainted short getDefaultReplication(ViewFileSystem.@Tainted InternalDirOfViewFs this, @Tainted Path f) {
      throw new @Tainted NotInMountpointException(f, "getDefaultReplication");
    }
  }
}
