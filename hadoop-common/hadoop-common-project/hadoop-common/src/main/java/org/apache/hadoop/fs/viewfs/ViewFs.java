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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.local.LocalConfigKeys;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree.INode;
import org.apache.hadoop.fs.viewfs.InodeTree.INodeLink;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;


/**
 * ViewFs (extends the AbstractFileSystem interface) implements a client-side
 * mount table. The viewFs file system is implemented completely in memory on
 * the client side. The client-side mount table allows a client to provide a 
 * customized view of a file system namespace that is composed from 
 * one or more individual file systems (a localFs or Hdfs, S3fs, etc).
 * For example one could have a mount table that provides links such as
 * <ul>
 * <li>  /user          -> hdfs://nnContainingUserDir/user
 * <li>  /project/foo   -> hdfs://nnProject1/projects/foo
 * <li>  /project/bar   -> hdfs://nnProject2/projects/bar
 * <li>  /tmp           -> hdfs://nnTmp/privateTmpForUserXXX
 * </ul> 
 * 
 * ViewFs is specified with the following URI: <b>viewfs:///</b> 
 * <p>
 * To use viewfs one would typically set the default file system in the
 * config  (i.e. fs.default.name< = viewfs:///) along with the
 * mount table config variables as described below. 
 * 
 * <p>
 * <b> ** Config variables to specify the mount table entries ** </b>
 * <p>
 * 
 * The file system is initialized from the standard Hadoop config through
 * config variables.
 * See {@link FsConstants} for URI and Scheme constants; 
 * See {@link Constants} for config var constants; 
 * see {@link ConfigUtil} for convenient lib.
 * 
 * <p>
 * All the mount table config entries for view fs are prefixed by 
 * <b>fs.viewfs.mounttable.</b>
 * For example the above example can be specified with the following
 *  config variables:
 *  <ul>
 *  <li> fs.viewfs.mounttable.default.link./user=
 *  hdfs://nnContainingUserDir/user
 *  <li> fs.viewfs.mounttable.default.link./project/foo=
 *  hdfs://nnProject1/projects/foo
 *  <li> fs.viewfs.mounttable.default.link./project/bar=
 *  hdfs://nnProject2/projects/bar
 *  <li> fs.viewfs.mounttable.default.link./tmp=
 *  hdfs://nnTmp/privateTmpForUserXXX
 *  </ul>
 *  
 * The default mount table (when no authority is specified) is 
 * from config variables prefixed by <b>fs.viewFs.mounttable.default </b>
 * The authority component of a URI can be used to specify a different mount
 * table. For example,
 * <ul>
 * <li>  viewfs://sanjayMountable/
 * </ul>
 * is initialized from fs.viewFs.mounttable.sanjayMountable.* config variables.
 * 
 *  <p> 
 *  <b> **** Merge Mounts **** </b>(NOTE: merge mounts are not implemented yet.)
 *  <p>
 *  
 *   One can also use "MergeMounts" to merge several directories (this is
 *   sometimes  called union-mounts or junction-mounts in the literature.
 *   For example of the home directories are stored on say two file systems
 *   (because they do not fit on one) then one could specify a mount
 *   entry such as following merges two dirs:
 *   <ul>
 *   <li> /user -> hdfs://nnUser1/user,hdfs://nnUser2/user
 *   </ul>
 *  Such a mergeLink can be specified with the following config var where ","
 *  is used as the separator for each of links to be merged:
 *  <ul>
 *  <li> fs.viewfs.mounttable.default.linkMerge./user=
 *  hdfs://nnUser1/user,hdfs://nnUser1/user
 *  </ul>
 *   A special case of the merge mount is where mount table's root is merged
 *   with the root (slash) of another file system:
 *   <ul>
 *   <li>    fs.viewfs.mounttable.default.linkMergeSlash=hdfs://nn99/
 *   </ul>
 *   In this cases the root of the mount table is merged with the root of
 *            <b>hdfs://nn99/ </b> 
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class ViewFs extends @Tainted AbstractFileSystem {
  final @Tainted long creationTime; // of the the mount table
  final @Tainted UserGroupInformation ugi; // the user/group of user who created mtable
  final @Tainted Configuration config;
  @Tainted
  InodeTree<@Tainted AbstractFileSystem> fsState;  // the fs state; ie the mount table
  @Tainted
  Path homeDir = null;
  
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
    Path getSrc(ViewFs.@Tainted MountPoint this) {
      return src;
    }
    @Tainted
    URI @Tainted [] getTargets(ViewFs.@Tainted MountPoint this) {
      return targets;
    }
  }
  
  public @Tainted ViewFs(final @Tainted Configuration conf) throws IOException,
      URISyntaxException {
    this(FsConstants.VIEWFS_URI, conf);
  }
  
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   * 
   * @param theUri which must be that of ViewFs
   * @param conf
   * @throws IOException
   * @throws URISyntaxException 
   */
  @Tainted
  ViewFs(final @Tainted URI theUri, final @Tainted Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, FsConstants.VIEWFS_SCHEME, false, -1);
    creationTime = Time.now();
    ugi = UserGroupInformation.getCurrentUser();
    config = conf;
    // Now build  client side view (i.e. client side mount table) from config.
    @Tainted
    String authority = theUri.getAuthority();
    fsState = new @Tainted InodeTree<@Tainted AbstractFileSystem>(conf, authority) {

      @Override
      protected
      @Tainted
      AbstractFileSystem getTargetFileSystem(final @Tainted URI uri)
        throws URISyntaxException, UnsupportedFileSystemException {
          @Tainted
          String pathString = uri.getPath();
          if (pathString.isEmpty()) {
            pathString = "/";
          }
          return new @Tainted ChRootedFs(
              AbstractFileSystem.createFileSystem(uri, config),
              new @Tainted Path(pathString));
      }

      @Override
      protected
      @Tainted
      AbstractFileSystem getTargetFileSystem(
          final @Tainted INodeDir<@Tainted AbstractFileSystem> dir) throws URISyntaxException {
        return new @Tainted InternalDirOfViewFs(dir, creationTime, ugi, getUri());
      }

      @Override
      protected
      @Tainted
      AbstractFileSystem getTargetFileSystem(@Tainted URI @Tainted [] mergeFsURIList)
          throws URISyntaxException, UnsupportedFileSystemException {
        throw new @Tainted UnsupportedFileSystemException("mergefs not implemented yet");
        // return MergeFs.createMergeFs(mergeFsURIList, config);
      }
    };
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted ViewFs this) throws IOException {
    return LocalConfigKeys.getServerDefaults(); 
  }

  @Override
  public @Tainted int getUriDefaultPort(@Tainted ViewFs this) {
    return -1;
  }
 
  @Override
  public @Tainted Path getHomeDirectory(@Tainted ViewFs this) {
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
  public @Tainted Path resolvePath(@Tainted ViewFs this, final @Tainted Path f) throws FileNotFoundException,
          AccessControlException, UnresolvedLinkException, IOException {
    final InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res;
      res = fsState.resolve(getUriPath(f), true);
    if (res.isInternalDir()) {
      return f;
    }
    return res.targetFileSystem.resolvePath(res.remainingPath);

  }
  
  @Override
  public @Tainted FSDataOutputStream createInternal(@Tainted ViewFs this, final @Tainted Path f,
      final @Tainted EnumSet<@Tainted CreateFlag> flag, final @Tainted FsPermission absolutePermission,
      final @Tainted int bufferSize, final @Tainted short replication, final @Tainted long blockSize,
      final @Tainted Progressable progress, final @Tainted ChecksumOpt checksumOpt,
      final @Tainted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (@Tainted FileNotFoundException e) {
      if (createParent) {
        throw readOnlyMountTable("create", f);
      } else {
        throw e;
      }
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.createInternal(res.remainingPath, flag,
        absolutePermission, bufferSize, replication,
        blockSize, progress, checksumOpt,
        createParent);
  }

  @Override
  public @Tainted boolean delete(@Tainted ViewFs this, final @Tainted Path f, final @Tainted boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.remainingPath == InodeTree.SlashPath) {
      throw new @Tainted AccessControlException(
          "Cannot delete internal mount table directory: " + f);
    }
    return res.targetFileSystem.delete(res.remainingPath, recursive);
  }

  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted ViewFs this, final @Tainted Path f, final @Tainted long start,
      final @Tainted long len) throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return
      res.targetFileSystem.getFileBlockLocations(res.remainingPath, start, len);
  }

  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted ViewFs this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }

  @Override
  public @Tainted FileStatus getFileStatus(@Tainted ViewFs this, final @Tainted Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);

    //  FileStatus#getPath is a fully qualified path relative to the root of 
    // target file system.
    // We need to change it to viewfs URI - relative to root of mount table.
    
    // The implementors of RawLocalFileSystem were trying to be very smart.
    // They implement FileStatus#getOwener lazily -- the object
    // returned is really a RawLocalFileSystem that expect the
    // FileStatus#getPath to be unchanged so that it can get owner when needed.
    // Hence we need to interpose a new ViewFsFileStatus that works around.
    
    
    @Tainted
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return new @Tainted ViewFsFileStatus(status, this.makeQualified(f));
  }

  @Override
  public @Tainted FileStatus getFileLinkStatus(@Tainted ViewFs this, final @Tainted Path f)
     throws AccessControlException, FileNotFoundException,
     UnsupportedFileSystemException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), false); // do not follow mount link
    return res.targetFileSystem.getFileLinkStatus(res.remainingPath);
  }
  
  @Override
  public @Tainted FsStatus getFsStatus(@Tainted ViewFs this) throws AccessControlException,
      FileNotFoundException, IOException {
    return new @Tainted FsStatus(0, 0, 0);
  }

  @Override
  public @Tainted RemoteIterator<@Tainted FileStatus> listStatusIterator(@Tainted ViewFs this, final @Tainted Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    final InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res =
      fsState.resolve(getUriPath(f), true);
    final @Tainted RemoteIterator<@Tainted FileStatus> fsIter =
      res.targetFileSystem.listStatusIterator(res.remainingPath);
    if (res.isInternalDir()) {
      return fsIter;
    }
    
    return new @Tainted RemoteIterator<@Tainted FileStatus>() {
      final @Tainted RemoteIterator<@Tainted FileStatus> myIter;
      final @Tainted ChRootedFs targetFs;
      { // Init
          myIter = fsIter;
          targetFs = (@Tainted ChRootedFs) res.targetFileSystem;
      }
      
      @Override
      public @Tainted boolean hasNext() throws IOException {
        return myIter.hasNext();
      }
      
      @Override
      public @Tainted FileStatus next() throws IOException {
        @Tainted
        FileStatus status =  myIter.next();
        @Tainted
        String suffix = targetFs.stripOutRoot(status.getPath());
        return new @Tainted ViewFsFileStatus(status, makeQualified(
            suffix.length() == 0 ? f : new @Tainted Path(res.resolvedPath, suffix)));
      }
    };
  }
  
  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted ViewFs this, final @Tainted Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res =
      fsState.resolve(getUriPath(f), true);
    
    @Tainted
    FileStatus @Tainted [] statusLst = res.targetFileSystem.listStatus(res.remainingPath);
    if (!res.isInternalDir()) {
      // We need to change the name in the FileStatus as described in
      // {@link #getFileStatus }
      @Tainted
      ChRootedFs targetFs;
      targetFs = (@Tainted ChRootedFs) res.targetFileSystem;
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
  public void mkdir(@Tainted ViewFs this, final @Tainted Path dir, final @Tainted FsPermission permission,
      final @Tainted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(dir), false);
    res.targetFileSystem.mkdir(res.remainingPath, permission, createParent);
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted ViewFs this, final @Tainted Path f, final @Tainted int bufferSize)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.open(res.remainingPath, bufferSize);
  }

  
  @Override
  public void renameInternal(@Tainted ViewFs this, final @Tainted Path src, final @Tainted Path dst,
      final @Tainted boolean overwrite) throws IOException, UnresolvedLinkException {
    // passing resolveLastComponet as false to catch renaming a mount point 
    // itself we need to catch this as an internal operation and fail.
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> resSrc = 
      fsState.resolve(getUriPath(src), false); 
  
    if (resSrc.isInternalDir()) {
      throw new @Tainted AccessControlException(
          "Cannot Rename within internal dirs of mount table: it is readOnly");
    }
      
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> resDst = 
                                fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
      throw new @Tainted AccessControlException(
          "Cannot Rename within internal dirs of mount table: it is readOnly");
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
    
    resSrc.targetFileSystem.renameInternal(resSrc.remainingPath,
      resDst.remainingPath, overwrite);
  }

  @Override
  public void renameInternal(@Tainted ViewFs this, final @Tainted Path src, final @Tainted Path dst)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnresolvedLinkException, IOException {
    renameInternal(src, dst, false);
  }
  
  @Override
  public @Tainted boolean supportsSymlinks(@Tainted ViewFs this) {
    return true;
  }
  
  @Override
  public void createSymlink(@Tainted ViewFs this, final @Tainted Path target, final @Tainted Path link,
      final @Tainted boolean createParent) throws IOException, UnresolvedLinkException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res;
    try {
      res = fsState.resolve(getUriPath(link), false);
    } catch (@Tainted FileNotFoundException e) {
      if (createParent) {
        throw readOnlyMountTable("createSymlink", link);
      } else {
        throw e;
      }
    }
    assert(res.remainingPath != null);
    res.targetFileSystem.createSymlink(target, res.remainingPath,
        createParent);  
  }

  @Override
  public @Tainted Path getLinkTarget(@Tainted ViewFs this, final @Tainted Path f) throws IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), false); // do not follow mount link
    return res.targetFileSystem.getLinkTarget(res.remainingPath);
  }

  @Override
  public void setOwner(@Tainted ViewFs this, final @Tainted Path f, final @Untainted String username,
      final @Untainted String groupname) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setOwner(res.remainingPath, username, groupname); 
  }

  @Override
  public void setPermission(@Tainted ViewFs this, final @Tainted Path f, final @Tainted FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setPermission(res.remainingPath, permission); 
    
  }

  @Override
  public @Tainted boolean setReplication(@Tainted ViewFs this, final @Tainted Path f, final @Tainted short replication)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.setReplication(res.remainingPath, replication);
  }

  @Override
  public void setTimes(@Tainted ViewFs this, final @Tainted Path f, final @Tainted long mtime, final @Tainted long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@Tainted ResolveResult<@Tainted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setTimes(res.remainingPath, mtime, atime); 
  }

  @Override
  public void setVerifyChecksum(@Tainted ViewFs this, final @Tainted boolean verifyChecksum)
      throws AccessControlException, IOException {
    // This is a file system level operations, however ViewFs 
    // points to many file systems. Noop for ViewFs. 
  }
  
  public @Tainted MountPoint @Tainted [] getMountPoints(@Tainted ViewFs this) {
    @Tainted
    List<InodeTree.@Tainted MountPoint<@Tainted AbstractFileSystem>> mountPoints = 
                  fsState.getMountPoints();
    
    @Tainted
    MountPoint @Tainted [] result = new @Tainted MountPoint @Tainted [mountPoints.size()];
    for ( @Tainted int i = 0; i < mountPoints.size(); ++i ) {
      result[i] = new @Tainted MountPoint(new @Tainted Path(mountPoints.get(i).src), 
                              mountPoints.get(i).target.targetDirLinkList);
    }
    return result;
  }
  
  @Override
  public @Tainted List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> getDelegationTokens(@Tainted ViewFs this, @Tainted String renewer) throws IOException {
    @Tainted
    List<InodeTree.@Tainted MountPoint<@Tainted AbstractFileSystem>> mountPoints = 
                fsState.getMountPoints();
    @Tainted
    int initialListSize  = 0;
    for (InodeTree.@Tainted MountPoint<@Tainted AbstractFileSystem> im : mountPoints) {
      initialListSize += im.target.targetDirLinkList.length; 
    }
    @Tainted
    List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> result = new @Tainted ArrayList<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>>(initialListSize);
    for ( @Tainted int i = 0; i < mountPoints.size(); ++i ) {
      @Tainted
      List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> tokens = 
        mountPoints.get(i).target.targetFileSystem.getDelegationTokens(renewer);
      if (tokens != null) {
        result.addAll(tokens);
      }
    }
    return result;
  }

  @Override
  public @Tainted boolean isValidName(@Tainted ViewFs this, @Tainted String src) {
    // Prefix validated at mount time and rest of path validated by mount target.
    return true;
  }

  
  
  /*
   * An instance of this class represents an internal dir of the viewFs 
   * ie internal dir of the mount table.
   * It is a ready only mount tbale and create, mkdir or delete operations
   * are not allowed.
   * If called on create or mkdir then this target is the parent of the
   * directory in which one is trying to create or mkdir; hence
   * in this case the path name passed in is the last component. 
   * Otherwise this target is the end point of the path and hence
   * the path name passed in is null. 
   */
  static class InternalDirOfViewFs extends @Tainted AbstractFileSystem {
    
    final InodeTree.@Tainted INodeDir<@Tainted AbstractFileSystem>  theInternalDir;
    final @Tainted long creationTime; // of the the mount table
    final @Tainted UserGroupInformation ugi; // the user/group of user who created mtable
    final @Tainted URI myUri; // the URI of the outer ViewFs
    
    public @Tainted InternalDirOfViewFs(final InodeTree.@Tainted INodeDir<@Tainted AbstractFileSystem> dir,
        final @Tainted long cTime, final @Tainted UserGroupInformation ugi, final @Tainted URI uri)
      throws URISyntaxException {
      super(FsConstants.VIEWFS_URI, FsConstants.VIEWFS_SCHEME, false, -1);
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
      myUri = uri;
    }

    static private void checkPathIsSlash(final @Tainted Path f) throws IOException {
      if (f != InodeTree.SlashPath) {
        throw new @Tainted IOException (
        "Internal implementation error: expected file name to be /" );
      }
    }

    @Override
    public @Tainted FSDataOutputStream createInternal(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f,
        final @Tainted EnumSet<@Tainted CreateFlag> flag, final @Tainted FsPermission absolutePermission,
        final @Tainted int bufferSize, final @Tainted short replication, final @Tainted long blockSize,
        final @Tainted Progressable progress, final @Tainted ChecksumOpt checksumOpt,
        final @Tainted boolean createParent) throws AccessControlException,
        FileAlreadyExistsException, FileNotFoundException,
        ParentNotDirectoryException, UnsupportedFileSystemException,
        UnresolvedLinkException, IOException {
      throw readOnlyMountTable("create", f);
    }

    @Override
    public @Tainted boolean delete(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted boolean recursive)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("delete", f);
    }

    @Override
    public @Tainted BlockLocation @Tainted [] getFileBlockLocations(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted long start,
        final @Tainted long len) throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @Tainted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @Tainted FileChecksum getFileChecksum(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @Tainted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @Tainted FileStatus getFileStatus(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f) throws IOException {
      checkPathIsSlash(f);
      return new @Tainted FileStatus(0, true, 0, 0, creationTime, creationTime, 
          PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
          new @Tainted Path(theInternalDir.fullPath).makeQualified(
              myUri, null));
    }
    
    @Override
    public @Tainted FileStatus getFileLinkStatus(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f)
        throws FileNotFoundException {
      // look up i internalDirs children - ignore first Slash
      @Tainted
      INode<@Tainted AbstractFileSystem> inode =
        theInternalDir.children.get(f.toUri().toString().substring(1)); 
      if (inode == null) {
        throw new @Tainted FileNotFoundException(
            "viewFs internal mount table - missing entry:" + f);
      }
      @Tainted
      FileStatus result;
      if (inode instanceof @Tainted INodeLink) {
        @Tainted
        INodeLink<@Tainted AbstractFileSystem> inodelink = 
          (@Tainted INodeLink<@Tainted AbstractFileSystem>) inode;
        result = new @Tainted FileStatus(0, false, 0, 0, creationTime, creationTime,
            PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
            inodelink.getTargetLink(),
            new @Tainted Path(inode.fullPath).makeQualified(
                myUri, null));
      } else {
        result = new @Tainted FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
          new @Tainted Path(inode.fullPath).makeQualified(
              myUri, null));
      }
      return result;
    }
    
    @Override
    public @Tainted FsStatus getFsStatus(ViewFs.@Tainted InternalDirOfViewFs this) {
      return new @Tainted FsStatus(0, 0, 0);
    }

    @Override
    public @Tainted FsServerDefaults getServerDefaults(ViewFs.@Tainted InternalDirOfViewFs this) throws IOException {
      throw new @Tainted IOException("FsServerDefaults not implemented yet");
    }

    @Override
    public @Tainted int getUriDefaultPort(ViewFs.@Tainted InternalDirOfViewFs this) {
      return -1;
    }

    @Override
    public @Tainted FileStatus @Tainted [] listStatus(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f) throws AccessControlException,
        IOException {
      checkPathIsSlash(f);
      @Tainted
      FileStatus @Tainted [] result = new @Tainted FileStatus @Tainted [theInternalDir.children.size()];
      @Tainted
      int i = 0;
      for (@Tainted Entry<@Tainted String, @Tainted INode<@Tainted AbstractFileSystem>> iEntry : 
                                          theInternalDir.children.entrySet()) {
        @Tainted
        INode<@Tainted AbstractFileSystem> inode = iEntry.getValue();

        
        if (inode instanceof @Tainted INodeLink ) {
          @Tainted
          INodeLink<@Tainted AbstractFileSystem> link = 
            (@Tainted INodeLink<@Tainted AbstractFileSystem>) inode;

          result[i++] = new @Tainted FileStatus(0, false, 0, 0,
            creationTime, creationTime,
            PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
            link.getTargetLink(),
            new @Tainted Path(inode.fullPath).makeQualified(
                myUri, null));
        } else {
          result[i++] = new @Tainted FileStatus(0, true, 0, 0,
            creationTime, creationTime,
            PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
            new @Tainted Path(inode.fullPath).makeQualified(
                myUri, null));
        }
      }
      return result;
    }

    @Override
    public void mkdir(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path dir, final @Tainted FsPermission permission,
        final @Tainted boolean createParent) throws AccessControlException,
        FileAlreadyExistsException {
      if (theInternalDir.isRoot && dir == null) {
        throw new @Tainted FileAlreadyExistsException("/ already exits");
      }
      throw readOnlyMountTable("mkdir", dir);
    }

    @Override
    public @Tainted FSDataInputStream open(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted int bufferSize)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @Tainted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public void renameInternal(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path src, final @Tainted Path dst)
        throws AccessControlException, IOException {
      checkPathIsSlash(src);
      checkPathIsSlash(dst);
      throw readOnlyMountTable("rename", src);     
    }

    @Override
    public @Tainted boolean supportsSymlinks(ViewFs.@Tainted InternalDirOfViewFs this) {
      return true;
    }
    
    @Override
    public void createSymlink(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path target, final @Tainted Path link,
        final @Tainted boolean createParent) throws AccessControlException {
      throw readOnlyMountTable("createSymlink", link);    
    }

    @Override
    public @Tainted Path getLinkTarget(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f) throws FileNotFoundException,
        IOException {
      return getFileLinkStatus(f).getSymlink();
    }

    @Override
    public void setOwner(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted String username,
        final @Tainted String groupname) throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setOwner", f);
    }

    @Override
    public void setPermission(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted FsPermission permission)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setPermission", f);    
    }

    @Override
    public @Tainted boolean setReplication(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted short replication)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setReplication", f);
    }

    @Override
    public void setTimes(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted Path f, final @Tainted long mtime, final @Tainted long atime)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setTimes", f);    
    }

    @Override
    public void setVerifyChecksum(ViewFs.@Tainted InternalDirOfViewFs this, final @Tainted boolean verifyChecksum)
        throws AccessControlException {
      throw readOnlyMountTable("setVerifyChecksum", "");   
    }
  }
}
