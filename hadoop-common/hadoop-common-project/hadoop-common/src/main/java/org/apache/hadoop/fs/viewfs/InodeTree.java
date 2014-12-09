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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;


/**
 * InodeTree implements a mount-table as a tree of inodes.
 * It is used to implement ViewFs and ViewFileSystem.
 * In order to use it the caller must subclass it and implement
 * the abstract methods {@link #getTargetFileSystem(INodeDir)}, etc.
 * 
 * The mountable is initialized from the config variables as 
 * specified in {@link ViewFs}
 *
 * @param <T> is AbstractFileSystem or FileSystem
 * 
 * The three main methods are
 * {@link #InodeTreel(Configuration)} // constructor
 * {@link #InodeTree(Configuration, String)} // constructor
 * {@link #resolve(String, boolean)} 
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable 
abstract class InodeTree<@Tainted T extends java.lang.@Tainted Object> {
  static enum ResultKind {  @Tainted  isInternalDir,  @Tainted  isExternalDir;};
  static final @Tainted Path SlashPath = new @Tainted Path("/");
  
  final @Tainted INodeDir<@Tainted T> root; // the root of the mount table
  
  final @Tainted String homedirPrefix; // the homedir config value for this mount table
  
  @Tainted
  List<@Tainted MountPoint<@Tainted T>> mountPoints = new @Tainted ArrayList<@Tainted MountPoint<T>>();
  
  
  static class MountPoint<@Tainted T extends java.lang.@Tainted Object> {
    @Tainted
    String src;
    @Tainted
    INodeLink<@Tainted T> target;
    @Tainted
    MountPoint(@Tainted String srcPath, @Tainted INodeLink<@Tainted T> mountLink) {
      src = srcPath;
      target = mountLink;
    }

  }
  
  /**
   * Breaks file path into component names.
   * @param path
   * @return array of names component names
   */
  static @Tainted String @Tainted [] breakIntoPathComponents(final @Tainted String path) {
    return path == null ? null : path.split(Path.SEPARATOR);
  } 
  
  /**
   * Internal class for inode tree
   * @param <T>
   */
  abstract static class INode<@Tainted T extends java.lang.@Tainted Object> {
    final @Tainted String fullPath; // the full path to the root
    public @Tainted INode(@Tainted String pathToNode, @Tainted UserGroupInformation aUgi) {
      fullPath = pathToNode;
    }
  };

  /**
   * Internal class to represent an internal dir of the mount table
   * @param <T>
   */
  static class INodeDir<@Tainted T extends java.lang.@Tainted Object> extends @Tainted INode<T> {
    final @Tainted Map<@Tainted String, @Tainted INode<@Tainted T>> children = new @Tainted HashMap<@Tainted String, @Tainted INode<T>>();
    @Tainted
    T InodeDirFs =  null; // file system of this internal directory of mountT
    @Tainted
    boolean isRoot = false;
    
    @Tainted
    INodeDir(final @Tainted String pathToNode, final @Tainted UserGroupInformation aUgi) {
      super(pathToNode, aUgi);
    }

    @Tainted
    INode<@Tainted T> resolve(InodeTree.@Tainted INodeDir<T> this, final @Tainted String pathComponent) throws FileNotFoundException {
      final @Tainted INode<T> result = resolveInternal(pathComponent);
      if (result == null) {
        throw new @Tainted FileNotFoundException();
      }
      return result;
    }
    
    @Tainted
    INode<@Tainted T> resolveInternal(InodeTree.@Tainted INodeDir<T> this, final @Tainted String pathComponent) {
      return children.get(pathComponent);
    }
    
    @Tainted
    INodeDir<@Tainted T> addDir(InodeTree.@Tainted INodeDir<T> this, final @Tainted String pathComponent,
        final @Tainted UserGroupInformation aUgi)
      throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new @Tainted FileAlreadyExistsException();
      }
      final @Tainted INodeDir<T> newDir = new @Tainted INodeDir<T>(fullPath+ (isRoot ? "" : "/") + 
          pathComponent, aUgi);
      children.put(pathComponent, newDir);
      return newDir;
    }
    
    void addLink(InodeTree.@Tainted INodeDir<T> this, final @Tainted String pathComponent, final @Tainted INodeLink<@Tainted T> link)
      throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new @Tainted FileAlreadyExistsException();
      }
      children.put(pathComponent, link);
    }
  }

  /**
   * In internal class to represent a mount link
   * A mount link can be single dir link or a merge dir link.

   * A merge dir link is  a merge (junction) of links to dirs:
   * example : <merge of 2 dirs
   *     /users -> hdfs:nn1//users
   *     /users -> hdfs:nn2//users
   * 
   * For a merge, each target is checked to be dir when created but if target
   * is changed later it is then ignored (a dir with null entries)
   */
  static class INodeLink<@Tainted T extends java.lang.@Tainted Object> extends @Tainted INode<T> {
    final @Tainted boolean isMergeLink; // true if MergeLink
    final @Tainted URI @Tainted [] targetDirLinkList;
    final @Tainted T targetFileSystem;   // file system object created from the link.
    
    /**
     * Construct a mergeLink
     */
    @Tainted
    INodeLink(final @Tainted String pathToNode, final @Tainted UserGroupInformation aUgi,
        final @Tainted T targetMergeFs, final @Tainted URI @Tainted [] aTargetDirLinkList) {
      super(pathToNode, aUgi);
      targetFileSystem = targetMergeFs;
      targetDirLinkList = aTargetDirLinkList;
      isMergeLink = true;
    }
    
    /**
     * Construct a simple link (i.e. not a mergeLink)
     */
    @Tainted
    INodeLink(final @Tainted String pathToNode, final @Tainted UserGroupInformation aUgi,
        final @Tainted T targetFs, final @Tainted URI aTargetDirLink) {
      super(pathToNode, aUgi);
      targetFileSystem = targetFs;
      targetDirLinkList = new @Tainted URI @Tainted [1];
      targetDirLinkList[0] = aTargetDirLink;
      isMergeLink = false;
    }
    
    /**
     * Get the target of the link
     * If a merge link then it returned as "," separated URI list.
     */
    @Tainted
    Path getTargetLink(InodeTree.@Tainted INodeLink<T> this) {
      // is merge link - use "," as separator between the merged URIs
      //String result = targetDirLinkList[0].toString();
      @Tainted
      StringBuilder result = new @Tainted StringBuilder(targetDirLinkList[0].toString());
      for (@Tainted int i=1; i < targetDirLinkList.length; ++i) { 
        result.append(',').append(targetDirLinkList[i].toString());
      }
      return new @Tainted Path(result.toString());
    }
  }


  private void createLink(@Tainted InodeTree<T> this, final @Tainted String src, final @Tainted String target,
      final @Tainted boolean isLinkMerge, final @Tainted UserGroupInformation aUgi)
      throws URISyntaxException, IOException,
    FileAlreadyExistsException, UnsupportedFileSystemException {
    // Validate that src is valid absolute path
    final @Tainted Path srcPath = new @Tainted Path(src); 
    if (!srcPath.isAbsoluteAndSchemeAuthorityNull()) {
      throw new @Tainted IOException("ViewFs:Non absolute mount name in config:" + src);
    }
 
    final @Tainted String @Tainted [] srcPaths = breakIntoPathComponents(src);
    @Tainted
    INodeDir<T> curInode = root;
    @Tainted
    int i;
    // Ignore first initial slash, process all except last component
    for (i = 1; i < srcPaths.length-1; i++) {
      final @Tainted String iPath = srcPaths[i];
      @Tainted
      INode<T> nextInode = curInode.resolveInternal(iPath);
      if (nextInode == null) {
        @Tainted
        INodeDir<T> newDir = curInode.addDir(iPath, aUgi);
        newDir.InodeDirFs = getTargetFileSystem(newDir);
        nextInode = newDir;
      }
      if (nextInode instanceof @Tainted INodeLink) {
        // Error - expected a dir but got a link
        throw new @Tainted FileAlreadyExistsException("Path " + nextInode.fullPath +
            " already exists as link");
      } else {
        assert(nextInode instanceof @Tainted INodeDir);
        curInode = (@Tainted INodeDir<T>) nextInode;
      }
    }
    
    // Now process the last component
    // Add the link in 2 cases: does not exist or a link exists
    @Tainted
    String iPath = srcPaths[i];// last component
    if (curInode.resolveInternal(iPath) != null) {
      //  directory/link already exists
      @Tainted
      StringBuilder strB = new @Tainted StringBuilder(srcPaths[0]);
      for (@Tainted int j = 1; j <= i; ++j) {
        strB.append('/').append(srcPaths[j]);
      }
      throw new @Tainted FileAlreadyExistsException("Path " + strB +
            " already exists as dir; cannot create link here");
    }
    
    final @Tainted INodeLink<T> newLink;
    final @Tainted String fullPath = curInode.fullPath + (curInode == root ? "" : "/")
        + iPath;
    if (isLinkMerge) { // Target is list of URIs
      @Tainted
      String @Tainted [] targetsList = StringUtils.getStrings(target);
      @Tainted
      URI @Tainted [] targetsListURI = new @Tainted URI @Tainted [targetsList.length];
      @Tainted
      int k = 0;
      for (@Tainted String itarget : targetsList) {
        targetsListURI[k++] = new @Tainted URI(itarget);
      }
      newLink = new @Tainted INodeLink<T>(fullPath, aUgi,
          getTargetFileSystem(targetsListURI), targetsListURI);
    } else {
      newLink = new @Tainted INodeLink<T>(fullPath, aUgi,
          getTargetFileSystem(new @Tainted URI(target)), new @Tainted URI(target));
    }
    curInode.addLink(iPath, newLink);
    mountPoints.add(new @Tainted MountPoint<T>(src, newLink));
  }
  
  /**
   * Below the "public" methods of InodeTree
   */
  
  /**
   * The user of this class must subclass and implement the following
   * 3 abstract methods.
   * @throws IOException 
   */
  protected abstract @Tainted T getTargetFileSystem(@Tainted InodeTree<T> this, final @Tainted URI uri)
    throws UnsupportedFileSystemException, URISyntaxException, IOException;
  
  protected abstract @Tainted T getTargetFileSystem(@Tainted InodeTree<T> this, final @Tainted INodeDir<@Tainted T> dir)
    throws URISyntaxException;
  
  protected abstract @Tainted T getTargetFileSystem(@Tainted InodeTree<T> this, final @Tainted URI @Tainted [] mergeFsURIList)
  throws UnsupportedFileSystemException, URISyntaxException;
  
  /**
   * Create Inode Tree from the specified mount-table specified in Config
   * @param config - the mount table keys are prefixed with 
   *       FsConstants.CONFIG_VIEWFS_PREFIX
   * @param viewName - the name of the mount table - if null use defaultMT name
   * @throws UnsupportedFileSystemException
   * @throws URISyntaxException
   * @throws FileAlreadyExistsException
   * @throws IOException
   */
  protected @Tainted InodeTree(final @Tainted Configuration config, final @Tainted String viewName)
      throws UnsupportedFileSystemException, URISyntaxException,
    FileAlreadyExistsException, IOException { 
    @Tainted
    String vName = viewName;
    if (vName == null) {
      vName = Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
    }
    homedirPrefix = ConfigUtil.getHomeDirValue(config, vName);
    root = new @Tainted INodeDir<T>("/", UserGroupInformation.getCurrentUser());
    root.InodeDirFs = getTargetFileSystem(root);
    root.isRoot = true;
    
    final @Tainted String mtPrefix = Constants.CONFIG_VIEWFS_PREFIX + "." + 
                            vName + ".";
    final @Tainted String linkPrefix = Constants.CONFIG_VIEWFS_LINK + ".";
    final @Tainted String linkMergePrefix = Constants.CONFIG_VIEWFS_LINK_MERGE + ".";
    @Tainted
    boolean gotMountTableEntry = false;
    final @Tainted UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    for (@Tainted Entry<@Tainted String, @Untainted String> si : config) {
      final @Tainted String key = si.getKey();
      if (key.startsWith(mtPrefix)) {
        gotMountTableEntry = true;
        @Tainted
        boolean isMergeLink = false;
        @Tainted
        String src = key.substring(mtPrefix.length());
        if (src.startsWith(linkPrefix)) {
          src = src.substring(linkPrefix.length());
        } else if (src.startsWith(linkMergePrefix)) { // A merge link
          isMergeLink = true;
          src = src.substring(linkMergePrefix.length());
        } else if (src.startsWith(Constants.CONFIG_VIEWFS_HOMEDIR)) {
          // ignore - we set home dir from config
          continue;
        } else {
          throw new @Tainted IOException(
          "ViewFs: Cannot initialize: Invalid entry in Mount table in config: "+ 
          src);
        }
        final @Tainted String target = si.getValue(); // link or merge link
        createLink(src, target, isMergeLink, ugi); 
      }
    }
    if (!gotMountTableEntry) {
      throw new @Tainted IOException(
          "ViewFs: Cannot initialize: Empty Mount table in config for " +
             "viewfs://" + vName + "/");
    }
  }

  /**
   * Resolve returns ResolveResult.
   * The caller can continue the resolution of the remainingPath
   * in the targetFileSystem.
   * 
   * If the input pathname leads to link to another file system then
   * the targetFileSystem is the one denoted by the link (except it is
   * file system chrooted to link target.
   * If the input pathname leads to an internal mount-table entry then
   * the target file system is one that represents the internal inode.
   */
  static class ResolveResult<@Tainted T extends java.lang.@Tainted Object> {
    final @Tainted ResultKind kind;
    final @Tainted T targetFileSystem;
    final @Tainted String resolvedPath;
    final @Tainted Path remainingPath;   // to resolve in the target FileSystem
    
    @Tainted
    ResolveResult(final @Tainted ResultKind k, final @Tainted T targetFs, final @Tainted String resolveP,
        final @Tainted Path remainingP) {
      kind = k;
      targetFileSystem = targetFs;
      resolvedPath = resolveP;
      remainingPath = remainingP;  
    }
    
    // isInternalDir of path resolution completed within the mount table 
    @Tainted
    boolean isInternalDir(InodeTree.@Tainted ResolveResult<T> this) {
      return (kind == ResultKind.isInternalDir);
    }
  }
  
  /**
   * Resolve the pathname p relative to root InodeDir
   * @param p - inout path
   * @param resolveLastComponent 
   * @return ResolveResult which allows further resolution of the remaining path
   * @throws FileNotFoundException
   */
  @Tainted
  ResolveResult<@Tainted T> resolve(@Tainted InodeTree<T> this, final @Tainted String p, final @Tainted boolean resolveLastComponent)
    throws FileNotFoundException {
    // TO DO: - more efficient to not split the path, but simply compare
    @Tainted
    String @Tainted [] path = breakIntoPathComponents(p); 
    if (path.length <= 1) { // special case for when path is "/"
      @Tainted
      ResolveResult<T> res = 
        new @Tainted ResolveResult<T>(ResultKind.isInternalDir, 
              root.InodeDirFs, root.fullPath, SlashPath);
      return res;
    }
    
    @Tainted
    INodeDir<T> curInode = root;
    @Tainted
    int i;
    // ignore first slash
    for (i = 1; i < path.length - (resolveLastComponent ? 0 : 1); i++) {
      @Tainted
      INode<T> nextInode = curInode.resolveInternal(path[i]);
      if (nextInode == null) {
        @Tainted
        StringBuilder failedAt = new @Tainted StringBuilder(path[0]);
        for ( @Tainted int j = 1; j <=i; ++j) {
          failedAt.append('/').append(path[j]);
        }
        throw (new @Tainted FileNotFoundException(failedAt.toString()));      
      }

      if (nextInode instanceof @Tainted INodeLink) {
        final @Tainted INodeLink<T> link = (@Tainted INodeLink<T>) nextInode;
        final @Tainted Path remainingPath;
        if (i >= path.length-1) {
          remainingPath = SlashPath;
        } else {
          @Tainted
          StringBuilder remainingPathStr = new @Tainted StringBuilder("/" + path[i+1]);
          for (@Tainted int j = i+2; j< path.length; ++j) {
            remainingPathStr.append('/').append(path[j]);
          }
          remainingPath = new @Tainted Path(remainingPathStr.toString());
        }
        final @Tainted ResolveResult<T> res = 
          new @Tainted ResolveResult<T>(ResultKind.isExternalDir,
              link.targetFileSystem, nextInode.fullPath, remainingPath);
        return res;
      } else if (nextInode instanceof @Tainted INodeDir) {
        curInode = (@Tainted INodeDir<T>) nextInode;
      }
    }

    // We have resolved to an internal dir in mount table.
    @Tainted
    Path remainingPath;
    if (resolveLastComponent) {
      remainingPath = SlashPath;
    } else {
      // note we have taken care of when path is "/" above
      // for internal dirs rem-path does not start with / since the lookup
      // that follows will do a children.get(remaningPath) and will have to
      // strip-out the initial /
      @Tainted
      StringBuilder remainingPathStr = new @Tainted StringBuilder("/" + path[i]);
      for (@Tainted int j = i+1; j< path.length; ++j) {
        remainingPathStr.append('/').append(path[j]);
      }
      remainingPath = new @Tainted Path(remainingPathStr.toString());
    }
    final @Tainted ResolveResult<T> res = 
       new @Tainted ResolveResult<T>(ResultKind.isInternalDir,
           curInode.InodeDirFs, curInode.fullPath, remainingPath); 
    return res;
  }
  
  @Tainted
  List<@Tainted MountPoint<@Tainted T>> getMountPoints(@Tainted InodeTree<T> this) { 
    return mountPoints;
  }
  
  /**
   * 
   * @return home dir value from mount table; null if no config value
   * was found.
   */
  @Tainted
  String getHomeDirPrefixValue(@Tainted InodeTree<T> this) {
    return homedirPrefix;
  }
}
