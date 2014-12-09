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
import org.checkerframework.checker.tainting.qual.PolyTainted;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/****************************************************************
 * A <code>FilterFileSystem</code> contains
 * some other file system, which it uses as
 * its  basic file system, possibly transforming
 * the data along the way or providing  additional
 * functionality. The class <code>FilterFileSystem</code>
 * itself simply overrides all  methods of
 * <code>FileSystem</code> with versions that
 * pass all requests to the contained  file
 * system. Subclasses of <code>FilterFileSystem</code>
 * may further override some of  these methods
 * and may also provide additional methods
 * and fields.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterFileSystem extends @Tainted FileSystem {
  
  protected @Tainted FileSystem fs;
  protected @Tainted String swapScheme;
  
  /*
   * so that extending classes can define it
   */
  public @Tainted FilterFileSystem() {
  }
  
  public @Tainted FilterFileSystem(@Tainted FileSystem fs) {
    this.fs = fs;
    this.statistics = fs.statistics;
  }

  /**
   * Get the raw file system 
   * @return FileSystem being filtered
   */
  public @Tainted FileSystem getRawFileSystem(@Tainted FilterFileSystem this) {
    return fs;
  }

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(@Tainted FilterFileSystem this, @Tainted URI name, @Tainted Configuration conf) throws IOException {
    super.initialize(name, conf);
    // this is less than ideal, but existing filesystems sometimes neglect
    // to initialize the embedded filesystem
    if (fs.getConf() == null) {
      fs.initialize(name, conf);
    }
    @Tainted
    String scheme = name.getScheme();
    if (!scheme.equals(fs.getUri().getScheme())) {
      swapScheme = scheme;
    }
  }

  /** Returns a URI whose scheme and authority identify this FileSystem.*/
  @Override
  public @Tainted URI getUri(@Tainted FilterFileSystem this) {
    return fs.getUri();
  }
  
  
  @Override
  protected @Tainted URI getCanonicalUri(@Tainted FilterFileSystem this) {
    return fs.getCanonicalUri();
  }

  @Override
  protected @PolyTainted URI canonicalizeUri(@Tainted FilterFileSystem this, @PolyTainted URI uri) {
    return fs.canonicalizeUri(uri);
  }

  /** Make sure that a path specifies a FileSystem. */
  @Override
  public @Tainted Path makeQualified(@Tainted FilterFileSystem this, @Tainted Path path) {
    @Tainted
    Path fqPath = fs.makeQualified(path);
    // swap in our scheme if the filtered fs is using a different scheme
    if (swapScheme != null) {
      try {
        // NOTE: should deal with authority, but too much other stuff is broken 
        fqPath = new @Tainted Path(
            new @Tainted URI(swapScheme, fqPath.toUri().getSchemeSpecificPart(), null)
        );
      } catch (@Tainted URISyntaxException e) {
        throw new @Tainted IllegalArgumentException(e);
      }
    }
    return fqPath;
  }
  
  ///////////////////////////////////////////////////////////////
  // FileSystem
  ///////////////////////////////////////////////////////////////

  /** Check that a Path belongs to this FileSystem. */
  @Override
  protected void checkPath(@Tainted FilterFileSystem this, @Tainted Path path) {
    fs.checkPath(path);
  }

  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted FilterFileSystem this, @Tainted FileStatus file, @Tainted long start,
    @Tainted
    long len) throws IOException {
      return fs.getFileBlockLocations(file, start, len);
  }

  @Override
  public @Tainted Path resolvePath(@Tainted FilterFileSystem this, final @Tainted Path p) throws IOException {
    return fs.resolvePath(p);
  }
  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public @Tainted FSDataInputStream open(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted int bufferSize) throws IOException {
    return fs.open(f, bufferSize);
  }

  @Override
  public @Tainted FSDataOutputStream append(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted int bufferSize,
      @Tainted
      Progressable progress) throws IOException {
    return fs.append(f, bufferSize, progress);
  }

  @Override
  public void concat(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted Path @Tainted [] psrcs) throws IOException {
    fs.concat(f, psrcs);
  }

  @Override
  public @Tainted FSDataOutputStream create(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    return fs.create(f, permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }
  

  
  @Override
  @Deprecated
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      EnumSet<@Tainted CreateFlag> flags, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    
    return fs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize,
        progress);
  }

  /**
   * Set replication for an existing file.
   * 
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  @Override
  public @Tainted boolean setReplication(@Tainted FilterFileSystem this, @Tainted Path src, @Tainted short replication) throws IOException {
    return fs.setReplication(src, replication);
  }
  
  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   */
  @Override
  public @Tainted boolean rename(@Tainted FilterFileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    return fs.rename(src, dst);
  }
  
  /** Delete a file */
  @Override
  public @Tainted boolean delete(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted boolean recursive) throws IOException {
    return fs.delete(f, recursive);
  }
  
  /** List files in a directory. */
  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted FilterFileSystem this, @Tainted Path f) throws IOException {
    return fs.listStatus(f);
  }

  @Override
  public @Tainted RemoteIterator<@Tainted Path> listCorruptFileBlocks(@Tainted FilterFileSystem this, @Tainted Path path)
    throws IOException {
    return fs.listCorruptFileBlocks(path);
  }

  /** List files and its block locations in a directory. */
  @Override
  public @Tainted RemoteIterator<@Tainted LocatedFileStatus> listLocatedStatus(@Tainted FilterFileSystem this, @Tainted Path f)
  throws IOException {
    return fs.listLocatedStatus(f);
  }
  
  @Override
  public @Tainted Path getHomeDirectory(@Tainted FilterFileSystem this) {
    return fs.getHomeDirectory();
  }


  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   * 
   * @param newDir
   */
  @Override
  public void setWorkingDirectory(@Tainted FilterFileSystem this, @Tainted Path newDir) {
    fs.setWorkingDirectory(newDir);
  }
  
  /**
   * Get the current working directory for the given file system
   * 
   * @return the directory pathname
   */
  @Override
  public @Tainted Path getWorkingDirectory(@Tainted FilterFileSystem this) {
    return fs.getWorkingDirectory();
  }
  
  @Override
  protected @Tainted Path getInitialWorkingDirectory(@Tainted FilterFileSystem this) {
    return fs.getInitialWorkingDirectory();
  }
  
  @Override
  public @Tainted FsStatus getStatus(@Tainted FilterFileSystem this, @Tainted Path p) throws IOException {
    return fs.getStatus(p);
  }
  
  @Override
  public @Tainted boolean mkdirs(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted FsPermission permission) throws IOException {
    return fs.mkdirs(f, permission);
  }


  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(@Tainted FilterFileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, src, dst);
  }
  
  /**
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(@Tainted FilterFileSystem this, @Tainted boolean delSrc, @Tainted boolean overwrite, 
                                @Tainted
                                Path @Tainted [] srcs, @Tainted Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }
  
  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(@Tainted FilterFileSystem this, @Tainted boolean delSrc, @Tainted boolean overwrite, 
                                @Tainted
                                Path src, @Tainted Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * delSrc indicates if the src will be removed or not.
   */   
  @Override
  public void copyToLocalFile(@Tainted FilterFileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    fs.copyToLocalFile(delSrc, src, dst);
  }
  
  /**
   * Returns a local File that the user can write output to.  The caller
   * provides both the eventual FS target name and the local working
   * file.  If the FS is local, we write directly into the target.  If
   * the FS is remote, we write into the tmp local area.
   */
  @Override
  public @Tainted Path startLocalOutput(@Tainted FilterFileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile)
    throws IOException {
    return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
  }

  /**
   * Called when we're all done writing to the target.  A local FS will
   * do nothing, because we've written to exactly the right place.  A remote
   * FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   */
  @Override
  public void completeLocalOutput(@Tainted FilterFileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile)
    throws IOException {
    fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  /** Return the total size of all files in the filesystem.*/
  @Override
  public @Tainted long getUsed(@Tainted FilterFileSystem this) throws IOException{
    return fs.getUsed();
  }
  
  @Override
  public @Tainted long getDefaultBlockSize(@Tainted FilterFileSystem this) {
    return fs.getDefaultBlockSize();
  }
  
  @Override
  public @Tainted short getDefaultReplication(@Tainted FilterFileSystem this) {
    return fs.getDefaultReplication();
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted FilterFileSystem this) throws IOException {
    return fs.getServerDefaults();
  }

  // path variants delegate to underlying filesystem 
  @Override
  public @Tainted ContentSummary getContentSummary(@Tainted FilterFileSystem this, @Tainted Path f) throws IOException {
    return fs.getContentSummary(f);
  }

  @Override
  public @Tainted long getDefaultBlockSize(@Tainted FilterFileSystem this, @Tainted Path f) {
    return fs.getDefaultBlockSize(f);
  }

  @Override
  public @Tainted short getDefaultReplication(@Tainted FilterFileSystem this, @Tainted Path f) {
    return fs.getDefaultReplication(f);
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted FilterFileSystem this, @Tainted Path f) throws IOException {
    return fs.getServerDefaults(f);
  }

  /**
   * Get file status.
   */
  @Override
  public @Tainted FileStatus getFileStatus(@Tainted FilterFileSystem this, @Tainted Path f) throws IOException {
    return fs.getFileStatus(f);
  }

  public void createSymlink(@Tainted FilterFileSystem this, final @Tainted Path target, final @Tainted Path link,
      final @Tainted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException {
    fs.createSymlink(target, link, createParent);
  }

  public @Tainted FileStatus getFileLinkStatus(@Tainted FilterFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fs.getFileLinkStatus(f);
  }

  public @Tainted boolean supportsSymlinks(@Tainted FilterFileSystem this) {
    return fs.supportsSymlinks();
  }

  public @Tainted Path getLinkTarget(@Tainted FilterFileSystem this, @Tainted Path f) throws IOException {
    return fs.getLinkTarget(f);
  }

  protected @Tainted Path resolveLink(@Tainted FilterFileSystem this, @Tainted Path f) throws IOException {
    return fs.resolveLink(f);
  }

  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted FilterFileSystem this, @Tainted Path f) throws IOException {
    return fs.getFileChecksum(f);
  }
  
  @Override
  public void setVerifyChecksum(@Tainted FilterFileSystem this, @Tainted boolean verifyChecksum) {
    fs.setVerifyChecksum(verifyChecksum);
  }
  
  @Override
  public void setWriteChecksum(@Tainted FilterFileSystem this, @Tainted boolean writeChecksum) {
    fs.setWriteChecksum(writeChecksum);
  }

  @Override
  public @Tainted Configuration getConf(@Tainted FilterFileSystem this) {
    return fs.getConf();
  }
  
  @Override
  public void close(@Tainted FilterFileSystem this) throws IOException {
    super.close();
    fs.close();
  }

  @Override
  public void setOwner(@Tainted FilterFileSystem this, @Tainted Path p, @Untainted String username, @Untainted String groupname
      ) throws IOException {
    fs.setOwner(p, username, groupname);
  }

  @Override
  public void setTimes(@Tainted FilterFileSystem this, @Tainted Path p, @Tainted long mtime, @Tainted long atime
      ) throws IOException {
    fs.setTimes(p, mtime, atime);
  }

  @Override
  public void setPermission(@Tainted FilterFileSystem this, @Tainted Path p, @Tainted FsPermission permission
      ) throws IOException {
    fs.setPermission(p, permission);
  }

  @Override
  protected @Tainted FSDataOutputStream primitiveCreate(@Tainted FilterFileSystem this, @Tainted Path f,
      @Tainted
      FsPermission absolutePermission, @Tainted EnumSet<@Tainted CreateFlag> flag,
      @Tainted
      int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress, @Tainted ChecksumOpt checksumOpt)
      throws IOException {
    return fs.primitiveCreate(f, absolutePermission, flag,
        bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  @SuppressWarnings("deprecation")
  protected @Tainted boolean primitiveMkdir(@Tainted FilterFileSystem this, @Tainted Path f, @Tainted FsPermission abdolutePermission)
      throws IOException {
    return fs.primitiveMkdir(f, abdolutePermission);
  }
  
  @Override // FileSystem
  public @Tainted FileSystem @Tainted [] getChildFileSystems(@Tainted FilterFileSystem this) {
    return new @Tainted FileSystem @Tainted []{fs};
  }

  @Override // FileSystem
  public @Tainted Path createSnapshot(@Tainted FilterFileSystem this, @Tainted Path path, @Tainted String snapshotName)
      throws IOException {
    return fs.createSnapshot(path, snapshotName);
  }
  
  @Override // FileSystem
  public void renameSnapshot(@Tainted FilterFileSystem this, @Tainted Path path, @Tainted String snapshotOldName,
      @Tainted
      String snapshotNewName) throws IOException {
    fs.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }
  
  @Override // FileSystem
  public void deleteSnapshot(@Tainted FilterFileSystem this, @Tainted Path path, @Tainted String snapshotName)
      throws IOException {
    fs.deleteSnapshot(path, snapshotName);
  }
}
