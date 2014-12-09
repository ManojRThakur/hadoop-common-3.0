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
import org.checkerframework.checker.tainting.qual.PolyTainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * Implement the FileSystem API for the raw local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RawLocalFileSystem extends @Tainted FileSystem {
  static final @Tainted URI NAME = URI.create("file:///");
  private @Tainted Path workingDir;
  private static final @Tainted boolean useDeprecatedFileStatus = !Stat.isAvailable();
  
  public @Tainted RawLocalFileSystem() {
    workingDir = getInitialWorkingDirectory();
  }
  
  private @Tainted Path makeAbsolute(@Tainted RawLocalFileSystem this, @Tainted Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new @Tainted Path(workingDir, f);
    }
  }
  
  /** Convert a path to a File. */
  @SuppressWarnings("ostrusted")// BUZZZSAWWW!
  public @Untainted File pathToFile(@Tainted RawLocalFileSystem this, @Tainted Path path) {
    checkPath(path);
    if (!path.isAbsolute()) {
      path = new @Tainted Path(getWorkingDirectory(), path);
    }
    return new @Untainted File(path.toUri().getPath());
  }

  @Override
  public @Tainted URI getUri(@Tainted RawLocalFileSystem this) { return NAME; }
  
  @Override
  public void initialize(@Tainted RawLocalFileSystem this, @Tainted URI uri, @Tainted Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
  }
  
  class TrackingFileInputStream extends @Tainted FileInputStream {
    public @Tainted TrackingFileInputStream(@Tainted File f) throws IOException {
      super(f);
    }
    
    @Override
    public @Tainted int read(@Tainted RawLocalFileSystem.TrackingFileInputStream this) throws IOException {
      @Tainted
      int result = super.read();
      if (result != -1) {
        statistics.incrementBytesRead(1);
      }
      return result;
    }
    
    @Override
    public @Tainted int read(@Tainted RawLocalFileSystem.TrackingFileInputStream this, @Tainted byte @Tainted [] data) throws IOException {
      @Tainted
      int result = super.read(data);
      if (result != -1) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }
    
    @Override
    public @Tainted int read(@Tainted RawLocalFileSystem.TrackingFileInputStream this, @Tainted byte @Tainted [] data, @Tainted int offset, @Tainted int length) throws IOException {
      @Tainted
      int result = super.read(data, offset, length);
      if (result != -1) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }
  }

  /*******************************************************
   * For open()'s FSInputStream.
   *******************************************************/
  class LocalFSFileInputStream extends @Tainted FSInputStream implements @Tainted HasFileDescriptor {
    private @Tainted FileInputStream fis;
    private @Tainted long position;

    public @Tainted LocalFSFileInputStream(@Tainted Path f) throws IOException {
      this.fis = new @Tainted TrackingFileInputStream(pathToFile(f));
    }
    
    @Override
    public void seek(@Tainted RawLocalFileSystem.LocalFSFileInputStream this, @Tainted long pos) throws IOException {
      fis.getChannel().position(pos);
      this.position = pos;
    }
    
    @Override
    public @Tainted long getPos(@Tainted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException {
      return this.position;
    }
    
    @Override
    public @Tainted boolean seekToNewSource(@Tainted RawLocalFileSystem.LocalFSFileInputStream this, @Tainted long targetPos) throws IOException {
      return false;
    }
    
    /*
     * Just forward to the fis
     */
    @Override
    public @Tainted int available(@Tainted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException { return fis.available(); }
    @Override
    public void close(@Tainted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException { fis.close(); }
    @Override
    public @Tainted boolean markSupported(@Tainted RawLocalFileSystem.LocalFSFileInputStream this) { return false; }
    
    @Override
    public @Tainted int read(@Tainted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException {
      try {
        @Tainted
        int value = fis.read();
        if (value >= 0) {
          this.position++;
        }
        return value;
      } catch (@Tainted IOException e) {                 // unexpected exception
        throw new @Tainted FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public @Tainted int read(@Tainted RawLocalFileSystem.LocalFSFileInputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
      try {
        @Tainted
        int value = fis.read(b, off, len);
        if (value > 0) {
          this.position += value;
        }
        return value;
      } catch (@Tainted IOException e) {                 // unexpected exception
        throw new @Tainted FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public @Tainted int read(@Tainted RawLocalFileSystem.LocalFSFileInputStream this, @Tainted long position, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len)
      throws IOException {
      @Tainted
      ByteBuffer bb = ByteBuffer.wrap(b, off, len);
      try {
        return fis.getChannel().read(bb, position);
      } catch (@Tainted IOException e) {
        throw new @Tainted FSError(e);
      }
    }
    
    @Override
    public @Tainted long skip(@Tainted RawLocalFileSystem.LocalFSFileInputStream this, @Tainted long n) throws IOException {
      @Tainted
      long value = fis.skip(n);
      if (value > 0) {
        this.position += value;
      }
      return value;
    }

    @Override
    public @Tainted FileDescriptor getFileDescriptor(@Tainted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException {
      return fis.getFD();
    }
  }
  
  @Override
  public @Tainted FSDataInputStream open(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted int bufferSize) throws IOException {
    if (!exists(f)) {
      throw new @Tainted FileNotFoundException(f.toString());
    }
    return new @Tainted FSDataInputStream(new @Tainted BufferedFSInputStream(
        new @Tainted LocalFSFileInputStream(f), bufferSize));
  }
  
  /*********************************************************
   * For create()'s FSOutputStream.
   *********************************************************/
  class LocalFSFileOutputStream extends @Tainted OutputStream {
    private @Tainted FileOutputStream fos;
    
    private @Tainted LocalFSFileOutputStream(@Tainted Path f, @Tainted boolean append) throws IOException {
      this.fos = new @Tainted FileOutputStream(pathToFile(f), append);
    }
    
    /*
     * Just forward to the fos
     */
    @Override
    public void close(@Tainted RawLocalFileSystem.LocalFSFileOutputStream this) throws IOException { fos.close(); }
    @Override
    public void flush(@Tainted RawLocalFileSystem.LocalFSFileOutputStream this) throws IOException { fos.flush(); }
    @Override
    public void write(@Tainted RawLocalFileSystem.LocalFSFileOutputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
      try {
        fos.write(b, off, len);
      } catch (@Tainted IOException e) {                // unexpected exception
        throw new @Tainted FSError(e);                  // assume native fs error
      }
    }
    
    @Override
    public void write(@Tainted RawLocalFileSystem.LocalFSFileOutputStream this, @Tainted int b) throws IOException {
      try {
        fos.write(b);
      } catch (@Tainted IOException e) {              // unexpected exception
        throw new @Tainted FSError(e);                // assume native fs error
      }
    }
  }

  @Override
  public @Tainted FSDataOutputStream append(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted int bufferSize,
      @Tainted
      Progressable progress) throws IOException {
    if (!exists(f)) {
      throw new @Tainted FileNotFoundException("File " + f + " not found");
    }
    if (getFileStatus(f).isDirectory()) {
      throw new @Tainted IOException("Cannot append to a diretory (=" + f + " )");
    }
    return new @Tainted FSDataOutputStream(new @Tainted BufferedOutputStream(
        new @Tainted LocalFSFileOutputStream(f, true), bufferSize), statistics);
  }

  @Override
  public @Tainted FSDataOutputStream create(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted boolean overwrite, @Tainted int bufferSize,
    @Tainted
    short replication, @Tainted long blockSize, @Tainted Progressable progress)
    throws IOException {
    return create(f, overwrite, true, bufferSize, replication, blockSize, progress);
  }

  private @Tainted FSDataOutputStream create(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted boolean overwrite,
      @Tainted
      boolean createParent, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    if (exists(f) && !overwrite) {
      throw new @Tainted IOException("File already exists: "+f);
    }
    @Tainted
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new @Tainted IOException("Mkdirs failed to create " + parent.toString());
    }
    return new @Tainted FSDataOutputStream(new @Tainted BufferedOutputStream(
        new @Tainted LocalFSFileOutputStream(f, false), bufferSize), statistics);
  }
  
  @Override
  @Deprecated
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      EnumSet<@Tainted CreateFlag> flags, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
      throw new @Tainted IOException("File already exists: "+f);
    }
    return new @Tainted FSDataOutputStream(new @Tainted BufferedOutputStream(
        new @Tainted LocalFSFileOutputStream(f, false), bufferSize), statistics);
  }

  @Override
  public @Tainted FSDataOutputStream create(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
    @Tainted
    boolean overwrite, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
    @Tainted
    Progressable progress) throws IOException {

    @Tainted
    FSDataOutputStream out = create(f,
        overwrite, bufferSize, replication, blockSize, progress);
    setPermission(f, permission);
    return out;
  }

  @Override
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite,
      @Tainted
      int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    @Tainted
    FSDataOutputStream out = create(f,
        overwrite, false, bufferSize, replication, blockSize, progress);
    setPermission(f, permission);
    return out;
  }

  @Override
  public @Tainted boolean rename(@Tainted RawLocalFileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    // Attempt rename using Java API.
    @Tainted
    File srcFile = pathToFile(src);
    @Tainted
    File dstFile = pathToFile(dst);
    if (srcFile.renameTo(dstFile)) {
      return true;
    }

    // Enforce POSIX rename behavior that a source directory replaces an existing
    // destination if the destination is an empty directory.  On most platforms,
    // this is already handled by the Java API call above.  Some platforms
    // (notably Windows) do not provide this behavior, so the Java API call above
    // fails.  Delete destination and attempt rename again.
    if (this.exists(dst)) {
      @Tainted
      FileStatus sdst = this.getFileStatus(dst);
      if (sdst.isDirectory() && dstFile.list().length == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deleting empty destination and renaming " + src + " to " +
            dst);
        }
        if (this.delete(dst, false) && srcFile.renameTo(dstFile)) {
          return true;
        }
      }
    }

    // The fallback behavior accomplishes the rename by a full copy.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Falling through to a copy of " + src + " to " + dst);
    }
    return FileUtil.copy(this, src, this, dst, true, getConf());
  }
  
  /**
   * Delete the given path to a file or directory.
   * @param p the path to delete
   * @param recursive to delete sub-directories
   * @return true if the file or directory and all its contents were deleted
   * @throws IOException if p is non-empty and recursive is false 
   */
  @Override
  public @Tainted boolean delete(@Tainted RawLocalFileSystem this, @Tainted Path p, @Tainted boolean recursive) throws IOException {
    @Tainted
    File f = pathToFile(p);
    if (f.isFile()) {
      return f.delete();
    } else if (!recursive && f.isDirectory() && 
        (FileUtil.listFiles(f).length != 0)) {
      throw new @Tainted IOException("Directory " + f.toString() + " is not empty");
    }
    return FileUtil.fullyDelete(f);
  }
 
  /**
   * {@inheritDoc}
   *
   * (<b>Note</b>: Returned list is not sorted in any given order,
   * due to reliance on Java's {@link File#list()} API.)
   */
  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted RawLocalFileSystem this, @Tainted Path f) throws IOException {
    @Tainted
    File localf = pathToFile(f);
    @Tainted
    FileStatus @Tainted [] results;

    if (!localf.exists()) {
      throw new @Tainted FileNotFoundException("File " + f + " does not exist");
    }
    if (localf.isFile()) {
      if (!useDeprecatedFileStatus) {
        return new @Tainted FileStatus @Tainted [] { getFileStatus(f) };
      }
      return new @Tainted FileStatus @Tainted [] {
        new @Tainted DeprecatedRawLocalFileStatus(localf, getDefaultBlockSize(f), this)};
    }

    @Tainted
    String @Tainted [] names = localf.list();
    if (names == null) {
      return null;
    }
    results = new @Tainted FileStatus @Tainted [names.length];
    @Tainted
    int j = 0;
    for (@Tainted int i = 0; i < names.length; i++) {
      try {
        // Assemble the path using the Path 3 arg constructor to make sure
        // paths with colon are properly resolved on Linux
        results[j] = getFileStatus(new @Tainted Path(f, new @Tainted Path(null, null, names[i])));
        j++;
      } catch (@Tainted FileNotFoundException e) {
        // ignore the files not found since the dir list may have have changed
        // since the names[] list was generated.
      }
    }
    if (j == names.length) {
      return results;
    }
    return Arrays.copyOf(results, j);
  }

  /**
   * Creates the specified directory hierarchy. Does not
   * treat existence as an error.
   */
  @Override
  public @Tainted boolean mkdirs(@Tainted RawLocalFileSystem this, @Tainted Path f) throws IOException {
    if(f == null) {
      throw new @Tainted IllegalArgumentException("mkdirs path arg is null");
    }
    @Tainted
    Path parent = f.getParent();
    @Tainted
    File p2f = pathToFile(f);
    if(parent != null) {
      @Tainted
      File parent2f = pathToFile(parent);
      if(parent2f != null && parent2f.exists() && !parent2f.isDirectory()) {
        throw new @Tainted FileAlreadyExistsException("Parent path is not a directory: " 
            + parent);
      }
    }
    return (parent == null || mkdirs(parent)) &&
      (p2f.mkdir() || p2f.isDirectory());
  }

  @Override
  public @Tainted boolean mkdirs(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted FsPermission permission) throws IOException {
    @Tainted
    boolean b = mkdirs(f);
    if(b) {
      setPermission(f, permission);
    }
    return b;
  }
  

  @Override
  protected @Tainted boolean primitiveMkdir(@Tainted RawLocalFileSystem this, @Tainted Path f, @Tainted FsPermission absolutePermission)
    throws IOException {
    @Tainted
    boolean b = mkdirs(f);
    setPermission(f, absolutePermission);
    return b;
  }
  
  
  @Override
  public @Tainted Path getHomeDirectory(@Tainted RawLocalFileSystem this) {
    return this.makeQualified(new @Tainted Path(System.getProperty("user.home")));
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(@Tainted RawLocalFileSystem this, @Tainted Path newDir) {
    workingDir = makeAbsolute(newDir);
    checkPath(workingDir);
  }
  
  @Override
  public @Tainted Path getWorkingDirectory(@Tainted RawLocalFileSystem this) {
    return workingDir;
  }
  
  @Override
  protected @Tainted Path getInitialWorkingDirectory(@Tainted RawLocalFileSystem this) {
    return this.makeQualified(new @Tainted Path(System.getProperty("user.dir")));
  }

  @Override
  public @Tainted FsStatus getStatus(@Tainted RawLocalFileSystem this, @Tainted Path p) throws IOException {
    @Tainted
    File partition = pathToFile(p == null ? new @Tainted Path("/") : p);
    //File provides getUsableSpace() and getFreeSpace()
    //File provides no API to obtain used space, assume used = total - free
    return new @Tainted FsStatus(partition.getTotalSpace(), 
      partition.getTotalSpace() - partition.getFreeSpace(),
      partition.getFreeSpace());
  }
  
  // In the case of the local filesystem, we can just rename the file.
  @Override
  public void moveFromLocalFile(@Tainted RawLocalFileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    rename(src, dst);
  }
  
  // We can write output directly to the final location
  @Override
  public @Tainted Path startLocalOutput(@Tainted RawLocalFileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile)
    throws IOException {
    return fsOutputFile;
  }
  
  // It's in the right place - nothing to do.
  @Override
  public void completeLocalOutput(@Tainted RawLocalFileSystem this, @Tainted Path fsWorkingFile, @Tainted Path tmpLocalFile)
    throws IOException {
  }
  
  @Override
  public void close(@Tainted RawLocalFileSystem this) throws IOException {
    super.close();
  }
  
  @Override
  public @Tainted String toString(@Tainted RawLocalFileSystem this) {
    return "LocalFS";
  }
  
  @Override
  public @Tainted FileStatus getFileStatus(@Tainted RawLocalFileSystem this, @Tainted Path f) throws IOException {
    return getFileLinkStatusInternal(f, true);
  }

  @Deprecated
  private @Tainted FileStatus deprecatedGetFileStatus(@Tainted RawLocalFileSystem this, @Tainted Path f) throws IOException {
    @Tainted
    File path = pathToFile(f);
    if (path.exists()) {
      return new @Tainted DeprecatedRawLocalFileStatus(pathToFile(f),
          getDefaultBlockSize(f), this);
    } else {
      throw new @Tainted FileNotFoundException("File " + f + " does not exist");
    }
  }

  @Deprecated
  static class DeprecatedRawLocalFileStatus extends @Tainted FileStatus {
    /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
     * We recognize if the information is already loaded by check if
     * onwer.equals("").
     */
    private @Tainted boolean isPermissionLoaded(RawLocalFileSystem.@Tainted DeprecatedRawLocalFileStatus this) {
      return !super.getOwner().isEmpty(); 
    }
    
    @Tainted
    DeprecatedRawLocalFileStatus(@Tainted File f, @Tainted long defaultBlockSize, @Tainted FileSystem fs) {
      super(f.length(), f.isDirectory(), 1, defaultBlockSize,
          f.lastModified(), new @Tainted Path(f.getPath()).makeQualified(fs.getUri(),
            fs.getWorkingDirectory()));
    }
    
    @Override
    public @Tainted FsPermission getPermission(RawLocalFileSystem.@Tainted DeprecatedRawLocalFileStatus this) {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getPermission();
    }

    @Override
    public @Untainted String getOwner(RawLocalFileSystem.@Tainted DeprecatedRawLocalFileStatus this) {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getOwner();
    }

    @Override
    public @Untainted String getGroup(RawLocalFileSystem.@Tainted DeprecatedRawLocalFileStatus this) {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getGroup();
    }

    /// loads permissions, owner, and group from `ls -ld`
    private void loadPermissionInfo(RawLocalFileSystem.@Tainted DeprecatedRawLocalFileStatus this) {
      @Tainted
      IOException e = null;
      try {
        @SuppressWarnings("ostrusted") // BUZZZSAW # 2
        @Tainted
        String output = FileUtil.execCommand(new @Untainted File(getPath().toString()), Shell.getGetPermissionCommand());
        @Tainted
        StringTokenizer t =
            new @Tainted StringTokenizer(output, Shell.TOKEN_SEPARATOR_REGEX);
        //expected format
        //-rw-------    1 username groupname ...
        @Tainted
        String permission = t.nextToken();
        if (permission.length() > 10) { //files with ACLs might have a '+'
          permission = permission.substring(0, 10);
        }
        setPermission(FsPermission.valueOf(permission));
        t.nextToken();

        @SuppressWarnings("ostrusted:cast.unsafe")
        @Untainted
        String owner = (@Untainted String) t.nextToken();
        // If on windows domain, token format is DOMAIN\\user and we want to
        // extract only the user name
        if (Shell.WINDOWS) {
          @Tainted
          int i = owner.indexOf('\\');
          if (i != -1) {
            @SuppressWarnings("ostrusted:cast.unsafe")
            @Untainted
            String ownerTmp = (@Untainted String) t.nextToken();

            owner = ownerTmp;
          }
        }
        setOwner(owner);

        @SuppressWarnings("ostrusted:cast.unsafe")
        @Untainted
        String grp = (@Untainted String) t.nextToken();
        setGroup(grp);
      } catch (Shell.@Tainted ExitCodeException ioe) {
        if (ioe.getExitCode() != 1) {
          e = ioe;
        } else {
          setPermission(null);
          setOwner(null);
          setGroup(null);
        }
      } catch (@Tainted IOException ioe) {
        e = ioe;
      } finally {
        if (e != null) {
          throw new @Tainted RuntimeException("Error while running command to get " +
                                     "file permissions : " + 
                                     StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public void write(RawLocalFileSystem.@Tainted DeprecatedRawLocalFileStatus this, @Tainted DataOutput out) throws IOException {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      super.write(out);
    }
  }

  /**
   * Use the command chown to set owner.
   */
  @Override
  public void setOwner(@Tainted RawLocalFileSystem this, @Tainted Path p, @Untainted String username, @Untainted String groupname)
    throws IOException {
    FileUtil.setOwner(pathToFile(p), username, groupname);
  }

  /**
   * Use the command chmod to set permission.
   */
  @Override
  public void setPermission(@Tainted RawLocalFileSystem this, @Tainted Path p, @Tainted FsPermission permission)
    throws IOException {
    if (NativeIO.isAvailable()) {
      NativeIO.POSIX.chmod(pathToFile(p).getCanonicalPath(),
                     permission.toShort());
    } else {
      @SuppressWarnings("ostrusted:cast.unsafe")
      @Untainted
      String perm = (@Untainted String) String.format("%04o", permission.toShort());
      Shell.execCommand(Shell.getSetPermissionCommand(perm, false,
        FileUtil.makeShellPath(pathToFile(p), true)));
    }
  }
 
  /**
   * Sets the {@link Path}'s last modified time <em>only</em> to the given
   * valid time.
   *
   * @param mtime the modification time to set (only if greater than zero).
   * @param atime currently ignored.
   * @throws IOException if setting the last modified time fails.
   */
  @Override
  public void setTimes(@Tainted RawLocalFileSystem this, @Tainted Path p, @Tainted long mtime, @Tainted long atime) throws IOException {
    @Tainted
    File f = pathToFile(p);
    if(mtime >= 0) {
      if(!f.setLastModified(mtime)) {
        throw new @Tainted IOException(
          "couldn't set last-modified time to " +
          mtime +
          " for " +
          f.getAbsolutePath());
      }
    }
  }

  @Override
  public @Tainted boolean supportsSymlinks(@Tainted RawLocalFileSystem this) {
    return true;
  }

  @Override
  public void createSymlink(@Tainted RawLocalFileSystem this, @Tainted Path target, @Tainted Path link, @Tainted boolean createParent)
      throws IOException {
    final @Tainted String targetScheme = target.toUri().getScheme();
    if (targetScheme != null && !"file".equals(targetScheme)) {
      throw new @Tainted IOException("Unable to create symlink to non-local file "+
                            "system: "+target.toString());
    }
    if (createParent) {
      mkdirs(link.getParent());
    }
    @SuppressWarnings("ostrusted:cast.unsafe") // Buzzsaw 3
    @Untainted
    String targetStr = (@Untainted String) target.toString();

    @SuppressWarnings("ostrusted:cast.unsafe") // Buzzsaw 4
    @Untainted
    String linkStr = (@Untainted String) makeAbsolute(link).toString();

    // NB: Use createSymbolicLink in java.nio.file.Path once available
    @Tainted
    int result = FileUtil.symLink(targetStr, linkStr);
    if (result != 0) {
      throw new @Tainted IOException("Error " + result + " creating symlink " +
          link + " to " + target);
    }
  }

  /**
   * Return a FileStatus representing the given path. If the path refers
   * to a symlink return a FileStatus representing the link rather than
   * the object the link refers to.
   */
  @Override
  public @Tainted FileStatus getFileLinkStatus(@Tainted RawLocalFileSystem this, final @Tainted Path f) throws IOException {
    @Tainted
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // getFileLinkStatus is supposed to return a symlink with a
    // qualified path
    if (fi.isSymlink()) {
      @Tainted
      Path targetQual = FSLinkResolver.qualifySymlinkTarget(this.getUri(),
          fi.getPath(), fi.getSymlink());
      fi.setSymlink(targetQual);
    }
    return fi;
  }

  /**
   * Public {@link FileStatus} methods delegate to this function, which in turn
   * either call the new {@link Stat} based implementation or the deprecated
   * methods based on platform support.
   * 
   * @param f Path to stat
   * @param dereference whether to dereference the final path component if a
   *          symlink
   * @return FileStatus of f
   * @throws IOException
   */
  private @Tainted FileStatus getFileLinkStatusInternal(@Tainted RawLocalFileSystem this, final @Tainted Path f,
      @Tainted
      boolean dereference) throws IOException {
    if (!useDeprecatedFileStatus) {
      return getNativeFileLinkStatus(f, dereference);
    } else if (dereference) {
      return deprecatedGetFileStatus(f);
    } else {
      return deprecatedGetFileLinkStatusInternal(f);
    }
  }

  /**
   * Deprecated. Remains for legacy support. Should be removed when {@link Stat}
   * gains support for Windows and other operating systems.
   */
  @Deprecated
  private @Tainted FileStatus deprecatedGetFileLinkStatusInternal(@Tainted RawLocalFileSystem this, final @Tainted Path f)
      throws IOException {
    @Tainted
    String target = FileUtil.readLink(pathToFile(f));

    try {
      @Tainted
      FileStatus fs = getFileStatus(f);
      // If f refers to a regular file or directory
      if (target.isEmpty()) {
        return fs;
      }
      // Otherwise f refers to a symlink
      return new @Tainted FileStatus(fs.getLen(),
          false,
          fs.getReplication(),
          fs.getBlockSize(),
          fs.getModificationTime(),
          fs.getAccessTime(),
          fs.getPermission(),
          fs.getOwner(),
          fs.getGroup(),
          new @Tainted Path(target),
          f);
    } catch (@Tainted FileNotFoundException e) {
      /* The exists method in the File class returns false for dangling
       * links so we can get a FileNotFoundException for links that exist.
       * It's also possible that we raced with a delete of the link. Use
       * the readBasicFileAttributes method in java.nio.file.attributes
       * when available.
       */
      if (!target.isEmpty()) {
        return new @Tainted FileStatus(0, false, 0, 0, 0, 0, FsPermission.getDefault(),
            "", "", new @Tainted Path(target), f);
      }
      // f refers to a file or directory that does not exist
      throw e;
    }
  }
  /**
   * Calls out to platform's native stat(1) implementation to get file metadata
   * (permissions, user, group, atime, mtime, etc). This works around the lack
   * of lstat(2) in Java 6.
   * 
   *  Currently, the {@link Stat} class used to do this only supports Linux
   *  and FreeBSD, so the old {@link #deprecatedGetFileLinkStatusInternal(Path)}
   *  implementation (deprecated) remains further OS support is added.
   *
   * @param f File to stat
   * @param dereference whether to dereference symlinks
   * @return FileStatus of f
   * @throws IOException
   */
  private @Tainted FileStatus getNativeFileLinkStatus(@Tainted RawLocalFileSystem this, final @Tainted Path f,
      @Tainted
      boolean dereference) throws IOException {
    checkPath(f);
    @SuppressWarnings("ostrusted:cast.unsafe")
    @Untainted Path p = (@Untainted Path) f; // Buzzsaw. need to validate before going to f.
    @Tainted
    Stat stat = new @Tainted Stat(p, getDefaultBlockSize(f), dereference, this);
    @Tainted
    FileStatus status = stat.getFileStatus();
    return status;
  }

  @Override
  public @Tainted Path getLinkTarget(@Tainted RawLocalFileSystem this, @Tainted Path f) throws IOException {
    @Tainted
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // return an unqualified symlink target
    return fi.getSymlink();
  }
}
