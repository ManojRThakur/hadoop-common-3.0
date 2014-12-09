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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/****************************************************************
 * Implement the FileSystem API for the checksumed local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LocalFileSystem extends @Tainted ChecksumFileSystem {
  static final @Tainted URI NAME = URI.create("file:///");
  static private @Tainted Random rand = new @Tainted Random();
  
  public @Tainted LocalFileSystem() {
    this(new @Tainted RawLocalFileSystem());
  }
  
  @Override
  public void initialize(@Tainted LocalFileSystem this, @Tainted URI name, @Tainted Configuration conf) throws IOException {
    if (fs.getConf() == null) {
      fs.initialize(name, conf);
    }
    @Tainted
    String scheme = name.getScheme();
    if (!scheme.equals(fs.getUri().getScheme())) {
      swapScheme = scheme;
    }
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>file</code>
   */
  @Override
  public @Tainted String getScheme(@Tainted LocalFileSystem this) {
    return "file";
  }

  public @Tainted FileSystem getRaw(@Tainted LocalFileSystem this) {
    return getRawFileSystem();
  }
    
  public @Tainted LocalFileSystem(@Tainted FileSystem rawLocalFileSystem) {
    super(rawLocalFileSystem);
  }
    
  /** Convert a path to a File. */
  public @Tainted File pathToFile(@Tainted LocalFileSystem this, @Tainted Path path) {
    return ((@Tainted RawLocalFileSystem)fs).pathToFile(path);
  }

  @Override
  public void copyFromLocalFile(@Tainted LocalFileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    FileUtil.copy(this, src, this, dst, delSrc, getConf());
  }

  @Override
  public void copyToLocalFile(@Tainted LocalFileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    FileUtil.copy(this, src, this, dst, delSrc, getConf());
  }

  /**
   * Moves files to a bad file directory on the same device, so that their
   * storage will not be reused.
   */
  @Override
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @Tainted boolean reportChecksumFailure(@Tainted LocalFileSystem this, @Tainted Path p, @Tainted FSDataInputStream in,
                                       @Tainted
                                       long inPos,
                                       @Tainted
                                       FSDataInputStream sums, @Tainted long sumsPos) {
    try {
      // canonicalize f
      @Untainted File f = (@Untainted File) ((@Tainted RawLocalFileSystem)fs).pathToFile(p).getCanonicalFile();

      //ostrusted check file path first to ensure safety, therefore we can have an @Tainted Path p argument
      checkPath(p);
      // find highest writable parent dir of f on the same device
      @Tainted
      String device = new @Tainted DF(f, getConf()).getMount();
      @Tainted
      File parent = f.getParentFile();
      @Tainted
      File dir = null;
      while (parent != null && FileUtil.canWrite(parent) &&
          parent.toString().startsWith(device)) {
        dir = parent;
        parent = parent.getParentFile();
      }

      if (dir==null) {
        throw new @Tainted IOException(
                              "not able to find the highest writable parent dir");
      }
        
      // move the file there
      @Tainted
      File badDir = new @Tainted File(dir, "bad_files");
      if (!badDir.mkdirs()) {
        if (!badDir.isDirectory()) {
          throw new @Tainted IOException("Mkdirs failed to create " + badDir.toString());
        }
      }
      @Tainted
      String suffix = "." + rand.nextInt();
      @Tainted
      File badFile = new @Tainted File(badDir, f.getName()+suffix);
      LOG.warn("Moving bad file " + f + " to " + badFile);
      in.close();                               // close it first
      @Tainted
      boolean b = f.renameTo(badFile);                      // rename it
      if (!b) {
        LOG.warn("Ignoring failure of renameTo");
      }
      // move checksum file too
      @Tainted
      File checkFile = ((@Tainted RawLocalFileSystem)fs).pathToFile(getChecksumFile(p));
      // close the stream before rename to release the file handle
      sums.close();
      b = checkFile.renameTo(new @Tainted File(badDir, checkFile.getName()+suffix));
      if (!b) {
          LOG.warn("Ignoring failure of renameTo");
        }
    } catch (@Tainted IOException e) {
      //ostrusted, avoid exception in QualifierHierarchy
      //LOG.warn("Error moving bad file " + p + ": " + e);
    }
    return false;
  }

  @Override
  public @Tainted boolean supportsSymlinks(@Tainted LocalFileSystem this) {
    return true;
  }

  @Override
  public void createSymlink(@Tainted LocalFileSystem this, @Tainted Path target, @Tainted Path link, @Tainted boolean createParent)
      throws IOException {
    fs.createSymlink(target, link, createParent);
  }

  @Override
  public @Tainted FileStatus getFileLinkStatus(@Tainted LocalFileSystem this, final @Tainted Path f) throws IOException {
    return fs.getFileLinkStatus(f);
  }

  @Override
  public @Tainted Path getLinkTarget(@Tainted LocalFileSystem this, @Tainted Path f) throws IOException {
    return fs.getLinkTarget(f);
  }
}
