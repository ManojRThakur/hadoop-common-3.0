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

package org.apache.hadoop.fs.shell;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.PathOperationException;
import org.apache.hadoop.io.IOUtils;

/**
 * Provides: argument processing to ensure the destination is valid
 * for the number of source arguments.  A processPaths that accepts both
 * a source and resolved target.  Sources are resolved as children of
 * a destination directory.
 */
abstract class CommandWithDestination extends @Tainted FsCommand {  
  protected @Tainted PathData dst;
  private @Tainted boolean overwrite = false;
  private @Tainted boolean preserve = false;
  private @Tainted boolean verifyChecksum = true;
  private @Tainted boolean writeChecksum = true;
  
  /**
   * 
   * This method is used to enable the force(-f)  option while copying the files.
   * 
   * @param flag true/false
   */
  protected void setOverwrite(@Tainted CommandWithDestination this, @Tainted boolean flag) {
    overwrite = flag;
  }
  
  protected void setVerifyChecksum(@Tainted CommandWithDestination this, @Tainted boolean flag) {
    verifyChecksum = flag;
  }
  
  protected void setWriteChecksum(@Tainted CommandWithDestination this, @Tainted boolean flag) {
    writeChecksum = flag;
  }
  
  /**
   * If true, the last modified time, last access time,
   * owner, group and permission information of the source
   * file will be preserved as far as target {@link FileSystem}
   * implementation allows.
   */
  protected void setPreserve(@Tainted CommandWithDestination this, @Tainted boolean preserve) {
    this.preserve = preserve;
  }

  /**
   *  The last arg is expected to be a local path, if only one argument is
   *  given then the destination will be the current directory 
   *  @param args is the list of arguments
   */
  protected void getLocalDestination(@Tainted CommandWithDestination this, @Tainted LinkedList<@Tainted String> args)
  throws IOException {
    @Tainted
    String pathString = (args.size() < 2) ? Path.CUR_DIR : args.removeLast();
    try {
      dst = new @Tainted PathData(new @Tainted URI(pathString), getConf());
    } catch (@Tainted URISyntaxException e) {
      if (Path.WINDOWS) {
        // Unlike URI, PathData knows how to parse Windows drive-letter paths.
        dst = new @Tainted PathData(pathString, getConf());
      } else {
        throw new @Tainted IOException("unexpected URISyntaxException", e);
      }
    }
  }

  /**
   *  The last arg is expected to be a remote path, if only one argument is
   *  given then the destination will be the remote user's directory 
   *  @param args is the list of arguments
   *  @throws PathIOException if path doesn't exist or matches too many times 
   */
  protected void getRemoteDestination(@Tainted CommandWithDestination this, @Tainted LinkedList<@Tainted String> args)
  throws IOException {
    if (args.size() < 2) {
      dst = new @Tainted PathData(Path.CUR_DIR, getConf());
    } else {
      @Tainted
      String pathString = args.removeLast();
      // if the path is a glob, then it must match one and only one path
      @Tainted
      PathData @Tainted [] items = PathData.expandAsGlob(pathString, getConf());
      switch (items.length) {
        case 0:
          throw new @Tainted PathNotFoundException(pathString);
        case 1:
          dst = items[0];
          break;
        default:
          throw new @Tainted PathIOException(pathString, "Too many matches");
      }
    }
  }

  @Override
  protected void processArguments(@Tainted CommandWithDestination this, @Tainted LinkedList<@Tainted PathData> args)
  throws IOException {
    // if more than one arg, the destination must be a directory
    // if one arg, the dst must not exist or must be a directory
    if (args.size() > 1) {
      if (!dst.exists) {
        throw new @Tainted PathNotFoundException(dst.toString());
      }
      if (!dst.stat.isDirectory()) {
        throw new @Tainted PathIsNotDirectoryException(dst.toString());
      }
    } else if (dst.exists) {
      if (!dst.stat.isDirectory() && !overwrite) {
        throw new @Tainted PathExistsException(dst.toString());
      }
    } else if (!dst.parentExists()) {
      throw new @Tainted PathNotFoundException(dst.toString());
    }
    super.processArguments(args);
  }

  @Override
  protected void processPathArgument(@Tainted CommandWithDestination this, @Tainted PathData src)
  throws IOException {
    if (src.stat.isDirectory() && src.fs.equals(dst.fs)) {
      @Tainted
      PathData target = getTargetPath(src);
      @Tainted
      String srcPath = src.fs.makeQualified(src.path).toString();
      @Tainted
      String dstPath = dst.fs.makeQualified(target.path).toString();
      if (dstPath.equals(srcPath)) {
        @Tainted
        PathIOException e = new @Tainted PathIOException(src.toString(),
            "are identical");
        e.setTargetPath(dstPath.toString());
        throw e;
      }
      if (dstPath.startsWith(srcPath+Path.SEPARATOR)) {
        @Tainted
        PathIOException e = new @Tainted PathIOException(src.toString(),
            "is a subdirectory of itself");
        e.setTargetPath(target.toString());
        throw e;
      }
    }
    super.processPathArgument(src);
  }

  @Override
  protected void processPath(@Tainted CommandWithDestination this, @Tainted PathData src) throws IOException {
    processPath(src, getTargetPath(src));
  }
  
  /**
   * Called with a source and target destination pair
   * @param src for the operation
   * @param target for the operation
   * @throws IOException if anything goes wrong
   */
  protected void processPath(@Tainted CommandWithDestination this, @Tainted PathData src, @Tainted PathData dst) throws IOException {
    if (src.stat.isSymlink()) {
      // TODO: remove when FileContext is supported, this needs to either
      // copy the symlink or deref the symlink
      throw new @Tainted PathOperationException(src.toString());        
    } else if (src.stat.isFile()) {
      copyFileToTarget(src, dst);
    } else if (src.stat.isDirectory() && !isRecursive()) {
      throw new @Tainted PathIsDirectoryException(src.toString());
    }
  }

  @Override
  protected void recursePath(@Tainted CommandWithDestination this, @Tainted PathData src) throws IOException {
    @Tainted
    PathData savedDst = dst;
    try {
      // modify dst as we descend to append the basename of the
      // current directory being processed
      dst = getTargetPath(src);
      if (dst.exists) {
        if (!dst.stat.isDirectory()) {
          throw new @Tainted PathIsNotDirectoryException(dst.toString());
        }
      } else {
        if (!dst.fs.mkdirs(dst.path)) {
          // too bad we have no clue what failed
          @Tainted
          PathIOException e = new @Tainted PathIOException(dst.toString());
          e.setOperation("mkdir");
          throw e;
        }    
        dst.refreshStatus(); // need to update stat to know it exists now
      }      
      super.recursePath(src);
    } finally {
      dst = savedDst;
    }
  }
  
  protected @Tainted PathData getTargetPath(@Tainted CommandWithDestination this, @Tainted PathData src) throws IOException {
    @Tainted
    PathData target;
    // on the first loop, the dst may be directory or a file, so only create
    // a child path if dst is a dir; after recursion, it's always a dir
    if ((getDepth() > 0) || (dst.exists && dst.stat.isDirectory())) {
      target = dst.getPathDataForChild(src);
    } else if (dst.representsDirectory()) { // see if path looks like a dir
      target = dst.getPathDataForChild(src);
    } else {
      target = dst;
    }
    return target;
  }
  
  /**
   * Copies the source file to the target.
   * @param src item to copy
   * @param target where to copy the item
   * @throws IOException if copy fails
   */ 
  protected void copyFileToTarget(@Tainted CommandWithDestination this, @Tainted PathData src, @Tainted PathData target) throws IOException {
    src.fs.setVerifyChecksum(verifyChecksum);
    @Tainted
    InputStream in = null;
    try {
      in = src.fs.open(src.path);
      copyStreamToTarget(in, target);
      if(preserve) {
        target.fs.setTimes(
          target.path,
          src.stat.getModificationTime(),
          src.stat.getAccessTime());
        target.fs.setOwner(
          target.path,
          src.stat.getOwner(),
          src.stat.getGroup());
        target.fs.setPermission(
          target.path,
          src.stat.getPermission());
      }
    } finally {
      IOUtils.closeStream(in);
    }
  }
  
  /**
   * Copies the stream contents to a temporary file.  If the copy is
   * successful, the temporary file will be renamed to the real path,
   * else the temporary file will be deleted.
   * @param in the input stream for the copy
   * @param target where to store the contents of the stream
   * @throws IOException if copy fails
   */ 
  protected void copyStreamToTarget(@Tainted CommandWithDestination this, @Tainted InputStream in, @Tainted PathData target)
  throws IOException {
    if (target.exists && (target.stat.isDirectory() || !overwrite)) {
      throw new @Tainted PathExistsException(target.toString());
    }
    @Tainted
    TargetFileSystem targetFs = new @Tainted TargetFileSystem(target.fs);
    try {
      @Tainted
      PathData tempTarget = target.suffix("._COPYING_");
      targetFs.setWriteChecksum(writeChecksum);
      targetFs.writeStreamToFile(in, tempTarget);
      targetFs.rename(tempTarget, target);
    } finally {
      targetFs.close(); // last ditch effort to ensure temp file is removed
    }
  }

  // Helper filter filesystem that registers created files as temp files to
  // be deleted on exit unless successfully renamed
  private static class TargetFileSystem extends @Tainted FilterFileSystem {
    @Tainted
    TargetFileSystem(@Tainted FileSystem fs) {
      super(fs);
    }

    void writeStreamToFile(CommandWithDestination.@Tainted TargetFileSystem this, @Tainted InputStream in, @Tainted PathData target) throws IOException {
      @Tainted
      FSDataOutputStream out = null;
      try {
        out = create(target);
        IOUtils.copyBytes(in, out, getConf(), true);
      } finally {
        IOUtils.closeStream(out); // just in case copyBytes didn't
      }
    }
    
    // tag created files as temp files
    @Tainted
    FSDataOutputStream create(CommandWithDestination.@Tainted TargetFileSystem this, @Tainted PathData item) throws IOException {
      try {
        return create(item.path, true);
      } finally { // might have been created but stream was interrupted
        deleteOnExit(item.path);
      }
    }

    void rename(CommandWithDestination.@Tainted TargetFileSystem this, @Tainted PathData src, @Tainted PathData target) throws IOException {
      // the rename method with an option to delete the target is deprecated
      if (target.exists && !delete(target.path, false)) {
        // too bad we don't know why it failed
        @Tainted
        PathIOException e = new @Tainted PathIOException(target.toString());
        e.setOperation("delete");
        throw e;
      }
      if (!rename(src.path, target.path)) {
        // too bad we don't know why it failed
        @Tainted
        PathIOException e = new @Tainted PathIOException(src.toString());
        e.setOperation("rename");
        e.setTargetPath(target.toString());
        throw e;
      }
      // cancel delete on exit if rename is successful
      cancelDeleteOnExit(src.path);
    }
    @Override
    public void close(CommandWithDestination.@Tainted TargetFileSystem this) {
      // purge any remaining temp files, but don't close underlying fs
      processDeleteOnExit();
    }
  }
}
