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

package org.apache.hadoop.fs.s3;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A block-based {@link FileSystem} backed by
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * </p>
 * @see NativeS3FileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class S3FileSystem extends @Tainted FileSystem {

  private @Tainted URI uri;

  private @Tainted FileSystemStore store;

  private @Tainted Path workingDir;

  public @Tainted S3FileSystem() {
    // set store in initialize()
  }
  
  public @Tainted S3FileSystem(@Tainted FileSystemStore store) {
    this.store = store;
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>s3</code>
   */
  @Override
  public @Tainted String getScheme(@Tainted S3FileSystem this) {
    return "s3";
  }

  @Override
  public @Tainted URI getUri(@Tainted S3FileSystem this) {
    return uri;
  }

  @Override
  public void initialize(@Tainted S3FileSystem this, @Tainted URI uri, @Tainted Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());    
    this.workingDir =
      new @Tainted Path("/user", System.getProperty("user.name")).makeQualified(this);
  }  

  private static @Tainted FileSystemStore createDefaultStore(@Tainted Configuration conf) {
    @Tainted
    FileSystemStore store = new @Tainted Jets3tFileSystemStore();
    
    @Tainted
    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                                                               conf.getInt("fs.s3.maxRetries", 4),
                                                                               conf.getLong("fs.s3.sleepTimeSeconds", 10), TimeUnit.SECONDS);
    @Tainted
    Map<@Tainted Class<@Tainted ? extends @Tainted Exception>, @Tainted RetryPolicy> exceptionToPolicyMap =
      new @Tainted HashMap<@Tainted Class<@Tainted ? extends @Tainted Exception>, @Tainted RetryPolicy>();
    exceptionToPolicyMap.put(IOException.class, basePolicy);
    exceptionToPolicyMap.put(S3Exception.class, basePolicy);
    
    @Tainted
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
                                                              RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    @Tainted
    Map<@Tainted String, @Tainted RetryPolicy> methodNameToPolicyMap = new @Tainted HashMap<@Tainted String, @Tainted RetryPolicy>();
    methodNameToPolicyMap.put("storeBlock", methodPolicy);
    methodNameToPolicyMap.put("retrieveBlock", methodPolicy);
    
    return (@Tainted FileSystemStore) RetryProxy.create(FileSystemStore.class,
                                               store, methodNameToPolicyMap);
  }

  @Override
  public @Tainted Path getWorkingDirectory(@Tainted S3FileSystem this) {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(@Tainted S3FileSystem this, @Tainted Path dir) {
    workingDir = makeAbsolute(dir);
  }

  private @Tainted Path makeAbsolute(@Tainted S3FileSystem this, @Tainted Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new @Tainted Path(workingDir, path);
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public @Tainted boolean mkdirs(@Tainted S3FileSystem this, @Tainted Path path, @Tainted FsPermission permission) throws IOException {
    @Tainted
    Path absolutePath = makeAbsolute(path);
    @Tainted
    List<@Tainted Path> paths = new @Tainted ArrayList<@Tainted Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);
    
    @Tainted
    boolean result = true;
    for (@Tainted Path p : paths) {
      result &= mkdir(p);
    }
    return result;
  }
  
  private @Tainted boolean mkdir(@Tainted S3FileSystem this, @Tainted Path path) throws IOException {
    @Tainted
    Path absolutePath = makeAbsolute(path);
    @Tainted
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      store.storeINode(absolutePath, INode.DIRECTORY_INODE);
    } else if (inode.isFile()) {
      throw new @Tainted IOException(String.format(
          "Can't make directory for path %s since it is a file.",
          absolutePath));
    }
    return true;
  }

  @Override
  public @Tainted boolean isFile(@Tainted S3FileSystem this, @Tainted Path path) throws IOException {
    @Tainted
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      return false;
    }
    return inode.isFile();
  }

  private @Tainted INode checkFile(@Tainted S3FileSystem this, @Tainted Path path) throws IOException {
    @Tainted
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      throw new @Tainted IOException("No such file.");
    }
    if (inode.isDirectory()) {
      throw new @Tainted IOException("Path " + path + " is a directory.");
    }
    return inode;
  }

  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted S3FileSystem this, @Tainted Path f) throws IOException {
    @Tainted
    Path absolutePath = makeAbsolute(f);
    @Tainted
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      throw new @Tainted FileNotFoundException("File " + f + " does not exist.");
    }
    if (inode.isFile()) {
      return new @Tainted FileStatus @Tainted [] {
        new @Tainted S3FileStatus(f.makeQualified(this), inode)
      };
    }
    @Tainted
    ArrayList<@Tainted FileStatus> ret = new @Tainted ArrayList<@Tainted FileStatus>();
    for (@Tainted Path p : store.listSubPaths(absolutePath)) {
      ret.add(getFileStatus(p.makeQualified(this)));
    }
    return ret.toArray(new @Tainted FileStatus @Tainted [0]);
  }

  /** This optional operation is not yet supported. */
  @Override
  public @Tainted FSDataOutputStream append(@Tainted S3FileSystem this, @Tainted Path f, @Tainted int bufferSize,
      @Tainted
      Progressable progress) throws IOException {
    throw new @Tainted IOException("Not supported");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public @Tainted FSDataOutputStream create(@Tainted S3FileSystem this, @Tainted Path file, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite, @Tainted int bufferSize,
      @Tainted
      short replication, @Tainted long blockSize, @Tainted Progressable progress)
    throws IOException {

    @Tainted
    INode inode = store.retrieveINode(makeAbsolute(file));
    if (inode != null) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new @Tainted IOException("File already exists: " + file);
      }
    } else {
      @Tainted
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new @Tainted IOException("Mkdirs failed to create " + parent.toString());
        }
      }      
    }
    return new @Tainted FSDataOutputStream
        (new @Tainted S3OutputStream(getConf(), store, makeAbsolute(file),
                            blockSize, progress, bufferSize),
         statistics);
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted S3FileSystem this, @Tainted Path path, @Tainted int bufferSize) throws IOException {
    @Tainted
    INode inode = checkFile(path);
    return new @Tainted FSDataInputStream(new @Tainted S3InputStream(getConf(), store, inode,
                                                   statistics));
  }

  @Override
  public @Tainted boolean rename(@Tainted S3FileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    @Tainted
    Path absoluteSrc = makeAbsolute(src);
    final @Tainted String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";
    @Tainted
    INode srcINode = store.retrieveINode(absoluteSrc);
    @Tainted
    boolean debugEnabled = LOG.isDebugEnabled();
    if (srcINode == null) {
      // src path doesn't exist
      if (debugEnabled) {
        LOG.debug(debugPreamble + "returning false as src does not exist");
      }
      return false; 
    }

    @Tainted
    Path absoluteDst = makeAbsolute(dst);

    //validate the parent dir of the destination
    @Tainted
    Path dstParent = absoluteDst.getParent();
    if (dstParent != null) {
      //if the dst parent is not root, make sure it exists
      @Tainted
      INode dstParentINode = store.retrieveINode(dstParent);
      if (dstParentINode == null) {
        // dst parent doesn't exist
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "returning false as dst parent does not exist");
        }
        return false;
      }
      if (dstParentINode.isFile()) {
        // dst parent exists but is a file
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "returning false as dst parent exists and is a file");
        }
        return false;
      }
    }

    //get status of source
    @Tainted
    boolean srcIsFile = srcINode.isFile();

    @Tainted
    INode dstINode = store.retrieveINode(absoluteDst);
    @Tainted
    boolean destExists = dstINode != null;
    @Tainted
    boolean destIsDir = destExists && !dstINode.isFile();
    if (srcIsFile) {

      //source is a simple file
      if (destExists) {
        if (destIsDir) {
          //outcome #1 dest exists and is dir -filename to subdir of dest
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying src file under dest dir to " + absoluteDst);
          }
          absoluteDst = new @Tainted Path(absoluteDst, absoluteSrc.getName());
        } else {
          //outcome #2 dest it's a file: fail iff different from src
          @Tainted
          boolean renamingOnToSelf = absoluteSrc.equals(absoluteDst);
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying file onto file, outcome is " + renamingOnToSelf);
          }
          return renamingOnToSelf;
        }
      } else {
        // #3 dest does not exist: use dest as path for rename
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "copying file onto file");
        }
      }
    } else {
      //here the source exists and is a directory
      // outcomes (given we know the parent dir exists if we get this far)
      // #1 destination is a file: fail
      // #2 destination is a directory: create a new dir under that one
      // #3 destination doesn't exist: create a new dir with that name
      // #3 and #4 are only allowed if the dest path is not == or under src

      if (destExists) {
        if (!destIsDir) {
          // #1 destination is a file: fail
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "returning false as src is a directory, but not dest");
          }
          return false;
        } else {
          // the destination dir exists
          // destination for rename becomes a subdir of the target name
          absoluteDst = new @Tainted Path(absoluteDst, absoluteSrc.getName());
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying src dir under dest dir to " + absoluteDst);
          }
        }
      }
      //the final destination directory is now know, so validate it for
      //illegal moves

      if (absoluteSrc.equals(absoluteDst)) {
        //you can't rename a directory onto itself
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "Dest==source && isDir -failing");
        }
        return false;
      }
      if (absoluteDst.toString().startsWith(absoluteSrc.toString() + "/")) {
        //you can't move a directory under itself
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "dst is equal to or under src dir -failing");
        }
        return false;
      }
    }
    //here the dest path is set up -so rename
    return renameRecursive(absoluteSrc, absoluteDst);
  }

  private @Tainted boolean renameRecursive(@Tainted S3FileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    @Tainted
    INode srcINode = store.retrieveINode(src);
    store.storeINode(dst, srcINode);
    store.deleteINode(src);
    if (srcINode.isDirectory()) {
      for (@Tainted Path oldSrc : store.listDeepSubPaths(src)) {
        @Tainted
        INode inode = store.retrieveINode(oldSrc);
        if (inode == null) {
          return false;
        }
        @Tainted
        String oldSrcPath = oldSrc.toUri().getPath();
        @Tainted
        String srcPath = src.toUri().getPath();
        @Tainted
        String dstPath = dst.toUri().getPath();
        @Tainted
        Path newDst = new @Tainted Path(oldSrcPath.replaceFirst(srcPath, dstPath));
        store.storeINode(newDst, inode);
        store.deleteINode(oldSrc);
      }
    }
    return true;
  }

  @Override
  public @Tainted boolean delete(@Tainted S3FileSystem this, @Tainted Path path, @Tainted boolean recursive) throws IOException {
   @Tainted
   Path absolutePath = makeAbsolute(path);
   @Tainted
   INode inode = store.retrieveINode(absolutePath);
   if (inode == null) {
     return false;
   }
   if (inode.isFile()) {
     store.deleteINode(absolutePath);
     for (@Tainted Block block: inode.getBlocks()) {
       store.deleteBlock(block);
     }
   } else {
     @Tainted
     FileStatus @Tainted [] contents = null; 
     try {
       contents = listStatus(absolutePath);
     } catch(@Tainted FileNotFoundException fnfe) {
       return false;
     }

     if ((contents.length !=0) && (!recursive)) {
       throw new @Tainted IOException("Directory " + path.toString() 
           + " is not empty.");
     }
     for (@Tainted FileStatus p:contents) {
       if (!delete(p.getPath(), recursive)) {
         return false;
       }
     }
     store.deleteINode(absolutePath);
   }
   return true;
  }
  
  /**
   * FileStatus for S3 file systems. 
   */
  @Override
  public @Tainted FileStatus getFileStatus(@Tainted S3FileSystem this, @Tainted Path f)  throws IOException {
    @Tainted
    INode inode = store.retrieveINode(makeAbsolute(f));
    if (inode == null) {
      throw new @Tainted FileNotFoundException(f + ": No such file or directory.");
    }
    return new @Tainted S3FileStatus(f.makeQualified(this), inode);
  }
  
  @Override
  public @Tainted long getDefaultBlockSize(@Tainted S3FileSystem this) {
    return getConf().getLong("fs.s3.block.size", 64 * 1024 * 1024);
  }

  // diagnostic methods

  void dump(@Tainted S3FileSystem this) throws IOException {
    store.dump();
  }

  void purge(@Tainted S3FileSystem this) throws IOException {
    store.purge();
  }

  private static class S3FileStatus extends @Tainted FileStatus {

    @Tainted
    S3FileStatus(@Tainted Path f, @Tainted INode inode) throws IOException {
      super(findLength(inode), inode.isDirectory(), 1,
            findBlocksize(inode), 0, f);
    }

    private static @Tainted long findLength(@Tainted INode inode) {
      if (!inode.isDirectory()) {
        @Tainted
        long length = 0L;
        for (@Tainted Block block : inode.getBlocks()) {
          length += block.getLength();
        }
        return length;
      }
      return 0;
    }

    private static @Tainted long findBlocksize(@Tainted INode inode) {
      final @Tainted Block @Tainted [] ret = inode.getBlocks();
      return ret == null ? 0L : ret[0].getLength();
    }
  }
}
