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
import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration; 

/** An implementation of a round-robin scheme for disk allocation for creating
 * files. The way it works is that it is kept track what disk was last
 * allocated for a file write. For the current request, the next disk from
 * the set of disks would be allocated if the free space on the disk is 
 * sufficient enough to accommodate the file that is being considered for
 * creation. If the space requirements cannot be met, the next disk in order
 * would be tried and so on till a disk is found with sufficient capacity.
 * Once a disk with sufficient space is identified, a check is done to make
 * sure that the disk is writable. Also, there is an API provided that doesn't
 * take the space requirements into consideration but just checks whether the
 * disk under consideration is writable (this should be used for cases where
 * the file size is not known apriori). An API is provided to read a path that
 * was created earlier. That API works by doing a scan of all the disks for the
 * input pathname.
 * This implementation also provides the functionality of having multiple 
 * allocators per JVM (one for each unique functionality or context, like 
 * mapred, dfs-client, etc.). It ensures that there is only one instance of
 * an allocator per context per JVM.
 * Note:
 * 1. The contexts referred above are actually the configuration items defined
 * in the Configuration class like "mapred.local.dir" (for which we want to 
 * control the dir allocations). The context-strings are exactly those 
 * configuration items.
 * 2. This implementation does not take into consideration cases where
 * a disk becomes read-only or goes out of space while a file is being written
 * to (disks are shared between multiple processes, and so the latter situation
 * is probable).
 * 3. In the class implementation, "Disk" is referred to as "Dir", which
 * actually points to the configured directory on the Disk which will be the
 * parent for all file write/read allocations.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class LocalDirAllocator {
  
  //A Map from the config item names like "mapred.local.dir"
  //to the instance of the AllocatorPerContext. This
  //is a static object to make sure there exists exactly one instance per JVM
  private static @Tainted Map <@Tainted String, @Tainted AllocatorPerContext> contexts = 
                 new @Tainted TreeMap<@Tainted String, @Tainted AllocatorPerContext>();
  private @Tainted String contextCfgItemName;

  /** Used when size of file to be allocated is unknown. */
  public static final @Tainted int SIZE_UNKNOWN = -1;

  /**Create an allocator object
   * @param contextCfgItemName
   */
  public @Tainted LocalDirAllocator(@Tainted String contextCfgItemName) {
    this.contextCfgItemName = contextCfgItemName;
  }
  
  /** This method must be used to obtain the dir allocation context for a 
   * particular value of the context name. The context name must be an item
   * defined in the Configuration object for which we want to control the 
   * dir allocations (e.g., <code>mapred.local.dir</code>). The method will
   * create a context for that name if it doesn't already exist.
   */
  private @Tainted AllocatorPerContext obtainContext(@Tainted LocalDirAllocator this, @Tainted String contextCfgItemName) {
    synchronized (contexts) {
      @Tainted
      AllocatorPerContext l = contexts.get(contextCfgItemName);
      if (l == null) {
        contexts.put(contextCfgItemName, 
                    (l = new @Tainted AllocatorPerContext(contextCfgItemName)));
      }
      return l;
    }
  }
  
  /** Get a path from the local FS. This method should be used if the size of 
   *  the file is not known apriori. We go round-robin over the set of disks
   *  (via the configured dirs) and return the first complete path where
   *  we could create the parent directory of the passed path. 
   *  @param pathStr the requested path (this will be created on the first 
   *  available disk)
   *  @param conf the Configuration object
   *  @return the complete path to the file on a local disk
   *  @throws IOException
   */
  public @Tainted Path getLocalPathForWrite(@Tainted LocalDirAllocator this, @Tainted String pathStr, 
      @Tainted
      Configuration conf) throws IOException {
    return getLocalPathForWrite(pathStr, SIZE_UNKNOWN, conf);
  }
  
  /** Get a path from the local FS. Pass size as 
   *  SIZE_UNKNOWN if not known apriori. We
   *  round-robin over the set of disks (via the configured dirs) and return
   *  the first complete path which has enough space 
   *  @param pathStr the requested path (this will be created on the first 
   *  available disk)
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @return the complete path to the file on a local disk
   *  @throws IOException
   */
  public @Tainted Path getLocalPathForWrite(@Tainted LocalDirAllocator this, @Tainted String pathStr, @Tainted long size, 
      @Tainted
      Configuration conf) throws IOException {
    return getLocalPathForWrite(pathStr, size, conf, true);
  }
  
  /** Get a path from the local FS. Pass size as 
   *  SIZE_UNKNOWN if not known apriori. We
   *  round-robin over the set of disks (via the configured dirs) and return
   *  the first complete path which has enough space 
   *  @param pathStr the requested path (this will be created on the first 
   *  available disk)
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @param checkWrite ensure that the path is writable
   *  @return the complete path to the file on a local disk
   *  @throws IOException
   */
  public @Tainted Path getLocalPathForWrite(@Tainted LocalDirAllocator this, @Tainted String pathStr, @Tainted long size, 
                                   @Tainted
                                   Configuration conf,
                                   @Tainted
                                   boolean checkWrite) throws IOException {
    @Tainted
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getLocalPathForWrite(pathStr, size, conf, checkWrite);
  }
  
  /** Get a path from the local FS for reading. We search through all the
   *  configured dirs for the file's existence and return the complete
   *  path to the file when we find one 
   *  @param pathStr the requested file (this will be searched)
   *  @param conf the Configuration object
   *  @return the complete path to the file on a local disk
   *  @throws IOException
   */
  public @Tainted Path getLocalPathToRead(@Tainted LocalDirAllocator this, @Tainted String pathStr, 
      @Tainted
      Configuration conf) throws IOException {
    @Tainted
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getLocalPathToRead(pathStr, conf);
  }
  
  /**
   * Get all of the paths that currently exist in the working directories.
   * @param pathStr the path underneath the roots
   * @param conf the configuration to look up the roots in
   * @return all of the paths that exist under any of the roots
   * @throws IOException
   */
  public @Tainted Iterable<@Tainted Path> getAllLocalPathsToRead(@Tainted LocalDirAllocator this, @Tainted String pathStr, 
                                               @Tainted
                                               Configuration conf
                                               ) throws IOException {
    @Tainted
    AllocatorPerContext context;
    synchronized (this) {
      context = obtainContext(contextCfgItemName);
    }
    return context.getAllLocalPathsToRead(pathStr, conf);    
  }

  /** Creates a temporary file in the local FS. Pass size as -1 if not known 
   *  apriori. We round-robin over the set of disks (via the configured dirs) 
   *  and select the first complete path which has enough space. A file is
   *  created on this directory. The file is guaranteed to go away when the
   *  JVM exits.
   *  @param pathStr prefix for the temporary file
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @return a unique temporary file
   *  @throws IOException
   */
  public @Tainted File createTmpFileForWrite(@Tainted LocalDirAllocator this, @Tainted String pathStr, @Tainted long size, 
      @Tainted
      Configuration conf) throws IOException {
    @Tainted
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.createTmpFileForWrite(pathStr, size, conf);
  }
  
  /** Method to check whether a context is valid
   * @param contextCfgItemName
   * @return true/false
   */
  public static @Tainted boolean isContextValid(@Tainted String contextCfgItemName) {
    synchronized (contexts) {
      return contexts.containsKey(contextCfgItemName);
    }
  }
  
  /**
   * Removes the context from the context config items
   * 
   * @param contextCfgItemName
   */
  @Deprecated
  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  public static void removeContext(@Tainted String contextCfgItemName) {
    synchronized (contexts) {
      contexts.remove(contextCfgItemName);
    }
  }
    
  /** We search through all the configured dirs for the file's existence
   *  and return true when we find  
   *  @param pathStr the requested file (this will be searched)
   *  @param conf the Configuration object
   *  @return true if files exist. false otherwise
   *  @throws IOException
   */
  public @Tainted boolean ifExists(@Tainted LocalDirAllocator this, @Tainted String pathStr, @Tainted Configuration conf) {
    @Tainted
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.ifExists(pathStr, conf);
  }

  /**
   * Get the current directory index for the given configuration item.
   * @return the current directory index for the given configuration item.
   */
  @Tainted
  int getCurrentDirectoryIndex(@Tainted LocalDirAllocator this) {
    @Tainted
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getCurrentDirectoryIndex();
  }
  
  private static class AllocatorPerContext {

    private final @Tainted Log LOG =
      LogFactory.getLog(AllocatorPerContext.class);

    private @Tainted int dirNumLastAccessed;
    private @Tainted Random dirIndexRandomizer = new @Tainted Random();
    private @Tainted FileSystem localFS;
    private @Tainted DF @Tainted [] dirDF;
    private @Tainted String contextCfgItemName;
    private @Untainted String @Tainted [] localDirs;
    private @Tainted String savedLocalDirs = "";

    public @Tainted AllocatorPerContext(@Tainted String contextCfgItemName) {
      this.contextCfgItemName = contextCfgItemName;
    }

    /** This method gets called everytime before any read/write to make sure
     * that any change to localDirs is reflected immediately.
     */
    private synchronized void confChanged(LocalDirAllocator.@Tainted AllocatorPerContext this, @Tainted Configuration conf) 
        throws IOException {
      @Untainted
      String newLocalDirs = conf.get(contextCfgItemName);
      if (null == newLocalDirs) {
        throw new @Tainted IOException(contextCfgItemName + " not configured");
      }
      if (!newLocalDirs.equals(savedLocalDirs)) {
        @SuppressWarnings("ostrusted:cast.unsafe") // Trusted input string
        @Untainted String @Tainted [] localDirs = (@Untainted String @Tainted [])  StringUtils.getTrimmedStrings(newLocalDirs);
        this.localDirs = localDirs;
        localFS = FileSystem.getLocal(conf);
        @Tainted
        int numDirs = localDirs.length;
        @Tainted
        ArrayList<@Untainted String> dirs = new @Tainted ArrayList<@Untainted String>(numDirs);
        @Tainted
        ArrayList<@Tainted DF> dfList = new @Tainted ArrayList<@Tainted DF>(numDirs);
        for (@Tainted int i = 0; i < numDirs; i++) {
          try {
            // filter problematic directories
            @Untainted
            Path tmpDir = new @Untainted Path(localDirs[i]);
            if(localFS.mkdirs(tmpDir)|| localFS.exists(tmpDir)) {
              try {

                @Untainted
                File tmpFile = tmpDir.isAbsolute()
                  ? new @Untainted File(localFS.makeQualified(tmpDir).toUri())
                  : new @Untainted File(localDirs[i]);

                DiskChecker.checkDir(tmpFile);
                @Untainted String tmpFilePath = new @Untainted String(tmpFile.getPath()); // Trusted file path
                dirs.add(tmpFilePath);
                dfList.add(new @Tainted DF(tmpFile, 30000));

              } catch (@Tainted DiskErrorException de) {
                LOG.warn( localDirs[i] + " is not writable\n", de);
              }
            } else {
              LOG.warn( "Failed to create " + localDirs[i]);
            }
          } catch (@Tainted IOException ie) { 
            LOG.warn( "Failed to create " + localDirs[i] + ": " +
                ie.getMessage() + "\n", ie);
          } //ignore
        }
        localDirs = dirs.toArray(new @Untainted String @Tainted [dirs.size()]);
        dirDF = dfList.toArray(new @Tainted DF @Tainted [dirs.size()]);
        savedLocalDirs = newLocalDirs;
        
        // randomize the first disk picked in the round-robin selection 
        dirNumLastAccessed = dirIndexRandomizer.nextInt(dirs.size());
      }
    }

    private @Tainted Path createPath(LocalDirAllocator.@Tainted AllocatorPerContext this, @Tainted String path, 
        @Tainted
        boolean checkWrite) throws IOException {
      @Tainted
      Path file = new @Tainted Path(new @Tainted Path(localDirs[dirNumLastAccessed]),
                                    path);
      if (checkWrite) {
        //check whether we are able to create a directory here. If the disk
        //happens to be RDONLY we will fail
        try {
          DiskChecker.checkDir(new @Tainted File(file.getParent().toUri().getPath()));
          return file;
        } catch (@Tainted DiskErrorException d) {
          LOG.warn("Disk Error Exception: ", d);
          return null;
        }
      }
      return file;
    }

    /**
     * Get the current directory index.
     * @return the current directory index.
     */
    @Tainted
    int getCurrentDirectoryIndex(LocalDirAllocator.@Tainted AllocatorPerContext this) {
      return dirNumLastAccessed;
    }

    /** Get a path from the local FS. If size is known, we go
     *  round-robin over the set of disks (via the configured dirs) and return
     *  the first complete path which has enough space.
     *  
     *  If size is not known, use roulette selection -- pick directories
     *  with probability proportional to their available space.
     */
    public synchronized @Tainted Path getLocalPathForWrite(LocalDirAllocator.@Tainted AllocatorPerContext this, @Tainted String pathStr, @Tainted long size, 
        @Tainted
        Configuration conf, @Tainted boolean checkWrite) throws IOException {
      confChanged(conf);
      @Tainted
      int numDirs = localDirs.length;
      @Tainted
      int numDirsSearched = 0;
      //remove the leading slash from the path (to make sure that the uri
      //resolution results in a valid path on the dir being checked)
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      @Tainted
      Path returnPath = null;
      
      if(size == SIZE_UNKNOWN) {  //do roulette selection: pick dir with probability 
                    //proportional to available size
        @Tainted
        long @Tainted [] availableOnDisk = new @Tainted long @Tainted [dirDF.length];
        @Tainted
        long totalAvailable = 0;
        
            //build the "roulette wheel"
        for(@Tainted int i =0; i < dirDF.length; ++i) {
          availableOnDisk[i] = dirDF[i].getAvailable();
          totalAvailable += availableOnDisk[i];
        }

        // Keep rolling the wheel till we get a valid path
        @Tainted
        Random r = new java.util.Random();
        while (numDirsSearched < numDirs && returnPath == null) {
          @Tainted
          long randomPosition = Math.abs(r.nextLong()) % totalAvailable;
          @Tainted
          int dir = 0;
          while (randomPosition > availableOnDisk[dir]) {
            randomPosition -= availableOnDisk[dir];
            dir++;
          }
          dirNumLastAccessed = dir;
          returnPath = createPath(pathStr, checkWrite);
          if (returnPath == null) {
            totalAvailable -= availableOnDisk[dir];
            availableOnDisk[dir] = 0; // skip this disk
            numDirsSearched++;
          }
        }
      } else {
        while (numDirsSearched < numDirs && returnPath == null) {
          @Tainted
          long capacity = dirDF[dirNumLastAccessed].getAvailable();
          if (capacity > size) {
            returnPath = createPath(pathStr, checkWrite);
          }
          dirNumLastAccessed++;
          dirNumLastAccessed = dirNumLastAccessed % numDirs; 
          numDirsSearched++;
        } 
      }
      if (returnPath != null) {
        return returnPath;
      }
      
      //no path found
      throw new @Tainted DiskErrorException("Could not find any valid local " +
          "directory for " + pathStr);
    }

    /** Creates a file on the local FS. Pass size as 
     * {@link LocalDirAllocator.SIZE_UNKNOWN} if not known apriori. We
     *  round-robin over the set of disks (via the configured dirs) and return
     *  a file on the first path which has enough space. The file is guaranteed
     *  to go away when the JVM exits.
     */
    public @Tainted File createTmpFileForWrite(LocalDirAllocator.@Tainted AllocatorPerContext this, @Tainted String pathStr, @Tainted long size, 
        @Tainted
        Configuration conf) throws IOException {

      // find an appropriate directory
      @Tainted
      Path path = getLocalPathForWrite(pathStr, size, conf, true);
      @Tainted
      File dir = new @Tainted File(path.getParent().toUri().getPath());
      @Tainted
      String prefix = path.getName();

      // create a temp file on this directory
      @Tainted
      File result = File.createTempFile(prefix, null, dir);
      result.deleteOnExit();
      return result;
    }

    /** Get a path from the local FS for reading. We search through all the
     *  configured dirs for the file's existence and return the complete
     *  path to the file when we find one 
     */
    public synchronized @Tainted Path getLocalPathToRead(LocalDirAllocator.@Tainted AllocatorPerContext this, @Tainted String pathStr, 
        @Tainted
        Configuration conf) throws IOException {
      confChanged(conf);
      @Tainted
      int numDirs = localDirs.length;
      @Tainted
      int numDirsSearched = 0;
      //remove the leading slash from the path (to make sure that the uri
      //resolution results in a valid path on the dir being checked)
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      while (numDirsSearched < numDirs) {
        @Tainted
        Path file = new @Tainted Path(localDirs[numDirsSearched], pathStr);
        if (localFS.exists(file)) {
          return file;
        }
        numDirsSearched++;
      }

      //no path found
      throw new @Tainted DiskErrorException ("Could not find " + pathStr +" in any of" +
      " the configured local directories");
    }

    private static class PathIterator implements @Tainted Iterator<@Tainted Path>, @Tainted Iterable<@Tainted Path> {
      private final @Tainted FileSystem fs;
      private final @Tainted String pathStr;
      private @Tainted int i = 0;
      private final @Tainted String @Tainted [] rootDirs;
      private @Tainted Path next = null;

      private @Tainted PathIterator(@Tainted FileSystem fs, @Tainted String pathStr, @Tainted String @Tainted [] rootDirs)
          throws IOException {
        this.fs = fs;
        this.pathStr = pathStr;
        this.rootDirs = rootDirs;
        advance();
      }

      @Override
      public @Tainted boolean hasNext(LocalDirAllocator.AllocatorPerContext.@Tainted PathIterator this) {
        return next != null;
      }

      private void advance(LocalDirAllocator.AllocatorPerContext.@Tainted PathIterator this) throws IOException {
        while (i < rootDirs.length) {
          next = new @Tainted Path(rootDirs[i++], pathStr);
          if (fs.exists(next)) {
            return;
          }
        }
        next = null;
      }

      @Override
      public @Tainted Path next(LocalDirAllocator.AllocatorPerContext.@Tainted PathIterator this) {
        final @Tainted Path result = next;
        try {
          advance();
        } catch (@Tainted IOException ie) {
          throw new @Tainted RuntimeException("Can't check existance of " + next, ie);
        }
        if (result == null) {
          throw new @Tainted NoSuchElementException();
        }
        return result;
      }

      @Override
      public void remove(LocalDirAllocator.AllocatorPerContext.@Tainted PathIterator this) {
        throw new @Tainted UnsupportedOperationException("read only iterator");
      }

      @Override
      public @Tainted Iterator<@Tainted Path> iterator(LocalDirAllocator.AllocatorPerContext.@Tainted PathIterator this) {
        return this;
      }
    }

    /**
     * Get all of the paths that currently exist in the working directories.
     * @param pathStr the path underneath the roots
     * @param conf the configuration to look up the roots in
     * @return all of the paths that exist under any of the roots
     * @throws IOException
     */
    synchronized @Tainted Iterable<@Tainted Path> getAllLocalPathsToRead(LocalDirAllocator.@Tainted AllocatorPerContext this, @Tainted String pathStr,
        @Tainted
        Configuration conf) throws IOException {
      confChanged(conf);
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      return new @Tainted PathIterator(localFS, pathStr, localDirs);
    }

    /** We search through all the configured dirs for the file's existence
     *  and return true when we find one 
     */
    public synchronized @Tainted boolean ifExists(LocalDirAllocator.@Tainted AllocatorPerContext this, @Tainted String pathStr, @Tainted Configuration conf) {
      try {
        @Tainted
        int numDirs = localDirs.length;
        @Tainted
        int numDirsSearched = 0;
        //remove the leading slash from the path (to make sure that the uri
        //resolution results in a valid path on the dir being checked)
        if (pathStr.startsWith("/")) {
          pathStr = pathStr.substring(1);
        }
        while (numDirsSearched < numDirs) {
          @Tainted
          Path file = new @Tainted Path(localDirs[numDirsSearched], pathStr);
          if (localFS.exists(file)) {
            return true;
          }
          numDirsSearched++;
        }
      } catch (@Tainted IOException e) {
        // IGNORE and try again
      }
      return false;
    }
  }
}
