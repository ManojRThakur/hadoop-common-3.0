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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A collection of file-processing util methods
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FileUtil {

  private static final @Tainted Log LOG = LogFactory.getLog(FileUtil.class);

  /* The error code is defined in winutils to indicate insufficient
   * privilege to create symbolic links. This value need to keep in
   * sync with the constant of the same name in:
   * "src\winutils\common.h"
   * */
  public static final @Tainted int SYMLINK_NO_PRIVILEGE = 2;

  /**
   * convert an array of FileStatus to an array of Path
   * 
   * @param stats
   *          an array of FileStatus objects
   * @return an array of paths corresponding to the input
   */
  public static @Tainted Path @Tainted [] stat2Paths(@Tainted FileStatus @Tainted [] stats) {
    if (stats == null)
      return null;
    @Tainted
    Path @Tainted [] ret = new @Tainted Path @Tainted [stats.length];
    for (@Tainted int i = 0; i < stats.length; ++i) {
      ret[i] = stats[i].getPath();
    }
    return ret;
  }

  /**
   * convert an array of FileStatus to an array of Path.
   * If stats if null, return path
   * @param stats
   *          an array of FileStatus objects
   * @param path
   *          default path to return in stats is null
   * @return an array of paths corresponding to the input
   */
  public static @Tainted Path @Tainted [] stat2Paths(@Tainted FileStatus @Tainted [] stats, @Tainted Path path) {
    if (stats == null)
      return new @Tainted Path @Tainted []{path};
    else
      return stat2Paths(stats);
  }
  
  /**
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   * (1) If dir is symlink to a file, the symlink is deleted. The file pointed
   *     to by the symlink is not deleted.
   * (2) If dir is symlink to a directory, symlink is deleted. The directory
   *     pointed to by symlink is not deleted.
   * (3) If dir is a normal file, it is deleted.
   * (4) If dir is a normal directory, then dir and all its contents recursively
   *     are deleted.
   */
  public static @Tainted boolean fullyDelete(final @Untainted File dir) {
    return fullyDelete(dir, false);
  }
  
  /**
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   * (1) If dir is symlink to a file, the symlink is deleted. The file pointed
   *     to by the symlink is not deleted.
   * (2) If dir is symlink to a directory, symlink is deleted. The directory
   *     pointed to by symlink is not deleted.
   * (3) If dir is a normal file, it is deleted.
   * (4) If dir is a normal directory, then dir and all its contents recursively
   *     are deleted.
   * @param dir the file or directory to be deleted
   * @param tryGrantPermissions true if permissions should be modified to delete a file.
   * @return true on success false on failure.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted boolean fullyDelete(final @Untainted File dir, @Tainted boolean tryGrantPermissions) {
    if (tryGrantPermissions) {
      // try to chmod +rwx the parent folder of the 'dir': 
      @Untainted File parent = (@Untainted File) dir.getParentFile();
      grantPermissions(parent);
    }
    if (deleteImpl(dir, false)) {
      // dir is (a) normal file, (b) symlink to a file, (c) empty directory or
      // (d) symlink to a directory
      return true;
    }
    // handle nonempty directory deletion
    if (!fullyDeleteContents(dir, tryGrantPermissions)) {
      return false;
    }
    return deleteImpl(dir, true);
  }

  /**
   * Returns the target of the given symlink. Returns the empty string if
   * the given path does not refer to a symlink or there is an error
   * accessing the symlink.
   * @param f File representing the symbolic link.
   * @return The target of the symbolic link, empty string on error or if not
   *         a symlink.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted String readLink(@Untainted File f) {
    /* NB: Use readSymbolicLink in java.nio.file.Path once available. Could
     * use getCanonicalPath in File to get the target of the symlink but that
     * does not indicate if the given path refers to a symlink.
     */
    try {
      return Shell.execCommand(
          Shell.getReadlinkCommand( (@Untainted String) f.toString())).trim();
    } catch (@Tainted IOException x) {
      return "";
    }
  }

  /*
   * Pure-Java implementation of "chmod +rwx f".
   */
  private static void grantPermissions(final @Untainted File f) {
      FileUtil.setExecutable(f, true);
      FileUtil.setReadable(f, true);
      FileUtil.setWritable(f, true);
  }

  private static @Tainted boolean deleteImpl(final @Tainted File f, final @Tainted boolean doLog) {
    if (f == null) {
      LOG.warn("null file argument.");
      return false;
    }
    final @Tainted boolean wasDeleted = f.delete();
    if (wasDeleted) {
      return true;
    }
    final @Tainted boolean ex = f.exists();
    if (doLog && ex) {
      LOG.warn("Failed to delete file or dir ["
          + f.getAbsolutePath() + "]: it still exists.");
    }
    return !ex;
  }
  
  /**
   * Delete the contents of a directory, not the directory itself.  If
   * we return false, the directory may be partially-deleted.
   * If dir is a symlink to a directory, all the contents of the actual
   * directory pointed to by dir will be deleted.
   */
  public static @Tainted boolean fullyDeleteContents(final @Untainted File dir) {
    return fullyDeleteContents(dir, false);
  }
  
  /**
   * Delete the contents of a directory, not the directory itself.  If
   * we return false, the directory may be partially-deleted.
   * If dir is a symlink to a directory, all the contents of the actual
   * directory pointed to by dir will be deleted.
   * @param tryGrantPermissions if 'true', try grant +rwx permissions to this 
   * and all the underlying directories before trying to delete their contents.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted boolean fullyDeleteContents(final @Untainted File dir, final @Tainted boolean tryGrantPermissions) {
    if (tryGrantPermissions) {
      // to be able to list the dir and delete files from it
      // we must grant the dir rwx permissions: 
      grantPermissions(dir);
    }
    @Tainted
    boolean deletionSucceeded = true;
    final @Untainted File @Tainted [] contents = (@Untainted File @Tainted []) dir.listFiles();
    if (contents != null) {
      for (@Tainted int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          if (!deleteImpl(contents[i], true)) {// normal file or symlink to another file
            deletionSucceeded = false;
            continue; // continue deletion of other files/dirs under dir
          }
        } else {
          // Either directory or symlink to another directory.
          // Try deleting the directory as this might be a symlink
          @Tainted
          boolean b = false;
          b = deleteImpl(contents[i], false);
          if (b){
            //this was indeed a symlink or an empty directory
            continue;
          }
          // if not an empty directory or symlink let
          // fullydelete handle it.
          if (!fullyDelete(contents[i], tryGrantPermissions)) {
            deletionSucceeded = false;
            // continue deletion of other files/dirs under dir
          }
        }
      }
    }
    return deletionSucceeded;
  }

  /**
   * Recursively delete a directory.
   * 
   * @param fs {@link FileSystem} on which the path is present
   * @param dir directory to recursively delete 
   * @throws IOException
   * @deprecated Use {@link FileSystem#delete(Path, boolean)}
   */
  @Deprecated
  public static void fullyDelete(@Tainted FileSystem fs, @Tainted Path dir) 
  throws IOException {
    fs.delete(dir, true);
  }

  //
  // If the destination is a subdirectory of the source, then
  // generate exception
  //
  private static void checkDependencies(@Tainted FileSystem srcFS, 
                                        @Tainted
                                        Path src, 
                                        @Tainted
                                        FileSystem dstFS, 
                                        @Tainted
                                        Path dst)
                                        throws IOException {
    if (srcFS == dstFS) {
      @Tainted
      String srcq = src.makeQualified(srcFS).toString() + Path.SEPARATOR;
      @Tainted
      String dstq = dst.makeQualified(dstFS).toString() + Path.SEPARATOR;
      if (dstq.startsWith(srcq)) {
        if (srcq.length() == dstq.length()) {
          throw new @Tainted IOException("Cannot copy " + src + " to itself.");
        } else {
          throw new @Tainted IOException("Cannot copy " + src + " to its subdirectory " +
                                dst);
        }
      }
    }
  }

  /** Copy files between FileSystems. */
  public static @Tainted boolean copy(@Tainted FileSystem srcFS, @Tainted Path src, 
                             @Tainted
                             FileSystem dstFS, @Tainted Path dst, 
                             @Tainted
                             boolean deleteSource,
                             @Tainted
                             Configuration conf) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
  }

  public static @Tainted boolean copy(@Tainted FileSystem srcFS, @Tainted Path @Tainted [] srcs, 
                             @Tainted
                             FileSystem dstFS, @Tainted Path dst,
                             @Tainted
                             boolean deleteSource, 
                             @Tainted
                             boolean overwrite, @Tainted Configuration conf)
                             throws IOException {
    @Tainted
    boolean gotException = false;
    @Tainted
    boolean returnVal = true;
    @Tainted
    StringBuilder exceptions = new @Tainted StringBuilder();

    if (srcs.length == 1)
      return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);

    // Check if dest is directory
    if (!dstFS.exists(dst)) {
      throw new @Tainted IOException("`" + dst +"': specified destination directory " +
                            "does not exist");
    } else {
      @Tainted
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (!sdst.isDirectory()) 
        throw new @Tainted IOException("copying multiple files, but last argument `" +
                              dst + "' is not a directory");
    }

    for (@Tainted Path src : srcs) {
      try {
        if (!copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf))
          returnVal = false;
      } catch (@Tainted IOException e) {
        gotException = true;
        exceptions.append(e.getMessage());
        exceptions.append("\n");
      }
    }
    if (gotException) {
      throw new @Tainted IOException(exceptions.toString());
    }
    return returnVal;
  }

  /** Copy files between FileSystems. */
  public static @Tainted boolean copy(@Tainted FileSystem srcFS, @Tainted Path src, 
                             @Tainted
                             FileSystem dstFS, @Tainted Path dst, 
                             @Tainted
                             boolean deleteSource,
                             @Tainted
                             boolean overwrite,
                             @Tainted
                             Configuration conf) throws IOException {
    @Tainted
    FileStatus fileStatus = srcFS.getFileStatus(src);
    return copy(srcFS, fileStatus, dstFS, dst, deleteSource, overwrite, conf);
  }

  /** Copy files between FileSystems. */
  private static @Tainted boolean copy(@Tainted FileSystem srcFS, @Tainted FileStatus srcStatus,
                              @Tainted
                              FileSystem dstFS, @Tainted Path dst,
                              @Tainted
                              boolean deleteSource,
                              @Tainted
                              boolean overwrite,
                              @Tainted
                              Configuration conf) throws IOException {
    @Tainted
    Path src = srcStatus.getPath();
    dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcStatus.isDirectory()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      @Tainted
      FileStatus contents @Tainted [] = srcFS.listStatus(src);
      for (@Tainted int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], dstFS,
             new @Tainted Path(dst, contents[i].getPath().getName()),
             deleteSource, overwrite, conf);
      }
    } else {
      @Tainted
      InputStream in=null;
      @Tainted
      OutputStream out = null;
      try {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        IOUtils.copyBytes(in, out, conf, true);
      } catch (@Tainted IOException e) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        throw e;
      }
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  
  }

  /** Copy all files in a directory to one output file (merge). */
  public static @Tainted boolean copyMerge(@Tainted FileSystem srcFS, @Tainted Path srcDir, 
                                  @Tainted
                                  FileSystem dstFS, @Tainted Path dstFile, 
                                  @Tainted
                                  boolean deleteSource,
                                  @Tainted
                                  Configuration conf, @Tainted String addString) throws IOException {
    dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

    if (!srcFS.getFileStatus(srcDir).isDirectory())
      return false;
   
    @Tainted
    OutputStream out = dstFS.create(dstFile);
    
    try {
      @Tainted
      FileStatus contents @Tainted [] = srcFS.listStatus(srcDir);
      Arrays.sort(contents);
      for (@Tainted int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          @Tainted
          InputStream in = srcFS.open(contents[i].getPath());
          try {
            IOUtils.copyBytes(in, out, conf, false);
            if (addString!=null)
              out.write(addString.getBytes("UTF-8"));
                
          } finally {
            in.close();
          } 
        }
      }
    } finally {
      out.close();
    }
    

    if (deleteSource) {
      return srcFS.delete(srcDir, true);
    } else {
      return true;
    }
  }  
  
  /** Copy local files to a FileSystem. */
  public static @Tainted boolean copy(@Untainted File src,
                             @Tainted
                             FileSystem dstFS, @Tainted Path dst,
                             @Tainted
                             boolean deleteSource,
                             @Tainted
                             Configuration conf) throws IOException {
    dst = checkDest(src.getName(), dstFS, dst, false);

    if (src.isDirectory()) {
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      @Untainted File contents @Tainted [] = listFiles(src);
      for (@Tainted int i = 0; i < contents.length; i++) {
        copy(contents[i], dstFS, new @Tainted Path(dst, contents[i].getName()),
             deleteSource, conf);
      }
    } else if (src.isFile()) {
      @Tainted
      InputStream in = null;
      @Tainted
      OutputStream out =null;
      try {
        in = new @Tainted FileInputStream(src);
        out = dstFS.create(dst);
        IOUtils.copyBytes(in, out, conf);
      } catch (@Tainted IOException e) {
        IOUtils.closeStream( out );
        IOUtils.closeStream( in );
        throw e;
      }
    } else {
      throw new @Tainted IOException(src.toString() + 
                            ": No such file or directory");
    }
    if (deleteSource) {
      return FileUtil.fullyDelete(src);
    } else {
      return true;
    }
  }

  /** Copy FileSystem files to local files. */
  public static @Tainted boolean copy(@Tainted FileSystem srcFS, @Tainted Path src, 
                             @Tainted
                             File dst, @Tainted boolean deleteSource,
                             @Tainted
                             Configuration conf) throws IOException {
    @Tainted
    FileStatus filestatus = srcFS.getFileStatus(src);
    return copy(srcFS, filestatus, dst, deleteSource, conf);
  }

  /** Copy FileSystem files to local files. */
  private static @Tainted boolean copy(@Tainted FileSystem srcFS, @Tainted FileStatus srcStatus,
                              @Tainted
                              File dst, @Tainted boolean deleteSource,
                              @Tainted
                              Configuration conf) throws IOException {
    @Tainted
    Path src = srcStatus.getPath();
    if (srcStatus.isDirectory()) {
      if (!dst.mkdirs()) {
        return false;
      }
      @Tainted
      FileStatus contents @Tainted [] = srcFS.listStatus(src);
      for (@Tainted int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i],
             new @Tainted File(dst, contents[i].getPath().getName()),
             deleteSource, conf);
      }
    } else {
      @Tainted
      InputStream in = srcFS.open(src);
      IOUtils.copyBytes(in, new @Tainted FileOutputStream(dst), conf);
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  }

  private static @Tainted Path checkDest(@Tainted String srcName, @Tainted FileSystem dstFS, @Tainted Path dst,
      @Tainted
      boolean overwrite) throws IOException {
    if (dstFS.exists(dst)) {
      @Tainted
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (sdst.isDirectory()) {
        if (null == srcName) {
          throw new @Tainted IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, new @Tainted Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new @Tainted IOException("Target " + dst + " already exists");
      }
    }
    return dst;
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param filename The filename to convert
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static @Untainted String makeShellPath(@Untainted String filename) throws IOException {
    return filename;
  }
  
  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param file The filename to convert
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static @Untainted String makeShellPath(@Untainted File file) throws IOException {
    return makeShellPath(file, false);
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param file The filename to convert
   * @param makeCanonicalPath 
   *          Whether to make canonical path for the file passed
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  //ostrusted, file is trusted therefore it's toString is trusted
  public static @Untainted String makeShellPath(@Untainted File file, @Tainted boolean makeCanonicalPath)
  throws IOException {
    if (makeCanonicalPath) {
      return makeShellPath((@Untainted String) file.getCanonicalPath());
    } else {
      return makeShellPath((@Untainted String) file.toString());
    }
  }

  /**
   * Takes an input dir and returns the du on that local directory. Very basic
   * implementation.
   * 
   * @param dir
   *          The input dir to get the disk space of this local dir
   * @return The total disk space of the input local directory
   */
  public static @Tainted long getDU(@Tainted File dir) {
    @Tainted
    long size = 0;
    if (!dir.exists())
      return 0;
    if (!dir.isDirectory()) {
      return dir.length();
    } else {
      @Tainted
      File @Tainted [] allFiles = dir.listFiles();
      if(allFiles != null) {
         for (@Tainted int i = 0; i < allFiles.length; i++) {
           @Tainted
           boolean isSymLink = true; // ME MAKE WORK GOOD
           //try {
//             isSymLink = /*@Tainted*/ false; //MODIFIED TO AVOID WEIRD CLASSPATH ERROR
	     //isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
           //} catch(IOException ioe) {
           //  isSymLink = true;
           //}
           if(!isSymLink) {
             size += getDU(allFiles[i]);
           }
         }
      }
      return size;
    }
  }
    
  /**
   * Given a File input it will unzip the file in a the unzip directory
   * passed as the second parameter
   * @param inFile The zip file as input
   * @param unzipDir The unzip directory where to unzip the zip file.
   * @throws IOException
   */
  public static void unZip(@Tainted File inFile, @Tainted File unzipDir) throws IOException {
    @Tainted
    Enumeration<@Tainted ? extends @Tainted ZipEntry> entries;
    @Tainted
    ZipFile zipFile = new @Tainted ZipFile(inFile);

    try {
      entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        @Tainted
        ZipEntry entry = entries.nextElement();
        if (!entry.isDirectory()) {
          @Tainted
          InputStream in = zipFile.getInputStream(entry);
          try {
            @Tainted
            File file = new @Tainted File(unzipDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {           
              if (!file.getParentFile().isDirectory()) {
                throw new @Tainted IOException("Mkdirs failed to create " + 
                                      file.getParentFile().toString());
              }
            }
            @Tainted
            OutputStream out = new @Tainted FileOutputStream(file);
            try {
              @Tainted
              byte @Tainted [] buffer = new @Tainted byte @Tainted [8192];
              @Tainted
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      zipFile.close();
    }
  }

  /**
   * Given a Tar File as input it will untar the file in a the untar directory
   * passed as the second parameter
   * 
   * This utility will untar ".tar" files and ".tar.gz","tgz" files.
   *  
   * @param inFile The tar file as input. 
   * @param untarDir The untar directory where to untar the tar file.
   * @throws IOException
   */
  //ostrusted, Need to be osTrusted for unTarUsingTar
  public static void unTar(@Untainted File inFile, @Untainted File untarDir) throws IOException {
    if (!untarDir.mkdirs()) {
      if (!untarDir.isDirectory()) {
        throw new @Tainted IOException("Mkdirs failed to create " + untarDir);
      }
    }

    @Tainted
    boolean gzipped = inFile.toString().endsWith("gz");
    if(Shell.WINDOWS) {
      // Tar is not native to Windows. Use simple Java based implementation for 
      // tests and simple tar archives
      unTarUsingJava(inFile, untarDir, gzipped);
    }
    else {
      // spawn tar utility to untar archive for full fledged unix behavior such 
      // as resolving symlinks in tar archives
      unTarUsingTar(inFile, untarDir, gzipped);
    }
  }

    @SuppressWarnings("ostrusted:cast.unsafe")
  private static void unTarUsingTar(@Untainted File inFile, @Untainted File untarDir,
      @Tainted
      boolean gzipped) throws IOException {
    @Tainted
    StringBuffer untarCommand = new @Tainted StringBuffer();
    if (gzipped) {
      untarCommand.append(" gzip -dc '");
      untarCommand.append(FileUtil.makeShellPath(inFile));
      untarCommand.append("' | (");
    } 
    untarCommand.append("cd '");
    untarCommand.append(FileUtil.makeShellPath(untarDir)); 
    untarCommand.append("' ; ");
    untarCommand.append("tar -xf ");

    if (gzipped) {
      untarCommand.append(" -)");
    } else {
      untarCommand.append(FileUtil.makeShellPath(inFile));
    }
    @Untainted String @Tainted [] shellCmd = (@Untainted String @Tainted [])
            new String @Tainted [] { "bash", "-c", untarCommand.toString() };
    @Tainted
    ShellCommandExecutor shexec = new @Tainted ShellCommandExecutor(shellCmd);
    shexec.execute();
    @Tainted
    int exitcode = shexec.getExitCode();
    if (exitcode != 0) {
      throw new @Tainted IOException("Error untarring file " + inFile + 
                  ". Tar process exited with exit code " + exitcode);
    }
  }
  
  private static void unTarUsingJava(@Tainted File inFile, @Tainted File untarDir,
      @Tainted
      boolean gzipped) throws IOException {
    @Tainted
    InputStream inputStream = null;
    @Tainted
    TarArchiveInputStream tis = null;
    try {
      if (gzipped) {
        inputStream = new @Tainted BufferedInputStream(new @Tainted GZIPInputStream(
            new @Tainted FileInputStream(inFile)));
      } else {
        inputStream = new @Tainted BufferedInputStream(new @Tainted FileInputStream(inFile));
      }

      tis = new @Tainted TarArchiveInputStream(inputStream);

      for (@Tainted TarArchiveEntry entry = tis.getNextTarEntry(); entry != null;) {
        unpackEntries(tis, entry, untarDir);
        entry = tis.getNextTarEntry();
      }
    } finally {
      IOUtils.cleanup(LOG, tis, inputStream);
    }
  }
  
  private static void unpackEntries(@Tainted TarArchiveInputStream tis,
      @Tainted
      TarArchiveEntry entry, @Tainted File outputDir) throws IOException {
    if (entry.isDirectory()) {
      @Tainted
      File subDir = new @Tainted File(outputDir, entry.getName());
      if (!subDir.mkdir() && !subDir.isDirectory()) {
        throw new @Tainted IOException("Mkdirs failed to create tar internal dir "
            + outputDir);
      }

      for (@Tainted TarArchiveEntry e : entry.getDirectoryEntries()) {
        unpackEntries(tis, e, subDir);
      }

      return;
    }

    @Tainted
    File outputFile = new @Tainted File(outputDir, entry.getName());
    if (!outputDir.exists()) {
      if (!outputDir.mkdirs()) {
        throw new @Tainted IOException("Mkdirs failed to create tar internal dir "
            + outputDir);
      }
    }

    @Tainted
    int count;
    @Tainted
    byte data @Tainted [] = new @Tainted byte @Tainted [2048];
    @Tainted
    BufferedOutputStream outputStream = new @Tainted BufferedOutputStream(
        new @Tainted FileOutputStream(outputFile));

    while ((count = tis.read(data)) != -1) {
      outputStream.write(data, 0, count);
    }

    outputStream.flush();
    outputStream.close();
  }
  
  /**
   * Class for creating hardlinks.
   * Supports Unix, WindXP.
   * @deprecated Use {@link org.apache.hadoop.fs.HardLink}
   */
  @Deprecated
  public static class HardLink extends org.apache.hadoop.fs.HardLink { 
    // This is a stub to assist with coordinated change between
    // COMMON and HDFS projects.  It will be removed after the
    // corresponding change is committed to HDFS.
  }

  /**
   * Create a soft link between a src and destination
   * only on a local disk. HDFS does not support this.
   * On Windows, when symlink creation fails due to security
   * setting, we will log a warning. The return code in this
   * case is 2.
   *
   * @param target the target for symlink 
   * @param linkname the symlink
   * @return 0 on success
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted int symLink(@Untainted String target, @Untainted String linkname) throws IOException{
    // Run the input paths through Java's File so that they are converted to the
    // native OS form
    @Tainted
    File targetFile = new @Tainted File(
        Path.getPathWithoutSchemeAndAuthority(new @Tainted Path(target)).toString());
    @Tainted
    File linkFile = new @Tainted File(
        Path.getPathWithoutSchemeAndAuthority(new @Tainted Path(linkname)).toString());

    // If not on Java7+, copy a file instead of creating a symlink since
    // Java6 has close to no support for symlinks on Windows. Specifically
    // File#length and File#renameTo do not work as expected.
    // (see HADOOP-9061 for additional details)
    // We still create symlinks for directories, since the scenario in this
    // case is different. The directory content could change in which
    // case the symlink loses its purpose (for example task attempt log folder
    // is symlinked under userlogs and userlogs are generated afterwards).
    if (Shell.WINDOWS && !Shell.isJava7OrAbove() && targetFile.isFile()) {
      try {
        LOG.warn("FileUtil#symlink: On Windows+Java6, copying file instead " +
            "of creating a symlink. Copying " + target + " -> " + linkname);

        if (!linkFile.getParentFile().exists()) {
          LOG.warn("Parent directory " + linkFile.getParent() +
              " does not exist.");
          return 1;
        } else {
          org.apache.commons.io.FileUtils.copyFile(targetFile, linkFile);
        }
      } catch (@Tainted IOException ex) {
        LOG.warn("FileUtil#symlink failed to copy the file with error: "
            + ex.getMessage());
        // Exit with non-zero exit code
        return 1;
      }
      return 0;
    }

    //ostrusted, targetFile and linkFile are osTrusted
    @Untainted String @Tainted [] cmd = Shell.getSymlinkCommand(
            (@Untainted String) targetFile.toString(),
            (@Untainted String) linkFile.toString());

    @Tainted
    ShellCommandExecutor shExec;
    try {
      if (Shell.WINDOWS &&
          linkFile.getParentFile() != null &&
          !new @Tainted Path(target).isAbsolute()) {
        // Relative links on Windows must be resolvable at the time of
        // creation. To ensure this we run the shell command in the directory
        // of the link.
        //
        shExec = new @Tainted ShellCommandExecutor(cmd, linkFile.getParentFile());
      } else {
        shExec = new @Tainted ShellCommandExecutor(cmd);
      }
      shExec.execute();
    } catch (Shell.@Tainted ExitCodeException ec) {
      @Tainted
      int returnVal = ec.getExitCode();
      if (Shell.WINDOWS && returnVal == SYMLINK_NO_PRIVILEGE) {
        LOG.warn("Fail to create symbolic links on Windows. "
            + "The default security settings in Windows disallow non-elevated "
            + "administrators and all non-administrators from creating symbolic links. "
            + "This behavior can be changed in the Local Security Policy management console");
      } else if (returnVal != 0) {
        LOG.warn("Command '" + StringUtils.join(" ", cmd) + "' failed "
            + returnVal + " with: " + ec.getMessage());
      }
      return returnVal;
    } catch (@Tainted IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error while create symlink " + linkname + " to " + target
            + "." + " Exception: " + StringUtils.stringifyException(e));
      }
      throw e;
    }
    return shExec.getExitCode();
  }

  /**
   * Change the permissions on a filename.
   * @param filename the name of the file to change
   * @param perm the permission string
   * @return the exit code from the command
   * @throws IOException
   * @throws InterruptedException
   */
  public static @Tainted int chmod(@Untainted String filename, @Untainted String perm
                          ) throws IOException, InterruptedException {
    return chmod(filename, perm, false);
  }

  /**
   * Change the permissions on a file / directory, recursively, if
   * needed.
   * @param filename name of the file whose permissions are to change
   * @param perm permission string
   * @param recursive true, if permissions should be changed recursively
   * @return the exit code from the command.
   * @throws IOException
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted int chmod(@Untainted String filename, @Untainted String perm, @Tainted boolean recursive)
                            throws IOException {
    @Untainted
    String @Tainted [] cmd = Shell.getSetPermissionCommand(perm, recursive);
    @Untainted
    String @Tainted [] args = new @Untainted String @Tainted [cmd.length + 1];
    System.arraycopy(cmd, 0, args, 0, cmd.length);
    //ostrusted, filename is trusted
    args[cmd.length] = (@Untainted String) (new @Tainted File(filename).getPath());
    @Tainted
    ShellCommandExecutor shExec = new @Tainted ShellCommandExecutor(args);
    try {
      shExec.execute();
    }catch(@Tainted IOException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Error while changing permission : " + filename 
                  +" Exception: " + StringUtils.stringifyException(e));
      }
    }
    return shExec.getExitCode();
  }

  /**
   * Set the ownership on a file / directory. User name and group name
   * cannot both be null.
   * @param file the file to change
   * @param username the new user owner name
   * @param groupname the new group owner name
   * @throws IOException
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  //ostrusted, ternary operators lead to errors, all inputs are OsTrusted
  public static void setOwner(@Untainted File file, @Untainted String username,
      @Untainted String groupname) throws IOException {
    if (username == null && groupname == null) {
      throw new @Tainted IOException("username == null && groupname == null");
    }
    @Untainted String arg = (@Untainted String)
            ((username == null ? "" : username)
             + (groupname == null ? "" : ":" + groupname));

    @Untainted String @Tainted [] cmd = Shell.getSetOwnerCommand(arg);
    execCommand(file, cmd);
  }

  /**
   * Platform independent implementation for {@link File#setReadable(boolean)}
   * File#setReadable does not work as expected on Windows.
   * @param f input file
   * @param readable
   * @return true on success, false otherwise
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted boolean setReadable(@Untainted File f, @Tainted boolean readable) {
    if (Shell.WINDOWS) {
      try {
        @Untainted String permission = (@Untainted String) (readable ? "u+r" : "u-r");
        FileUtil.chmod((@Untainted String) f.getCanonicalPath(), permission, false);
        return true;
      } catch (@Tainted IOException ex) {
        return false;
      }
    } else {
      return f.setReadable(readable);
    }
  }

  /**
   * Platform independent implementation for {@link File#setWritable(boolean)}
   * File#setWritable does not work as expected on Windows.
   * @param f input file
   * @param writable
   * @return true on success, false otherwise
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted boolean setWritable(@Untainted File f, @Untainted boolean writable) {
    if (Shell.WINDOWS) {
      try {
        @Untainted String permission = (@Untainted String) (writable ? "u+w" : "u-w");
        FileUtil.chmod((@Untainted String) f.getCanonicalPath(), permission, false);
        return true;
      } catch (@Tainted IOException ex) {
        return false;
      }
    } else {
      return f.setWritable(writable);
    }
  }

  /**
   * Platform independent implementation for {@link File#setExecutable(boolean)}
   * File#setExecutable does not work as expected on Windows.
   * Note: revoking execute permission on folders does not have the same
   * behavior on Windows as on Unix platforms. Creating, deleting or renaming
   * a file within that folder will still succeed on Windows.
   * @param f input file
   * @param executable
   * @return true on success, false otherwise
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Tainted boolean setExecutable(@Untainted File f, @Tainted boolean executable) {
    if (Shell.WINDOWS) {
      try {
        @Untainted String permission = (@Untainted String) (executable ? "u+x" : "u-x");
        FileUtil.chmod((@Untainted String)  f.getCanonicalPath(), permission, false);
        return true;
      } catch (@Tainted IOException ex) {
        return false;
      }
    } else {
      return f.setExecutable(executable);
    }
  }

  /**
   * Platform independent implementation for {@link File#canRead()}
   * @param f input file
   * @return On Unix, same as {@link File#canRead()}
   *         On Windows, true if process has read access on the path
   */
  public static @Tainted boolean canRead(@Tainted File f) {
    if (Shell.WINDOWS) {
      try {
        return NativeIO.Windows.access(f.getCanonicalPath(),
            NativeIO.Windows.AccessRight.ACCESS_READ);
      } catch (@Tainted IOException e) {
        return false;
      }
    } else {
      return f.canRead();
    }
  }

  /**
   * Platform independent implementation for {@link File#canWrite()}
   * @param f input file
   * @return On Unix, same as {@link File#canWrite()}
   *         On Windows, true if process has write access on the path
   */
  public static @Tainted boolean canWrite(@Tainted File f) {
    if (Shell.WINDOWS) {
      try {
        return NativeIO.Windows.access(f.getCanonicalPath(),
            NativeIO.Windows.AccessRight.ACCESS_WRITE);
      } catch (@Tainted IOException e) {
        return false;
      }
    } else {
      return f.canWrite();
    }
  }

  /**
   * Platform independent implementation for {@link File#canExecute()}
   * @param f input file
   * @return On Unix, same as {@link File#canExecute()}
   *         On Windows, true if process has execute access on the path
   */
  public static @Tainted boolean canExecute(@Tainted File f) {
    if (Shell.WINDOWS) {
      try {
        return NativeIO.Windows.access(f.getCanonicalPath(),
            NativeIO.Windows.AccessRight.ACCESS_EXECUTE);
      } catch (@Tainted IOException e) {
        return false;
      }
    } else {
      return f.canExecute();
    }
  }

  /**
   * Set permissions to the required value. Uses the java primitives instead
   * of forking if group == other.
   * @param f the file to change
   * @param permission the new permissions
   * @throws IOException
   */
  public static void setPermission(@Untainted File f, @Tainted FsPermission permission
                                   ) throws IOException {
    @Tainted
    FsAction user = permission.getUserAction();
    @Tainted
    FsAction group = permission.getGroupAction();
    @Tainted
    FsAction other = permission.getOtherAction();

    // use the native/fork if the group/other permissions are different
    // or if the native is available or on Windows
    if (group != other || NativeIO.isAvailable() || Shell.WINDOWS) {
      execSetPermission(f, permission);
      return;
    }
    
    @Tainted
    boolean rv = true;
    
    // read perms
    rv = f.setReadable(group.implies(FsAction.READ), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.READ) != user.implies(FsAction.READ)) {
      rv = f.setReadable(user.implies(FsAction.READ), true);
      checkReturnValue(rv, f, permission);
    }

    // write perms
    rv = f.setWritable(group.implies(FsAction.WRITE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.WRITE) != user.implies(FsAction.WRITE)) {
      rv = f.setWritable(user.implies(FsAction.WRITE), true);
      checkReturnValue(rv, f, permission);
    }

    // exec perms
    rv = f.setExecutable(group.implies(FsAction.EXECUTE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.EXECUTE) != user.implies(FsAction.EXECUTE)) {
      rv = f.setExecutable(user.implies(FsAction.EXECUTE), true);
      checkReturnValue(rv, f, permission);
    }
  }

  private static void checkReturnValue(@Tainted boolean rv, @Tainted File p,
                                       @Tainted
                                       FsPermission permission
                                       ) throws IOException {
    if (!rv) {
      throw new @Tainted IOException("Failed to set permissions of path: " + p + 
                            " to " + 
                            String.format("%04o", permission.toShort()));
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  //ostrusted, shorts are trusted
  private static void execSetPermission(@Untainted File f,
                                        @Tainted
                                        FsPermission permission
                                       )  throws IOException {
    if (NativeIO.isAvailable()) {
      NativeIO.POSIX.chmod(f.getCanonicalPath(), permission.toShort());
    } else {
      execCommand(f, Shell.getSetPermissionCommand(
                  (@Untainted String) String.format("%04o", permission.toShort()), false));
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  static @Tainted String execCommand(@Untainted File f, @Untainted String @Tainted ... cmd) throws IOException {
    @Untainted
    String @Tainted [] args = new @Untainted String @Tainted [cmd.length + 1];
    System.arraycopy(cmd, 0, args, 0, cmd.length);
    args[cmd.length] = (@Untainted String) f.getCanonicalPath();
    @Tainted
    String output = Shell.execCommand(args);
    return output;
  }

  /**
   * Create a tmp file for a base file.
   * @param basefile the base file of the tmp
   * @param prefix file name prefix of tmp
   * @param isDeleteOnExit if true, the tmp will be deleted when the VM exits
   * @return a newly created tmp file
   * @exception IOException If a tmp file cannot created
   * @see java.io.File#createTempFile(String, String, File)
   * @see java.io.File#deleteOnExit()
   */
  public static final @Tainted File createLocalTempFile(final @Tainted File basefile,
                                               final @Tainted String prefix,
                                               final @Tainted boolean isDeleteOnExit)
    throws IOException {
    @Tainted
    File tmp = File.createTempFile(prefix + basefile.getName(),
                                   "", basefile.getParentFile());
    if (isDeleteOnExit) {
      tmp.deleteOnExit();
    }
    return tmp;
  }

  /**
   * Move the src file to the name specified by target.
   * @param src the source file
   * @param target the target file
   * @exception IOException If this operation fails
   */
  public static void replaceFile(@Tainted File src, @Tainted File target) throws IOException {
    /* renameTo() has two limitations on Windows platform.
     * src.renameTo(target) fails if
     * 1) If target already exists OR
     * 2) If target is already open for reading/writing.
     */
    if (!src.renameTo(target)) {
      @Tainted
      int retries = 5;
      while (target.exists() && !target.delete() && retries-- >= 0) {
        try {
          Thread.sleep(1000);
        } catch (@Tainted InterruptedException e) {
          throw new @Tainted IOException("replaceFile interrupted.");
        }
      }
      if (!src.renameTo(target)) {
        throw new @Tainted IOException("Unable to rename " + src +
                              " to " + target);
      }
    }
  }
  
  /**
   * A wrapper for {@link File#listFiles()}. This java.io API returns null 
   * when a dir is not a directory or for any I/O error. Instead of having
   * null check everywhere File#listFiles() is used, we will add utility API
   * to get around this problem. For the majority of cases where we prefer 
   * an IOException to be thrown.
   * @param dir directory for which listing should be performed
   * @return list of files or empty list
   * @exception IOException for invalid directory or for a bad disk.
   */
  @SuppressWarnings("ostrusted:return.type.incompatible")
  public static @PolyTainted File @Tainted [] listFiles(@PolyTainted File dir) throws IOException {
    @Tainted File @Tainted [] files = dir.listFiles();
    if(files == null) {
      throw new @Tainted IOException("Invalid directory or I/O error occurred for dir: "
                + dir.toString());
    }
    return files;
  }  
  
  /**
   * A wrapper for {@link File#list()}. This java.io API returns null 
   * when a dir is not a directory or for any I/O error. Instead of having
   * null check everywhere File#list() is used, we will add utility API
   * to get around this problem. For the majority of cases where we prefer 
   * an IOException to be thrown.
   * @param dir directory for which listing should be performed
   * @return list of file names or empty string list
   * @exception IOException for invalid directory or for a bad disk.
   */
  @SuppressWarnings("ostrusted:return.type.incompatible")
  public static @PolyTainted String @Tainted [] list(@PolyTainted File dir) throws IOException {
    @Tainted String @Tainted [] fileNames = dir.list();
    if(fileNames == null) {
      throw new @Tainted IOException("Invalid directory or I/O error occurred for dir: "
                + dir.toString());
    }
    return fileNames;
  }  
  
  /**
   * Create a jar file at the given path, containing a manifest with a classpath
   * that references all specified entries.
   * 
   * Some platforms may have an upper limit on command line length.  For example,
   * the maximum command line length on Windows is 8191 characters, but the
   * length of the classpath may exceed this.  To work around this limitation,
   * use this method to create a small intermediate jar with a manifest that
   * contains the full classpath.  It returns the absolute path to the new jar,
   * which the caller may set as the classpath for a new process.
   * 
   * Environment variable evaluation is not supported within a jar manifest, so
   * this method expands environment variables before inserting classpath entries
   * to the manifest.  The method parses environment variables according to
   * platform-specific syntax (%VAR% on Windows, or $VAR otherwise).  On Windows,
   * environment variables are case-insensitive.  For example, %VAR% and %var%
   * evaluate to the same value.
   * 
   * Specifying the classpath in a jar manifest does not support wildcards, so
   * this method expands wildcards internally.  Any classpath entry that ends
   * with * is translated to all files at that path with extension .jar or .JAR.
   * 
   * @param inputClassPath String input classpath to bundle into the jar manifest
   * @param pwd Path to working directory to save jar
   * @param callerEnv Map<String, String> caller's environment variables to use
   *   for expansion
   * @return String absolute path to new jar
   * @throws IOException if there is an I/O error while writing the jar file
   */
  public static @Tainted String createJarWithClassPath(@Tainted String inputClassPath, @Tainted Path pwd,
      @Tainted
      Map<@Tainted String, @Tainted String> callerEnv) throws IOException {
    // Replace environment variables, case-insensitive on Windows
    @SuppressWarnings({"unchecked", "ostrusted"}) // The raw type of CaseInsensitiveMap causes issues.
    @Tainted
    Map<@Tainted String, @Tainted String> env = Shell.WINDOWS ? new @Tainted CaseInsensitiveMap(callerEnv) :
      callerEnv;
    @Tainted
    String @Tainted [] classPathEntries = inputClassPath.split(File.pathSeparator);
    for (@Tainted int i = 0; i < classPathEntries.length; ++i) {
      classPathEntries[i] = StringUtils.replaceTokens(classPathEntries[i],
        StringUtils.ENV_VAR_PATTERN, env);
    }
    @Tainted
    File workingDir = new @Tainted File(pwd.toString());
    if (!workingDir.mkdirs()) {
      // If mkdirs returns false because the working directory already exists,
      // then this is acceptable.  If it returns false due to some other I/O
      // error, then this method will fail later with an IOException while saving
      // the jar.
      LOG.debug("mkdirs false for " + workingDir + ", execution will continue");
    }

    // Append all entries
    @Tainted
    List<@Tainted String> classPathEntryList = new @Tainted ArrayList<@Tainted String>(
      classPathEntries.length);
    for (@Tainted String classPathEntry: classPathEntries) {
      if (classPathEntry.length() == 0) {
        continue;
      }
      if (classPathEntry.endsWith("*")) {
        // Append all jars that match the wildcard
        @Tainted
        Path globPath = new @Tainted Path(classPathEntry).suffix("{.jar,.JAR}");
        @Tainted
        FileStatus @Tainted [] wildcardJars = FileContext.getLocalFSFileContext().util()
          .globStatus(globPath);
        if (wildcardJars != null) {
          for (@Tainted FileStatus wildcardJar: wildcardJars) {
            classPathEntryList.add(wildcardJar.getPath().toUri().toURL()
              .toExternalForm());
          }
        }
      } else {
        // Append just this entry
        @Tainted
        File fileCpEntry = null;
        if(!new @Tainted Path(classPathEntry).isAbsolute()) {
          fileCpEntry = new @Tainted File(workingDir, classPathEntry);
        }
        else {
          fileCpEntry = new @Tainted File(classPathEntry);
        }
        @Tainted
        String classPathEntryUrl = fileCpEntry.toURI().toURL()
          .toExternalForm();

        // File.toURI only appends trailing '/' if it can determine that it is a
        // directory that already exists.  (See JavaDocs.)  If this entry had a
        // trailing '/' specified by the caller, then guarantee that the
        // classpath entry in the manifest has a trailing '/', and thus refers to
        // a directory instead of a file.  This can happen if the caller is
        // creating a classpath jar referencing a directory that hasn't been
        // created yet, but will definitely be created before running.
        if (classPathEntry.endsWith(Path.SEPARATOR) &&
            !classPathEntryUrl.endsWith(Path.SEPARATOR)) {
          classPathEntryUrl = classPathEntryUrl + Path.SEPARATOR;
        }
        classPathEntryList.add(classPathEntryUrl);
      }
    }
    @Tainted
    String jarClassPath = StringUtils.join(" ", classPathEntryList);

    // Create the manifest
    @Tainted
    Manifest jarManifest = new @Tainted Manifest();
    jarManifest.getMainAttributes().putValue(
        Attributes.Name.MANIFEST_VERSION.toString(), "1.0");
    jarManifest.getMainAttributes().putValue(
        Attributes.Name.CLASS_PATH.toString(), jarClassPath);

    // Write the manifest to output JAR file
    @Tainted
    File classPathJar = File.createTempFile("classpath-", ".jar", workingDir);
    @Tainted
    FileOutputStream fos = null;
    @Tainted
    BufferedOutputStream bos = null;
    @Tainted
    JarOutputStream jos = null;
    try {
      fos = new @Tainted FileOutputStream(classPathJar);
      bos = new @Tainted BufferedOutputStream(fos);
      jos = new @Tainted JarOutputStream(bos, jarManifest);
    } finally {
      IOUtils.cleanup(LOG, jos, bos, fos);
    }

    return classPathJar.getCanonicalPath();
  }
}
