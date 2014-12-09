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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.util.Shell;

/**
 * Class for creating hardlinks.
 * Supports Unix/Linux, Windows via winutils , and Mac OS X.
 * 
 * The HardLink class was formerly a static inner class of FSUtil,
 * and the methods provided were blatantly non-thread-safe.
 * To enable volume-parallel Update snapshots, we now provide static 
 * threadsafe methods that allocate new buffer string arrays
 * upon each call.  We also provide an API to hardlink all files in a
 * directory with a single command, which is up to 128 times more 
 * efficient - and minimizes the impact of the extra buffer creations.
 */
public class HardLink { 

  private static @Tainted HardLinkCommandGetter getHardLinkCommand;
  
  public final @Tainted LinkStats linkStats; //not static
  
  //initialize the command "getters" statically, so can use their 
  //methods without instantiating the HardLink object
  static { 
    if (Shell.WINDOWS) {
      // Windows
      getHardLinkCommand = new @Tainted HardLinkCGWin();
    } else {
      // Unix or Linux
      getHardLinkCommand = new @Tainted HardLinkCGUnix();
      //override getLinkCountCommand for the particular Unix variant
      //Linux is already set as the default - {"stat","-c%h", null}
      if (Shell.MAC || Shell.FREEBSD) {
        @Untainted
        String @Tainted [] linkCountCmdTemplate = new @Untainted String @Tainted [] {"/usr/bin/stat","-f%l", null};
        HardLinkCGUnix.setLinkCountCmdTemplate(linkCountCmdTemplate);
      } else if (Shell.SOLARIS) {
        @Untainted String @Tainted [] linkCountCmdTemplate = new @Untainted String @Tainted [] {"ls","-l", null};
        HardLinkCGUnix.setLinkCountCmdTemplate(linkCountCmdTemplate);        
      }
    }
  }

  public @Tainted HardLink() {
    linkStats = new @Tainted LinkStats();
  }
  
  /**
   * This abstract class bridges the OS-dependent implementations of the 
   * needed functionality for creating hardlinks and querying link counts.
   * The particular implementation class is chosen during 
   * static initialization phase of the HardLink class.
   * The "getter" methods construct shell command strings for various purposes.
   */
  private static abstract class HardLinkCommandGetter {

    /**
     * Get the command string needed to hardlink a bunch of files from
     * a single source directory into a target directory.  The source directory
     * is not specified here, but the command will be executed using the source
     * directory as the "current working directory" of the shell invocation.
     * 
     * @param fileBaseNames - array of path-less file names, relative
     *            to the source directory
     * @param linkDir - target directory where the hardlinks will be put
     * @return - an array of Strings suitable for use as a single shell command
     *            with {@link Runtime.exec()}
     * @throws IOException - if any of the file or path names misbehave
     */
    abstract @Untainted String @Tainted [] linkMult(HardLink.@Tainted HardLinkCommandGetter this, @Untainted String @Tainted [] fileBaseNames, @Untainted File linkDir)
                          throws IOException;
    
    /**
     * Get the command string needed to hardlink a single file
     */
    abstract @Untainted String @Tainted [] linkOne(HardLink.@Tainted HardLinkCommandGetter this, @Untainted File file, @Untainted File linkName) throws IOException;
    
    /**
     * Get the command string to query the hardlink count of a file
     */
    abstract @Untainted String @Tainted [] linkCount(HardLink.@Tainted HardLinkCommandGetter this, @Untainted File file) throws IOException;
    
    /**
     * Calculate the total string length of the shell command
     * resulting from execution of linkMult, plus the length of the
     * source directory name (which will also be provided to the shell)
     * 
     * @param fileDir - source directory, parent of fileBaseNames
     * @param fileBaseNames - array of path-less file names, relative
     *            to the source directory
     * @param linkDir - target directory where the hardlinks will be put
     * @return - total data length (must not exceed maxAllowedCmdArgLength)
     * @throws IOException
     */
    abstract @Tainted int getLinkMultArgLength(
                     HardLink.@Tainted HardLinkCommandGetter this,
                     @Untainted File fileDir,
                     @Untainted String @Tainted [] fileBaseNames,
                     @Untainted File linkDir )
                     throws IOException;
    
    /**
     * Get the maximum allowed string length of a shell command on this OS,
     * which is just the documented minimum guaranteed supported command
     * length - aprx. 32KB for Unix, and 8KB for Windows.
     */
    abstract @Tainted int getMaxAllowedCmdArgLength(HardLink.@Tainted HardLinkCommandGetter this); 
  }
  
  /**
   * Implementation of HardLinkCommandGetter class for Unix
   */
  static class HardLinkCGUnix extends @Tainted HardLinkCommandGetter {
    private static @Untainted String @Tainted [] hardLinkCommand = new String @Tainted [] {"ln", null, null};
    private static @Untainted String @Tainted [] hardLinkMultPrefix = new String @Tainted [] {"ln"};
    private static @Untainted String @Tainted [] hardLinkMultSuffix = new String @Tainted [] {null};
    private static @Untainted String @Tainted [] getLinkCountCommand = new String @Tainted [] {"stat","-c%h", null};
    //Unix guarantees at least 32K bytes cmd length.
    //Subtract another 64b to allow for Java 'exec' overhead
    private static final @Tainted int maxAllowedCmdArgLength = 32*1024 - 65;
    
    private static synchronized void setLinkCountCmdTemplate(@Untainted String @Tainted [] template) {
      //May update this for specific unix variants, 
      //after static initialization phase
      getLinkCountCommand = template;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkOne(java.io.File, java.io.File)
     */
     //ostrusted, we should probably ensure that System.arraycopy is treated like an assignment
    @Override
    @Untainted  String @Tainted [] linkOne(HardLink.@Tainted HardLinkCGUnix this, @Untainted File file, @Untainted File linkName)
    throws IOException {
      @Untainted String @Tainted [] buf = new @Untainted String @Tainted [hardLinkCommand.length];
      System.arraycopy(hardLinkCommand, 0, buf, 0, hardLinkCommand.length);
      //unix wants argument order: "ln <existing> <new>"
      buf[1] = FileUtil.makeShellPath(file, true); 
      buf[2] = FileUtil.makeShellPath(linkName, true);
      return buf;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkMult(java.lang.String[], java.io.File)
     */
    @Override
    @Untainted String @Tainted [] linkMult(HardLink.@Tainted HardLinkCGUnix this, @Untainted String @Tainted [] fileBaseNames, @Untainted File linkDir)
    throws IOException {
      @Untainted
      String @Tainted [] buf = new @Untainted String @Tainted [fileBaseNames.length
                                + hardLinkMultPrefix.length 
                                + hardLinkMultSuffix.length];
      @Tainted
      int mark=0;
      System.arraycopy(hardLinkMultPrefix, 0, buf, mark, 
                       hardLinkMultPrefix.length);
      mark += hardLinkMultPrefix.length;
      System.arraycopy(fileBaseNames, 0, buf, mark, fileBaseNames.length);
      mark += fileBaseNames.length;
      buf[mark] = FileUtil.makeShellPath(linkDir, true);
      return buf;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkCount(java.io.File)
     */
    @Override
    @Untainted
    String @Tainted [] linkCount(HardLink.@Tainted HardLinkCGUnix this, @Untainted File file)
    throws IOException {
      @Untainted
      String @Tainted [] buf = new @Untainted String @Tainted [getLinkCountCommand.length];
      System.arraycopy(getLinkCountCommand, 0, buf, 0, 
                       getLinkCountCommand.length);
      buf[getLinkCountCommand.length - 1] = FileUtil.makeShellPath(file, true);
      return buf;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#getLinkMultArgLength(java.io.File, java.lang.String[], java.io.File)
     */
    @Override
    @Tainted
    int getLinkMultArgLength(HardLink.@Tainted HardLinkCGUnix this, @Untainted File fileDir, @Untainted String @Tainted [] fileBaseNames, @Untainted File linkDir)
    throws IOException{
      @Tainted
      int sum = 0;
      for (@Tainted String x : fileBaseNames) {
        // add 1 to account for terminal null or delimiter space
        sum += 1 + ((x == null) ? 0 : x.length());
      }
      sum += 2 + FileUtil.makeShellPath(fileDir, true).length()
             + FileUtil.makeShellPath(linkDir, true).length();
      //add the fixed overhead of the hardLinkMult prefix and suffix
      sum += 3; //length("ln") + 1
      return sum;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#getMaxAllowedCmdArgLength()
     */
    @Override
    @Tainted
    int getMaxAllowedCmdArgLength(HardLink.@Tainted HardLinkCGUnix this) {
      return maxAllowedCmdArgLength;
    }
  }
  
  
  /**
   * Implementation of HardLinkCommandGetter class for Windows
   */
  static class HardLinkCGWin extends @Tainted HardLinkCommandGetter {
    //The Windows command getter impl class and its member fields are
    //package-private ("default") access instead of "private" to assist 
    //unit testing (sort of) on non-Win servers

    static @Untainted String @Tainted [] hardLinkCommand = new @Untainted String @Tainted [] {
                        Shell.WINUTILS,"hardlink","create", null, null};
    static @Untainted String @Tainted [] hardLinkMultPrefix = new @Untainted String @Tainted [] {
                        "cmd","/q","/c","for", "%f", "in", "("};
    static @Untainted String   hardLinkMultDir = "\\%f";
    static @Untainted String @Tainted [] hardLinkMultSuffix = new @Untainted String @Tainted [] {
                        ")", "do", Shell.WINUTILS, "hardlink", "create", null,
                        "%f", "1>NUL"};
    static @Untainted String @Tainted [] getLinkCountCommand = new @Untainted String @Tainted [] {
                        Shell.WINUTILS, "hardlink",
                        "stat", null};
    //Windows guarantees only 8K - 1 bytes cmd length.
    //Subtract another 64b to allow for Java 'exec' overhead
    static final @Tainted int maxAllowedCmdArgLength = 8*1024 - 65;

    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkOne(java.io.File, java.io.File)
     */
    @Override
    @SuppressWarnings("ostrusted:cast.unsafe")
    @Untainted  String @Tainted [] linkOne(HardLink.@Tainted HardLinkCGWin this, @Untainted File file, @Untainted File linkName)
    throws IOException {
      @Untainted String @Tainted [] buf = new @Untainted String @Tainted [hardLinkCommand.length];
      System.arraycopy(hardLinkCommand, 0, buf, 0, hardLinkCommand.length);
      //windows wants argument order: "create <new> <existing>"
      buf[4] = (@Untainted String) file.getCanonicalPath();
      buf[3] = (@Untainted String) linkName.getCanonicalPath();
      return buf;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkMult(java.lang.String[], java.io.File)
     */
    @Override
    @SuppressWarnings("ostrusted:cast.unsafe")
    @Untainted String @Tainted [] linkMult(HardLink.@Tainted HardLinkCGWin this, @Untainted String @Tainted [] fileBaseNames, @Untainted File linkDir)
    throws IOException {
      @Untainted String @Tainted [] buf =
              new @Untainted String @Tainted [fileBaseNames.length
                                                + hardLinkMultPrefix.length
                                                + hardLinkMultSuffix.length];
      @Untainted String td = ( (@Untainted String) linkDir.getCanonicalPath() ) + hardLinkMultDir;
      @Tainted
      int mark=0;
      System.arraycopy(hardLinkMultPrefix, 0, buf, mark, 
                       hardLinkMultPrefix.length);
      mark += hardLinkMultPrefix.length;
      System.arraycopy(fileBaseNames, 0, buf, mark, fileBaseNames.length);
      mark += fileBaseNames.length;
      System.arraycopy(hardLinkMultSuffix, 0, buf, mark, 
                       hardLinkMultSuffix.length);
      mark += hardLinkMultSuffix.length;
      buf[mark - 3] = td;
      return buf;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkCount(java.io.File)
     */
    @Override
    @SuppressWarnings("ostrusted:cast.unsafe")
    @Untainted  String @Tainted [] linkCount(HardLink.@Tainted HardLinkCGWin this, @Tainted File file)
    throws IOException {
      @Untainted  String @Tainted [] buf = new @Untainted String @Tainted [getLinkCountCommand.length];
      System.arraycopy(getLinkCountCommand, 0, buf, 0, 
                       getLinkCountCommand.length);
      buf[getLinkCountCommand.length - 1] = (@Untainted String) file.getCanonicalPath();
      return buf;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#getLinkMultArgLength(java.io.File, java.lang.String[], java.io.File)
     */
    @Override
    @Tainted
    int getLinkMultArgLength(HardLink.@Tainted HardLinkCGWin this, @Tainted File fileDir, @Tainted String @Tainted [] fileBaseNames, @Tainted File linkDir)
    throws IOException {
      @Tainted
      int sum = 0;
      for (@Tainted String x : fileBaseNames) {
        // add 1 to account for terminal null or delimiter space
        sum += 1 + ((x == null) ? 0 : x.length());
      }
      sum += 2 + fileDir.getCanonicalPath().length() +
               linkDir.getCanonicalPath().length();
      //add the fixed overhead of the hardLinkMult command 
      //(prefix, suffix, and Dir suffix)
      sum += ("cmd.exe /q /c for %f in ( ) do "
              + Shell.WINUTILS + " hardlink create \\%f %f 1>NUL ").length();
      return sum;
    }
    
    /*
     * @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#getMaxAllowedCmdArgLength()
     */
    @Override
    @Tainted
    int getMaxAllowedCmdArgLength(HardLink.@Tainted HardLinkCGWin this) {
      return maxAllowedCmdArgLength;
    }
  }
  
  
  /**
   * Calculate the nominal length of all contributors to the total 
   * commandstring length, including fixed overhead of the OS-dependent 
   * command.  It's protected rather than private, to assist unit testing,
   * but real clients are not expected to need it -- see the way 
   * createHardLinkMult() uses it internally so the user doesn't need to worry
   * about it.
   * 
   * @param fileDir - source directory, parent of fileBaseNames
   * @param fileBaseNames - array of path-less file names, relative
   *            to the source directory
   * @param linkDir - target directory where the hardlinks will be put
   * @return - total data length (must not exceed maxAllowedCmdArgLength)
   * @throws IOException
   */
  protected static @Tainted int getLinkMultArgLength(
          @Untainted
          File fileDir, @Untainted String @Tainted [] fileBaseNames, @Untainted File linkDir)
  throws IOException {
    return getHardLinkCommand.getLinkMultArgLength(fileDir, fileBaseNames, linkDir);
  }
  
  /**
   * Return this private value for use by unit tests.
   * Shell commands are not allowed to have a total string length
   * exceeding this size.
   */
  protected static @Tainted int getMaxAllowedCmdArgLength() {
    return getHardLinkCommand.getMaxAllowedCmdArgLength();
  }
  
  /*
   * ****************************************************
   * Complexity is above.  User-visible functionality is below
   * ****************************************************
   */

  /**
   * Creates a hardlink 
   * @param file - existing source file
   * @param linkName - desired target link file
   */
  public static void createHardLink(@Untainted File file, @Untainted File linkName)
  throws IOException {
    if (file == null) {
      throw new @Tainted IOException(
          "invalid arguments to createHardLink: source file is null");
    }
    if (linkName == null) {
      throw new @Tainted IOException(
          "invalid arguments to createHardLink: link name is null");
    }
	  // construct and execute shell command
    @Untainted String @Tainted [] hardLinkCommand = getHardLinkCommand.linkOne(file, linkName);
    @Tainted Process process = Runtime.getRuntime().exec(hardLinkCommand);
    try {
      if (process.waitFor() != 0) {
        @Tainted
        String errMsg = new @Tainted BufferedReader(new @Tainted InputStreamReader(
            process.getInputStream())).readLine();
        if (errMsg == null)  errMsg = "";
        @Tainted
        String inpMsg = new @Tainted BufferedReader(new @Tainted InputStreamReader(
            process.getErrorStream())).readLine();
        if (inpMsg == null)  inpMsg = "";
        throw new @Tainted IOException(errMsg + inpMsg);
      }
    } catch (@Tainted InterruptedException e) {
      throw new @Tainted IOException(e);
    } finally {
      process.destroy();
    }
  }

  /**
   * Creates hardlinks from multiple existing files within one parent
   * directory, into one target directory.
   * @param parentDir - directory containing source files
   * @param fileBaseNames - list of path-less file names, as returned by 
   *                        parentDir.list()
   * @param linkDir - where the hardlinks should be put.  It must already exist.
   * 
   * If the list of files is too long (overflows maxAllowedCmdArgLength),
   * we will automatically split it into multiple invocations of the
   * underlying method.
   */
  public static void createHardLinkMult(@Untainted File parentDir, @Untainted String @Tainted [] fileBaseNames,
      @Untainted
      File linkDir) throws IOException {
    //This is the public method all non-test clients are expected to use.
    //Normal case - allow up to maxAllowedCmdArgLength characters in the cmd
    createHardLinkMult(parentDir, fileBaseNames, linkDir, 
                       getHardLinkCommand.getMaxAllowedCmdArgLength());
  }

  /*
   * Implements {@link createHardLinkMult} with added variable  "maxLength",
   * to ease unit testing of the auto-splitting feature for long lists.
   * Likewise why it returns "callCount", the number of sub-arrays that
   * the file list had to be split into.
   * Non-test clients are expected to call the public method instead.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  protected static @Tainted int createHardLinkMult(@Untainted File parentDir,
      @Untainted String @Tainted [] fileBaseNames, @Untainted File linkDir, @Tainted int maxLength)
  throws IOException {
    if (parentDir == null) {
      throw new @Tainted IOException(
          "invalid arguments to createHardLinkMult: parent directory is null");
    }
    if (linkDir == null) {
      throw new @Tainted IOException(
          "invalid arguments to createHardLinkMult: link directory is null");
    }
    if (fileBaseNames == null) {
      throw new @Tainted IOException(
          "invalid arguments to createHardLinkMult: "
          + "filename list can be empty but not null");
    }
    if (fileBaseNames.length == 0) {
      //the OS cmds can't handle empty list of filenames, 
      //but it's legal, so just return.
      return 0; 
    }
    if (!linkDir.exists()) {
      throw new @Tainted FileNotFoundException(linkDir + " not found.");
    }

    //if the list is too long, split into multiple invocations
    @Tainted
    int callCount = 0;
    if (getLinkMultArgLength(parentDir, fileBaseNames, linkDir) > maxLength
          && fileBaseNames.length > 1) {
      @Untainted String @Tainted [] list1 = (@Untainted String @Tainted []) Arrays.copyOf(fileBaseNames, fileBaseNames.length/2);
      callCount += createHardLinkMult(parentDir, list1, linkDir, maxLength);
      @Untainted
      String @Tainted [] list2 = (@Untainted String @Tainted []) Arrays.copyOfRange(fileBaseNames, fileBaseNames.length/2,
          fileBaseNames.length);
      callCount += createHardLinkMult(parentDir, list2, linkDir, maxLength);  
      return callCount;
    } else {
      callCount = 1;
    }
    
    // construct and execute shell command
    @Untainted String @Tainted [] hardLinkCommand = getHardLinkCommand.linkMult(fileBaseNames, linkDir);
    @Tainted Process process = Runtime.getRuntime().exec(hardLinkCommand, null, parentDir);
    try {
      if (process.waitFor() != 0) {
        @Tainted
        String errMsg = new @Tainted BufferedReader(new @Tainted InputStreamReader(
            process.getInputStream())).readLine();
        if (errMsg == null)  errMsg = "";
        @Tainted
        String inpMsg = new @Tainted BufferedReader(new @Tainted InputStreamReader(
            process.getErrorStream())).readLine();
        if (inpMsg == null)  inpMsg = "";
        throw new @Tainted IOException(errMsg + inpMsg);
      }
    } catch (@Tainted InterruptedException e) {
      throw new @Tainted IOException(e);
    } finally {
      process.destroy();
    }
    return callCount;
  }

   /**
   * Retrieves the number of links to the specified file.
   */
  public static @Tainted int getLinkCount(@Untainted File fileName) throws IOException {
    if (fileName == null) {
      throw new @Tainted IOException(
          "invalid argument to getLinkCount: file name is null");
    }
    if (!fileName.exists()) {
      throw new @Tainted FileNotFoundException(fileName + " not found.");
    }

    // construct and execute shell command
    @Untainted String @Tainted [] cmd = getHardLinkCommand.linkCount(fileName);
    @Tainted String inpMsg = null;
    @Tainted String errMsg = null;
    @Tainted int exitValue = -1;
    @Tainted BufferedReader in = null;
    @Tainted BufferedReader err = null;

    @Tainted Process process = Runtime.getRuntime().exec(cmd);
    try {
      exitValue = process.waitFor();
      in = new @Tainted BufferedReader(new @Tainted InputStreamReader(
                                  process.getInputStream()));
      inpMsg = in.readLine();
      err = new @Tainted BufferedReader(new @Tainted InputStreamReader(
                                   process.getErrorStream()));
      errMsg = err.readLine();
      if (inpMsg == null || exitValue != 0) {
        throw createIOException(fileName, inpMsg, errMsg, exitValue, null);
      }
      if (Shell.SOLARIS) {
        @Tainted
        String @Tainted [] result = inpMsg.split("\\s+");
        return Integer.parseInt(result[1]);
      } else {
        return Integer.parseInt(inpMsg);
      }
    } catch (@Tainted NumberFormatException e) {
      throw createIOException(fileName, inpMsg, errMsg, exitValue, e);
    } catch (@Tainted InterruptedException e) {
      throw createIOException(fileName, inpMsg, errMsg, exitValue, e);
    } finally {
      process.destroy();
      if (in != null) in.close();
      if (err != null) err.close();
    }
  }
  
  /* Create an IOException for failing to get link count. */
  private static @Tainted IOException createIOException(@Tainted File f, @Tainted String message,
      @Tainted
      String error, @Tainted int exitvalue, @Tainted Exception cause) {

    final @Tainted String s = "Failed to get link count on file " + f
        + ": message=" + message
        + "; error=" + error
        + "; exit value=" + exitvalue;
    return (cause == null) ? new @Tainted IOException(s) : new @Tainted IOException(s, cause);
  }
  
  
  /**
   * HardLink statistics counters and methods.
   * Not multi-thread safe, obviously.
   * Init is called during HardLink instantiation, above.
   * 
   * These are intended for use by knowledgeable clients, not internally, 
   * because many of the internal methods are static and can't update these
   * per-instance counters.
   */
  public static class LinkStats {
    public @Tainted int countDirs = 0; 
    public @Tainted int countSingleLinks = 0; 
    public @Tainted int countMultLinks = 0; 
    public @Tainted int countFilesMultLinks = 0; 
    public @Tainted int countEmptyDirs = 0; 
    public @Tainted int countPhysicalFileCopies = 0;
  
    public void clear(HardLink.@Tainted LinkStats this) {
      countDirs = 0; 
      countSingleLinks = 0; 
      countMultLinks = 0; 
      countFilesMultLinks = 0; 
      countEmptyDirs = 0; 
      countPhysicalFileCopies = 0;
    }
    
    public @Tainted String report(HardLink.@Tainted LinkStats this) {
      return "HardLinkStats: " + countDirs + " Directories, including " 
      + countEmptyDirs + " Empty Directories, " 
      + countSingleLinks 
      + " single Link operations, " + countMultLinks 
      + " multi-Link operations, linking " + countFilesMultLinks 
      + " files, total " + (countSingleLinks + countFilesMultLinks) 
      + " linkable files.  Also physically copied " 
      + countPhysicalFileCopies + " other files.";
    }
  }
}

