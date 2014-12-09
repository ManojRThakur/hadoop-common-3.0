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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import com.google.common.annotations.VisibleForTesting;

/** Filesystem disk space usage statistics.
 * Uses the unix 'df' program to get mount points, and java.io.File for
 * space utilization. Tested on Linux, FreeBSD, Windows. */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DF extends @Tainted Shell {

  /** Default DF refresh interval. */
  public static final @Tainted long DF_INTERVAL_DEFAULT = 3 * 1000;

  private final @Untainted String dirPath;
  private final @Untainted File dirFile;
  private @Tainted String filesystem;
  private @Tainted String mount;
  
  private @Tainted ArrayList<@Tainted String> output;

  public @Tainted DF(@Untainted File path, @Tainted Configuration conf) throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, DF.DF_INTERVAL_DEFAULT));
  }

  /**
   *
   * @param path
   * @param dfInterval
   * @throws IOException
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @Tainted DF(@Untainted File path, @Tainted long dfInterval) throws IOException {
    super(dfInterval);
    this.dirPath = (@Untainted String) path.getCanonicalPath();
    this.dirFile = (@Untainted File) (new File(this.dirPath));
    this.output = new @Tainted ArrayList<@Tainted String>();
  }

  /// ACCESSORS

  /** @return the canonical path to the volume we're checking. */
  public @Untainted String getDirPath(@Tainted DF this) {
    return dirPath;
  }

  /** @return a string indicating which filesystem volume we're checking. */
  public @Tainted String getFilesystem(@Tainted DF this) throws IOException {
    if (Shell.WINDOWS) {
      this.filesystem = dirFile.getCanonicalPath().substring(0, 2);
      return this.filesystem;
    } else {
      run();
      return filesystem;
    }
  }

  /** @return the capacity of the measured filesystem in bytes. */
  public @Tainted long getCapacity(@Tainted DF this) {
    return dirFile.getTotalSpace();
  }

  /** @return the total used space on the filesystem in bytes. */
  public @Tainted long getUsed(@Tainted DF this) {
    return dirFile.getTotalSpace() - dirFile.getFreeSpace();
  }

  /** @return the usable space remaining on the filesystem in bytes. */
  public @Tainted long getAvailable(@Tainted DF this) {
    return dirFile.getUsableSpace();
  }

  /** @return the amount of the volume full, as a percent. */
  public @Tainted int getPercentUsed(@Tainted DF this) {
    @Tainted
    double cap = (@Tainted double) getCapacity();
    @Tainted
    double used = (cap - (@Tainted double) getAvailable());
    return (@Tainted int) (used * 100.0 / cap);
  }

  /** @return the filesystem mount point for the indicated volume */
  public @Tainted String getMount(@Tainted DF this) throws IOException {
    // Abort early if specified path does not exist
    if (!dirFile.exists()) {
      throw new @Tainted FileNotFoundException("Specified path " + dirFile.getPath()
          + "does not exist");
    }

    if (Shell.WINDOWS) {
      // Assume a drive letter for a mount point
      this.mount = dirFile.getCanonicalPath().substring(0, 2);
    } else {
      run();
      // Skip parsing if df was not successful
      if (getExitCode() != 0) {
        @Tainted
        StringBuffer sb = new @Tainted StringBuffer("df could not be run successfully: ");
        for (@Tainted String line: output) {
          sb.append(line);
        }
        throw new @Tainted IOException(sb.toString());
      }
      parseOutput();
    }

    return mount;
  }
  
  @Override
  public @Tainted String toString(@Tainted DF this) {
    return
      "df -k " + mount +"\n" +
      filesystem + "\t" +
      getCapacity() / 1024 + "\t" +
      getUsed() / 1024 + "\t" +
      getAvailable() / 1024 + "\t" +
      getPercentUsed() + "%\t" +
      mount;
  }

  @Override
  protected @Untainted String @Tainted [] getExecString(@Tainted DF this) {
    // ignoring the error since the exit code it enough
    if (Shell.WINDOWS){
      throw new @Tainted AssertionError(
          "DF.getExecString() should never be called on Windows");
    } else {
      return new @Untainted String @Tainted [] {"bash","-c","exec 'df' '-k' '-P' '" + dirPath 
                      + "' 2>/dev/null"};
    }
  }

  @Override
  protected void parseExecResult(@Tainted DF this, @Tainted BufferedReader lines) throws IOException {
    output.clear();
    @Tainted
    String line = lines.readLine();
    while (line != null) {
      output.add(line);
      line = lines.readLine();
    }
  }
  
  @VisibleForTesting
  protected void parseOutput(@Tainted DF this) throws IOException {
    if (output.size() < 2) {
      @Tainted
      StringBuffer sb = new @Tainted StringBuffer("Fewer lines of output than expected");
      if (output.size() > 0) {
        sb.append(": " + output.get(0));
      }
      throw new @Tainted IOException(sb.toString());
    }
    
    @Tainted
    String line = output.get(1);
    @Tainted
    StringTokenizer tokens =
      new @Tainted StringTokenizer(line, " \t\n\r\f%");
    
    try {
      this.filesystem = tokens.nextToken();
    } catch (@Tainted NoSuchElementException e) {
      throw new @Tainted IOException("Unexpected empty line");
    }
    if (!tokens.hasMoreTokens()) {            // for long filesystem name
      if (output.size() > 2) {
        line = output.get(2);
      } else {
        throw new @Tainted IOException("Expecting additional output after line: "
            + line);
      }
      tokens = new @Tainted StringTokenizer(line, " \t\n\r\f%");
    }

    try {
      Long.parseLong(tokens.nextToken()); // capacity
      Long.parseLong(tokens.nextToken()); // used
      Long.parseLong(tokens.nextToken()); // available
      Integer.parseInt(tokens.nextToken()); // pct used
      this.mount = tokens.nextToken();
    } catch (@Tainted NoSuchElementException e) {
      throw new @Tainted IOException("Could not parse line: " + line);
    } catch (@Tainted NumberFormatException e) {
      throw new @Tainted IOException("Could not parse line: " + line);
    }
  }

  //TODO: Is this an actual error?  Should the users
  //ostrusted, depending on the thread model we call this either "needing sanitation" or trusted
  //because only a trusted user should be running this from the command line
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void main(@Tainted String @Tainted [] args) throws Exception {
    @Tainted
    String path = ".";
    if (args.length > 0)
      path = args[0];

    System.out.println(new @Tainted DF( (@Untainted File) (new File(path)), DF_INTERVAL_DEFAULT).toString());
  }
}
