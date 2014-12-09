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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper for the Unix stat(1) command. Used to workaround the lack of 
 * lstat(2) in Java 6.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Stat extends @Tainted Shell {

  private final @Tainted Path original;
  private final @Tainted Path qualified;
  private final @Untainted Path path;
  private final @Tainted long blockSize;
  private final @Tainted boolean dereference;

  private @Tainted FileStatus stat;

  public @Tainted Stat(@Untainted Path path, @Tainted long blockSize, @Tainted boolean deref, @Tainted FileSystem fs)
      throws IOException {
    super(0L, true);
    // Original path
    this.original = path;
    // Qualify the original and strip out URI fragment via toUri().getPath()
    @Tainted
    Path stripped = new @Tainted Path(
        original.makeQualified(fs.getUri(), fs.getWorkingDirectory())
        .toUri().getPath());
    // Re-qualify the bare stripped path and store it
    this.qualified = 
        stripped.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    // Strip back down to a plain path
    this.path = new @Untainted Path(qualified.toUri().getPath());
    this.blockSize = blockSize;
    this.dereference = deref;
    // LANG = C setting
    @Tainted
    Map<@Tainted String, @Tainted String> env = new @Tainted HashMap<@Tainted String, @Tainted String>();
    env.put("LANG", "C");
    setEnvironment(env);
  }

  public @Tainted FileStatus getFileStatus(@Tainted Stat this) throws IOException {
    run();
    return stat;
  }

  /**
   * Whether Stat is supported on the current platform
   * @return
   */
  public static @Tainted boolean isAvailable() {
    if (Shell.LINUX || Shell.FREEBSD || Shell.MAC) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  @Tainted
  FileStatus getFileStatusForTesting(@Tainted Stat this) {
    return stat;
  }

  @Override
  @SuppressWarnings("ostrusted") // path.ToString is trusted because path is trusted.
  protected @Untainted String @Tainted [] getExecString(@Tainted Stat this) {
    @Tainted
    String derefFlag = "-";
    if (dereference) {
      derefFlag = "-L";
    }
    if (Shell.LINUX) {
      return new @Untainted String @Tainted [] {
          "stat", derefFlag + "c", "%s,%F,%Y,%X,%a,%U,%G,%N", path.toString() };
    } else if (Shell.FREEBSD || Shell.MAC) {
      return new @Untainted String @Tainted [] {
          "stat", derefFlag + "f", "%z,%HT,%m,%a,%Op,%Su,%Sg,`link' -> `%Y'",
          path.toString() };
    } else {
      throw new @Tainted UnsupportedOperationException(
          "stat is not supported on this platform");
    }
  }

  @Override
  protected void parseExecResult(@Tainted Stat this, @Tainted BufferedReader lines) throws IOException {
    // Reset stat
    stat = null;

    @Tainted
    String line = lines.readLine();
    if (line == null) {
      throw new @Tainted IOException("Unable to stat path: " + original);
    }
    if (line.endsWith("No such file or directory") ||
        line.endsWith("Not a directory")) {
      throw new @Tainted FileNotFoundException("File " + original + " does not exist");
    }
    if (line.endsWith("Too many levels of symbolic links")) {
      throw new @Tainted IOException("Possible cyclic loop while following symbolic" +
          " link " + original);
    }
    // 6,symbolic link,6,1373584236,1373584236,lrwxrwxrwx,andrew,andrew,`link' -> `target'
    @Tainted
    StringTokenizer tokens = new @Tainted StringTokenizer(line, ",");
    try {
      @Tainted
      long length = Long.parseLong(tokens.nextToken());
      @Tainted
      boolean isDir = tokens.nextToken().equalsIgnoreCase("directory") ? true
          : false;
      // Convert from seconds to milliseconds
      @Tainted
      long modTime = Long.parseLong(tokens.nextToken())*1000;
      @Tainted
      long accessTime = Long.parseLong(tokens.nextToken())*1000;
      @Tainted
      String octalPerms = tokens.nextToken();
      // FreeBSD has extra digits beyond 4, truncate them
      if (octalPerms.length() > 4) {
        @Tainted
        int len = octalPerms.length();
        octalPerms = octalPerms.substring(len-4, len);
      }
      @Tainted
      FsPermission perms = new @Tainted FsPermission(Short.parseShort(octalPerms, 8));
      @SuppressWarnings("ostrusted:cast.unsafe")
      @Untainted
      String owner = (@Untainted String) tokens.nextToken(); // Assume that this is the result of a shell exec, which is trusted.
      @SuppressWarnings("ostrusted:cast.unsafe")
      @Untainted
      String group = (@Untainted String) tokens.nextToken();
      @Tainted
      String symStr = tokens.nextToken();
      // 'notalink'
      // 'link' -> `target'
      // '' -> ''
      @Tainted
      Path symlink = null;
      @Tainted
      StringTokenizer symTokens = new @Tainted StringTokenizer(symStr, "`");
      symTokens.nextToken();
      try {
        @Tainted
        String target = symTokens.nextToken();
        target = target.substring(0, target.length()-1);
        if (!target.isEmpty()) {
          symlink = new @Tainted Path(target);
        }
      } catch (@Tainted NoSuchElementException e) {
        // null if not a symlink
      }
      // Set stat
      stat = new @Tainted FileStatus(length, isDir, 1, blockSize, modTime, accessTime,
          perms, owner, group, symlink, qualified);
    } catch (@Tainted NumberFormatException e) {
      throw new @Tainted IOException("Unexpected stat output: " + line, e);
    } catch (@Tainted NoSuchElementException e) {
      throw new @Tainted IOException("Unexpected stat output: " + line, e);
    }
  }
}
