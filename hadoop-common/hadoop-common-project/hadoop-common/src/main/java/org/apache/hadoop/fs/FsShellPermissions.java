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
import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.ChmodParser;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.util.Shell;

/**
 * This class is the home for file permissions related commands.
 * Moved to this separate class since FsShell is getting too large.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsShellPermissions extends @Tainted FsCommand {

  static @Tainted Log LOG = FsShell.LOG;
  
  /**
   * Register the permission related commands with the factory
   * @param factory the command factory
   */
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(Chmod.class, "-chmod");
    factory.addClass(Chown.class, "-chown");
    factory.addClass(Chgrp.class, "-chgrp");
  }

  /**
   * The pattern is almost as flexible as mode allowed by chmod shell command.
   * The main restriction is that we recognize only rwxXt. To reduce errors we
   * also enforce octal mode specifications of either 3 digits without a sticky
   * bit setting or four digits with a sticky bit setting.
   */
  public static class Chmod extends @Tainted FsShellPermissions {
    public static final @Tainted String NAME = "chmod";
    public static final @Tainted String USAGE = "[-R] <MODE[,MODE]... | OCTALMODE> PATH...";
    public static final @Tainted String DESCRIPTION =
      "Changes permissions of a file.\n" +
      "\tThis works similar to shell's chmod with a few exceptions.\n\n" +
      "-R\tmodifies the files recursively. This is the only option\n" +
      "\tcurrently supported.\n\n" +
      "MODE\tMode is same as mode used for chmod shell command.\n" +
      "\tOnly letters recognized are 'rwxXt'. E.g. +t,a+r,g-w,+rwx,o=r\n\n" +
      "OCTALMODE Mode specifed in 3 or 4 digits. If 4 digits, the first may\n" +
      "be 1 or 0 to turn the sticky bit on or off, respectively.  Unlike " +
      "shell command, it is not possible to specify only part of the mode\n" +
      "\tE.g. 754 is same as u=rwx,g=rx,o=r\n\n" +
      "\tIf none of 'augo' is specified, 'a' is assumed and unlike\n" +
      "\tshell command, no umask is applied.";

    protected @Tainted ChmodParser pp;

    @Override
    protected void processOptions(FsShellPermissions.@Tainted Chmod this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(2, Integer.MAX_VALUE, "R", null);
      cf.parse(args);
      setRecursive(cf.getOpt("R"));

      @Tainted
      String modeStr = args.removeFirst();
      try {
        pp = new @Tainted ChmodParser(modeStr);
      } catch (@Tainted IllegalArgumentException iea) {
        // TODO: remove "chmod : " so it's not doubled up in output, but it's
        // here for backwards compatibility...
        throw new @Tainted IllegalArgumentException(
            "chmod : mode '" + modeStr + "' does not match the expected pattern.");      
      }
    }
    
    @Override
    protected void processPath(FsShellPermissions.@Tainted Chmod this, @Tainted PathData item) throws IOException {
      @Tainted
      short newperms = pp.applyNewPermission(item.stat);
      if (item.stat.getPermission().toShort() != newperms) {
        try {
          item.fs.setPermission(item.path, new @Tainted FsPermission(newperms));
        } catch (@Tainted IOException e) {
          LOG.debug("Error changing permissions of " + item, e);
          throw new @Tainted IOException(
              "changing permissions of '" + item + "': " + e.getMessage());
        }
      }
    }    
  }
  
  // used by chown/chgrp
  static private @Tainted String allowedChars = Shell.WINDOWS ? "[-_./@a-zA-Z0-9 ]" :
    "[-_./@a-zA-Z0-9]";

  /**
   * Used to change owner and/or group of files 
   */
  public static class Chown extends @Tainted FsShellPermissions {
    public static final @Tainted String NAME = "chown";
    public static final @Tainted String USAGE = "[-R] [OWNER][:[GROUP]] PATH...";
    public static final @Tainted String DESCRIPTION =
      "Changes owner and group of a file.\n" +
      "\tThis is similar to shell's chown with a few exceptions.\n\n" +
      "\t-R\tmodifies the files recursively. This is the only option\n" +
      "\tcurrently supported.\n\n" +
      "\tIf only owner or group is specified then only owner or\n" +
      "\tgroup is modified.\n\n" +
      "\tThe owner and group names may only consist of digits, alphabet,\n"+
      "\tand any of " + allowedChars + ". The names are case sensitive.\n\n" +
      "\tWARNING: Avoid using '.' to separate user name and group though\n" +
      "\tLinux allows it. If user names have dots in them and you are\n" +
      "\tusing local file system, you might see surprising results since\n" +
      "\tshell command 'chown' is used for local files.";

    ///allows only "allowedChars" above in names for owner and group
    static private final @Tainted Pattern chownPattern = Pattern.compile(
        "^\\s*(" + allowedChars + "+)?([:](" + allowedChars + "*))?\\s*$");

    protected @Untainted String owner = null;
    protected @Untainted String group = null;

    @Override
    protected void processOptions(FsShellPermissions.@Tainted Chown this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(2, Integer.MAX_VALUE, "R");
      cf.parse(args);
      setRecursive(cf.getOpt("R"));
      parseOwnerGroup(args.removeFirst());
    }
    
    /**
     * Parse the first argument into an owner and group
     * @param ownerStr string describing new ownership
     */
    protected void parseOwnerGroup(FsShellPermissions.@Tainted Chown this, @Tainted String ownerStr) {
      @Tainted
      Matcher matcher = chownPattern.matcher(ownerStr);
      if (!matcher.matches()) {
        throw new @Tainted IllegalArgumentException(
            "'" + ownerStr + "' does not match expected pattern for [owner][:group].");
      }
      @SuppressWarnings("ostrusted:cast.unsafe") // Matcher has safe regex
      @Untainted
      String tmpOwner = (@Untainted String) matcher.group(1);
      @SuppressWarnings("ostrusted:cast.unsafe")
      @Untainted
      String tmpGroup = (@Untainted String) matcher.group(3);
      owner = tmpOwner;
      group = tmpGroup;
      if (group != null && group.length() == 0) {
        group = null;
      }
      if (owner == null && group == null) {
        throw new @Tainted IllegalArgumentException(
            "'" + ownerStr + "' does not specify owner or group.");
      }    
    }
    
    @Override
    protected void processPath(FsShellPermissions.@Tainted Chown this, @Tainted PathData item) throws IOException {
      //Should we do case insensitive match?
      @Untainted
      String newOwner = (owner == null || owner.equals(item.stat.getOwner())) ?
                        null : owner;
      @Untainted
      String newGroup = (group == null || group.equals(item.stat.getGroup())) ?
                        null : group;

      if (newOwner != null || newGroup != null) {
        try {
          item.fs.setOwner(item.path, newOwner, newGroup);
        } catch (@Tainted IOException e) {
          LOG.debug("Error changing ownership of " + item, e);
          throw new @Tainted IOException(
              "changing ownership of '" + item + "': " + e.getMessage());
        }
      }
    }
  }

  /**
   * Used to change group of files 
   */
  public static class Chgrp extends @Tainted Chown {
    public static final @Tainted String NAME = "chgrp";
    public static final @Tainted String USAGE = "[-R] GROUP PATH...";
    public static final @Tainted String DESCRIPTION =
      "This is equivalent to -chown ... :GROUP ...";

    static private final @Tainted Pattern chgrpPattern = 
      Pattern.compile("^\\s*(" + allowedChars + "+)\\s*$");

    @Override
    protected void parseOwnerGroup(FsShellPermissions.@Tainted Chgrp this, @Tainted String groupStr) {
      @Tainted
      Matcher matcher = chgrpPattern.matcher(groupStr);
      if (!matcher.matches()) {
        throw new @Tainted IllegalArgumentException(
            "'" + groupStr + "' does not match expected pattern for group");
      }
      owner = null;
      @SuppressWarnings("ostrusted:cast.unsafe") // matcher safe
      @Untainted String tmpGroup = (@Untainted String) matcher.group(1);
      group = tmpGroup;
    }
  }
}
