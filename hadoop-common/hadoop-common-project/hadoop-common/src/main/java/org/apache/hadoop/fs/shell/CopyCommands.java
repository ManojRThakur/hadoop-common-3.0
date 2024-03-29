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
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

/** Various commands for copy files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class CopyCommands {  
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(Merge.class, "-getmerge");
    factory.addClass(Cp.class, "-cp");
    factory.addClass(CopyFromLocal.class, "-copyFromLocal");
    factory.addClass(CopyToLocal.class, "-copyToLocal");
    factory.addClass(Get.class, "-get");
    factory.addClass(Put.class, "-put");
    factory.addClass(AppendToFile.class, "-appendToFile");
  }

  /** merge multiple files together */
  public static class Merge extends @Tainted FsCommand {
    public static final @Tainted String NAME = "getmerge";    
    public static final @Tainted String USAGE = "[-nl] <src> <localdst>";
    public static final @Tainted String DESCRIPTION =
      "Get all the files in the directories that\n" +
      "match the source file pattern and merge and sort them to only\n" +
      "one file on local fs. <src> is kept.\n" +
      "  -nl   Add a newline character at the end of each file.";

    protected @Tainted PathData dst = null;
    protected @Tainted String delimiter = null;
    protected @Tainted List<@Tainted PathData> srcs = null;

    @Override
    protected void processOptions(CopyCommands.@Tainted Merge this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      try {
        @Tainted
        CommandFormat cf = new @Tainted CommandFormat(2, Integer.MAX_VALUE, "nl");
        cf.parse(args);

        delimiter = cf.getOpt("nl") ? "\n" : null;

        dst = new @Tainted PathData(new @Tainted URI(args.removeLast()), getConf());
        if (dst.exists && dst.stat.isDirectory()) {
          throw new @Tainted PathIsDirectoryException(dst.toString());
        }
        srcs = new @Tainted LinkedList<@Tainted PathData>();
      } catch (@Tainted URISyntaxException e) {
        throw new @Tainted IOException("unexpected URISyntaxException", e);
      }
    }

    @Override
    protected void processArguments(CopyCommands.@Tainted Merge this, @Tainted LinkedList<@Tainted PathData> items)
    throws IOException {
      super.processArguments(items);
      if (exitCode != 0) { // check for error collecting paths
        return;
      }
      @Tainted
      FSDataOutputStream out = dst.fs.create(dst.path);
      try {
        for (@Tainted PathData src : srcs) {
          @Tainted
          FSDataInputStream in = src.fs.open(src.path);
          try {
            IOUtils.copyBytes(in, out, getConf(), false);
            if (delimiter != null) {
              out.write(delimiter.getBytes("UTF-8"));
            }
          } finally {
            in.close();
          }
        }
      } finally {
        out.close();
      }      
    }
 
    @Override
    protected void processNonexistentPath(CopyCommands.@Tainted Merge this, @Tainted PathData item) throws IOException {
      exitCode = 1; // flag that a path is bad
      super.processNonexistentPath(item);
    }

    // this command is handled a bit differently than others.  the paths
    // are batched up instead of actually being processed.  this avoids
    // unnecessarily streaming into the merge file and then encountering
    // a path error that should abort the merge
    
    @Override
    protected void processPath(CopyCommands.@Tainted Merge this, @Tainted PathData src) throws IOException {
      // for directories, recurse one level to get its files, else skip it
      if (src.stat.isDirectory()) {
        if (getDepth() == 0) {
          recursePath(src);
        } // skip subdirs
      } else {
        srcs.add(src);
      }
    }
  }

  static class Cp extends @Tainted CommandWithDestination {
    public static final @Tainted String NAME = "cp";
    public static final @Tainted String USAGE = "[-f] [-p] <src> ... <dst>";
    public static final @Tainted String DESCRIPTION =
      "Copy files that match the file pattern <src> to a\n" +
      "destination.  When copying multiple files, the destination\n" +
      "must be a directory. Passing -p preserves access and\n" +
      "modification times, ownership and the mode. Passing -f\n" +
      "overwrites the destination if it already exists.\n";
    
    @Override
    protected void processOptions(CopyCommands.@Tainted Cp this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(2, Integer.MAX_VALUE, "f", "p");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      // should have a -r option
      setRecursive(true);
      getRemoteDestination(args);
    }
  }
  
  /** 
   * Copy local files to a remote filesystem
   */
  public static class Get extends @Tainted CommandWithDestination {
    public static final @Tainted String NAME = "get";
    public static final @Tainted String USAGE =
      "[-p] [-ignoreCrc] [-crc] <src> ... <localdst>";
    public static final @Tainted String DESCRIPTION =
      "Copy files that match the file pattern <src>\n" +
      "to the local name.  <src> is kept.  When copying multiple,\n" +
      "files, the destination must be a directory. Passing\n" +
      "-p preserves access and modification times,\n" +
      "ownership and the mode.\n";

    @Override
    protected void processOptions(CopyCommands.@Tainted Get this, @Tainted LinkedList<@Tainted String> args)
    throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(
          1, Integer.MAX_VALUE, "crc", "ignoreCrc", "p");
      cf.parse(args);
      setWriteChecksum(cf.getOpt("crc"));
      setVerifyChecksum(!cf.getOpt("ignoreCrc"));
      setPreserve(cf.getOpt("p"));
      setRecursive(true);
      getLocalDestination(args);
    }
  }

  /**
   *  Copy local files to a remote filesystem
   */
  public static class Put extends @Tainted CommandWithDestination {
    public static final @Tainted String NAME = "put";
    public static final @Tainted String USAGE = "[-f] [-p] <localsrc> ... <dst>";
    public static final @Tainted String DESCRIPTION =
      "Copy files from the local file system\n" +
      "into fs. Copying fails if the file already\n" +
      "exists, unless the -f flag is given. Passing\n" +
      "-p preserves access and modification times,\n" +
      "ownership and the mode. Passing -f overwrites\n" +
      "the destination if it already exists.\n";

    @Override
    protected void processOptions(CopyCommands.@Tainted Put this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(1, Integer.MAX_VALUE, "f", "p");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      getRemoteDestination(args);
      // should have a -r option
      setRecursive(true);
    }

    // commands operating on local paths have no need for glob expansion
    @Override
    protected @Tainted List<@Tainted PathData> expandArgument(CopyCommands.@Tainted Put this, @Tainted String arg) throws IOException {
      @Tainted
      List<@Tainted PathData> items = new @Tainted LinkedList<@Tainted PathData>();
      try {
        items.add(new @Tainted PathData(new @Tainted URI(arg), getConf()));
      } catch (@Tainted URISyntaxException e) {
        if (Path.WINDOWS) {
          // Unlike URI, PathData knows how to parse Windows drive-letter paths.
          items.add(new @Tainted PathData(arg, getConf()));
        } else {
          throw new @Tainted IOException("unexpected URISyntaxException", e);
        }
      }
      return items;
    }

    @Override
    protected void processArguments(CopyCommands.@Tainted Put this, @Tainted LinkedList<@Tainted PathData> args)
    throws IOException {
      // NOTE: this logic should be better, mimics previous implementation
      if (args.size() == 1 && args.get(0).toString().equals("-")) {
        copyStreamToTarget(System.in, getTargetPath(args.get(0)));
        return;
      }
      super.processArguments(args);
    }
  }

  public static class CopyFromLocal extends @Tainted Put {
    public static final @Tainted String NAME = "copyFromLocal";
    public static final @Tainted String USAGE = Put.USAGE;
    public static final @Tainted String DESCRIPTION = "Identical to the -put command.";
  }
 
  public static class CopyToLocal extends @Tainted Get {
    public static final @Tainted String NAME = "copyToLocal";
    public static final @Tainted String USAGE = Get.USAGE;
    public static final @Tainted String DESCRIPTION = "Identical to the -get command.";
  }

  /**
   *  Append the contents of one or more local files to a remote
   *  file.
   */
  public static class AppendToFile extends @Tainted CommandWithDestination {
    public static final @Tainted String NAME = "appendToFile";
    public static final @Tainted String USAGE = "<localsrc> ... <dst>";
    public static final @Tainted String DESCRIPTION =
        "Appends the contents of all the given local files to the\n" +
            "given dst file. The dst file will be created if it does\n" +
            "not exist. If <localSrc> is -, then the input is read\n" +
            "from stdin.";

    private static final @Tainted int DEFAULT_IO_LENGTH = 1024 * 1024;
    @Tainted
    boolean readStdin = false;

    // commands operating on local paths have no need for glob expansion
    @Override
    protected @Tainted List<@Tainted PathData> expandArgument(CopyCommands.@Tainted AppendToFile this, @Tainted String arg) throws IOException {
      @Tainted
      List<@Tainted PathData> items = new @Tainted LinkedList<@Tainted PathData>();
      if (arg.equals("-")) {
        readStdin = true;
      } else {
        try {
          items.add(new @Tainted PathData(new @Tainted URI(arg), getConf()));
        } catch (@Tainted URISyntaxException e) {
          if (Path.WINDOWS) {
            // Unlike URI, PathData knows how to parse Windows drive-letter paths.
            items.add(new @Tainted PathData(arg, getConf()));
          } else {
            throw new @Tainted IOException("Unexpected URISyntaxException: " + e.toString());
          }
        }
      }
      return items;
    }

    @Override
    protected void processOptions(CopyCommands.@Tainted AppendToFile this, @Tainted LinkedList<@Tainted String> args)
        throws IOException {

      if (args.size() < 2) {
        throw new @Tainted IOException("missing destination argument");
      }

      getRemoteDestination(args);
      super.processOptions(args);
    }

    @Override
    protected void processArguments(CopyCommands.@Tainted AppendToFile this, @Tainted LinkedList<@Tainted PathData> args)
        throws IOException {

      if (!dst.exists) {
        dst.fs.create(dst.path, false).close();
      }

      @Tainted
      InputStream is = null;
      @Tainted
      FSDataOutputStream fos = dst.fs.append(dst.path);

      try {
        if (readStdin) {
          if (args.size() == 0) {
            IOUtils.copyBytes(System.in, fos, DEFAULT_IO_LENGTH);
          } else {
            throw new @Tainted IOException(
                "stdin (-) must be the sole input argument when present");
          }
        }

        // Read in each input file and write to the target.
        for (@Tainted PathData source : args) {
          is = new @Tainted FileInputStream(source.toFile());
          IOUtils.copyBytes(is, fos, DEFAULT_IO_LENGTH);
          IOUtils.closeStream(is);
          is = null;
        }
      } finally {
        if (is != null) {
          IOUtils.closeStream(is);
        }

        if (fos != null) {
          IOUtils.closeStream(fos);
        }
      }
    }
  }
}
