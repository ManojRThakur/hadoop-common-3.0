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
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.shell.CopyCommands.CopyFromLocal;

/** Various commands for moving files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class MoveCommands {
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(MoveFromLocal.class, "-moveFromLocal");
    factory.addClass(MoveToLocal.class, "-moveToLocal");
    factory.addClass(Rename.class, "-mv");
  }

  /**
   *  Move local files to a remote filesystem
   */
  public static class MoveFromLocal extends @Tainted CopyFromLocal {
    public static final @Tainted String NAME = "moveFromLocal";
    public static final @Tainted String USAGE = "<localsrc> ... <dst>";
    public static final @Tainted String DESCRIPTION = 
      "Same as -put, except that the source is\n" +
      "deleted after it's copied.";

    @Override
    protected void processPath(MoveCommands.@Tainted MoveFromLocal this, @Tainted PathData src, @Tainted PathData target) throws IOException {
      // unlike copy, don't merge existing dirs during move
      if (target.exists && target.stat.isDirectory()) {
        throw new @Tainted PathExistsException(target.toString());
      }
      super.processPath(src, target);
    }
    
    @Override
    protected void postProcessPath(MoveCommands.@Tainted MoveFromLocal this, @Tainted PathData src) throws IOException {
      if (!src.fs.delete(src.path, false)) {
        // we have no way to know the actual error...
        @Tainted
        PathIOException e = new @Tainted PathIOException(src.toString());
        e.setOperation("remove");
        throw e;
      }
    }
  }

  /**
   *  Move remote files to a local filesystem
   */
  public static class MoveToLocal extends @Tainted FsCommand { 
    public static final @Tainted String NAME = "moveToLocal";
    public static final @Tainted String USAGE = "<src> <localdst>";
    public static final @Tainted String DESCRIPTION = "Not implemented yet";

    @Override
    protected void processOptions(MoveCommands.@Tainted MoveToLocal this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      throw new @Tainted IOException("Option '-moveToLocal' is not implemented yet.");
    }
  }

  /** move/rename paths on the same fileystem */
  public static class Rename extends @Tainted CommandWithDestination {
    public static final @Tainted String NAME = "mv";
    public static final @Tainted String USAGE = "<src> ... <dst>";
    public static final @Tainted String DESCRIPTION = 
      "Move files that match the specified file pattern <src>\n" +
      "to a destination <dst>.  When moving multiple files, the\n" +
      "destination must be a directory.";

    @Override
    protected void processOptions(MoveCommands.@Tainted Rename this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(2, Integer.MAX_VALUE);
      cf.parse(args);
      getRemoteDestination(args);
    }

    @Override
    protected void processPath(MoveCommands.@Tainted Rename this, @Tainted PathData src, @Tainted PathData target) throws IOException {
      if (!src.fs.getUri().equals(target.fs.getUri())) {
        throw new @Tainted PathIOException(src.toString(),
            "Does not match target filesystem");
      }
      if (!target.fs.rename(src.path, target.path)) {
        // we have no way to know the actual error...
        throw new @Tainted PathIOException(src.toString());
      }
    }
  }
}
