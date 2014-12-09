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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;

import com.google.common.base.Preconditions;

/**
 * Snapshot related operations
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class SnapshotCommands extends @Tainted FsCommand {
  private final static @Tainted String CREATE_SNAPSHOT = "createSnapshot";
  private final static @Tainted String DELETE_SNAPSHOT = "deleteSnapshot";
  private final static @Tainted String RENAME_SNAPSHOT = "renameSnapshot";
  
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(CreateSnapshot.class, "-" + CREATE_SNAPSHOT);
    factory.addClass(DeleteSnapshot.class, "-" + DELETE_SNAPSHOT);
    factory.addClass(RenameSnapshot.class, "-" + RENAME_SNAPSHOT);
  }
  
  /**
   *  Create a snapshot
   */
  public static class CreateSnapshot extends @Tainted FsCommand {
    public static final @Tainted String NAME = CREATE_SNAPSHOT;
    public static final @Tainted String USAGE = "<snapshotDir> [<snapshotName>]";
    public static final @Tainted String DESCRIPTION = "Create a snapshot on a directory";

    private @Tainted String snapshotName = null;

    @Override
    protected void processPath(SnapshotCommands.@Tainted CreateSnapshot this, @Tainted PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new @Tainted PathIsNotDirectoryException(item.toString());
      }
    }
    
    @Override
    protected void processOptions(SnapshotCommands.@Tainted CreateSnapshot this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      if (args.size() == 0) {
        throw new @Tainted IllegalArgumentException("<snapshotDir> is missing.");
      } 
      if (args.size() > 2) {
        throw new @Tainted IllegalArgumentException("Too many arguements.");
      }
      if (args.size() == 2) {
        snapshotName = args.removeLast();
      }
    }

    @Override
    protected void processArguments(SnapshotCommands.@Tainted CreateSnapshot this, @Tainted LinkedList<@Tainted PathData> items)
    throws IOException {
      super.processArguments(items);
      if (numErrors != 0) { // check for error collecting paths
        return;
      }
      assert(items.size() == 1);
      @Tainted
      PathData sroot = items.getFirst();
      @Tainted
      Path snapshotPath = sroot.fs.createSnapshot(sroot.path, snapshotName);
      out.println("Created snapshot " + snapshotPath);
    }    
  }

  /**
   * Delete a snapshot
   */
  public static class DeleteSnapshot extends @Tainted FsCommand {
    public static final @Tainted String NAME = DELETE_SNAPSHOT;
    public static final @Tainted String USAGE = "<snapshotDir> <snapshotName>";
    public static final @Tainted String DESCRIPTION = 
        "Delete a snapshot from a directory";

    private @Tainted String snapshotName;

    @Override
    protected void processPath(SnapshotCommands.@Tainted DeleteSnapshot this, @Tainted PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new @Tainted PathIsNotDirectoryException(item.toString());
      }
    }

    @Override
    protected void processOptions(SnapshotCommands.@Tainted DeleteSnapshot this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      if (args.size() != 2) {
        throw new @Tainted IOException("args number not 2: " + args.size());
      }
      snapshotName = args.removeLast();
    }

    @Override
    protected void processArguments(SnapshotCommands.@Tainted DeleteSnapshot this, @Tainted LinkedList<@Tainted PathData> items)
        throws IOException {
      super.processArguments(items);
      if (numErrors != 0) { // check for error collecting paths
        return;
      }
      assert (items.size() == 1);
      @Tainted
      PathData sroot = items.getFirst();
      sroot.fs.deleteSnapshot(sroot.path, snapshotName);
    }
  }
  
  /**
   * Rename a snapshot
   */
  public static class RenameSnapshot extends @Tainted FsCommand {
    public static final @Tainted String NAME = RENAME_SNAPSHOT;
    public static final @Tainted String USAGE = "<snapshotDir> <oldName> <newName>";
    public static final @Tainted String DESCRIPTION = 
        "Rename a snapshot from oldName to newName";
    
    private @Tainted String oldName;
    private @Tainted String newName;
    
    @Override
    protected void processPath(SnapshotCommands.@Tainted RenameSnapshot this, @Tainted PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new @Tainted PathIsNotDirectoryException(item.toString());
      }
    }

    @Override
    protected void processOptions(SnapshotCommands.@Tainted RenameSnapshot this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      if (args.size() != 3) {
        throw new @Tainted IOException("args number not 3: " + args.size());
      }
      newName = args.removeLast();
      oldName = args.removeLast();
    }

    @Override
    protected void processArguments(SnapshotCommands.@Tainted RenameSnapshot this, @Tainted LinkedList<@Tainted PathData> items)
        throws IOException {
      super.processArguments(items);
      if (numErrors != 0) { // check for error collecting paths
        return;
      }
      Preconditions.checkArgument(items.size() == 1);
      @Tainted
      PathData sroot = items.getFirst();
      sroot.fs.renameSnapshot(sroot.path, oldName, newName);
    }
    
  }
}

