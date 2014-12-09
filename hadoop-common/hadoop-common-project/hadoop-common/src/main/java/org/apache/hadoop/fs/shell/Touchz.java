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
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;

/**
 * Unix touch like commands 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Touch extends @Tainted FsCommand {
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(Touchz.class, "-touchz");
  }

  /**
   * (Re)create zero-length file at the specified path.
   * This will be replaced by a more UNIX-like touch when files may be
   * modified.
   */
  public static class Touchz extends @Tainted Touch {
    public static final @Tainted String NAME = "touchz";
    public static final @Tainted String USAGE = "<path> ...";
    public static final @Tainted String DESCRIPTION =
      "Creates a file of zero length\n" +
      "at <path> with current time as the timestamp of that <path>.\n" +
      "An error is returned if the file exists with non-zero length\n";

    @Override
    protected void processOptions(Touch.@Tainted Touchz this, @Tainted LinkedList<@Tainted String> args) {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(1, Integer.MAX_VALUE);
      cf.parse(args);
    }

    @Override
    protected void processPath(Touch.@Tainted Touchz this, @Tainted PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        // TODO: handle this
        throw new @Tainted PathIsDirectoryException(item.toString());
      }
      if (item.stat.getLen() != 0) {
        throw new @Tainted PathIOException(item.toString(), "Not a zero-length file");
      }
      touchz(item);
    }

    @Override
    protected void processNonexistentPath(Touch.@Tainted Touchz this, @Tainted PathData item) throws IOException {
      if (!item.parentExists()) {
        throw new @Tainted PathNotFoundException(item.toString());
      }
      touchz(item);
    }

    private void touchz(Touch.@Tainted Touchz this, @Tainted PathData item) throws IOException {
      item.fs.create(item.path).close();
    }
  }
}
