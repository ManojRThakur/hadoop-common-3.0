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
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;

/**
 * Create the given dir
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Mkdir extends @Tainted FsCommand {
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(Mkdir.class, "-mkdir");
  }
  
  public static final @Tainted String NAME = "mkdir";
  public static final @Tainted String USAGE = "[-p] <path> ...";
  public static final @Tainted String DESCRIPTION =
    "Create a directory in specified location.\n" +
    "  -p  Do not fail if the directory already exists";

  private @Tainted boolean createParents;
  
  @Override
  protected void processOptions(@Tainted Mkdir this, @Tainted LinkedList<@Tainted String> args) {
    @Tainted
    CommandFormat cf = new @Tainted CommandFormat(1, Integer.MAX_VALUE, "p");
    cf.parse(args);
    createParents = cf.getOpt("p");
  }

  @Override
  protected void processPath(@Tainted Mkdir this, @Tainted PathData item) throws IOException {
    if (item.stat.isDirectory()) {
      if (!createParents) {
        throw new @Tainted PathExistsException(item.toString());
      }
    } else {
      throw new @Tainted PathIsNotDirectoryException(item.toString());
    }
  }

  @Override
  protected void processNonexistentPath(@Tainted Mkdir this, @Tainted PathData item) throws IOException {
    // check if parent exists. this is complicated because getParent(a/b/c/) returns a/b/c, but
    // we want a/b
    if (!item.fs.exists(new @Tainted Path(item.path.toString()).getParent()) && !createParents) {
      throw new @Tainted PathNotFoundException(item.toString());
    }
    if (!item.fs.mkdirs(item.path)) {
      throw new @Tainted PathIOException(item.toString());
    }
  }
}
