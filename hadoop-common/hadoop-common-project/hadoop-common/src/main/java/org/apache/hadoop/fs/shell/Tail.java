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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.io.IOUtils;

/**
 * Get a listing of all files in that match the file patterns.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Tail extends @Tainted FsCommand {
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(Tail.class, "-tail");
  }
  
  public static final @Tainted String NAME = "tail";
  public static final @Tainted String USAGE = "[-f] <file>";
  public static final @Tainted String DESCRIPTION =
    "Show the last 1KB of the file.\n" +
    "\t\tThe -f option shows appended data as the file grows.\n";

  private @Tainted long startingOffset = -1024;
  private @Tainted boolean follow = false;
  private @Tainted long followDelay = 5000; // milliseconds
  
  @Override
  protected void processOptions(@Tainted Tail this, @Tainted LinkedList<@Tainted String> args) throws IOException {
    @Tainted
    CommandFormat cf = new @Tainted CommandFormat(1, 1, "f");
    cf.parse(args);
    follow = cf.getOpt("f");
  }

  // TODO: HADOOP-7234 will add glob support; for now, be backwards compat
  @Override
  protected @Tainted List<@Tainted PathData> expandArgument(@Tainted Tail this, @Tainted String arg) throws IOException {
    @Tainted
    List<@Tainted PathData> items = new @Tainted LinkedList<@Tainted PathData>();
    items.add(new @Tainted PathData(arg, getConf()));
    return items;
  }
      
  @Override
  protected void processPath(@Tainted Tail this, @Tainted PathData item) throws IOException {
    if (item.stat.isDirectory()) {
      throw new @Tainted PathIsDirectoryException(item.toString());
    }

    @Tainted
    long offset = dumpFromOffset(item, startingOffset);
    while (follow) {
      try {
        Thread.sleep(followDelay);
      } catch (@Tainted InterruptedException e) {
        break;
      }
      offset = dumpFromOffset(item, offset);
    }
  }

  private @Tainted long dumpFromOffset(@Tainted Tail this, @Tainted PathData item, @Tainted long offset) throws IOException {
    @Tainted
    long fileSize = item.refreshStatus().getLen();
    if (offset > fileSize) return fileSize;
    // treat a negative offset as relative to end of the file, floor of 0
    if (offset < 0) {
      offset = Math.max(fileSize + offset, 0);
    }
    
    @Tainted
    FSDataInputStream in = item.fs.open(item.path);
    try {
      in.seek(offset);
      // use conf so the system configured io block size is used
      IOUtils.copyBytes(in, System.out, getConf(), false);
      offset = in.getPos();
    } finally {
      in.close();
    }
    return offset;
  }
}
