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
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FsShell;

/**
 * Count the number of directories, files, bytes, quota, and remaining quota.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class Count extends @Tainted FsCommand {
  /**
   * Register the names for the count command
   * @param factory the command factory that will instantiate this class
   */
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(Count.class, "-count");
  }

  public static final @Tainted String NAME = "count";
  public static final @Tainted String USAGE = "[-q] <path> ...";
  public static final @Tainted String DESCRIPTION = 
      "Count the number of directories, files and bytes under the paths\n" +
      "that match the specified file pattern.  The output columns are:\n" +
      "DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME or\n" +
      "QUOTA REMAINING_QUATA SPACE_QUOTA REMAINING_SPACE_QUOTA \n" +
      "      DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME";
  
  private @Tainted boolean showQuotas;

  /** Constructor */
  public @Tainted Count() {}
  
  /** Constructor
   * @deprecated invoke via {@link FsShell}
   * @param cmd the count command
   * @param pos the starting index of the arguments 
   * @param conf configuration
   */
  @Deprecated
  public @Tainted Count(@Tainted String @Tainted [] cmd, @Tainted int pos, @Tainted Configuration conf) {
    super(conf);
    this.args = Arrays.copyOfRange(cmd, pos, cmd.length);
  }

  @Override
  protected void processOptions(@Tainted Count this, @Tainted LinkedList<@Tainted String> args) {
    @Tainted
    CommandFormat cf = new @Tainted CommandFormat(1, Integer.MAX_VALUE, "q");
    cf.parse(args);
    if (args.isEmpty()) { // default path is the current working directory
      args.add(".");
    }
    showQuotas = cf.getOpt("q");
  }

  @Override
  protected void processPath(@Tainted Count this, @Tainted PathData src) throws IOException {
    @Tainted
    ContentSummary summary = src.fs.getContentSummary(src.path);
    out.println(summary.toString(showQuotas) + src);
  }
}
