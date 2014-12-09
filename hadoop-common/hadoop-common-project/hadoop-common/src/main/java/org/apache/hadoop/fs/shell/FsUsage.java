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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/** Base class for commands related to viewing filesystem usage, such as
 * du and df
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class FsUsage extends @Tainted FsCommand {
  public static void registerCommands(@Tainted CommandFactory factory) {
    factory.addClass(Df.class, "-df");
    factory.addClass(Du.class, "-du");
    factory.addClass(Dus.class, "-dus");
  }

  protected @Tainted boolean humanReadable = false;
  protected @Tainted TableBuilder usagesTable;
  
  protected @Tainted String formatSize(@Tainted FsUsage this, @Tainted long size) {
    return humanReadable
        ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
        : String.valueOf(size);
  }

  /** Show the size of a partition in the filesystem */
  public static class Df extends @Tainted FsUsage {
    public static final @Tainted String NAME = "df";
    public static final @Tainted String USAGE = "[-h] [<path> ...]";
    public static final @Tainted String DESCRIPTION =
      "Shows the capacity, free and used space of the filesystem.\n"+
      "If the filesystem has multiple partitions, and no path to a\n" +
      "particular partition is specified, then the status of the root\n" +
      "partitions will be shown.\n" +
      "  -h   Formats the sizes of files in a human-readable fashion\n" +
      "       rather than a number of bytes.\n\n";
    
    @Override
    protected void processOptions(FsUsage.@Tainted Df this, @Tainted LinkedList<@Tainted String> args)
    throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(0, Integer.MAX_VALUE, "h");
      cf.parse(args);
      humanReadable = cf.getOpt("h");
      if (args.isEmpty()) args.add(Path.SEPARATOR);
    }

    @Override
    protected void processArguments(FsUsage.@Tainted Df this, @Tainted LinkedList<@Tainted PathData> args)
    throws IOException {
      usagesTable = new @Tainted TableBuilder(
          "Filesystem", "Size", "Used", "Available", "Use%");
      usagesTable.setRightAlign(1, 2, 3, 4);
      
      super.processArguments(args);
      if (!usagesTable.isEmpty()) {
        usagesTable.printToStream(out);
      }
    }

    @Override
    protected void processPath(FsUsage.@Tainted Df this, @Tainted PathData item) throws IOException {
      @Tainted
      FsStatus fsStats = item.fs.getStatus(item.path);
      @Tainted
      long size = fsStats.getCapacity();
      @Tainted
      long used = fsStats.getUsed();
      @Tainted
      long free = fsStats.getRemaining();

      usagesTable.addRow(
          item.fs.getUri(),
          formatSize(size),
          formatSize(used),
          formatSize(free),
          StringUtils.formatPercent((@Tainted double)used/(@Tainted double)size, 0)
      );
    }
  }

  /** show disk usage */
  public static class Du extends @Tainted FsUsage {
    public static final @Tainted String NAME = "du";
    public static final @Tainted String USAGE = "[-s] [-h] <path> ...";
    public static final @Tainted String DESCRIPTION =
    "Show the amount of space, in bytes, used by the files that\n" +
    "match the specified file pattern. The following flags are optional:\n" +
    "  -s   Rather than showing the size of each individual file that\n" +
    "       matches the pattern, shows the total (summary) size.\n" +
    "  -h   Formats the sizes of files in a human-readable fashion\n" +
    "       rather than a number of bytes.\n\n" +
    "Note that, even without the -s option, this only shows size summaries\n" +
    "one level deep into a directory.\n" +
    "The output is in the form \n" + 
    "\tsize\tname(full path)\n"; 

    protected @Tainted boolean summary = false;
    
    @Override
    protected void processOptions(FsUsage.@Tainted Du this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      @Tainted
      CommandFormat cf = new @Tainted CommandFormat(0, Integer.MAX_VALUE, "h", "s");
      cf.parse(args);
      humanReadable = cf.getOpt("h");
      summary = cf.getOpt("s");
      if (args.isEmpty()) args.add(Path.CUR_DIR);
    }

    @Override
    protected void processPathArgument(FsUsage.@Tainted Du this, @Tainted PathData item) throws IOException {
      usagesTable = new @Tainted TableBuilder(2);
      // go one level deep on dirs from cmdline unless in summary mode
      if (!summary && item.stat.isDirectory()) {
        recursePath(item);
      } else {
        super.processPathArgument(item);
      }
      usagesTable.printToStream(out);
    }

    @Override
    protected void processPath(FsUsage.@Tainted Du this, @Tainted PathData item) throws IOException {
      @Tainted
      long length;
      if (item.stat.isDirectory()) {
        length = item.fs.getContentSummary(item.path).getLength();
      } else {
        length = item.stat.getLen();
      }
      usagesTable.addRow(formatSize(length), item);
    }
  }

  /** show disk usage summary */
  public static class Dus extends @Tainted Du {
    public static final @Tainted String NAME = "dus";

    @Override
    protected void processOptions(FsUsage.@Tainted Dus this, @Tainted LinkedList<@Tainted String> args) throws IOException {
      args.addFirst("-s");
      super.processOptions(args);
    }
    
    @Override
    public @Tainted String getReplacementCommand(FsUsage.@Tainted Dus this) {
      return "du -s";
    }
  }

  /**
   * Creates a table of aligned values based on the maximum width of each
   * column as a string
   */
  private static class TableBuilder {
    protected @Tainted boolean hasHeader = false;
    protected @Tainted List<@Tainted String @Tainted []> rows;
    protected @Tainted int @Tainted [] widths;
    protected @Tainted boolean @Tainted [] rightAlign;
    
    /**
     * Create a table w/o headers
     * @param columns number of columns
     */
    public @Tainted TableBuilder(@Tainted int columns) {
      rows = new @Tainted ArrayList<@Tainted String @Tainted []>();
      widths = new @Tainted int @Tainted [columns];
      rightAlign = new @Tainted boolean @Tainted [columns];
    }

    /**
     * Create a table with headers
     * @param headers list of headers
     */
    public @Tainted TableBuilder(@Tainted Object @Tainted ... headers) {
      this(headers.length);
      this.addRow(headers);
      hasHeader = true;
    }

    /**
     * Change the default left-align of columns
     * @param indexes of columns to right align
     */
    public void setRightAlign(FsUsage.@Tainted TableBuilder this, @Tainted int @Tainted ... indexes) {
      for (@Tainted int i : indexes) rightAlign[i] = true;
    }
    
    /**
     * Add a row of objects to the table
     * @param objects the values
     */
    public void addRow(FsUsage.@Tainted TableBuilder this, @Tainted Object @Tainted ... objects) {
      @Tainted
      String @Tainted [] row = new @Tainted String @Tainted [widths.length];
      for (@Tainted int col=0; col < widths.length; col++) {
        row[col] = String.valueOf(objects[col]);
        widths[col] = Math.max(widths[col], row[col].length());
      }
      rows.add(row);
    }

    /**
     * Render the table to a stream 
     * @param out PrintStream for output
     */
    public void printToStream(FsUsage.@Tainted TableBuilder this, @Tainted PrintStream out) {
      if (isEmpty()) return;

      @Tainted
      StringBuilder fmt = new @Tainted StringBuilder();      
      for (@Tainted int i=0; i < widths.length; i++) {
        if (fmt.length() != 0) fmt.append("  ");
        if (rightAlign[i]) {
          fmt.append("%"+widths[i]+"s");
        } else if (i != widths.length-1) {
          fmt.append("%-"+widths[i]+"s");
        } else {
          // prevent trailing spaces if the final column is left-aligned
          fmt.append("%s");
        }
      }

      for (@Tainted Object @Tainted [] row : rows) {
        out.println(String.format(fmt.toString(), row));
      }
    }
    
    /**
     * Number of rows excluding header 
     * @return rows
     */
    public @Tainted int size(FsUsage.@Tainted TableBuilder this) {
      return rows.size() - (hasHeader ? 1 : 0);
    }

    /**
     * Does table have any rows 
     * @return boolean
     */
    public @Tainted boolean isEmpty(FsUsage.@Tainted TableBuilder this) {
      return size() == 0;
    }
  }
}