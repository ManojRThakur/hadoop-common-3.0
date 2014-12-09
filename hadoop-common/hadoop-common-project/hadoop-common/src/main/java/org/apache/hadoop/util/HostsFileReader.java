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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.*;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
// namenode/jobtracker.
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class HostsFileReader {
  private @Tainted Set<@Tainted String> includes;
  private @Tainted Set<@Tainted String> excludes;
  private @Tainted String includesFile;
  private @Tainted String excludesFile;
  
  private static final @Tainted Log LOG = LogFactory.getLog(HostsFileReader.class);

  public @Tainted HostsFileReader(@Tainted String inFile, 
                         @Tainted
                         String exFile) throws IOException {
    includes = new @Tainted HashSet<@Tainted String>();
    excludes = new @Tainted HashSet<@Tainted String>();
    includesFile = inFile;
    excludesFile = exFile;
    refresh();
  }

  public static void readFileToSet(@Tainted String type,
      @Tainted
      String filename, @Tainted Set<@Tainted String> set) throws IOException {
    @Tainted
    File file = new @Tainted File(filename);
    @Tainted
    FileInputStream fis = new @Tainted FileInputStream(file);
    @Tainted
    BufferedReader reader = null;
    try {
      reader = new @Tainted BufferedReader(new @Tainted InputStreamReader(fis));
      @Tainted
      String line;
      while ((line = reader.readLine()) != null) {
        @Tainted
        String @Tainted [] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (@Tainted int i = 0; i < nodes.length; i++) {
            if (nodes[i].trim().startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!nodes[i].isEmpty()) {
              LOG.info("Adding " + nodes[i] + " to the list of " + type +
                  " hosts from " + filename);
              set.add(nodes[i]);
            }
          }
        }
      }   
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }  
  }

  public synchronized void refresh(@Tainted HostsFileReader this) throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    if (!includesFile.isEmpty()) {
      @Tainted
      Set<@Tainted String> newIncludes = new @Tainted HashSet<@Tainted String>();
      readFileToSet("included", includesFile, newIncludes);
      // switch the new hosts that are to be included
      includes = newIncludes;
    }
    if (!excludesFile.isEmpty()) {
      @Tainted
      Set<@Tainted String> newExcludes = new @Tainted HashSet<@Tainted String>();
      readFileToSet("excluded", excludesFile, newExcludes);
      // switch the excluded hosts
      excludes = newExcludes;
    }
  }

  public synchronized @Tainted Set<@Tainted String> getHosts(@Tainted HostsFileReader this) {
    return includes;
  }

  public synchronized @Tainted Set<@Tainted String> getExcludedHosts(@Tainted HostsFileReader this) {
    return excludes;
  }

  public synchronized void setIncludesFile(@Tainted HostsFileReader this, @Tainted String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    this.includesFile = includesFile;
  }
  
  public synchronized void setExcludesFile(@Tainted HostsFileReader this, @Tainted String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    this.excludesFile = excludesFile;
  }

  public synchronized void updateFileNames(@Tainted HostsFileReader this, @Tainted String includesFile, 
                                           @Tainted
                                           String excludesFile) 
                                           throws IOException {
    setIncludesFile(includesFile);
    setExcludesFile(excludesFile);
  }
}
