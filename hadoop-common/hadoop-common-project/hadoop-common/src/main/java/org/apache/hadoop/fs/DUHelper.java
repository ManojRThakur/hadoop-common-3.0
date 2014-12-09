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
import java.io.File;
import org.apache.hadoop.util.Shell;

public class DUHelper {

  private @Tainted int folderCount=0;
  private @Tainted int fileCount=0;
  private @Tainted double usage = 0;
  private @Tainted long folderSize = -1;

  private @Tainted DUHelper() {

  }

  public static @Tainted long getFolderUsage(@Tainted String folder) {
    return new @Tainted DUHelper().calculateFolderSize(folder);
  }

  private @Tainted long calculateFolderSize(@Tainted DUHelper this, @Tainted String folder) {
    if (folder == null)
      throw new @Tainted IllegalArgumentException("folder");
    @Tainted
    File f = new @Tainted File(folder);
    return folderSize = getFileSize(f);
  }

  public @Tainted String check(@Tainted DUHelper this, @Tainted String folder) {
    if (folder == null)
      throw new @Tainted IllegalArgumentException("folder");
    @Tainted
    File f = new @Tainted File(folder);

    folderSize = getFileSize(f);
    usage = 1.0*(f.getTotalSpace() - f.getFreeSpace())/ f.getTotalSpace();
    return String.format("used %d files %d disk in use %f", folderSize, fileCount, usage);
  }

  public @Tainted long getFileCount(@Tainted DUHelper this) {
    return fileCount;
  }

  public @Tainted double getUsage(@Tainted DUHelper this) {
    return usage;
  }

  private @Tainted long getFileSize(@Tainted DUHelper this, @Tainted File folder) {

    folderCount++;
    //Counting the total folders
    @Tainted
    long foldersize = 0;
    if (folder.isFile())
      return folder.length();
    @Tainted
    File @Tainted [] filelist = folder.listFiles();
    if (filelist == null) {
      return 0;
    }
    for (@Tainted int i = 0; i < filelist.length; i++) {
      if (filelist[i].isDirectory()) {
        foldersize += getFileSize(filelist[i]);
      } else {
        fileCount++; //Counting the total files
        foldersize += filelist[i].length();
      }
    }
    return foldersize;    
  }

  public static void main(@Tainted String @Tainted [] args) {
    if (Shell.WINDOWS)
      System.out.println("Windows: "+ DUHelper.getFolderUsage(args[0]));
    else
      System.out.println("Other: " + DUHelper.getFolderUsage(args[0]));
  }
}