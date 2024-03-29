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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/** Store the summary of a content (a directory or a file). */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ContentSummary implements @Tainted Writable{
  private @Tainted long length;
  private @Tainted long fileCount;
  private @Tainted long directoryCount;
  private @Tainted long quota;
  private @Tainted long spaceConsumed;
  private @Tainted long spaceQuota;
  

  /** Constructor */
  public @Tainted ContentSummary() {}
  
  /** Constructor */
  public @Tainted ContentSummary(@Tainted long length, @Tainted long fileCount, @Tainted long directoryCount) {
    this(length, fileCount, directoryCount, -1L, length, -1L);
  }

  /** Constructor */
  public @Tainted ContentSummary(
      @Tainted
      long length, @Tainted long fileCount, @Tainted long directoryCount, @Tainted long quota,
      @Tainted
      long spaceConsumed, @Tainted long spaceQuota) {
    this.length = length;
    this.fileCount = fileCount;
    this.directoryCount = directoryCount;
    this.quota = quota;
    this.spaceConsumed = spaceConsumed;
    this.spaceQuota = spaceQuota;
  }

  /** @return the length */
  public @Tainted long getLength(@Tainted ContentSummary this) {return length;}

  /** @return the directory count */
  public @Tainted long getDirectoryCount(@Tainted ContentSummary this) {return directoryCount;}

  /** @return the file count */
  public @Tainted long getFileCount(@Tainted ContentSummary this) {return fileCount;}
  
  /** Return the directory quota */
  public @Tainted long getQuota(@Tainted ContentSummary this) {return quota;}
  
  /** Retuns (disk) space consumed */ 
  public @Tainted long getSpaceConsumed(@Tainted ContentSummary this) {return spaceConsumed;}

  /** Returns (disk) space quota */
  public @Tainted long getSpaceQuota(@Tainted ContentSummary this) {return spaceQuota;}
  
  @Override
  @InterfaceAudience.Private
  public void write(@Tainted ContentSummary this, @Tainted DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeLong(fileCount);
    out.writeLong(directoryCount);
    out.writeLong(quota);
    out.writeLong(spaceConsumed);
    out.writeLong(spaceQuota);
  }

  @Override
  @InterfaceAudience.Private
  public void readFields(@Tainted ContentSummary this, @Tainted DataInput in) throws IOException {
    this.length = in.readLong();
    this.fileCount = in.readLong();
    this.directoryCount = in.readLong();
    this.quota = in.readLong();
    this.spaceConsumed = in.readLong();
    this.spaceQuota = in.readLong();
  }
  
  /** 
   * Output format:
   * <----12----> <----12----> <-------18------->
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE FILE_NAME    
   */
  private static final @Tainted String STRING_FORMAT = "%12d %12d %18d ";
  /** 
   * Output format:
   * <----12----> <----15----> <----15----> <----15----> <----12----> <----12----> <-------18------->
   *    QUOTA   REMAINING_QUATA SPACE_QUOTA SPACE_QUOTA_REM DIR_COUNT   FILE_COUNT   CONTENT_SIZE     FILE_NAME    
   */
  private static final @Tainted String QUOTA_STRING_FORMAT = "%12s %15s ";
  private static final @Tainted String SPACE_QUOTA_STRING_FORMAT = "%15s %15s ";
  
  /** The header string */
  private static final @Tainted String HEADER = String.format(
      STRING_FORMAT.replace('d', 's'), "directories", "files", "bytes");

  private static final @Tainted String QUOTA_HEADER = String.format(
      QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT, 
      "quota", "remaining quota", "space quota", "reamaining quota") +
      HEADER;
  
  /** Return the header of the output.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   * 
   * @param qOption a flag indicating if quota needs to be printed or not
   * @return the header of the output
   */
  public static @Tainted String getHeader(@Tainted boolean qOption) {
    return qOption ? QUOTA_HEADER : HEADER;
  }
  
  @Override
  public @Tainted String toString(@Tainted ContentSummary this) {
    return toString(true);
  }

  /** Return the string representation of the object in the output format.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   * 
   * @param qOption a flag indicating if quota needs to be printed or not
   * @return the string representation of the object
   */
  public @Tainted String toString(@Tainted ContentSummary this, @Tainted boolean qOption) {
    @Tainted
    String prefix = "";
    if (qOption) {
      @Tainted
      String quotaStr = "none";
      @Tainted
      String quotaRem = "inf";
      @Tainted
      String spaceQuotaStr = "none";
      @Tainted
      String spaceQuotaRem = "inf";
      
      if (quota>0) {
        quotaStr = Long.toString(quota);
        quotaRem = Long.toString(quota-(directoryCount+fileCount));
      }
      if (spaceQuota>0) {
        spaceQuotaStr = Long.toString(spaceQuota);
        spaceQuotaRem = Long.toString(spaceQuota - spaceConsumed);        
      }
      
      prefix = String.format(QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT, 
                             quotaStr, quotaRem, spaceQuotaStr, spaceQuotaRem);
    }
    
    return prefix + String.format(STRING_FORMAT, directoryCount, 
                                  fileCount, length);
  }
}
