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
import org.checkerframework.checker.tainting.qual.Untainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** Filesystem disk space usage statistics.  Uses the unix 'du' program*/
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DU extends @Tainted Shell {
  private @Untainted String  dirPath;

  private @Tainted AtomicLong used = new @Tainted AtomicLong();
  private volatile @Tainted boolean shouldRun = true;
  private @Tainted Thread refreshUsed;
  private @Tainted IOException duException = null;
  private @Tainted long refreshInterval;
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param interval refresh the disk usage at this interval
   * @throws IOException if we fail to refresh the disk usage
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @Tainted DU(@Untainted File path, @Tainted long interval) throws IOException {
    super(0);
    
    //we set the Shell interval to 0 so it will always run our command
    //and use this one to set the thread sleep interval
    this.refreshInterval = interval;
    this.dirPath = (@Untainted String) path.getCanonicalPath();
    
    //populate the used variable
    run();
  }
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param conf configuration object
   * @throws IOException if we fail to refresh the disk usage
   */
  public @Tainted DU(@Untainted File path, @Tainted Configuration conf) throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY,
                CommonConfigurationKeys.FS_DU_INTERVAL_DEFAULT));
  }

  /**
   * This thread refreshes the "used" variable.
   * 
   * Future improvements could be to not permanently
   * run this thread, instead run when getUsed is called.
   **/
  class DURefreshThread implements @Tainted Runnable {
    
    @Override
    public void run(@Tainted DU.DURefreshThread this) {
      
      while(shouldRun) {

        try {
          Thread.sleep(refreshInterval);
          
          try {
            //update the used variable
            DU.this.run();
          } catch (@Tainted IOException e) {
            synchronized (DU.this) {
              //save the latest exception so we can return it in getUsed()
              duException = e;
            }
            
            LOG.warn("Could not get disk usage information", e);
          }
        } catch (@Tainted InterruptedException e) {
        }
      }
    }
  }
  
  /**
   * Decrease how much disk space we use.
   * @param value decrease by this value
   */
  public void decDfsUsed(@Tainted DU this, @Tainted long value) {
    used.addAndGet(-value);
  }

  /**
   * Increase how much disk space we use.
   * @param value increase by this value
   */
  public void incDfsUsed(@Tainted DU this, @Tainted long value) {
    used.addAndGet(value);
  }
  
  /**
   * @return disk space used 
   * @throws IOException if the shell command fails
   */
  public @Tainted long getUsed(@Tainted DU this) throws IOException {
    //if the updating thread isn't started, update on demand
    if(refreshUsed == null) {
      run();
    } else {
      synchronized (DU.this) {
        //if an exception was thrown in the last run, rethrow
        if(duException != null) {
          @Tainted
          IOException tmp = duException;
          duException = null;
          throw tmp;
        }
      }
    }
    
    return Math.max(used.longValue(), 0L);
  }

  /**
   * @return the path of which we're keeping track of disk usage
   */
  public @Tainted String getDirPath(@Tainted DU this) {
    return dirPath;
  }


  /**
   * Override to hook in DUHelper class. Maybe this can be used more
   * generally as well on Unix/Linux based systems
   */
  @Override
  protected void run(@Tainted DU this) throws IOException {
    if (WINDOWS) {
      used.set(DUHelper.getFolderUsage(dirPath));
      return;
    }
    super.run();
  }
  
  /**
   * Start the disk usage checking thread.
   */
  public void start(@Tainted DU this) {
    //only start the thread if the interval is sane
    if(refreshInterval > 0) {
      refreshUsed = new @Tainted Thread(new @Tainted DURefreshThread(), 
          "refreshUsed-"+dirPath);
      refreshUsed.setDaemon(true);
      refreshUsed.start();
    }
  }
  
  /**
   * Shut down the refreshing thread.
   */
  public void shutdown(@Tainted DU this) {
    this.shouldRun = false;
    
    if(this.refreshUsed != null) {
      this.refreshUsed.interrupt();
    }
  }
  
  @Override
  public @Tainted String toString(@Tainted DU this) {
    return
      "du -sk " + dirPath +"\n" +
      used + "\t" + dirPath;
  }

  @Override
  protected @Untainted String @Tainted [] getExecString(@Tainted DU this) {
    return new @Untainted String @Tainted [] {"du", "-sk", dirPath};
  }
  
  @Override
  protected void parseExecResult(@Tainted DU this, @Tainted BufferedReader lines) throws IOException {
    @Tainted
    String line = lines.readLine();
    if (line == null) {
      throw new @Tainted IOException("Expecting a line not the end of stream");
    }
    @Tainted
    String @Tainted [] tokens = line.split("\t");
    if(tokens.length == 0) {
      throw new @Tainted IOException("Illegal du output");
    }
    this.used.set(Long.parseLong(tokens[0])*1024);
  }

  //see comments on DF.main, they apply here
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void main(@Tainted String @Tainted [] args) throws Exception {
    @Tainted
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }

    System.out.println(new @Tainted DU(new @Untainted File(path), new @Tainted Configuration()).toString());
  }
}
