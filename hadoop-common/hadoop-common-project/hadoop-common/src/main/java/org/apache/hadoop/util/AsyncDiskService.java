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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/*
 * This class is a container of multiple thread pools, each for a volume,
 * so that we can schedule async disk operations easily.
 * 
 * Examples of async disk operations are deletion of files.
 * We can move the files to a "TO_BE_DELETED" folder before asychronously
 * deleting it, to make sure the caller can run it faster.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class AsyncDiskService {
  
  public static final @Tainted Log LOG = LogFactory.getLog(AsyncDiskService.class);
  
  // ThreadPool core pool size
  private static final @Tainted int CORE_THREADS_PER_VOLUME = 1;
  // ThreadPool maximum pool size
  private static final @Tainted int MAXIMUM_THREADS_PER_VOLUME = 4;
  // ThreadPool keep-alive time for threads over core pool size
  private static final @Tainted long THREADS_KEEP_ALIVE_SECONDS = 60; 
  
  private final @Tainted ThreadGroup threadGroup = new @Tainted ThreadGroup("async disk service");
  
  private @Tainted ThreadFactory threadFactory;
  
  private @Tainted HashMap<@Tainted String, @Tainted ThreadPoolExecutor> executors
      = new @Tainted HashMap<@Tainted String, @Tainted ThreadPoolExecutor>();
  
  /**
   * Create a AsyncDiskServices with a set of volumes (specified by their
   * root directories).
   * 
   * The AsyncDiskServices uses one ThreadPool per volume to do the async
   * disk operations.
   * 
   * @param volumes The roots of the file system volumes.
   */
  public @Tainted AsyncDiskService(@Tainted String @Tainted [] volumes) throws IOException {
    
    threadFactory = new @Tainted ThreadFactory() {
      @Override
      public @Tainted Thread newThread(@Tainted Runnable r) {
        return new @Tainted Thread(threadGroup, r);
      }
    };
    
    // Create one ThreadPool per volume
    for (@Tainted int v = 0 ; v < volumes.length; v++) {
      @Tainted
      ThreadPoolExecutor executor = new @Tainted ThreadPoolExecutor(
          CORE_THREADS_PER_VOLUME, MAXIMUM_THREADS_PER_VOLUME, 
          THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, 
          new @Tainted LinkedBlockingQueue<@Tainted Runnable>(), threadFactory);

      // This can reduce the number of running threads
      executor.allowCoreThreadTimeOut(true);
      executors.put(volumes[v], executor);
    }
    
  }
  
  /**
   * Execute the task sometime in the future, using ThreadPools.
   */
  public synchronized void execute(@Tainted AsyncDiskService this, @Tainted String root, @Tainted Runnable task) {
    @Tainted
    ThreadPoolExecutor executor = executors.get(root);
    if (executor == null) {
      throw new @Tainted RuntimeException("Cannot find root " + root
          + " for execution of task " + task);
    } else {
      executor.execute(task);
    }
  }
  
  /**
   * Gracefully start the shut down of all ThreadPools.
   */
  public synchronized void shutdown(@Tainted AsyncDiskService this) {
    
    LOG.info("Shutting down all AsyncDiskService threads...");
    
    for (Map.@Tainted Entry<@Tainted String, @Tainted ThreadPoolExecutor> e
        : executors.entrySet()) {
      e.getValue().shutdown();
    }
  }

  /**
   * Wait for the termination of the thread pools.
   * 
   * @param milliseconds  The number of milliseconds to wait
   * @return   true if all thread pools are terminated without time limit
   * @throws InterruptedException 
   */
  public synchronized @Tainted boolean awaitTermination(@Tainted AsyncDiskService this, @Tainted long milliseconds) 
      throws InterruptedException {

    @Tainted
    long end = Time.now() + milliseconds;
    for (Map.@Tainted Entry<@Tainted String, @Tainted ThreadPoolExecutor> e:
        executors.entrySet()) {
      @Tainted
      ThreadPoolExecutor executor = e.getValue();
      if (!executor.awaitTermination(
          Math.max(end - Time.now(), 0),
          TimeUnit.MILLISECONDS)) {
        LOG.warn("AsyncDiskService awaitTermination timeout.");
        return false;
      }
    }
    LOG.info("All AsyncDiskService threads are terminated.");
    return true;
  }
  
  /**
   * Shut down all ThreadPools immediately.
   */
  public synchronized @Tainted List<@Tainted Runnable> shutdownNow(@Tainted AsyncDiskService this) {
    
    LOG.info("Shutting down all AsyncDiskService threads immediately...");
    
    @Tainted
    List<@Tainted Runnable> list = new @Tainted ArrayList<@Tainted Runnable>();
    for (Map.@Tainted Entry<@Tainted String, @Tainted ThreadPoolExecutor> e
        : executors.entrySet()) {
      list.addAll(e.getValue().shutdownNow());
    }
    return list;
  }
  
}
