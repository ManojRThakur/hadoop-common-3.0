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
package org.apache.hadoop.io;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.FileDescriptor;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.nativeio.NativeIO;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages a pool of threads which can issue readahead requests on file descriptors.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReadaheadPool {
  static final @Tainted Log LOG = LogFactory.getLog(ReadaheadPool.class);
  private static final @Tainted int POOL_SIZE = 4;
  private static final @Tainted int MAX_POOL_SIZE = 16;
  private static final @Tainted int CAPACITY = 1024;
  private final @Tainted ThreadPoolExecutor pool;
  
  private static @Tainted ReadaheadPool instance;

  /**
   * Return the singleton instance for the current process.
   */
  public static @Tainted ReadaheadPool getInstance() {
    synchronized (ReadaheadPool.class) {
      if (instance == null && NativeIO.isAvailable()) {
        instance = new @Tainted ReadaheadPool();
      }
      return instance;
    }
  }
  
  private @Tainted ReadaheadPool() {
    pool = new @Tainted ThreadPoolExecutor(POOL_SIZE, MAX_POOL_SIZE, 3L, TimeUnit.SECONDS,
        new @Tainted ArrayBlockingQueue<@Tainted Runnable>(CAPACITY));
    pool.setRejectedExecutionHandler(new ThreadPoolExecutor.@Tainted DiscardOldestPolicy());
    pool.setThreadFactory(new @Tainted ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Readahead Thread #%d")
      .build());
  }

  /**
   * Issue a request to readahead on the given file descriptor.
   * 
   * @param identifier a textual identifier that will be used in error
   * messages (e.g. the file name)
   * @param fd the file descriptor to read ahead
   * @param curPos the current offset at which reads are being issued
   * @param readaheadLength the configured length to read ahead
   * @param maxOffsetToRead the maximum offset that will be readahead
   *        (useful if, for example, only some segment of the file is
   *        requested by the user). Pass {@link Long.MAX_VALUE} to allow
   *        readahead to the end of the file.
   * @param lastReadahead the result returned by the previous invocation
   *        of this function on this file descriptor, or null if this is
   *        the first call
   * @return an object representing this outstanding request, or null
   *        if no readahead was performed
   */
  public @Tainted ReadaheadRequest readaheadStream(
      @Tainted ReadaheadPool this, @Tainted
      String identifier,
      @Tainted
      FileDescriptor fd,
      @Tainted
      long curPos,
      @Tainted
      long readaheadLength,
      @Tainted
      long maxOffsetToRead,
      @Tainted
      ReadaheadRequest lastReadahead) {
    
    Preconditions.checkArgument(curPos <= maxOffsetToRead,
        "Readahead position %s higher than maxOffsetToRead %s",
        curPos, maxOffsetToRead);

    if (readaheadLength <= 0) {
      return null;
    }
    
    @Tainted
    long lastOffset = Long.MIN_VALUE;
    
    if (lastReadahead != null) {
      lastOffset = lastReadahead.getOffset();
    }

    // trigger each readahead when we have reached the halfway mark
    // in the previous readahead. This gives the system time
    // to satisfy the readahead before we start reading the data.
    @Tainted
    long nextOffset = lastOffset + readaheadLength / 2; 
    if (curPos >= nextOffset) {
      // cancel any currently pending readahead, to avoid
      // piling things up in the queue. Each reader should have at most
      // one outstanding request in the queue.
      if (lastReadahead != null) {
        lastReadahead.cancel();
        lastReadahead = null;
      }
      
      @Tainted
      long length = Math.min(readaheadLength,
          maxOffsetToRead - curPos);

      if (length <= 0) {
        // we've reached the end of the stream
        return null;
      }
      
      return submitReadahead(identifier, fd, curPos, length);
    } else {
      return lastReadahead;
    }
  }
      
  /**
   * Submit a request to readahead on the given file descriptor.
   * @param identifier a textual identifier used in error messages, etc.
   * @param fd the file descriptor to readahead
   * @param off the offset at which to start the readahead
   * @param len the number of bytes to read
   * @return an object representing this pending request
   */
  public @Tainted ReadaheadRequest submitReadahead(
      @Tainted ReadaheadPool this, @Tainted
      String identifier, @Tainted FileDescriptor fd, @Tainted long off, @Tainted long len) {
    @Tainted
    ReadaheadRequestImpl req = new @Tainted ReadaheadRequestImpl(
        identifier, fd, off, len);
    pool.execute(req);
    if (LOG.isTraceEnabled()) {
      LOG.trace("submit readahead: " + req);
    }
    return req;
  }
  
  /**
   * An outstanding readahead request that has been submitted to
   * the pool. This request may be pending or may have been
   * completed.
   */
  public interface ReadaheadRequest {
    /**
     * Cancels the request for readahead. This should be used
     * if the reader no longer needs the requested data, <em>before</em>
     * closing the related file descriptor.
     * 
     * It is safe to use even if the readahead request has already
     * been fulfilled.
     */
    public void cancel(ReadaheadPool.@Tainted ReadaheadRequest this);
    
    /**
     * @return the requested offset
     */
    public @Tainted long getOffset(ReadaheadPool.@Tainted ReadaheadRequest this);

    /**
     * @return the requested length
     */
    public @Tainted long getLength(ReadaheadPool.@Tainted ReadaheadRequest this);
  }
  
  private static class ReadaheadRequestImpl implements @Tainted Runnable, @Tainted ReadaheadRequest {
    private final @Tainted String identifier;
    private final @Tainted FileDescriptor fd;
    private final @Tainted long off, len;
    private volatile @Tainted boolean canceled = false;
    
    private @Tainted ReadaheadRequestImpl(@Tainted String identifier, @Tainted FileDescriptor fd, @Tainted long off, @Tainted long len) {
      this.identifier = identifier;
      this.fd = fd;
      this.off = off;
      this.len = len;
    }
    
    @Override
    public void run(ReadaheadPool.@Tainted ReadaheadRequestImpl this) {
      if (canceled) return;
      // There's a very narrow race here that the file will close right at
      // this instant. But if that happens, we'll likely receive an EBADF
      // error below, and see that it's canceled, ignoring the error.
      // It's also possible that we'll end up requesting readahead on some
      // other FD, which may be wasted work, but won't cause a problem.
      try {
        NativeIO.POSIX.posixFadviseIfPossible(identifier, fd, off, len,
            NativeIO.POSIX.POSIX_FADV_WILLNEED);
      } catch (@Tainted IOException ioe) {
        if (canceled) {
          // no big deal - the reader canceled the request and closed
          // the file.
          return;
        }
        LOG.warn("Failed readahead on " + identifier,
            ioe);
      }
    }

    @Override
    public void cancel(ReadaheadPool.@Tainted ReadaheadRequestImpl this) {
      canceled = true;
      // We could attempt to remove it from the work queue, but that would
      // add complexity. In practice, the work queues remain very short,
      // so removing canceled requests has no gain.
    }

    @Override
    public @Tainted long getOffset(ReadaheadPool.@Tainted ReadaheadRequestImpl this) {
      return off;
    }

    @Override
    public @Tainted long getLength(ReadaheadPool.@Tainted ReadaheadRequestImpl this) {
      return len;
    }

    @Override
    public @Tainted String toString(ReadaheadPool.@Tainted ReadaheadRequestImpl this) {
      return "ReadaheadRequestImpl [identifier='" + identifier + "', fd=" + fd
          + ", off=" + off + ", len=" + len + "]";
    }
  }
}
