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
package org.apache.hadoop.ipc;


import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.Arrays;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.LightWeightCache;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Maintains a cache of non-idempotent requests that have been successfully
 * processed by the RPC server implementation, to handle the retries. A request
 * is uniquely identified by the unique client ID + call ID of the RPC request.
 * On receiving retried request, an entry will be found in the
 * {@link RetryCache} and the previous response is sent back to the request.
 * <p>
 * To look an implementation using this cache, see HDFS FSNamesystem class.
 */
@InterfaceAudience.Private
public class RetryCache {
  public static final @Tainted Log LOG = LogFactory.getLog(RetryCache.class);
  /**
   * CacheEntry is tracked using unique client ID and callId of the RPC request
   */
  public static class CacheEntry implements LightWeightCache.@Tainted Entry {
    /**
     * Processing state of the requests
     */
    private static @Tainted byte INPROGRESS = 0;
    private static @Tainted byte SUCCESS = 1;
    private static @Tainted byte FAILED = 2;

    private @Tainted byte state = INPROGRESS;
    
    // Store uuid as two long for better memory utilization
    private final @Tainted long clientIdMsb; // Most signficant bytes
    private final @Tainted long clientIdLsb; // Least significant bytes
    
    private final @Tainted int callId;
    private final @Tainted long expirationTime;
    private LightWeightGSet.@Tainted LinkedElement next;

    @Tainted
    CacheEntry(@Tainted byte @Tainted [] clientId, @Tainted int callId, @Tainted long expirationTime) {
      // ClientId must be a UUID - that is 16 octets.
      Preconditions.checkArgument(clientId.length == ClientId.BYTE_LENGTH,
          "Invalid clientId - length is " + clientId.length
              + " expected length " + ClientId.BYTE_LENGTH);
      // Convert UUID bytes to two longs
      clientIdMsb = ClientId.getMsb(clientId);
      clientIdLsb = ClientId.getLsb(clientId);
      this.callId = callId;
      this.expirationTime = expirationTime;
    }

    @Tainted
    CacheEntry(@Tainted byte @Tainted [] clientId, @Tainted int callId, @Tainted long expirationTime,
        @Tainted
        boolean success) {
      this(clientId, callId, expirationTime);
      this.state = success ? SUCCESS : FAILED;
    }

    private static @Tainted int hashCode(@Tainted long value) {
      return (@Tainted int)(value ^ (value >>> 32));
    }
    
    @Override
    public @Tainted int hashCode(RetryCache.@Tainted CacheEntry this) {
      return (hashCode(clientIdMsb) * 31 + hashCode(clientIdLsb)) * 31 + callId;
    }

    @Override
    public @Tainted boolean equals(RetryCache.@Tainted CacheEntry this, @Tainted Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof @Tainted CacheEntry)) {
        return false;
      }
      @Tainted
      CacheEntry other = (@Tainted CacheEntry) obj;
      return callId == other.callId && clientIdMsb == other.clientIdMsb
          && clientIdLsb == other.clientIdLsb;
    }

    @Override
    public void setNext(RetryCache.@Tainted CacheEntry this, @Tainted LinkedElement next) {
      this.next = next;
    }

    @Override
    public @Tainted LinkedElement getNext(RetryCache.@Tainted CacheEntry this) {
      return next;
    }

    synchronized void completed(RetryCache.@Tainted CacheEntry this, @Tainted boolean success) {
      state = success ? SUCCESS : FAILED;
      this.notifyAll();
    }

    public synchronized @Tainted boolean isSuccess(RetryCache.@Tainted CacheEntry this) {
      return state == SUCCESS;
    }

    @Override
    public void setExpirationTime(RetryCache.@Tainted CacheEntry this, @Tainted long timeNano) {
      // expiration time does not change
    }

    @Override
    public @Tainted long getExpirationTime(RetryCache.@Tainted CacheEntry this) {
      return expirationTime;
    }
    
    @Override
    public @Tainted String toString(RetryCache.@Tainted CacheEntry this) {
      return (new @Tainted UUID(this.clientIdMsb, this.clientIdLsb)).toString() + ":"
          + this.callId + ":" + this.state;
    }
  }

  /**
   * CacheEntry with payload that tracks the previous response or parts of
   * previous response to be used for generating response for retried requests.
   */
  public static class CacheEntryWithPayload extends @Tainted CacheEntry {
    private @Tainted Object payload;

    @Tainted
    CacheEntryWithPayload(@Tainted byte @Tainted [] clientId, @Tainted int callId, @Tainted Object payload,
        @Tainted
        long expirationTime) {
      super(clientId, callId, expirationTime);
      this.payload = payload;
    }

    @Tainted
    CacheEntryWithPayload(@Tainted byte @Tainted [] clientId, @Tainted int callId, @Tainted Object payload,
        @Tainted
        long expirationTime, @Tainted boolean success) {
     super(clientId, callId, expirationTime, success);
     this.payload = payload;
   }

    /** Override equals to avoid findbugs warnings */
    @Override
    public @Tainted boolean equals(RetryCache.@Tainted CacheEntryWithPayload this, @Tainted Object obj) {
      return super.equals(obj);
    }

    /** Override hashcode to avoid findbugs warnings */
    @Override
    public @Tainted int hashCode(RetryCache.@Tainted CacheEntryWithPayload this) {
      return super.hashCode();
    }

    public @Tainted Object getPayload(RetryCache.@Tainted CacheEntryWithPayload this) {
      return payload;
    }
  }

  private final @Tainted LightWeightGSet<@Tainted CacheEntry, @Tainted CacheEntry> set;
  private final @Tainted long expirationTime;

  /**
   * Constructor
   * @param cacheName name to identify the cache by
   * @param percentage percentage of total java heap space used by this cache
   * @param expirationTime time for an entry to expire in nanoseconds
   */
  public @Tainted RetryCache(@Tainted String cacheName, @Tainted double percentage, @Tainted long expirationTime) {
    @Tainted
    int capacity = LightWeightGSet.computeCapacity(percentage, cacheName);
    capacity = capacity > 16 ? capacity : 16;
    this.set = new @Tainted LightWeightCache<@Tainted CacheEntry, @Tainted CacheEntry>(capacity, capacity,
        expirationTime, 0);
    this.expirationTime = expirationTime;
  }

  private static @Tainted boolean skipRetryCache() {
    // Do not track non RPC invocation or RPC requests with
    // invalid callId or clientId in retry cache
    return !Server.isRpcInvocation() || Server.getCallId() < 0
        || Arrays.equals(Server.getClientId(), RpcConstants.DUMMY_CLIENT_ID);
  }
  
  @VisibleForTesting
  public @Tainted LightWeightGSet<@Tainted CacheEntry, @Tainted CacheEntry> getCacheSet(@Tainted RetryCache this) {
    return set;
  }

  /**
   * This method handles the following conditions:
   * <ul>
   * <li>If retry is not to be processed, return null</li>
   * <li>If there is no cache entry, add a new entry {@code newEntry} and return
   * it.</li>
   * <li>If there is an existing entry, wait for its completion. If the
   * completion state is {@link CacheEntry#FAILED}, the expectation is that the
   * thread that waited for completion, retries the request. the
   * {@link CacheEntry} state is set to {@link CacheEntry#INPROGRESS} again.
   * <li>If the completion state is {@link CacheEntry#SUCCESS}, the entry is
   * returned so that the thread that waits for it can can return previous
   * response.</li>
   * <ul>
   * 
   * @return {@link CacheEntry}.
   */
  private @Tainted CacheEntry waitForCompletion(@Tainted RetryCache this, @Tainted CacheEntry newEntry) {
    @Tainted
    CacheEntry mapEntry = null;
    synchronized (this) {
      mapEntry = set.get(newEntry);
      // If an entry in the cache does not exist, add a new one
      if (mapEntry == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Adding Rpc request clientId "
              + newEntry.clientIdMsb + newEntry.clientIdLsb + " callId "
              + newEntry.callId + " to retryCache");
        }
        set.put(newEntry);
        return newEntry;
      }
    }
    // Entry already exists in cache. Wait for completion and return its state
    Preconditions.checkNotNull(mapEntry,
        "Entry from the cache should not be null");
    // Wait for in progress request to complete
    synchronized (mapEntry) {
      while (mapEntry.state == CacheEntry.INPROGRESS) {
        try {
          mapEntry.wait();
        } catch (@Tainted InterruptedException ie) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
      // Previous request has failed, the expectation is is that it will be
      // retried again.
      if (mapEntry.state != CacheEntry.SUCCESS) {
        mapEntry.state = CacheEntry.INPROGRESS;
      }
    }
    return mapEntry;
  }
  
  /** 
   * Add a new cache entry into the retry cache. The cache entry consists of 
   * clientId and callId extracted from editlog.
   */
  public void addCacheEntry(@Tainted RetryCache this, @Tainted byte @Tainted [] clientId, @Tainted int callId) {
    @Tainted
    CacheEntry newEntry = new @Tainted CacheEntry(clientId, callId, System.nanoTime()
        + expirationTime, true);
    synchronized(this) {
      set.put(newEntry);
    }
  }
  
  public void addCacheEntryWithPayload(@Tainted RetryCache this, @Tainted byte @Tainted [] clientId, @Tainted int callId,
      @Tainted
      Object payload) {
    // since the entry is loaded from editlog, we can assume it succeeded.    
    @Tainted
    CacheEntry newEntry = new @Tainted CacheEntryWithPayload(clientId, callId, payload,
        System.nanoTime() + expirationTime, true);
    synchronized(this) {
      set.put(newEntry);
    }
  }

  private static @Tainted CacheEntry newEntry(@Tainted long expirationTime) {
    return new @Tainted CacheEntry(Server.getClientId(), Server.getCallId(),
        System.nanoTime() + expirationTime);
  }

  private static @Tainted CacheEntryWithPayload newEntry(@Tainted Object payload,
      @Tainted
      long expirationTime) {
    return new @Tainted CacheEntryWithPayload(Server.getClientId(), Server.getCallId(),
        payload, System.nanoTime() + expirationTime);
  }

  /** Static method that provides null check for retryCache */
  public static @Tainted CacheEntry waitForCompletion(@Tainted RetryCache cache) {
    if (skipRetryCache()) {
      return null;
    }
    return cache != null ? cache
        .waitForCompletion(newEntry(cache.expirationTime)) : null;
  }

  /** Static method that provides null check for retryCache */
  public static @Tainted CacheEntryWithPayload waitForCompletion(@Tainted RetryCache cache,
      @Tainted
      Object payload) {
    if (skipRetryCache()) {
      return null;
    }
    return (@Tainted CacheEntryWithPayload) (cache != null ? cache
        .waitForCompletion(newEntry(payload, cache.expirationTime)) : null);
  }

  public static void setState(@Tainted CacheEntry e, @Tainted boolean success) {
    if (e == null) {
      return;
    }
    e.completed(success);
  }

  public static void setState(@Tainted CacheEntryWithPayload e, @Tainted boolean success,
      @Tainted
      Object payload) {
    if (e == null) {
      return;
    }
    e.payload = payload;
    e.completed(success);
  }

  public static void clear(@Tainted RetryCache cache) {
    if (cache != null) {
      cache.set.clear();
    }
  }
}
