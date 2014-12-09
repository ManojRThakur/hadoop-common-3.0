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
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A low memory footprint Cache which extends {@link LightWeightGSet}.
 * An entry in the cache is expired if
 * (1) it is added to the cache longer than the creation-expiration period, and
 * (2) it is not accessed for the access-expiration period.
 * When an entry is expired, it may be evicted from the cache.
 * When the size limit of the cache is set, the cache will evict the entries
 * with earliest expiration time, even if they are not expired.
 * 
 * It is guaranteed that number of entries in the cache is less than or equal
 * to the size limit.  However, It is not guaranteed that expired entries are
 * evicted from the cache. An expired entry may possibly be accessed after its
 * expiration time. In such case, the expiration time may be updated.
 *
 * This class does not support null entry.
 *
 * This class is not thread safe.
 *
 * @param <K> Key type for looking up the entries
 * @param <E> Entry type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link Entry} interface, and
 */
@InterfaceAudience.Private
public class LightWeightCache<@Tainted K extends java.lang.@Tainted Object, @Tainted E extends @Tainted K> extends @Tainted LightWeightGSet<K, E> {
  /** Limit the number of entries in each eviction. */
  private static final @Tainted int EVICTION_LIMIT = 1 << 16;

  /**
   * Entries of {@link LightWeightCache}.
   */
  public static interface Entry extends @Tainted LinkedElement {
    /** Set the expiration time. */
    public void setExpirationTime(LightWeightCache.@Tainted Entry this, @Tainted long timeNano);

    /** Get the expiration time. */
    public @Tainted long getExpirationTime(LightWeightCache.@Tainted Entry this);
  }

  /** Comparator for sorting entries by expiration time in ascending order. */
  private static final @Tainted Comparator<@Tainted Entry> expirationTimeComparator
      = new @Tainted Comparator<@Tainted Entry>() {
    @Override
    public @Tainted int compare(@Tainted Entry left, @Tainted Entry right) {
      final @Tainted long l = left.getExpirationTime();
      final @Tainted long r = right.getExpirationTime();
      return l > r? 1: l < r? -1: 0;
    }
  };

  /** A clock for measuring time so that it can be mocked in unit tests. */
  static class Clock {
    /** @return the current time. */
    @Tainted
    long currentTime(LightWeightCache.@Tainted Clock this) {
      return System.nanoTime();
    }
  }
  
  private static @Tainted int updateRecommendedLength(@Tainted int recommendedLength,
      @Tainted
      int sizeLimit) {
    return sizeLimit > 0 && sizeLimit < recommendedLength?
        (sizeLimit/4*3) // 0.75 load factor
        : recommendedLength;
  }

  /*
   * The memory footprint for java.util.PriorityQueue is low but the
   * remove(Object) method runs in linear time. We may improve it by using a
   * balanced tree. However, we do not yet have a low memory footprint balanced
   * tree implementation.
   */
  private final @Tainted PriorityQueue<@Tainted Entry> queue;
  private final @Tainted long creationExpirationPeriod;
  private final @Tainted long accessExpirationPeriod;
  private final @Tainted int sizeLimit;
  private final @Tainted Clock clock;

  /**
   * @param recommendedLength Recommended size of the internal array.
   * @param sizeLimit the limit of the size of the cache.
   *            The limit is disabled if it is <= 0.
   * @param creationExpirationPeriod the time period C > 0 in nanoseconds that
   *            the creation of an entry is expired if it is added to the cache
   *            longer than C.
   * @param accessExpirationPeriod the time period A >= 0 in nanoseconds that
   *            the access of an entry is expired if it is not accessed
   *            longer than A. 
   */
  public @Tainted LightWeightCache(final @Tainted int recommendedLength,
      final @Tainted int sizeLimit,
      final @Tainted long creationExpirationPeriod,
      final @Tainted long accessExpirationPeriod) {
    this(recommendedLength, sizeLimit,
        creationExpirationPeriod, accessExpirationPeriod, new @Tainted Clock());
  }

  @VisibleForTesting
  @Tainted
  LightWeightCache(final @Tainted int recommendedLength,
      final @Tainted int sizeLimit,
      final @Tainted long creationExpirationPeriod,
      final @Tainted long accessExpirationPeriod,
      final @Tainted Clock clock) {
    super(updateRecommendedLength(recommendedLength, sizeLimit));

    this.sizeLimit = sizeLimit;

    if (creationExpirationPeriod <= 0) {
      throw new @Tainted IllegalArgumentException("creationExpirationPeriod = "
          + creationExpirationPeriod + " <= 0");
    }
    this.creationExpirationPeriod = creationExpirationPeriod;

    if (accessExpirationPeriod < 0) {
      throw new @Tainted IllegalArgumentException("accessExpirationPeriod = "
          + accessExpirationPeriod + " < 0");
    }
    this.accessExpirationPeriod = accessExpirationPeriod;

    this.queue = new @Tainted PriorityQueue<@Tainted Entry>(
        sizeLimit > 0? sizeLimit + 1: 1 << 10, expirationTimeComparator);
    this.clock = clock;
  }

  void setExpirationTime(@Tainted LightWeightCache<K, E> this, final @Tainted Entry e, final @Tainted long expirationPeriod) {
    e.setExpirationTime(clock.currentTime() + expirationPeriod);
  }

  @Tainted
  boolean isExpired(@Tainted LightWeightCache<K, E> this, final @Tainted Entry e, final @Tainted long now) {
    return now > e.getExpirationTime();
  }

  private @Tainted E evict(@Tainted LightWeightCache<K, E> this) {
    @SuppressWarnings("unchecked")
    final E polled = (E)queue.poll();
    final E removed = super.remove(polled);
    Preconditions.checkState(removed == polled);
    return polled;
  }

  /** Evict expired entries. */
  private void evictExpiredEntries(@Tainted LightWeightCache<K, E> this) {
    final @Tainted long now = clock.currentTime();
    for(@Tainted int i = 0; i < EVICTION_LIMIT; i++) {
      final @Tainted Entry peeked = queue.peek();
      if (peeked == null || !isExpired(peeked, now)) {
        return;
      }

      final E evicted = evict();
      Preconditions.checkState(evicted == peeked);
    }
  }

  /** Evict entries in order to enforce the size limit of the cache. */
  private void evictEntries(@Tainted LightWeightCache<K, E> this) {
    if (sizeLimit > 0) {
      for(@Tainted int i = size(); i > sizeLimit; i--) {
        evict();
      }
    }
  }
  
  @Override
  public @Tainted E get(@Tainted LightWeightCache<K, E> this, @Tainted K key) {
    final E entry = super.get(key);
    if (entry != null) {
      if (accessExpirationPeriod > 0) {
        // update expiration time
        final @Tainted Entry existing = (@Tainted Entry)entry;
        Preconditions.checkState(queue.remove(existing));
        setExpirationTime(existing, accessExpirationPeriod);
        queue.offer(existing);
      }
    }
    return entry;
  }

  @Override
  public @Tainted E put(@Tainted LightWeightCache<K, E> this, final @Tainted E entry) {
    if (!(entry instanceof @Tainted Entry)) {
      throw new @Tainted HadoopIllegalArgumentException(
          "!(entry instanceof Entry), entry.getClass()=" + entry.getClass());
    }

    evictExpiredEntries();

    final E existing = super.put(entry);
    if (existing != null) {
      queue.remove(existing);
    }

    final @Tainted Entry e = (@Tainted Entry)entry;
    setExpirationTime(e, creationExpirationPeriod);
    queue.offer(e);
    
    evictEntries();
    return existing;
  }

  @Override
  public @Tainted E remove(@Tainted LightWeightCache<K, E> this, @Tainted K key) {
    evictExpiredEntries();

    final E removed = super.remove(key);
    if (removed != null) {
      Preconditions.checkState(queue.remove(removed));
    }
    return removed;
  }
}
