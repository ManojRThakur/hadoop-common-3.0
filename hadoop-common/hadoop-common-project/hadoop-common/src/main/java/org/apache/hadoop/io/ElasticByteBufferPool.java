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
import com.google.common.collect.ComparisonChain;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a simple ByteBufferPool which just creates ByteBuffers as needed.
 * It also caches ByteBuffers after they're released.  It will always return
 * the smallest cached buffer with at least the capacity you request.
 * We don't try to do anything clever here like try to limit the maximum cache
 * size.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class ElasticByteBufferPool implements @Tainted ByteBufferPool {
  private static final class Key implements @Tainted Comparable<@Tainted Key> {
    private final @Tainted int capacity;
    private final @Tainted long insertionTime;

    @Tainted
    Key(@Tainted int capacity, @Tainted long insertionTime) {
      this.capacity = capacity;
      this.insertionTime = insertionTime;
    }

    @Override
    public @Tainted int compareTo(ElasticByteBufferPool.@Tainted Key this, @Tainted Key other) {
      return ComparisonChain.start().
          compare(capacity, other.capacity).
          compare(insertionTime, other.insertionTime).
          result();
    }

    @Override
    public @Tainted boolean equals(ElasticByteBufferPool.@Tainted Key this, @Tainted Object rhs) {
      if (rhs == null) {
        return false;
      }
      try {
        @Tainted
        Key o = (@Tainted Key)rhs;
        return (compareTo(o) == 0);
      } catch (@Tainted ClassCastException e) {
        return false;
      }
    }

    @Override
    public @Tainted int hashCode(ElasticByteBufferPool.@Tainted Key this) {
      return new @Tainted HashCodeBuilder().
          append(capacity).
          append(insertionTime).
          toHashCode();
    }
  }

  private final @Tainted TreeMap<@Tainted Key, @Tainted ByteBuffer> buffers =
      new @Tainted TreeMap<@Tainted Key, @Tainted ByteBuffer>();

  private final @Tainted TreeMap<@Tainted Key, @Tainted ByteBuffer> directBuffers =
      new @Tainted TreeMap<@Tainted Key, @Tainted ByteBuffer>();

  private final @Tainted TreeMap<@Tainted Key, @Tainted ByteBuffer> getBufferTree(@Tainted ElasticByteBufferPool this, @Tainted boolean direct) {
    return direct ? directBuffers : buffers;
  }
  
  @Override
  public synchronized @Tainted ByteBuffer getBuffer(@Tainted ElasticByteBufferPool this, @Tainted boolean direct, @Tainted int length) {
    @Tainted
    TreeMap<@Tainted Key, @Tainted ByteBuffer> tree = getBufferTree(direct);
    Map.@Tainted Entry<@Tainted Key, @Tainted ByteBuffer> entry =
        tree.ceilingEntry(new @Tainted Key(length, 0));
    if (entry == null) {
      return direct ? ByteBuffer.allocateDirect(length) :
                      ByteBuffer.allocate(length);
    }
    tree.remove(entry.getKey());
    return entry.getValue();
  }

  @Override
  public synchronized void putBuffer(@Tainted ElasticByteBufferPool this, @Tainted ByteBuffer buffer) {
    @Tainted
    TreeMap<@Tainted Key, @Tainted ByteBuffer> tree = getBufferTree(buffer.isDirect());
    while (true) {
      @Tainted
      Key key = new @Tainted Key(buffer.capacity(), System.nanoTime());
      if (!tree.containsKey(key)) {
        tree.put(key, buffer);
        return;
      }
      // Buffers are indexed by (capacity, time).
      // If our key is not unique on the first try, we try again, since the
      // time will be different.  Since we use nanoseconds, it's pretty
      // unlikely that we'll loop even once, unless the system clock has a
      // poor granularity.
    }
  }
}
