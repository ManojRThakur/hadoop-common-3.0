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

package org.apache.hadoop.metrics2.impl;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.ConcurrentModificationException;

/**
 * A half-blocking (nonblocking for producers, blocking for consumers) queue
 * for metrics sinks.
 *
 * New elements are dropped when the queue is full to preserve "interesting"
 * elements at the onset of queue filling events
 */
class SinkQueue<@Tainted T extends java.lang.@Tainted Object> {

  interface Consumer<@Tainted T extends java.lang.@Tainted Object> {
    void consume(SinkQueue.@Tainted Consumer<T> this, @Tainted T object) throws InterruptedException;
  }

  // A fixed size circular buffer to minimize garbage
  private final @Tainted T @Tainted [] data;
  private @Tainted int head; // head position
  private @Tainted int tail; // tail position
  private @Tainted int size; // number of elements
  private @Tainted Thread currentConsumer = null;

  @SuppressWarnings("unchecked")
  @Tainted
  SinkQueue(@Tainted int capacity) {
    this.data = (T @Tainted []) new @Tainted Object @Tainted [Math.max(1, capacity)];
    head = tail = size = 0;
  }

  synchronized @Tainted boolean enqueue(@Tainted SinkQueue<T> this, @Tainted T e) {
    if (data.length == size) {
      return false;
    }
    ++size;
    tail = (tail + 1) % data.length;
    data[tail] = e;
    notify();
    return true;
  }

  /**
   * Consume one element, will block if queue is empty
   * Only one consumer at a time is allowed
   * @param consumer  the consumer callback object
   */
  void consume(@Tainted SinkQueue<T> this, @Tainted Consumer<@Tainted T> consumer) throws InterruptedException {
    T e = waitForData();

    try {
      consumer.consume(e);  // can take forever
      _dequeue();
    }
    finally {
      clearConsumerLock();
    }
  }

  /**
   * Consume all the elements, will block if queue is empty
   * @param consumer  the consumer callback object
   * @throws InterruptedException
   */
  void consumeAll(@Tainted SinkQueue<T> this, @Tainted Consumer<@Tainted T> consumer) throws InterruptedException {
    waitForData();

    try {
      for (@Tainted int i = size(); i-- > 0; ) {
        consumer.consume(front()); // can take forever
        _dequeue();
      }
    }
    finally {
      clearConsumerLock();
    }
  }

  /**
   * Dequeue one element from head of the queue, will block if queue is empty
   * @return  the first element
   * @throws InterruptedException
   */
  synchronized @Tainted T dequeue(@Tainted SinkQueue<T> this) throws InterruptedException {
    checkConsumer();

    while (0 == size) {
      wait();
    }
    return _dequeue();
  }

  private synchronized @Tainted T waitForData(@Tainted SinkQueue<T> this) throws InterruptedException {
    checkConsumer();

    while (0 == size) {
      wait();
    }
    setConsumerLock();
    return front();
  }

  private synchronized void checkConsumer(@Tainted SinkQueue<T> this) {
    if (currentConsumer != null) {
      throw new @Tainted ConcurrentModificationException("The "+
          currentConsumer.getName() +" thread is consuming the queue.");
    }
  }

  private synchronized void setConsumerLock(@Tainted SinkQueue<T> this) {
    currentConsumer = Thread.currentThread();
  }

  private synchronized void clearConsumerLock(@Tainted SinkQueue<T> this) {
    currentConsumer = null;
  }

  private synchronized @Tainted T _dequeue(@Tainted SinkQueue<T> this) {
    if (0 == size) {
      throw new @Tainted IllegalStateException("Size must > 0 here.");
    }
    --size;
    head = (head + 1) % data.length;
    T ret = data[head];
    data[head] = null;  // hint to gc
    return ret;
  }

  synchronized @Tainted T front(@Tainted SinkQueue<T> this) {
    return data[(head + 1) % data.length];
  }

  synchronized @Tainted T back(@Tainted SinkQueue<T> this) {
    return data[tail];
  }

  synchronized void clear(@Tainted SinkQueue<T> this) {
    checkConsumer();

    for (@Tainted int i = data.length; i-- > 0; ) {
      data[i] = null;
    }
    size = 0;
  }

  synchronized @Tainted int size(@Tainted SinkQueue<T> this) {
    return size;
  }

  @Tainted
  int capacity(@Tainted SinkQueue<T> this) {
    return data.length;
  }
}
