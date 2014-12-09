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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Sequential number generator.
 * 
 * This class is thread safe.
 */
@InterfaceAudience.Private
public abstract class SequentialNumber implements @Tainted IdGenerator {
  private final @Tainted AtomicLong currentValue;

  /** Create a new instance with the given initial value. */
  protected @Tainted SequentialNumber(final @Tainted long initialValue) {
    currentValue = new @Tainted AtomicLong(initialValue);
  }

  /** @return the current value. */
  public @Tainted long getCurrentValue(@Tainted SequentialNumber this) {
    return currentValue.get();
  }

  /** Set current value. */
  public void setCurrentValue(@Tainted SequentialNumber this, @Tainted long value) {
    currentValue.set(value);
  }

  /** Increment and then return the next value. */
  public @Tainted long nextValue(@Tainted SequentialNumber this) {
    return currentValue.incrementAndGet();
  }

  /** Skip to the new value. */
  public void skipTo(@Tainted SequentialNumber this, @Tainted long newValue) throws IllegalStateException {
    for(;;) {
      final @Tainted long c = getCurrentValue();
      if (newValue < c) {
        throw new @Tainted IllegalStateException(
            "Cannot skip to less than the current value (="
            + c + "), where newValue=" + newValue);
      }

      if (currentValue.compareAndSet(c, newValue)) {
        return;
      }
    }
  }

  @Override
  public @Tainted boolean equals(@Tainted SequentialNumber this, final @Tainted Object that) {
    if (that == null || this.getClass() != that.getClass()) {
      return false;
    }
    final @Tainted AtomicLong thatValue = ((@Tainted SequentialNumber)that).currentValue;
    return currentValue.equals(thatValue);
  }

  @Override
  public @Tainted int hashCode(@Tainted SequentialNumber this) {
    final @Tainted long v = currentValue.get();
    return (@Tainted int)v ^ (@Tainted int)(v >>> 32);
  }
}
