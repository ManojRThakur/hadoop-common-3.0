/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

class CompareUtils {
  /**
   * Prevent the instantiation of class.
   */
  private @Tainted CompareUtils() {
    // nothing
  }

  /**
   * A comparator to compare anything that implements {@link RawComparable}
   * using a customized comparator.
   */
  public static final class BytesComparator implements
      @Tainted
      Comparator<@Tainted RawComparable> {
    private @Tainted RawComparator<@Tainted Object> cmp;

    public @Tainted BytesComparator(@Tainted RawComparator<@Tainted Object> cmp) {
      this.cmp = cmp;
    }

    @Override
    public @Tainted int compare(CompareUtils.@Tainted BytesComparator this, @Tainted RawComparable o1, @Tainted RawComparable o2) {
      return compare(o1.buffer(), o1.offset(), o1.size(), o2.buffer(), o2
          .offset(), o2.size());
    }

    public @Tainted int compare(CompareUtils.@Tainted BytesComparator this, @Tainted byte @Tainted [] a, @Tainted int off1, @Tainted int len1, @Tainted byte @Tainted [] b, @Tainted int off2,
        @Tainted
        int len2) {
      return cmp.compare(a, off1, len1, b, off2, len2);
    }
  }

  /**
   * Interface for all objects that has a single integer magnitude.
   */
  static interface Scalar {
    @Tainted
    long magnitude(CompareUtils.@Tainted Scalar this);
  }

  static final class ScalarLong implements @Tainted Scalar {
    private @Tainted long magnitude;

    public @Tainted ScalarLong(@Tainted long m) {
      magnitude = m;
    }

    @Override
    public @Tainted long magnitude(CompareUtils.@Tainted ScalarLong this) {
      return magnitude;
    }
  }

  public static final class ScalarComparator implements @Tainted Comparator<@Tainted Scalar>, @Tainted Serializable {
    @Override
    public @Tainted int compare(CompareUtils.@Tainted ScalarComparator this, @Tainted Scalar o1, @Tainted Scalar o2) {
      @Tainted
      long diff = o1.magnitude() - o2.magnitude();
      if (diff < 0) return -1;
      if (diff > 0) return 1;
      return 0;
    }
  }

  public static final class MemcmpRawComparator implements
      @Tainted
      RawComparator<@Tainted Object>, @Tainted Serializable {
    @Override
    public @Tainted int compare(CompareUtils.@Tainted MemcmpRawComparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1, @Tainted byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
    }

    @Override
    public @Tainted int compare(CompareUtils.@Tainted MemcmpRawComparator this, @Tainted Object o1, @Tainted Object o2) {
      throw new @Tainted RuntimeException("Object comparison not supported");
    }
  }
}
