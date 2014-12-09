/*
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;

/** An implementation of the core algorithm of MergeSort. */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MergeSort {
  //Reusable IntWritables
  @Tainted
  IntWritable I = new @Tainted IntWritable(0);
  @Tainted
  IntWritable J = new @Tainted IntWritable(0);
  
  //the comparator that the algo should use
  private @Tainted Comparator<@Tainted IntWritable> comparator;
  
  public @Tainted MergeSort(@Tainted Comparator<@Tainted IntWritable> comparator) {
    this.comparator = comparator;
  }
  
  public void mergeSort(@Tainted MergeSort this, @Tainted int src @Tainted [], @Tainted int dest @Tainted [], @Tainted int low, @Tainted int high) {
    @Tainted
    int length = high - low;

    // Insertion sort on smallest arrays
    if (length < 7) {
      for (@Tainted int i=low; i<high; i++) {
        for (@Tainted int j=i;j > low; j--) {
          I.set(dest[j-1]);
          J.set(dest[j]);
          if (comparator.compare(I, J)>0)
            swap(dest, j, j-1);
        }
      }
      return;
    }

    // Recursively sort halves of dest into src
    @Tainted
    int mid = (low + high) >>> 1;
    mergeSort(dest, src, low, mid);
    mergeSort(dest, src, mid, high);

    I.set(src[mid-1]);
    J.set(src[mid]);
    // If list is already sorted, just copy from src to dest.  This is an
    // optimization that results in faster sorts for nearly ordered lists.
    if (comparator.compare(I, J) <= 0) {
      System.arraycopy(src, low, dest, low, length);
      return;
    }

    // Merge sorted halves (now in src) into dest
    for (@Tainted int i = low, p = low, q = mid; i < high; i++) {
      if (q < high && p < mid) {
        I.set(src[p]);
        J.set(src[q]);
      }
      if (q>=high || p<mid && comparator.compare(I, J) <= 0)
        dest[i] = src[p++];
      else
        dest[i] = src[q++];
    }
  }

  private void swap(@Tainted MergeSort this, @Tainted int x @Tainted [], @Tainted int a, @Tainted int b) {
    @Tainted
    int t = x[a];
    x[a] = x[b];
    x[b] = t;
  }
}
