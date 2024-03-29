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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface for collections capable of being sorted by {@link IndexedSorter}
 * algorithms.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public interface IndexedSortable {

  /**
   * Compare items at the given addresses consistent with the semantics of
   * {@link java.util.Comparator#compare(Object, Object)}.
   */
  @Tainted
  int compare(@Tainted IndexedSortable this, @Tainted int i, @Tainted int j);

  /**
   * Swap items at the given addresses.
   */
  void swap(@Tainted IndexedSortable this, @Tainted int i, @Tainted int j);
}
