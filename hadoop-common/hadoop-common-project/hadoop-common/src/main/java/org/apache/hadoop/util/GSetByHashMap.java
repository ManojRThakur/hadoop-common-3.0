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
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A {@link GSet} implementation by {@link HashMap}.
 */
@InterfaceAudience.Private
public class GSetByHashMap<@Tainted K extends java.lang.@Tainted Object, @Tainted E extends @Tainted K> implements @Tainted GSet<K, E> {
  private final @Tainted HashMap<@Tainted K, @Tainted E> m;

  public @Tainted GSetByHashMap(@Tainted int initialCapacity, @Tainted float loadFactor) {
    m = new @Tainted HashMap<K, E>(initialCapacity, loadFactor);
  }

  @Override
  public @Tainted int size(@Tainted GSetByHashMap<K, E> this) {
    return m.size();
  }

  @Override
  public @Tainted boolean contains(@Tainted GSetByHashMap<K, E> this, @Tainted K k) {
    return m.containsKey(k);
  }

  @Override
  public @Tainted E get(@Tainted GSetByHashMap<K, E> this, @Tainted K k) {
    return m.get(k);
  }

  @Override
  public @Tainted E put(@Tainted GSetByHashMap<K, E> this, @Tainted E element) {
    if (element == null) {
      throw new @Tainted UnsupportedOperationException("Null element is not supported.");
    }
    return m.put(element, element);
  }

  @Override
  public @Tainted E remove(@Tainted GSetByHashMap<K, E> this, @Tainted K k) {
    return m.remove(k);
  }

  @Override
  public @Tainted Iterator<@Tainted E> iterator(@Tainted GSetByHashMap<K, E> this) {
    return m.values().iterator();
  }
  
  @Override
  public void clear(@Tainted GSetByHashMap<K, E> this) {
    m.clear();
  }
}
