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

package org.apache.hadoop.io.serializer;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.RawComparator;

/**
 * <p>
 * A {@link RawComparator} that uses a {@link JavaSerialization}
 * {@link Deserializer} to deserialize objects that are then compared via
 * their {@link Comparable} interfaces.
 * </p>
 * @param <T>
 * @see JavaSerialization
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class JavaSerializationComparator<@Tainted T extends Serializable&Comparable<T>>
  extends @Tainted DeserializerComparator<T> {

  @InterfaceAudience.Private
  public @Tainted JavaSerializationComparator() throws IOException {
    super(new JavaSerialization.@Tainted JavaSerializationDeserializer<T>());
  }

  @Override
  @InterfaceAudience.Private
  @SuppressWarnings("ostrusted") // TODO: weirdness in typechecking required generics.
  public @Tainted int compare(@Tainted JavaSerializationComparator<T> this, @Tainted T o1, @Tainted T o2) {
    return o1.compareTo(o2);
  }

}
