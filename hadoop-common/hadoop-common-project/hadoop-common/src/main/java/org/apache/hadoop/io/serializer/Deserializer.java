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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * Provides a facility for deserializing objects of type <T> from an
 * {@link InputStream}.
 * </p>
 * 
 * <p>
 * Deserializers are stateful, but must not buffer the input since
 * other producers may read from the input between calls to
 * {@link #deserialize(Object)}.
 * </p>
 * @param <T>
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public interface Deserializer<@Tainted T extends java.lang.@Tainted Object> {
  /**
   * <p>Prepare the deserializer for reading.</p>
   */
  void open(@Tainted Deserializer<T> this, @Tainted InputStream in) throws IOException;
  
  /**
   * <p>
   * Deserialize the next object from the underlying input stream.
   * If the object <code>t</code> is non-null then this deserializer
   * <i>may</i> set its internal state to the next object read from the input
   * stream. Otherwise, if the object <code>t</code> is null a new
   * deserialized object will be created.
   * </p>
   * @return the deserialized object
   */
  @Tainted
  T deserialize(@Tainted Deserializer<T> this, @Tainted T t) throws IOException;
  
  /**
   * <p>Close the underlying input stream and clear up any resources.</p>
   */
  void close(@Tainted Deserializer<T> this) throws IOException;
}
