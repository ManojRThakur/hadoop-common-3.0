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

package org.apache.hadoop.fs;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CanSetDropBehind {
  /**
   * Configure whether the stream should drop the cache.
   *
   * @param dropCache     Whether to drop the cache.  null means to use the
   *                      default value.
   * @throws IOException  If there was an error changing the dropBehind
   *                      setting.
   *         UnsupportedOperationException  If this stream doesn't support
   *                                        setting the drop-behind.
   */
  public void setDropBehind(@Tainted CanSetDropBehind this, @Tainted Boolean dropCache) 
      throws IOException, UnsupportedOperationException;
}
