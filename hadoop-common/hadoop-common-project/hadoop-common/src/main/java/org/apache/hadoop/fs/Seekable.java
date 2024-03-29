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
import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 *  Stream that permits seeking.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Seekable {
  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   */
  void seek(@Tainted Seekable this, @Tainted long pos) throws IOException;
  
  /**
   * Return the current offset from the start of the file
   */
  @Tainted
  long getPos(@Tainted Seekable this) throws IOException;

  /**
   * Seeks a different copy of the data.  Returns true if 
   * found a new source, false otherwise.
   */
  @InterfaceAudience.Private
  @Tainted
  boolean seekToNewSource(@Tainted Seekable this, @Tainted long targetPos) throws IOException;
}
