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

/** Stream that permits positional reading. */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PositionedReadable {
  /**
   * Read upto the specified number of bytes, from a given
   * position within a file, and return the number of bytes read. This does not
   * change the current offset of a file, and is thread-safe.
   */
  public @Tainted int read(@Tainted PositionedReadable this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length)
    throws IOException;
  
  /**
   * Read the specified number of bytes, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   */
  public void readFully(@Tainted PositionedReadable this, @Tainted long position, @Tainted byte @Tainted [] buffer, @Tainted int offset, @Tainted int length)
    throws IOException;
  
  /**
   * Read number of bytes equal to the length of the buffer, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   */
  public void readFully(@Tainted PositionedReadable this, @Tainted long position, @Tainted byte @Tainted [] buffer) throws IOException;
}
