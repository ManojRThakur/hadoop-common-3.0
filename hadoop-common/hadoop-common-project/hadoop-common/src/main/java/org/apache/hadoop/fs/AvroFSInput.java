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
import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Adapts an {@link FSDataInputStream} to Avro's SeekableInput interface. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AvroFSInput implements @Tainted Closeable, @Tainted SeekableInput {
  private final @Tainted FSDataInputStream stream;
  private final @Tainted long len;

  /** Construct given an {@link FSDataInputStream} and its length. */
  public @Tainted AvroFSInput(final @Tainted FSDataInputStream in, final @Tainted long len) {
    this.stream = in;
    this.len = len;
  }

  /** Construct given a {@link FileContext} and a {@link Path}. */
  public @Tainted AvroFSInput(final @Tainted FileContext fc, final @Tainted Path p) throws IOException {
    @Tainted
    FileStatus status = fc.getFileStatus(p);
    this.len = status.getLen();
    this.stream = fc.open(p);
  }

  @Override
  public @Tainted long length(@Tainted AvroFSInput this) {
    return len;
  }

  @Override
  public @Tainted int read(@Tainted AvroFSInput this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public void seek(@Tainted AvroFSInput this, @Tainted long p) throws IOException {
    stream.seek(p);
  }

  @Override
  public @Tainted long tell(@Tainted AvroFSInput this) throws IOException {
    return stream.getPos();
  }

  @Override
  public void close(@Tainted AvroFSInput this) throws IOException {
    stream.close();
  }
}
