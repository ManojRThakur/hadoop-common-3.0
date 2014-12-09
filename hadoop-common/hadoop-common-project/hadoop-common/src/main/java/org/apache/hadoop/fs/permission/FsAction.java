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
package org.apache.hadoop.fs.permission;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * File system actions, e.g. read, write, etc.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public enum FsAction {
  // POSIX style

@Tainted  NONE("---"),

@Tainted  EXECUTE("--x"),

@Tainted  WRITE("-w-"),

@Tainted  WRITE_EXECUTE("-wx"),

@Tainted  READ("r--"),

@Tainted  READ_EXECUTE("r-x"),

@Tainted  READ_WRITE("rw-"),

@Tainted  ALL("rwx");

  /** Retain reference to value array. */
  private final static @Tainted FsAction @Tainted [] vals = values();

  /** Symbolic representation */
  public final @Tainted String SYMBOL;

  private @Tainted FsAction(@Tainted String s) {
    SYMBOL = s;
  }

  /**
   * Return true if this action implies that action.
   * @param that
   */
  public @Tainted boolean implies(@Tainted FsAction this, @Tainted FsAction that) {
    if (that != null) {
      return (ordinal() & that.ordinal()) == that.ordinal();
    }
    return false;
  }

  /** AND operation. */
  public @Tainted FsAction and(@Tainted FsAction this, @Tainted FsAction that) {
    return vals[ordinal() & that.ordinal()];
  }
  /** OR operation. */
  public @Tainted FsAction or(@Tainted FsAction this, @Tainted FsAction that) {
    return vals[ordinal() | that.ordinal()];
  }
  /** NOT operation. */
  public @Tainted FsAction not(@Tainted FsAction this) {
    return vals[7 - ordinal()];
  }
}
