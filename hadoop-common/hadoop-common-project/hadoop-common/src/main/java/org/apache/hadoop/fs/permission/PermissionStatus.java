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
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Store permission related information.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class PermissionStatus implements @Tainted Writable {
  static final @Tainted WritableFactory FACTORY = new @Tainted WritableFactory() {
    @Override
    public @Tainted Writable newInstance() { return new @Tainted PermissionStatus(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(PermissionStatus.class, FACTORY);
  }

  /** Create an immutable {@link PermissionStatus} object. */
  public static @Tainted PermissionStatus createImmutable(
      @Tainted
      String user, @Tainted String group, @Tainted FsPermission permission) {
    return new @Tainted PermissionStatus(user, group, permission) {
      @Override
      public @Tainted PermissionStatus applyUMask(@Tainted FsPermission umask) {
        throw new @Tainted UnsupportedOperationException();
      }
      @Override
      public void readFields(@Tainted DataInput in) throws IOException {
        throw new @Tainted UnsupportedOperationException();
      }
    };
  }

  private @Tainted String username;
  private @Tainted String groupname;
  private @Tainted FsPermission permission;

  private @Tainted PermissionStatus() {}

  /** Constructor */
  public @Tainted PermissionStatus(@Tainted String user, @Tainted String group, @Tainted FsPermission permission) {
    username = user;
    groupname = group;
    this.permission = permission;
  }

  /** Return user name */
  public @Tainted String getUserName(@Tainted PermissionStatus this) {return username;}

  /** Return group name */
  public @Tainted String getGroupName(@Tainted PermissionStatus this) {return groupname;}

  /** Return permission */
  public @Tainted FsPermission getPermission(@Tainted PermissionStatus this) {return permission;}

  /**
   * Apply umask.
   * @see FsPermission#applyUMask(FsPermission)
   */
  public @Tainted PermissionStatus applyUMask(@Tainted PermissionStatus this, @Tainted FsPermission umask) {
    permission = permission.applyUMask(umask);
    return this;
  }

  @Override
  public void readFields(@Tainted PermissionStatus this, @Tainted DataInput in) throws IOException {
    username = Text.readString(in, Text.DEFAULT_MAX_LEN);
    groupname = Text.readString(in, Text.DEFAULT_MAX_LEN);
    permission = FsPermission.read(in);
  }

  @Override
  public void write(@Tainted PermissionStatus this, @Tainted DataOutput out) throws IOException {
    write(out, username, groupname, permission);
  }

  /**
   * Create and initialize a {@link PermissionStatus} from {@link DataInput}.
   */
  public static @Tainted PermissionStatus read(@Tainted DataInput in) throws IOException {
    @Tainted
    PermissionStatus p = new @Tainted PermissionStatus();
    p.readFields(in);
    return p;
  }

  /**
   * Serialize a {@link PermissionStatus} from its base components.
   */
  public static void write(@Tainted DataOutput out,
                           @Tainted
                           String username, 
                           @Tainted
                           String groupname,
                           @Tainted
                           FsPermission permission) throws IOException {
    Text.writeString(out, username, Text.DEFAULT_MAX_LEN);
    Text.writeString(out, groupname, Text.DEFAULT_MAX_LEN);
    permission.write(out);
  }

  @Override
  public @Tainted String toString(@Tainted PermissionStatus this) {
    return username + ":" + groupname + ":" + permission;
  }
}
