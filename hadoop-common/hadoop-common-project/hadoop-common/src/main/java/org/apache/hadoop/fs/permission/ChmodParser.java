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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

/**
 * Parse a permission mode passed in from a chmod command and apply that
 * mode against an existing file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings("ostrusted:cast.unsafe")
public class ChmodParser extends @Tainted PermissionParser {
  private static @Untainted Pattern chmodOctalPattern =
          (@Untainted Pattern) Pattern.compile("^\\s*[+]?([01]?)([0-7]{3})\\s*$");
  private static @Untainted Pattern chmodNormalPattern =
          (@Untainted Pattern) Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwxXt]+)([,\\s]*)\\s*");
  
  public @Tainted ChmodParser(@Tainted String modeStr) throws IllegalArgumentException {
    super(modeStr, chmodNormalPattern, chmodOctalPattern);
  }

  /**
   * Apply permission against specified file and determine what the
   * new mode would be
   * @param file File against which to apply mode
   * @return File's new mode if applied.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @Untainted short applyNewPermission(@Tainted ChmodParser this, @Tainted FileStatus file) {
    @Tainted
    FsPermission perms = file.getPermission();

    //TODO: Is this iffy?  I assume we want to make FsPermission gotten from a file trusted?
    @Untainted int existing = ((@Untainted Short) perms.toShort());
    @Tainted
    boolean exeOk = file.isDirectory() || (existing & 0111) != 0;
    
    return (@Untainted short) combineModes(existing, exeOk);
  }
}
