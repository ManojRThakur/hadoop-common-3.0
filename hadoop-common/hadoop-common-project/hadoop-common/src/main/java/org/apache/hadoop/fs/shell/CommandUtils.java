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
package org.apache.hadoop.fs.shell;
import org.checkerframework.checker.tainting.qual.Tainted;

final class CommandUtils {
  static @Tainted String formatDescription(@Tainted String usage, @Tainted String @Tainted ... desciptions) {
    @Tainted
    StringBuilder b = new @Tainted StringBuilder(usage + ": " + desciptions[0]);
    for(@Tainted int i = 1; i < desciptions.length; i++) {
      b.append("\n\t\t" + desciptions[i]);
    }
    return b.toString();
  }
}
