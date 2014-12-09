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
package org.apache.hadoop.record.compiler;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A wrapper around StringBuffer that automatically does indentation
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CodeBuffer {
  
  static private @Tainted ArrayList<@Tainted Character> startMarkers = new @Tainted ArrayList<@Tainted Character>();
  static private @Tainted ArrayList<@Tainted Character> endMarkers = new @Tainted ArrayList<@Tainted Character>();
  
  static {
    addMarkers('{', '}');
    addMarkers('(', ')');
  }
  
  static void addMarkers(@Tainted char ch1, @Tainted char ch2) {
    startMarkers.add(ch1);
    endMarkers.add(ch2);
  }
  
  private @Tainted int level = 0;
  private @Tainted int numSpaces = 2;
  private @Tainted boolean firstChar = true;
  private @Tainted StringBuffer sb;
  
  /** Creates a new instance of CodeBuffer */
  @Tainted
  CodeBuffer() {
    this(2, "");
  }
  
  @Tainted
  CodeBuffer(@Tainted String s) {
    this(2, s);
  }
  
  @Tainted
  CodeBuffer(@Tainted int numSpaces, @Tainted String s) {
    sb = new @Tainted StringBuffer();
    this.numSpaces = numSpaces;
    this.append(s);
  }
  
  void append(@Tainted CodeBuffer this, @Tainted String s) {
    @Tainted
    int length = s.length();
    for (@Tainted int idx = 0; idx < length; idx++) {
      @Tainted
      char ch = s.charAt(idx);
      append(ch);
    }
  }
  
  void append(@Tainted CodeBuffer this, @Tainted char ch) {
    if (endMarkers.contains(ch)) {
      level--;
    }
    if (firstChar) {
      for (@Tainted int idx = 0; idx < level; idx++) {
        for (@Tainted int num = 0; num < numSpaces; num++) {
          rawAppend(' ');
        }
      }
    }
    rawAppend(ch);
    firstChar = false;
    if (startMarkers.contains(ch)) {
      level++;
    }
    if (ch == '\n') {
      firstChar = true;
    }
  }

  private void rawAppend(@Tainted CodeBuffer this, @Tainted char ch) {
    sb.append(ch);
  }
  
  @Override
  public @Tainted String toString(@Tainted CodeBuffer this) {
    return sb.toString();
  }
}
