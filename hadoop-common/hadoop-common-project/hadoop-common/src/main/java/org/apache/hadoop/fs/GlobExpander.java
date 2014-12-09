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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class GlobExpander {
  
  static class StringWithOffset {
    @Tainted
    String string;
    @Tainted
    int offset;
    public @Tainted StringWithOffset(@Tainted String string, @Tainted int offset) {
      super();
      this.string = string;
      this.offset = offset;
    }
  }
  
  /**
   * Expand globs in the given <code>filePattern</code> into a collection of
   * file patterns so that in the expanded set no file pattern has a slash
   * character ("/") in a curly bracket pair.
   * <p>
   * Some examples of how the filePattern is expanded:<br>
   * <pre>
   * <b>
   * filePattern         - Expanded file pattern </b>
   * {a/b}               - a/b
   * /}{a/b}             - /}a/b
   * p{a/b,c/d}s         - pa/bs, pc/ds
   * {a/b,c/d,{e,f}}     - a/b, c/d, {e,f}
   * {a/b,c/d}{e,f}      - a/b{e,f}, c/d{e,f}
   * {a,b}/{b,{c/d,e/f}} - {a,b}/b, {a,b}/c/d, {a,b}/e/f
   * {a,b}/{c/\d}        - {a,b}/c/d
   * </pre>
   * 
   * @param filePattern
   * @return expanded file patterns
   * @throws IOException
   */
  public static @Tainted List<@Tainted String> expand(@Tainted String filePattern) throws IOException {
    @Tainted
    List<@Tainted String> fullyExpanded = new @Tainted ArrayList<@Tainted String>();
    @Tainted
    List<@Tainted StringWithOffset> toExpand = new @Tainted ArrayList<@Tainted StringWithOffset>();
    toExpand.add(new @Tainted StringWithOffset(filePattern, 0));
    while (!toExpand.isEmpty()) {
      @Tainted
      StringWithOffset path = toExpand.remove(0);
      @Tainted
      List<@Tainted StringWithOffset> expanded = expandLeftmost(path);
      if (expanded == null) {
        fullyExpanded.add(path.string);
      } else {
        toExpand.addAll(0, expanded);
      }
    }
    return fullyExpanded;
  }
  
  /**
   * Expand the leftmost outer curly bracket pair containing a
   * slash character ("/") in <code>filePattern</code>.
   * @param filePatternWithOffset
   * @return expanded file patterns
   * @throws IOException 
   */
  private static @Tainted List<@Tainted StringWithOffset> expandLeftmost(@Tainted StringWithOffset
      filePatternWithOffset) throws IOException {
    
    @Tainted
    String filePattern = filePatternWithOffset.string;
    @Tainted
    int leftmost = leftmostOuterCurlyContainingSlash(filePattern,
        filePatternWithOffset.offset);
    if (leftmost == -1) {
      return null;
    }
    @Tainted
    int curlyOpen = 0;
    @Tainted
    StringBuilder prefix = new @Tainted StringBuilder(filePattern.substring(0, leftmost));
    @Tainted
    StringBuilder suffix = new @Tainted StringBuilder();
    @Tainted
    List<@Tainted String> alts = new @Tainted ArrayList<@Tainted String>();
    @Tainted
    StringBuilder alt = new @Tainted StringBuilder();
    @Tainted
    StringBuilder cur = prefix;
    for (@Tainted int i = leftmost; i < filePattern.length(); i++) {
      @Tainted
      char c = filePattern.charAt(i);
      if (cur == suffix) {
        cur.append(c);
      } else if (c == '\\') {
        i++;
        if (i >= filePattern.length()) {
          throw new @Tainted IOException("Illegal file pattern: "
              + "An escaped character does not present for glob "
              + filePattern + " at " + i);
        }
        c = filePattern.charAt(i);
        cur.append(c);
      } else if (c == '{') {
        if (curlyOpen++ == 0) {
          alt.setLength(0);
          cur = alt;
        } else {
          cur.append(c);
        }

      } else if (c == '}' && curlyOpen > 0) {
        if (--curlyOpen == 0) {
          alts.add(alt.toString());
          alt.setLength(0);
          cur = suffix;
        } else {
          cur.append(c);
        }
      } else if (c == ',') {
        if (curlyOpen == 1) {
          alts.add(alt.toString());
          alt.setLength(0);
        } else {
          cur.append(c);
        }
      } else {
        cur.append(c);
      }
    }
    @Tainted
    List<@Tainted StringWithOffset> exp = new @Tainted ArrayList<@Tainted StringWithOffset>();
    for (@Tainted String string : alts) {
      exp.add(new @Tainted StringWithOffset(prefix + string + suffix, prefix.length()));
    }
    return exp;
  }
  
  /**
   * Finds the index of the leftmost opening curly bracket containing a
   * slash character ("/") in <code>filePattern</code>.
   * @param filePattern
   * @return the index of the leftmost opening curly bracket containing a
   * slash character ("/"), or -1 if there is no such bracket
   * @throws IOException 
   */
  private static @Tainted int leftmostOuterCurlyContainingSlash(@Tainted String filePattern,
      @Tainted
      int offset) throws IOException {
    @Tainted
    int curlyOpen = 0;
    @Tainted
    int leftmost = -1;
    @Tainted
    boolean seenSlash = false;
    for (@Tainted int i = offset; i < filePattern.length(); i++) {
      @Tainted
      char c = filePattern.charAt(i);
      if (c == '\\') {
        i++;
        if (i >= filePattern.length()) {
          throw new @Tainted IOException("Illegal file pattern: "
              + "An escaped character does not present for glob "
              + filePattern + " at " + i);
        }
      } else if (c == '{') {
        if (curlyOpen++ == 0) {
          leftmost = i;
        }
      } else if (c == '}' && curlyOpen > 0) {
        if (--curlyOpen == 0 && leftmost != -1 && seenSlash) {
          return leftmost;
        }
      } else if (c == '/' && curlyOpen > 0) {
        seenSlash = true;
      }
    }
    return -1;
  }

}
