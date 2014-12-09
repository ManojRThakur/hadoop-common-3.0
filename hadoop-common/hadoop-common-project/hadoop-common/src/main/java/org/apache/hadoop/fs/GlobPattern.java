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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class for POSIX glob pattern with brace expansions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GlobPattern {
  private static final @Tainted char BACKSLASH = '\\';
  private @Tainted Pattern compiled;
  private @Tainted boolean hasWildcard = false;

  /**
   * Construct the glob pattern object with a glob pattern string
   * @param globPattern the glob pattern string
   */
  public @Tainted GlobPattern(@Tainted String globPattern) {
    set(globPattern);
  }

  /**
   * @return the compiled pattern
   */
  public @Tainted Pattern compiled(@Tainted GlobPattern this) {
    return compiled;
  }

  /**
   * Compile glob pattern string
   * @param globPattern the glob pattern
   * @return the pattern object
   */
  public static @Tainted Pattern compile(@Tainted String globPattern) {
    return new @Tainted GlobPattern(globPattern).compiled();
  }

  /**
   * Match input against the compiled glob pattern
   * @param s input chars
   * @return true for successful matches
   */
  public @Tainted boolean matches(@Tainted GlobPattern this, @Tainted CharSequence s) {
    return compiled.matcher(s).matches();
  }

  /**
   * Set and compile a glob pattern
   * @param glob  the glob pattern string
   */
  public void set(@Tainted GlobPattern this, @Tainted String glob) {
    @Tainted
    StringBuilder regex = new @Tainted StringBuilder();
    @Tainted
    int setOpen = 0;
    @Tainted
    int curlyOpen = 0;
    @Tainted
    int len = glob.length();
    hasWildcard = false;

    for (@Tainted int i = 0; i < len; i++) {
      @Tainted
      char c = glob.charAt(i);

      switch (c) {
        case BACKSLASH:
          if (++i >= len) {
            error("Missing escaped character", glob, i);
          }
          regex.append(c).append(glob.charAt(i));
          continue;
        case '.':
        case '$':
        case '(':
        case ')':
        case '|':
        case '+':
          // escape regex special chars that are not glob special chars
          regex.append(BACKSLASH);
          break;
        case '*':
          regex.append('.');
          hasWildcard = true;
          break;
        case '?':
          regex.append('.');
          hasWildcard = true;
          continue;
        case '{': // start of a group
          regex.append("(?:"); // non-capturing
          curlyOpen++;
          hasWildcard = true;
          continue;
        case ',':
          regex.append(curlyOpen > 0 ? '|' : c);
          continue;
        case '}':
          if (curlyOpen > 0) {
            // end of a group
            curlyOpen--;
            regex.append(")");
            continue;
          }
          break;
        case '[':
          if (setOpen > 0) {
            error("Unclosed character class", glob, i);
          }
          setOpen++;
          hasWildcard = true;
          break;
        case '^': // ^ inside [...] can be unescaped
          if (setOpen == 0) {
            regex.append(BACKSLASH);
          }
          break;
        case '!': // [! needs to be translated to [^
          regex.append(setOpen > 0 && '[' == glob.charAt(i - 1) ? '^' : '!');
          continue;
        case ']':
          // Many set errors like [][] could not be easily detected here,
          // as []], []-] and [-] are all valid POSIX glob and java regex.
          // We'll just let the regex compiler do the real work.
          setOpen = 0;
          break;
        default:
      }
      regex.append(c);
    }

    if (setOpen > 0) {
      error("Unclosed character class", glob, len);
    }
    if (curlyOpen > 0) {
      error("Unclosed group", glob, len);
    }
    compiled = Pattern.compile(regex.toString());
  }

  /**
   * @return true if this is a wildcard pattern (with special chars)
   */
  public @Tainted boolean hasWildcard(@Tainted GlobPattern this) {
    return hasWildcard;
  }

  private static void error(@Tainted String message, @Tainted String pattern, @Tainted int pos) {
    throw new @Tainted PatternSyntaxException(message, pattern, pos);
  }
}
