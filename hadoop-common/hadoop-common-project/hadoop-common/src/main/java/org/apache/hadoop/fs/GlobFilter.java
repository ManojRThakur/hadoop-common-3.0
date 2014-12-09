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
import java.util.regex.PatternSyntaxException;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A filter for POSIX glob pattern with brace expansions.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GlobFilter implements @Tainted PathFilter {
  private final static @Tainted PathFilter DEFAULT_FILTER = new @Tainted PathFilter() {
      @Override
      public @Tainted boolean accept(@Tainted Path file) {
        return true;
      }
    };

  private @Tainted PathFilter userFilter = DEFAULT_FILTER;
  private @Tainted GlobPattern pattern;

  /**
   * Creates a glob filter with the specified file pattern.
   *
   * @param filePattern the file pattern.
   * @throws IOException thrown if the file pattern is incorrect.
   */
  public @Tainted GlobFilter(@Tainted String filePattern) throws IOException {
    init(filePattern, DEFAULT_FILTER);
  }

  /**
   * Creates a glob filter with the specified file pattern and an user filter.
   *
   * @param filePattern the file pattern.
   * @param filter user filter in addition to the glob pattern.
   * @throws IOException thrown if the file pattern is incorrect.
   */
  public @Tainted GlobFilter(@Tainted String filePattern, @Tainted PathFilter filter) throws IOException {
    init(filePattern, filter);
  }

  void init(@Tainted GlobFilter this, @Tainted String filePattern, @Tainted PathFilter filter) throws IOException {
    try {
      userFilter = filter;
      pattern = new @Tainted GlobPattern(filePattern);
    }
    catch (@Tainted PatternSyntaxException e) {
      // Existing code expects IOException startWith("Illegal file pattern")
      throw new @Tainted IOException("Illegal file pattern: "+ e.getMessage(), e);
    }
  }

  @Tainted
  boolean hasPattern(@Tainted GlobFilter this) {
    return pattern.hasWildcard();
  }

  @Override
  public @Tainted boolean accept(@Tainted GlobFilter this, @Tainted Path path) {
    return pattern.matches(path.getName()) && userFilter.accept(path);
  }
}
