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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Globber {
  public static final @Tainted Log LOG = LogFactory.getLog(Globber.class.getName());

  private final @Tainted FileSystem fs;
  private final @Tainted FileContext fc;
  private final @Tainted Path pathPattern;
  private final @Tainted PathFilter filter;
  
  public @Tainted Globber(@Tainted FileSystem fs, @Tainted Path pathPattern, @Tainted PathFilter filter) {
    this.fs = fs;
    this.fc = null;
    this.pathPattern = pathPattern;
    this.filter = filter;
  }

  public @Tainted Globber(@Tainted FileContext fc, @Tainted Path pathPattern, @Tainted PathFilter filter) {
    this.fs = null;
    this.fc = fc;
    this.pathPattern = pathPattern;
    this.filter = filter;
  }

  private @Tainted FileStatus getFileStatus(@Tainted Globber this, @Tainted Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.getFileStatus(path);
      } else {
        return fc.getFileStatus(path);
      }
    } catch (@Tainted FileNotFoundException e) {
      return null;
    }
  }

  private @Tainted FileStatus @Tainted [] listStatus(@Tainted Globber this, @Tainted Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.listStatus(path);
      } else {
        return fc.util().listStatus(path);
      }
    } catch (@Tainted FileNotFoundException e) {
      return new @Tainted FileStatus @Tainted [0];
    }
  }

  private @Tainted Path fixRelativePart(@Tainted Globber this, @Tainted Path path) {
    if (fs != null) {
      return fs.fixRelativePart(path);
    } else {
      return fc.fixRelativePart(path);
    }
  }

  /**
   * Convert a path component that contains backslash ecape sequences to a
   * literal string.  This is necessary when you want to explicitly refer to a
   * path that contains globber metacharacters.
   */
  private static @Tainted String unescapePathComponent(@Tainted String name) {
    return name.replaceAll("\\\\(.)", "$1");
  }

  /**
   * Translate an absolute path into a list of path components.
   * We merge double slashes into a single slash here.
   * POSIX root path, i.e. '/', does not get an entry in the list.
   */
  private static @Tainted List<@Tainted String> getPathComponents(@Tainted String path)
      throws IOException {
    @Tainted
    ArrayList<@Tainted String> ret = new @Tainted ArrayList<@Tainted String>();
    for (@Tainted String component : path.split(Path.SEPARATOR)) {
      if (!component.isEmpty()) {
        ret.add(component);
      }
    }
    return ret;
  }

  private @Tainted String schemeFromPath(@Tainted Globber this, @Tainted Path path) throws IOException {
    @Tainted
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      if (fs != null) {
        scheme = fs.getUri().getScheme();
      } else {
        scheme = fc.getDefaultFileSystem().getUri().getScheme();
      }
    }
    return scheme;
  }

  private @Tainted String authorityFromPath(@Tainted Globber this, @Tainted Path path) throws IOException {
    @Tainted
    String authority = path.toUri().getAuthority();
    if (authority == null) {
      if (fs != null) {
        authority = fs.getUri().getAuthority();
      } else {
        authority = fc.getDefaultFileSystem().getUri().getAuthority();
      }
    }
    return authority ;
  }

  public @Tainted FileStatus @Tainted [] glob(@Tainted Globber this) throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    @Tainted
    String scheme = schemeFromPath(pathPattern);
    @Tainted
    String authority = authorityFromPath(pathPattern);

    // Next we strip off everything except the pathname itself, and expand all
    // globs.  Expansion is a process which turns "grouping" clauses,
    // expressed as brackets, into separate path patterns.
    @Tainted
    String pathPatternString = pathPattern.toUri().getPath();
    @Tainted
    List<@Tainted String> flattenedPatterns = GlobExpander.expand(pathPatternString);

    // Now loop over all flattened patterns.  In every case, we'll be trying to
    // match them to entries in the filesystem.
    @Tainted
    ArrayList<@Tainted FileStatus> results = 
        new @Tainted ArrayList<@Tainted FileStatus>(flattenedPatterns.size());
    @Tainted
    boolean sawWildcard = false;
    for (@Tainted String flatPattern : flattenedPatterns) {
      // Get the absolute path for this flattened pattern.  We couldn't do 
      // this prior to flattening because of patterns like {/,a}, where which
      // path you go down influences how the path must be made absolute.
      @Tainted
      Path absPattern = fixRelativePart(new @Tainted Path(
          flatPattern.isEmpty() ? Path.CUR_DIR : flatPattern));
      // Now we break the flattened, absolute pattern into path components.
      // For example, /a/*/c would be broken into the list [a, *, c]
      @Tainted
      List<@Tainted String> components =
          getPathComponents(absPattern.toUri().getPath());
      // Starting out at the root of the filesystem, we try to match
      // filesystem entries against pattern components.
      @Tainted
      ArrayList<@Tainted FileStatus> candidates = new @Tainted ArrayList<@Tainted FileStatus>(1);
      if (Path.WINDOWS && !components.isEmpty()
          && Path.isWindowsAbsolutePath(absPattern.toUri().getPath(), true)) {
        // On Windows the path could begin with a drive letter, e.g. /E:/foo.
        // We will skip matching the drive letter and start from listing the
        // root of the filesystem on that drive.
        @Tainted
        String driveLetter = components.remove(0);
        candidates.add(new @Tainted FileStatus(0, true, 0, 0, 0, new @Tainted Path(scheme,
            authority, Path.SEPARATOR + driveLetter + Path.SEPARATOR)));
      } else {
        candidates.add(new @Tainted FileStatus(0, true, 0, 0, 0,
            new @Tainted Path(scheme, authority, Path.SEPARATOR)));
      }
      
      for (@Tainted int componentIdx = 0; componentIdx < components.size();
          componentIdx++) {
        @Tainted
        ArrayList<@Tainted FileStatus> newCandidates =
            new @Tainted ArrayList<@Tainted FileStatus>(candidates.size());
        @Tainted
        GlobFilter globFilter = new @Tainted GlobFilter(components.get(componentIdx));
        @Tainted
        String component = unescapePathComponent(components.get(componentIdx));
        if (globFilter.hasPattern()) {
          sawWildcard = true;
        }
        if (candidates.isEmpty() && sawWildcard) {
          // Optimization: if there are no more candidates left, stop examining 
          // the path components.  We can only do this if we've already seen
          // a wildcard component-- otherwise, we still need to visit all path 
          // components in case one of them is a wildcard.
          break;
        }
        if ((componentIdx < components.size() - 1) &&
            (!globFilter.hasPattern())) {
          // Optimization: if this is not the terminal path component, and we 
          // are not matching against a glob, assume that it exists.  If it 
          // doesn't exist, we'll find out later when resolving a later glob
          // or the terminal path component.
          for (@Tainted FileStatus candidate : candidates) {
            candidate.setPath(new @Tainted Path(candidate.getPath(), component));
          }
          continue;
        }
        for (@Tainted FileStatus candidate : candidates) {
          if (globFilter.hasPattern()) {
            @Tainted
            FileStatus @Tainted [] children = listStatus(candidate.getPath());
            if (children.length == 1) {
              // If we get back only one result, this could be either a listing
              // of a directory with one entry, or it could reflect the fact
              // that what we listed resolved to a file.
              //
              // Unfortunately, we can't just compare the returned paths to
              // figure this out.  Consider the case where you have /a/b, where
              // b is a symlink to "..".  In that case, listing /a/b will give
              // back "/a/b" again.  If we just went by returned pathname, we'd
              // incorrectly conclude that /a/b was a file and should not match
              // /a/*/*.  So we use getFileStatus of the path we just listed to
              // disambiguate.
              if (!getFileStatus(candidate.getPath()).isDirectory()) {
                continue;
              }
            }
            for (@Tainted FileStatus child : children) {
              // Set the child path based on the parent path.
              child.setPath(new @Tainted Path(candidate.getPath(),
                      child.getPath().getName()));
              if (globFilter.accept(child.getPath())) {
                newCandidates.add(child);
              }
            }
          } else {
            // When dealing with non-glob components, use getFileStatus 
            // instead of listStatus.  This is an optimization, but it also
            // is necessary for correctness in HDFS, since there are some
            // special HDFS directories like .reserved and .snapshot that are
            // not visible to listStatus, but which do exist.  (See HADOOP-9877)
            @Tainted
            FileStatus childStatus = getFileStatus(
                new @Tainted Path(candidate.getPath(), component));
            if (childStatus != null) {
              newCandidates.add(childStatus);
             }
           }
        }
        candidates = newCandidates;
      }
      for (@Tainted FileStatus status : candidates) {
        // HADOOP-3497 semantics: the user-defined filter is applied at the
        // end, once the full path is built up.
        if (filter.accept(status.getPath())) {
          results.add(status);
        }
      }
    }
    /*
     * When the input pattern "looks" like just a simple filename, and we
     * can't find it, we return null rather than an empty array.
     * This is a special case which the shell relies on.
     *
     * To be more precise: if there were no results, AND there were no
     * groupings (aka brackets), and no wildcards in the input (aka stars),
     * we return null.
     */
    if ((!sawWildcard) && results.isEmpty() &&
        (flattenedPatterns.size() <= 1)) {
      return null;
    }
    return results.toArray(new @Tainted FileStatus @Tainted [0]);
  }
}
