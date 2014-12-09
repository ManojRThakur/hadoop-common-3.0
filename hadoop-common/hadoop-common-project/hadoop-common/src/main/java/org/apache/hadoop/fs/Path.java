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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.apache.avro.reflect.Stringable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/** Names a file or directory in a {@link FileSystem}.
 * Path strings use slash as the directory separator.  A path string is
 * absolute if it begins with a slash.
 */
@Stringable
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Path implements Comparable {

  /** The directory separator, a slash. */
  public static final @Tainted String SEPARATOR = "/";
  public static final @Tainted char SEPARATOR_CHAR = '/';
  
  public static final @Tainted String CUR_DIR = ".";
  
  public static final @Tainted boolean WINDOWS
    = System.getProperty("os.name").startsWith("Windows");

  /**
   *  Pre-compiled regular expressions to detect path formats.
   */
  private static final @Tainted Pattern hasUriScheme =
      Pattern.compile("^[a-zA-Z][a-zA-Z0-9+-.]+:");
  private static final @Tainted Pattern hasDriveLetterSpecifier =
      Pattern.compile("^/?[a-zA-Z]:");

  private @Tainted URI uri;                                // a hierarchical uri

  /**
   * Pathnames with scheme and relative path are illegal.
   * @param path to be checked
   */
  void checkNotSchemeWithRelative(@Tainted Path this) {
    if (toUri().isAbsolute() && !isUriPathAbsolute()) {
      throw new @Tainted HadoopIllegalArgumentException(
          "Unsupported name: has scheme but relative path-part");
    }
  }

  void checkNotRelative(@Tainted Path this) {
    if (!isAbsolute() && toUri().getScheme() == null) {
      throw new @Tainted HadoopIllegalArgumentException("Path is relative");
    }
  }

  public static @Tainted Path getPathWithoutSchemeAndAuthority(@Tainted Path path) {
    // This code depends on Path.toString() to remove the leading slash before
    // the drive specification on Windows.
    @Tainted
    Path newPath = path.isUriPathAbsolute() ?
      new @Tainted Path(null, null, path.toUri().getPath()) :
      path;
    return newPath;
  }

  /** Resolve a child path against a parent path. */
  public @Tainted Path(@Tainted String parent, @Tainted String child) {
    this(new @Tainted Path(parent), new @Tainted Path(child));
  }

  /** Resolve a child path against a parent path. */
  public @Tainted Path(@Tainted Path parent, @Tainted String child) {
    this(parent, new @Tainted Path(child));
  }

  /** Resolve a child path against a parent path. */
  public @Tainted Path(@Tainted String parent, @Tainted Path child) {
    this(new @Tainted Path(parent), child);
  }

  /** Resolve a child path against a parent path. */
  public @Tainted Path(@Tainted Path parent, @Tainted Path child) {
    // Add a slash to parent's path so resolution is compatible with URI's
    @Tainted
    URI parentUri = parent.uri;
    @Tainted
    String parentPath = parentUri.getPath();
    if (!(parentPath.equals("/") || parentPath.isEmpty())) {
      try {
        parentUri = new @Tainted URI(parentUri.getScheme(), parentUri.getAuthority(),
                      parentUri.getPath()+"/", null, parentUri.getFragment());
      } catch (@Tainted URISyntaxException e) {
        throw new @Tainted IllegalArgumentException(e);
      }
    }
    @Tainted
    URI resolved = parentUri.resolve(child.uri);
    initialize(resolved.getScheme(), resolved.getAuthority(),
               resolved.getPath(), resolved.getFragment());
  }

  private void checkPathArg( @Tainted Path this, @Tainted String path ) throws IllegalArgumentException {
    // disallow construction of a Path from an empty string
    if ( path == null ) {
      throw new @Tainted IllegalArgumentException(
          "Can not create a Path from a null string");
    }
    if( path.length() == 0 ) {
       throw new @Tainted IllegalArgumentException(
           "Can not create a Path from an empty string");
    }   
  }
  
  /** Construct a path from a String.  Path strings are URIs, but with
   * unescaped elements and some additional normalization. */
  public @Tainted Path(@Tainted String pathString) throws IllegalArgumentException {
    checkPathArg( pathString );
    
    // We can't use 'new URI(String)' directly, since it assumes things are
    // escaped, which we don't require of Paths. 
    
    // add a slash in front of paths with Windows drive letters
    if (hasWindowsDrive(pathString) && pathString.charAt(0) != '/') {
      pathString = "/" + pathString;
    }

    // parse uri components
    @Tainted
    String scheme = null;
    @Tainted
    String authority = null;

    @Tainted
    int start = 0;

    // parse uri scheme, if any
    @Tainted
    int colon = pathString.indexOf(':');
    @Tainted
    int slash = pathString.indexOf('/');
    if ((colon != -1) &&
        ((slash == -1) || (colon < slash))) {     // has a scheme
      scheme = pathString.substring(0, colon);
      start = colon+1;
    }

    // parse uri authority, if any
    if (pathString.startsWith("//", start) &&
        (pathString.length()-start > 2)) {       // has authority
      @Tainted
      int nextSlash = pathString.indexOf('/', start+2);
      @Tainted
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start+2, authEnd);
      start = authEnd;
    }

    // uri path is the rest of the string -- query & fragment not supported
    @Tainted
    String path = pathString.substring(start, pathString.length());

    initialize(scheme, authority, path, null);
  }

  /**
   * Construct a path from a URI
   */
  public @Tainted Path(@Tainted URI aUri) {
    uri = aUri.normalize();
  }
  
  /** Construct a Path from components. */
  public @Tainted Path(@Tainted String scheme, @Tainted String authority, @Tainted String path) {
    checkPathArg( path );

    // add a slash in front of paths with Windows drive letters
    if (hasWindowsDrive(path) && path.charAt(0) != '/') {
      path = "/" + path;
    }

    // add "./" in front of Linux relative paths so that a path containing
    // a colon e.q. "a:b" will not be interpreted as scheme "a".
    if (!WINDOWS && path.charAt(0) != '/') {
      path = "./" + path;
    }

    initialize(scheme, authority, path, null);
  }

  private void initialize(@Tainted Path this, @Tainted String scheme, @Tainted String authority, @Tainted String path,
      @Tainted
      String fragment) {
    try {
      this.uri = new @Tainted URI(scheme, authority, normalizePath(scheme, path), null, fragment)
        .normalize();
    } catch (@Tainted URISyntaxException e) {
      throw new @Tainted IllegalArgumentException(e);
    }
  }

  /**
   * Merge 2 paths such that the second path is appended relative to the first.
   * The returned path has the scheme and authority of the first path.  On
   * Windows, the drive specification in the second path is discarded.
   * 
   * @param path1 Path first path
   * @param path2 Path second path, to be appended relative to path1
   * @return Path merged path
   */
  public static @Tainted Path mergePaths(@Tainted Path path1, @Tainted Path path2) {
    @Tainted
    String path2Str = path2.toUri().getPath();
    if(hasWindowsDrive(path2Str)) {
      path2Str = path2Str.substring(path2Str.indexOf(':')+1);
    }
    return new @Tainted Path(path1 + path2Str);
  }

  /**
   * Normalize a path string to use non-duplicated forward slashes as
   * the path separator and remove any trailing path separators.
   * @param scheme Supplies the URI scheme. Used to deduce whether we
   *               should replace backslashes or not.
   * @param path Supplies the scheme-specific part
   * @return Normalized path string.
   */
  private static @Tainted String normalizePath(@Tainted String scheme, @Tainted String path) {
    // Remove double forward slashes.
    path = StringUtils.replace(path, "//", "/");

    // Remove backslashes if this looks like a Windows path. Avoid
    // the substitution if it looks like a non-local URI.
    if (WINDOWS &&
        (hasWindowsDrive(path) ||
         (scheme == null) ||
         (scheme.isEmpty()) ||
         (scheme.equals("file")))) {
      path = StringUtils.replace(path, "\\", "/");
    }
    
    // trim trailing slash from non-root path (ignoring windows drive)
    @Tainted
    int minLength = hasWindowsDrive(path) ? 4 : 1;
    if (path.length() > minLength && path.endsWith("/")) {
      path = path.substring(0, path.length()-1);
    }
    
    return path;
  }

  private static @Tainted boolean hasWindowsDrive(@Tainted String path) {
    return (WINDOWS && hasDriveLetterSpecifier.matcher(path).find());
  }

  /**
   * Determine whether a given path string represents an absolute path on
   * Windows. e.g. "C:/a/b" is an absolute path. "C:a/b" is not.
   *
   * @param pathString Supplies the path string to evaluate.
   * @param slashed true if the given path is prefixed with "/".
   * @return true if the supplied path looks like an absolute path with a Windows
   * drive-specifier.
   */
  public static @Tainted boolean isWindowsAbsolutePath(final @Tainted String pathString,
                                              final @Tainted boolean slashed) {
    @Tainted
    int start = (slashed ? 1 : 0);

    return
        hasWindowsDrive(pathString) &&
        pathString.length() >= (start + 3) &&
        ((pathString.charAt(start + 2) == SEPARATOR_CHAR) ||
          (pathString.charAt(start + 2) == '\\'));
  }

  /** Convert this to a URI. */
  public @Tainted URI toUri(@Tainted Path this) { return uri; }

  /** Return the FileSystem that owns this Path. */
  public @Tainted FileSystem getFileSystem(@Tainted Path this, @Tainted Configuration conf) throws IOException {
    return FileSystem.get(this.toUri(), conf);
  }

  /**
   * Is an absolute path (ie a slash relative path part)
   *  AND  a scheme is null AND  authority is null.
   */
  public @Tainted boolean isAbsoluteAndSchemeAuthorityNull(@Tainted Path this) {
    return  (isUriPathAbsolute() && 
        uri.getScheme() == null && uri.getAuthority() == null);
  }
  
  /**
   *  True if the path component (i.e. directory) of this URI is absolute.
   */
  public @Tainted boolean isUriPathAbsolute(@Tainted Path this) {
    @Tainted
    int start = hasWindowsDrive(uri.getPath()) ? 3 : 0;
    return uri.getPath().startsWith(SEPARATOR, start);
   }
  
  /** True if the path component of this URI is absolute. */
  /**
   * There is some ambiguity here. An absolute path is a slash
   * relative name without a scheme or an authority.
   * So either this method was incorrectly named or its
   * implementation is incorrect. This method returns true
   * even if there is a scheme and authority.
   */
  public @Tainted boolean isAbsolute(@Tainted Path this) {
     return isUriPathAbsolute();
  }

  /**
   * @return true if and only if this path represents the root of a file system
   */
  public @Tainted boolean isRoot(@Tainted Path this) {
    return getParent() == null;
  }

  /** Returns the final component of this path.*/
  public @Tainted String getName(@Tainted Path this) {
    @Tainted
    String path = uri.getPath();
    @Tainted
    int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash+1);
  }

  /** Returns the parent of a path or null if at root. */
  public @Tainted Path getParent(@Tainted Path this) {
    @Tainted
    String path = uri.getPath();
    @Tainted
    int lastSlash = path.lastIndexOf('/');
    @Tainted
    int start = hasWindowsDrive(path) ? 3 : 0;
    if ((path.length() == start) ||               // empty path
        (lastSlash == start && path.length() == start+1)) { // at root
      return null;
    }
    @Tainted
    String parent;
    if (lastSlash==-1) {
      parent = CUR_DIR;
    } else {
      @Tainted
      int end = hasWindowsDrive(path) ? 3 : 0;
      parent = path.substring(0, lastSlash==end?end+1:lastSlash);
    }
    return new @Tainted Path(uri.getScheme(), uri.getAuthority(), parent);
  }

  /** Adds a suffix to the final name in the path.*/
  public @Tainted Path suffix(@Tainted Path this, @Tainted String suffix) {
    return new @Tainted Path(getParent(), getName()+suffix);
  }

  @Override
  public @Tainted String toString(@Tainted Path this) {
    // we can't use uri.toString(), which escapes everything, because we want
    // illegal characters unescaped in the string, for glob processing, etc.
    @Tainted
    StringBuilder buffer = new @Tainted StringBuilder();
    if (uri.getScheme() != null) {
      buffer.append(uri.getScheme());
      buffer.append(":");
    }
    if (uri.getAuthority() != null) {
      buffer.append("//");
      buffer.append(uri.getAuthority());
    }
    if (uri.getPath() != null) {
      @Tainted
      String path = uri.getPath();
      if (path.indexOf('/')==0 &&
          hasWindowsDrive(path) &&                // has windows drive
          uri.getScheme() == null &&              // but no scheme
          uri.getAuthority() == null)             // or authority
        path = path.substring(1);                 // remove slash before drive
      buffer.append(path);
    }
    if (uri.getFragment() != null) {
      buffer.append("#");
      buffer.append(uri.getFragment());
    }
    return buffer.toString();
  }

  @Override
  public @Tainted boolean equals(@Tainted Path this, @Tainted Object o) {
    if (!(o instanceof @Tainted Path)) {
      return false;
    }
    @Tainted
    Path that = (@Tainted Path)o;
    return this.uri.equals(that.uri);
  }

  @Override
  public @Tainted int hashCode(@Tainted Path this) {
    return uri.hashCode();
  }

  @Override
  public @Tainted int compareTo(@Tainted Path this, @Tainted Object o) {
    @Tainted
    Path that = (@Tainted Path)o;
    return this.uri.compareTo(that.uri);
  }
  
  /** Return the number of elements in this path. */
  public @Tainted int depth(@Tainted Path this) {
    @Tainted
    String path = uri.getPath();
    @Tainted
    int depth = 0;
    @Tainted
    int slash = path.length()==1 && path.charAt(0)=='/' ? -1 : 0;
    while (slash != -1) {
      depth++;
      slash = path.indexOf(SEPARATOR, slash+1);
    }
    return depth;
  }

  /**
   *  Returns a qualified path object.
   *  
   *  Deprecated - use {@link #makeQualified(URI, Path)}
   */
  @Deprecated
  public @Tainted Path makeQualified(@Tainted Path this, @Tainted FileSystem fs) {
    return makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }
  
  /** Returns a qualified path object. */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public @Tainted Path makeQualified(@Tainted Path this, @Tainted URI defaultUri, @Tainted Path workingDir ) {
    @Tainted
    Path path = this;
    if (!isAbsolute()) {
      path = new @Tainted Path(workingDir, this);
    }

    @Tainted
    URI pathUri = path.toUri();
      
    @Tainted
    String scheme = pathUri.getScheme();
    @Tainted
    String authority = pathUri.getAuthority();
    @Tainted
    String fragment = pathUri.getFragment();

    if (scheme != null &&
        (authority != null || defaultUri.getAuthority() == null))
      return path;

    if (scheme == null) {
      scheme = defaultUri.getScheme();
    }

    if (authority == null) {
      authority = defaultUri.getAuthority();
      if (authority == null) {
        authority = "";
      }
    }
    
    @Tainted
    URI newUri = null;
    try {
      newUri = new @Tainted URI(scheme, authority , 
        normalizePath(scheme, pathUri.getPath()), null, fragment);
    } catch (@Tainted URISyntaxException e) {
      throw new @Tainted IllegalArgumentException(e);
    }
    return new @Tainted Path(newUri);
  }
}
