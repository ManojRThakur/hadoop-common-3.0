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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;

/**
 * Encapsulates a Path (path), its FileStatus (stat), and its FileSystem (fs).
 * PathData ensures that the returned path string will be the same as the
 * one passed in during initialization (unlike Path objects which can
 * modify the path string).
 * The stat field will be null if the path does not exist.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

public class PathData implements @Tainted Comparable<@Tainted PathData> {
  protected final @Tainted URI uri;
  public final @Tainted FileSystem fs;
  public final @Tainted Path path;
  public @Tainted FileStatus stat;
  public @Tainted boolean exists;

  /* True if the URI scheme was not present in the pathString but inferred.
   */
  private @Tainted boolean inferredSchemeFromPath = false;

  /**
   *  Pre-compiled regular expressions to detect path formats.
   */
  private static final @Tainted Pattern potentialUri =
      Pattern.compile("^[a-zA-Z][a-zA-Z0-9+-.]+:");
  private static final @Tainted Pattern windowsNonUriAbsolutePath1 =
      Pattern.compile("^/?[a-zA-Z]:\\\\");
  private static final @Tainted Pattern windowsNonUriAbsolutePath2 =
      Pattern.compile("^/?[a-zA-Z]:/");

  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it
   * @param pathString a string for a path
   * @param conf the configuration file
   * @throws IOException if anything goes wrong...
   */
  public @Tainted PathData(@Tainted String pathString, @Tainted Configuration conf) throws IOException {
    this(FileSystem.get(stringToUri(pathString), conf), pathString);
  }
  
  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it
   * @param localPath a local URI
   * @param conf the configuration file
   * @throws IOException if anything goes wrong...
   */
  public @Tainted PathData(@Tainted URI localPath, @Tainted Configuration conf) throws IOException {
    this(FileSystem.getLocal(conf), localPath.getPath());
  }

  /**
   * Looks up the file status for a path.  If the path
   * doesn't exist, then the status will be null
   * @param fs the FileSystem for the path
   * @param pathString a string for a path 
   * @throws IOException if anything goes wrong
   */
  private @Tainted PathData(@Tainted FileSystem fs, @Tainted String pathString) throws IOException {
    this(fs, pathString, lookupStat(fs, pathString, true));
  }

  /**
   * Validates the given Windows path.
   * @param pathString a String of the path suppliued by the user.
   * @return true if the URI scheme was not present in the pathString but
   * inferred; false, otherwise.
   * @throws IOException if anything goes wrong
   */
  private static @Tainted boolean checkIfSchemeInferredFromPath(@Tainted String pathString)
  throws IOException
  {
    if (windowsNonUriAbsolutePath1.matcher(pathString).find()) {
      // Forward slashes disallowed in a backslash-separated path.
      if (pathString.indexOf('/') != -1) {
        throw new @Tainted IOException("Invalid path string " + pathString);
      }

      return true;
    }

    // Is it a forward slash-separated absolute path?
    if (windowsNonUriAbsolutePath2.matcher(pathString).find()) {
      return true;
    }

    // Does it look like a URI? If so then just leave it alone.
    if (potentialUri.matcher(pathString).find()) {
      return false;
    }

    // Looks like a relative path on Windows.
    return false;
  }

  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it.
   * @param fs the FileSystem
   * @param pathString a String of the path
   * @param stat the FileStatus (may be null if the path doesn't exist)
   */
  private @Tainted PathData(@Tainted FileSystem fs, @Tainted String pathString, @Tainted FileStatus stat)
  throws IOException {
    this.fs = fs;
    this.uri = stringToUri(pathString);
    this.path = fs.makeQualified(new @Tainted Path(uri));
    setStat(stat);

    if (Path.WINDOWS) {
      inferredSchemeFromPath = checkIfSchemeInferredFromPath(pathString);
    }
  }

  // need a static method for the ctor above
  /**
   * Get the FileStatus info
   * @param ignoreFNF if true, stat will be null if the path doesn't exist
   * @return FileStatus for the given path
   * @throws IOException if anything goes wrong
   */
  private static
  @Tainted
  FileStatus lookupStat(@Tainted FileSystem fs, @Tainted String pathString, @Tainted boolean ignoreFNF)
  throws IOException {
    @Tainted
    FileStatus status = null;
    try {
      status = fs.getFileStatus(new @Tainted Path(pathString));
    } catch (@Tainted FileNotFoundException e) {
      if (!ignoreFNF) throw new @Tainted PathNotFoundException(pathString);
    }
    // TODO: should consider wrapping other exceptions into Path*Exceptions
    return status;
  }
  
  private void setStat(@Tainted PathData this, @Tainted FileStatus stat) {
    this.stat = stat;
    exists = (stat != null);
  }

  /**
   * Updates the paths's file status
   * @return the updated FileStatus
   * @throws IOException if anything goes wrong...
   */
  public @Tainted FileStatus refreshStatus(@Tainted PathData this) throws IOException {
    @Tainted
    FileStatus status = null;
    try {
      status = lookupStat(fs, toString(), false);
    } finally {
      // always set the status.  the caller must get the correct result
      // if it catches the exception and later interrogates the status
      setStat(status);
    }
    return status;
  }

  protected enum FileTypeRequirement {

@Tainted  SHOULD_NOT_BE_DIRECTORY,  @Tainted  SHOULD_BE_DIRECTORY
  };

  /**
   * Ensure that the file exists and if it is or is not a directory
   * @param typeRequirement Set it to the desired requirement.
   * @throws PathIOException if file doesn't exist or the type does not match
   * what was specified in typeRequirement.
   */
  private void checkIfExists(@Tainted PathData this, @Tainted FileTypeRequirement typeRequirement) 
  throws PathIOException {
    if (!exists) {
      throw new @Tainted PathNotFoundException(toString());      
    }

    if ((typeRequirement == FileTypeRequirement.SHOULD_BE_DIRECTORY)
       && !stat.isDirectory()) {
      throw new @Tainted PathIsNotDirectoryException(toString());
    } else if ((typeRequirement == FileTypeRequirement.SHOULD_NOT_BE_DIRECTORY)
              && stat.isDirectory()) {
      throw new @Tainted PathIsDirectoryException(toString());
    }
  }
  
  /**
   * Returns a new PathData with the given extension.
   * @param extension for the suffix
   * @return PathData
   * @throws IOException shouldn't happen
   */
  public @Tainted PathData suffix(@Tainted PathData this, @Tainted String extension) throws IOException {
    return new @Tainted PathData(fs, this+extension);
  }

  /**
   * Test if the parent directory exists
   * @return boolean indicating parent exists
   * @throws IOException upon unexpected error
   */
  public @Tainted boolean parentExists(@Tainted PathData this) throws IOException {
    return representsDirectory()
        ? fs.exists(path) : fs.exists(path.getParent());
  }

  /**
   * Check if the path represents a directory as determined by the basename
   * being "." or "..", or the path ending with a directory separator 
   * @return boolean if this represents a directory
   */
  public @Tainted boolean representsDirectory(@Tainted PathData this) {
    @Tainted
    String uriPath = uri.getPath();
    @Tainted
    String name = uriPath.substring(uriPath.lastIndexOf("/")+1);
    // Path will munch off the chars that indicate a dir, so there's no way
    // to perform this test except by examining the raw basename we maintain
    return (name.isEmpty() || name.equals(".") || name.equals(".."));
  }
  
  /**
   * Returns a list of PathData objects of the items contained in the given
   * directory.
   * @return list of PathData objects for its children
   * @throws IOException if anything else goes wrong...
   */
  public @Tainted PathData @Tainted [] getDirectoryContents(@Tainted PathData this) throws IOException {
    checkIfExists(FileTypeRequirement.SHOULD_BE_DIRECTORY);
    @Tainted
    FileStatus @Tainted [] stats = fs.listStatus(path);
    @Tainted
    PathData @Tainted [] items = new @Tainted PathData @Tainted [stats.length];
    for (@Tainted int i=0; i < stats.length; i++) {
      // preserve relative paths
      @Tainted
      String child = getStringForChildPath(stats[i].getPath());
      items[i] = new @Tainted PathData(fs, child, stats[i]);
    }
    Arrays.sort(items);
    return items;
  }

  /**
   * Creates a new object for a child entry in this directory
   * @param child the basename will be appended to this object's path
   * @return PathData for the child
   * @throws IOException if this object does not exist or is not a directory
   */
  public @Tainted PathData getPathDataForChild(@Tainted PathData this, @Tainted PathData child) throws IOException {
    checkIfExists(FileTypeRequirement.SHOULD_BE_DIRECTORY);
    return new @Tainted PathData(fs, getStringForChildPath(child.path));
  }

  /**
   * Given a child of this directory, use the directory's path and the child's
   * basename to construct the string to the child.  This preserves relative
   * paths since Path will fully qualify.
   * @param childPath a path contained within this directory
   * @return String of the path relative to this directory
   */
  private @Tainted String getStringForChildPath(@Tainted PathData this, @Tainted Path childPath) {
    @Tainted
    String basename = childPath.getName();
    if (Path.CUR_DIR.equals(toString())) {
      return basename;
    }
    // check getPath() so scheme slashes aren't considered part of the path
    @Tainted
    String separator = uri.getPath().endsWith(Path.SEPARATOR)
        ? "" : Path.SEPARATOR;
    return uriToString(uri, inferredSchemeFromPath) + separator + basename;
  }
  
  protected enum PathType {  @Tainted  HAS_SCHEME,  @Tainted  SCHEMELESS_ABSOLUTE,  @Tainted  RELATIVE };
  
  /**
   * Expand the given path as a glob pattern.  Non-existent paths do not
   * throw an exception because creation commands like touch and mkdir need
   * to create them.  The "stat" field will be null if the path does not
   * exist.
   * @param pattern the pattern to expand as a glob
   * @param conf the hadoop configuration
   * @return list of {@link PathData} objects.  if the pattern is not a glob,
   * and does not exist, the list will contain a single PathData with a null
   * stat 
   * @throws IOException anything else goes wrong...
   */
  public static @Tainted PathData @Tainted [] expandAsGlob(@Tainted String pattern, @Tainted Configuration conf)
  throws IOException {
    @Tainted
    Path globPath = new @Tainted Path(pattern);
    @Tainted
    FileSystem fs = globPath.getFileSystem(conf);    
    @Tainted
    FileStatus @Tainted [] stats = fs.globStatus(globPath);
    @Tainted
    PathData @Tainted [] items = null;
    
    if (stats == null) {
      // remove any quoting in the glob pattern
      pattern = pattern.replaceAll("\\\\(.)", "$1");
      // not a glob & file not found, so add the path with a null stat
      items = new @Tainted PathData @Tainted []{ new @Tainted PathData(fs, pattern, null) };
    } else {
      // figure out what type of glob path was given, will convert globbed
      // paths to match the type to preserve relativity
      @Tainted
      PathType globType;
      @Tainted
      URI globUri = globPath.toUri();
      if (globUri.getScheme() != null) {
        globType = PathType.HAS_SCHEME;
      } else if (!globUri.getPath().isEmpty() &&
                 new @Tainted Path(globUri.getPath()).isAbsolute()) {
        globType = PathType.SCHEMELESS_ABSOLUTE;
      } else {
        globType = PathType.RELATIVE;
      }

      // convert stats to PathData
      items = new @Tainted PathData @Tainted [stats.length];
      @Tainted
      int i=0;
      for (@Tainted FileStatus stat : stats) {
        @Tainted
        URI matchUri = stat.getPath().toUri();
        @Tainted
        String globMatch = null;
        switch (globType) {
          case HAS_SCHEME: // use as-is, but remove authority if necessary
            if (globUri.getAuthority() == null) {
              matchUri = removeAuthority(matchUri);
            }
            globMatch = uriToString(matchUri, false);
            break;
          case SCHEMELESS_ABSOLUTE: // take just the uri's path
            globMatch = matchUri.getPath();
            break;
          case RELATIVE: // make it relative to the current working dir
            @Tainted
            URI cwdUri = fs.getWorkingDirectory().toUri();
            globMatch = relativize(cwdUri, matchUri, stat.isDirectory());
            break;
        }
        items[i++] = new @Tainted PathData(fs, globMatch, stat);
      }
    }
    Arrays.sort(items);
    return items;
  }

  private static @Tainted URI removeAuthority(@Tainted URI uri) {
    try {
      uri = new @Tainted URI(
          uri.getScheme(), "",
          uri.getPath(), uri.getQuery(), uri.getFragment()
      );
    } catch (@Tainted URISyntaxException e) {
      throw new @Tainted IllegalArgumentException(e.getLocalizedMessage());
    }
    return uri;
  }
  
  private static @Tainted String relativize(@Tainted URI cwdUri, @Tainted URI srcUri, @Tainted boolean isDir) {
    @Tainted
    String uriPath = srcUri.getPath();
    @Tainted
    String cwdPath = cwdUri.getPath();
    if (cwdPath.equals(uriPath)) {
      return Path.CUR_DIR;
    }

    // find common ancestor
    @Tainted
    int lastSep = findLongestDirPrefix(cwdPath, uriPath, isDir);
    
    @Tainted
    StringBuilder relPath = new @Tainted StringBuilder();    
    // take the remaining path fragment after the ancestor
    if (lastSep < uriPath.length()) {
      relPath.append(uriPath.substring(lastSep+1));
    }

    // if cwd has a path fragment after the ancestor, convert them to ".."
    if (lastSep < cwdPath.length()) {
      while (lastSep != -1) {
        if (relPath.length() != 0) relPath.insert(0, Path.SEPARATOR);
        relPath.insert(0, "..");
        lastSep = cwdPath.indexOf(Path.SEPARATOR, lastSep+1);
      }
    }
    return relPath.toString();
  }

  private static @Tainted int findLongestDirPrefix(@Tainted String cwd, @Tainted String path, @Tainted boolean isDir) {
    // add the path separator to dirs to simplify finding the longest match
    if (!cwd.endsWith(Path.SEPARATOR)) {
      cwd += Path.SEPARATOR;
    }
    if (isDir && !path.endsWith(Path.SEPARATOR)) {
      path += Path.SEPARATOR;
    }

    // find longest directory prefix 
    @Tainted
    int len = Math.min(cwd.length(), path.length());
    @Tainted
    int lastSep = -1;
    for (@Tainted int i=0; i < len; i++) {
      if (cwd.charAt(i) != path.charAt(i)) break;
      if (cwd.charAt(i) == Path.SEPARATOR_CHAR) lastSep = i;
    }
    return lastSep;
  }
  
  /**
   * Returns the printable version of the path that is either the path
   * as given on the commandline, or the full path
   * @return String of the path
   */
  @Override
  public @Tainted String toString(@Tainted PathData this) {
    return uriToString(uri, inferredSchemeFromPath);
  }
 
  private static @Tainted String uriToString(@Tainted URI uri, @Tainted boolean inferredSchemeFromPath) {
    @Tainted
    String scheme = uri.getScheme();
    // No interpretation of symbols. Just decode % escaped chars.
    @Tainted
    String decodedRemainder = uri.getSchemeSpecificPart();

    // Drop the scheme if it was inferred to ensure fidelity between
    // the input and output path strings.
    if ((scheme == null) || (inferredSchemeFromPath)) {
      if (Path.isWindowsAbsolutePath(decodedRemainder, true)) {
        // Strip the leading '/' added in stringToUri so users see a valid
        // Windows path.
        decodedRemainder = decodedRemainder.substring(1);
      }
      return decodedRemainder;
    } else {
      @Tainted
      StringBuilder buffer = new @Tainted StringBuilder();
      buffer.append(scheme);
      buffer.append(":");
      buffer.append(decodedRemainder);
      return buffer.toString();
    }
  }
  
  /**
   * Get the path to a local file
   * @return File representing the local path
   * @throws IllegalArgumentException if this.fs is not the LocalFileSystem
   */
  public @Tainted File toFile(@Tainted PathData this) {
    if (!(fs instanceof @Tainted LocalFileSystem)) {
       throw new @Tainted IllegalArgumentException("Not a local path: " + path);
    }
    return ((@Tainted LocalFileSystem)fs).pathToFile(path);
  }

  /** Normalize the given Windows path string. This does the following:
   *    1. Adds "file:" scheme for absolute paths.
   *    2. Ensures the scheme-specific part starts with '/' per RFC2396.
   *    3. Replaces backslash path separators with forward slashes.
   *    @param pathString Path string supplied by the user.
   *    @return normalized absolute path string. Returns the input string
   *            if it is not a Windows absolute path.
   */
  private static @Tainted String normalizeWindowsPath(@Tainted String pathString)
  throws IOException
  {
    if (!Path.WINDOWS) {
      return pathString;
    }

    @Tainted
    boolean slashed =
        ((pathString.length() >= 1) && (pathString.charAt(0) == '/'));

    // Is it a backslash-separated absolute path?
    if (windowsNonUriAbsolutePath1.matcher(pathString).find()) {
      // Forward slashes disallowed in a backslash-separated path.
      if (pathString.indexOf('/') != -1) {
        throw new @Tainted IOException("Invalid path string " + pathString);
      }

      pathString = pathString.replace('\\', '/');
      return "file:" + (slashed ? "" : "/") + pathString;
    }

    // Is it a forward slash-separated absolute path?
    if (windowsNonUriAbsolutePath2.matcher(pathString).find()) {
      return "file:" + (slashed ? "" : "/") + pathString;
    }

    // Is it a backslash-separated relative file path (no scheme and
    // no drive-letter specifier)?
    if ((pathString.indexOf(':') == -1) && (pathString.indexOf('\\') != -1)) {
      pathString = pathString.replace('\\', '/');
    }

    return pathString;
  }

  /** Construct a URI from a String with unescaped special characters
   *  that have non-standard semantics. e.g. /, ?, #. A custom parsing
   *  is needed to prevent misbehavior.
   *  @param pathString The input path in string form
   *  @return URI
   */
  private static @Tainted URI stringToUri(@Tainted String pathString) throws IOException {
    // We can't use 'new URI(String)' directly. Since it doesn't do quoting
    // internally, the internal parser may fail or break the string at wrong
    // places. Use of multi-argument ctors will quote those chars for us,
    // but we need to do our own parsing and assembly.
    
    // parse uri components
    @Tainted
    String scheme = null;
    @Tainted
    String authority = null;
    @Tainted
    int start = 0;

    pathString = normalizeWindowsPath(pathString);

    // parse uri scheme, if any
    @Tainted
    int colon = pathString.indexOf(':');
    @Tainted
    int slash = pathString.indexOf('/');
    if (colon > 0 && (slash == colon +1)) {
      // has a non zero-length scheme
      scheme = pathString.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathString.startsWith("//", start) &&
        (pathString.length()-start > 2)) {
      start += 2;
      @Tainted
      int nextSlash = pathString.indexOf('/', start);
      @Tainted
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start, authEnd);
      start = authEnd;
    }
    // uri path is the rest of the string. ? or # are not interpreted,
    // but any occurrence of them will be quoted by the URI ctor.
    @Tainted
    String path = pathString.substring(start, pathString.length());

    // Construct the URI
    try {
      return new @Tainted URI(scheme, authority, path, null, null);
    } catch (@Tainted URISyntaxException e) {
      throw new @Tainted IllegalArgumentException(e);
    }
  }

  @Override
  public @Tainted int compareTo(@Tainted PathData this, @Tainted PathData o) {
    return path.compareTo(((@Tainted PathData)o).path);
  }
  
  @Override
  public @Tainted boolean equals(@Tainted PathData this, @Tainted Object o) {
    return (o != null) &&
           (o instanceof @Tainted PathData) &&
           path.equals(((@Tainted PathData)o).path);
  }
  
  @Override
  public @Tainted int hashCode(@Tainted PathData this) {
    return path.hashCode();
  }
}
