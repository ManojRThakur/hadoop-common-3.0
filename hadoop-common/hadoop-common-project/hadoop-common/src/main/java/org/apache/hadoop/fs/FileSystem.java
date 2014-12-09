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
import org.checkerframework.checker.tainting.qual.Untainted;
import org.checkerframework.checker.tainting.qual.PolyTainted;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;

import com.google.common.annotations.VisibleForTesting;

/****************************************************************
 * An abstract base class for a fairly generic filesystem.  It
 * may be implemented as a distributed filesystem, or as a "local"
 * one that reflects the locally-connected disk.  The local version
 * exists for small Hadoop instances and for testing.
 *
 * <p>
 *
 * All user code that may potentially use the Hadoop Distributed
 * File System should be written to use a FileSystem object.  The
 * Hadoop DFS is a multi-machine system that appears as a single
 * disk.  It's useful because of its fault tolerance and potentially
 * very large capacity.
 * 
 * <p>
 * The local implementation is {@link LocalFileSystem} and distributed
 * implementation is DistributedFileSystem.
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileSystem extends @Tainted Configured implements @Tainted Closeable {
  public static final @Tainted String FS_DEFAULT_NAME_KEY = 
                   CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
  public static final @Tainted String DEFAULT_FS = 
                   CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT;

  public static final @Tainted Log LOG = LogFactory.getLog(FileSystem.class);

  /**
   * Priority of the FileSystem shutdown hook.
   */
  public static final @Tainted int SHUTDOWN_HOOK_PRIORITY = 10;

  /** FileSystem cache */
  static final @Tainted Cache CACHE = new @Tainted Cache();

  /** The key this instance is stored under in the cache. */
  private Cache.@Tainted Key key;

  /** Recording statistics per a FileSystem class */
  private static final @Tainted Map<@Tainted Class<@Tainted ? extends @Tainted FileSystem>, @Tainted Statistics> 
    statisticsTable =
      new @Tainted IdentityHashMap<@Tainted Class<@Tainted ? extends @Tainted FileSystem>, @Tainted Statistics>();
  
  /**
   * The statistics for this file system.
   */
  protected @Tainted Statistics statistics;

  /**
   * A cache of files that should be deleted when filsystem is closed
   * or the JVM is exited.
   */
  private @Tainted Set<@Tainted Path> deleteOnExit = new @Tainted TreeSet<@Tainted Path>();
  
  @Tainted
  boolean resolveSymlinks;
  /**
   * This method adds a file system for testing so that we can find it later. It
   * is only for testing.
   * @param uri the uri to store it under
   * @param conf the configuration to store it under
   * @param fs the file system to store
   * @throws IOException
   */
  static void addFileSystemForTesting(@Tainted URI uri, @Tainted Configuration conf,
      @Tainted
      FileSystem fs) throws IOException {
    CACHE.map.put(new Cache.@Tainted Key(uri, conf), fs);
  }

  /**
   * Get a filesystem instance based on the uri, the passed
   * configuration and the user
   * @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
   * @return the filesystem instance
   * @throws IOException
   * @throws InterruptedException
   */
  public static @Tainted FileSystem get(final @Tainted URI uri, final @Tainted Configuration conf,
        final @Tainted String user) throws IOException, InterruptedException {
    @Tainted
    String ticketCachePath =
      conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
    @Tainted
    UserGroupInformation ugi =
        UserGroupInformation.getBestUGI(ticketCachePath, user);
    return ugi.doAs(new @Tainted PrivilegedExceptionAction<@Tainted FileSystem>() {
      @Override
      public @Tainted FileSystem run() throws IOException {
        return get(uri, conf);
      }
    });
  }

  /**
   * Returns the configured filesystem implementation.
   * @param conf the configuration to use
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Untainted FileSystem get(@Tainted Configuration conf) throws IOException {
    return (@Untainted FileSystem) get(getDefaultUri(conf), conf);
  }
  
  /** Get the default filesystem URI from a configuration.
   * @param conf the configuration to use
   * @return the uri of the default filesystem
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @Untainted URI getDefaultUri(@Tainted Configuration conf) {
    return (@Untainted URI) URI.create(fixName(conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS)));
  }

  /** Set the default filesystem URI in a configuration.
   * @param conf the configuration to alter
   * @param uri the new default filesystem uri
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void setDefaultUri(@Tainted Configuration conf, @Untainted URI uri) {
    conf.set(FS_DEFAULT_NAME_KEY, (@Untainted String) uri.toString());
  }

  /** Set the default filesystem URI in a configuration.
   * @param conf the configuration to alter
   * @param uri the new default filesystem uri
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void setDefaultUri(@Tainted Configuration conf, @Untainted String uri) {
    setDefaultUri(conf, (@Untainted URI) URI.create(fixName(uri)));
  }

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  public void initialize(@Tainted FileSystem this, @Tainted URI name, @Tainted Configuration conf) throws IOException {
    statistics = getStatistics(name.getScheme(), getClass());    
    resolveSymlinks = conf.getBoolean(
        CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY,
        CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT);
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   * This implementation throws an <code>UnsupportedOperationException</code>.
   *
   * @return the protocol scheme for the FileSystem.
   */
  public @Tainted String getScheme(@Tainted FileSystem this) {
    throw new @Tainted UnsupportedOperationException("Not implemented by the " + getClass().getSimpleName() + " FileSystem implementation");
  }

  /** Returns a URI whose scheme and authority identify this FileSystem.*/
  public abstract @Tainted URI getUri(@Tainted FileSystem this);
  
  /**
   * Return a canonicalized form of this FileSystem's URI.
   * 
   * The default implementation simply calls {@link #canonicalizeUri(URI)}
   * on the filesystem's own URI, so subclasses typically only need to
   * implement that method.
   *
   * @see #canonicalizeUri(URI)
   */
  protected @Tainted URI getCanonicalUri(@Tainted FileSystem this) {
    return canonicalizeUri(getUri());
  }
  
  /**
   * Canonicalize the given URI.
   * 
   * This is filesystem-dependent, but may for example consist of
   * canonicalizing the hostname using DNS and adding the default
   * port if not specified.
   * 
   * The default implementation simply fills in the default port if
   * not specified and if the filesystem has a default port.
   *
   * @return URI
   * @see NetUtils#getCanonicalUri(URI, int)
   */
  @SuppressWarnings({"ostrusted:return.type.incompatible", "ostrusted:assignment.type.incompatible"})
  protected @PolyTainted URI canonicalizeUri(@Tainted FileSystem this, @PolyTainted URI uri) {
    if (uri.getPort() == -1 && getDefaultPort() > 0) {
      // reconstruct the uri with the default port set
      try {
        uri = new @Tainted URI(uri.getScheme(), uri.getUserInfo(),
            uri.getHost(), getDefaultPort(),
            uri.getPath(), uri.getQuery(), uri.getFragment());
      } catch (@Tainted URISyntaxException e) {
        // Should never happen!
        throw new @Tainted AssertionError("Valid URI became unparseable: " +
            uri);
      }
    }
    
    return uri;
  }
  
  /**
   * Get the default port for this file system.
   * @return the default port or 0 if there isn't one
   */
  protected @Tainted int getDefaultPort(@Tainted FileSystem this) {
    return 0;
  }

  protected static @Tainted FileSystem getFSofPath(final @Tainted Path absOrFqPath,
      final @Tainted Configuration conf)
      throws UnsupportedFileSystemException, IOException {
    absOrFqPath.checkNotSchemeWithRelative();
    absOrFqPath.checkNotRelative();

    // Uses the default file system if not fully qualified
    return get(absOrFqPath.toUri(), conf);
  }

  /**
   * Get a canonical service name for this file system.  The token cache is
   * the only user of the canonical service name, and uses it to lookup this
   * filesystem's service tokens.
   * If file system provides a token of its own then it must have a canonical
   * name, otherwise canonical name can be null.
   * 
   * Default Impl: If the file system has child file systems 
   * (such as an embedded file system) then it is assumed that the fs has no
   * tokens of its own and hence returns a null name; otherwise a service
   * name is built using Uri and port.
   * 
   * @return a service string that uniquely identifies this file system, null
   *         if the filesystem does not implement tokens
   * @see SecurityUtil#buildDTServiceName(URI, int) 
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
  public @Tainted String getCanonicalServiceName(@Tainted FileSystem this) {
    return (getChildFileSystems() == null)
      ? SecurityUtil.buildDTServiceName(getUri(), getDefaultPort())
      : null;
  }

  /** @deprecated call #getUri() instead.*/
  @Deprecated
  public @Tainted String getName(@Tainted FileSystem this) { return getUri().toString(); }

  /** @deprecated call #get(URI,Configuration) instead. */
  @Deprecated
  public static @Tainted FileSystem getNamed(@Tainted String name, @Tainted Configuration conf)
    throws IOException {
    return get(URI.create(fixName(name)), conf);
  }
  
  /** Update old-format filesystem names, for back-compatibility.  This should
   * eventually be replaced with a checkName() method that throws an exception
   * for old-format names. */ 
  private static @Tainted String fixName(@Tainted String name) {
    // convert old-format name to new-format name
    if (name.equals("local")) {         // "local" is now "file:///".
      LOG.warn("\"local\" is a deprecated filesystem name."
               +" Use \"file:///\" instead.");
      name = "file:///";
    } else if (name.indexOf('/')==-1) {   // unqualified is "hdfs://"
      LOG.warn("\""+name+"\" is a deprecated filesystem name."
               +" Use \"hdfs://"+name+"/\" instead.");
      name = "hdfs://"+name;
    }
    return name;
  }

  /**
   * Get the local file system.
   * @param conf the configuration to configure the file system with
   * @return a LocalFileSystem
   */
  public static @Tainted LocalFileSystem getLocal(@Tainted Configuration conf)
    throws IOException {
    return (@Tainted LocalFileSystem)get(LocalFileSystem.NAME, conf);
  }

  /** Returns the FileSystem for this URI's scheme and authority.  The scheme
   * of the URI determines a configuration property name,
   * <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
   * The entire URI is passed to the FileSystem instance's initialize method.
   */
  public static @Tainted FileSystem get(@Tainted URI uri, @Tainted Configuration conf) throws IOException {
    @Tainted
    String scheme = uri.getScheme();
    @Tainted
    String authority = uri.getAuthority();

    if (scheme == null && authority == null) {     // use default FS
      return get(conf);
    }

    if (scheme != null && authority == null) {     // no authority
      @Tainted
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return get(defaultUri, conf);              // return default
      }
    }
    
    @Tainted
    String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
    if (conf.getBoolean(disableCacheName, false)) {
      return createFileSystem(uri, conf);
    }

    return CACHE.get(uri, conf);
  }

  /**
   * Returns the FileSystem for this URI's scheme and authority and the 
   * passed user. Internally invokes {@link #newInstance(URI, Configuration)}
   * @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
   * @return filesystem instance
   * @throws IOException
   * @throws InterruptedException
   */
  public static @Tainted FileSystem newInstance(final @Tainted URI uri, final @Tainted Configuration conf,
      final @Tainted String user) throws IOException, InterruptedException {
    @Tainted
    String ticketCachePath =
      conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
    @Tainted
    UserGroupInformation ugi =
        UserGroupInformation.getBestUGI(ticketCachePath, user);
    return ugi.doAs(new @Tainted PrivilegedExceptionAction<@Tainted FileSystem>() {
      @Override
      public @Tainted FileSystem run() throws IOException {
        return newInstance(uri,conf); 
      }
    });
  }
  /** Returns the FileSystem for this URI's scheme and authority.  The scheme
   * of the URI determines a configuration property name,
   * <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
   * The entire URI is passed to the FileSystem instance's initialize method.
   * This always returns a new FileSystem object.
   */
  public static @Tainted FileSystem newInstance(@Tainted URI uri, @Tainted Configuration conf) throws IOException {
    @Tainted
    String scheme = uri.getScheme();
    @Tainted
    String authority = uri.getAuthority();

    if (scheme == null) {                       // no scheme: use default FS
      return newInstance(conf);
    }

    if (authority == null) {                       // no authority
      @Tainted
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return newInstance(defaultUri, conf);              // return default
      }
    }
    return CACHE.getUnique(uri, conf);
  }

  /** Returns a unique configured filesystem implementation.
   * This always returns a new FileSystem object.
   * @param conf the configuration to use
   */
  public static @Tainted FileSystem newInstance(@Tainted Configuration conf) throws IOException {
    return newInstance(getDefaultUri(conf), conf);
  }

  /**
   * Get a unique local file system object
   * @param conf the configuration to configure the file system with
   * @return a LocalFileSystem
   * This always returns a new FileSystem object.
   */
  public static @Tainted LocalFileSystem newInstanceLocal(@Tainted Configuration conf)
    throws IOException {
    return (@Tainted LocalFileSystem)newInstance(LocalFileSystem.NAME, conf);
  }

  /**
   * Close all cached filesystems. Be sure those filesystems are not
   * used anymore.
   * 
   * @throws IOException
   */
  public static void closeAll() throws IOException {
    CACHE.closeAll();
  }

  /**
   * Close all cached filesystems for a given UGI. Be sure those filesystems 
   * are not used anymore.
   * @param ugi user group info to close
   * @throws IOException
   */
  public static void closeAllForUGI(@Tainted UserGroupInformation ugi) 
  throws IOException {
    CACHE.closeAll(ugi);
  }

  /** 
   * Make sure that a path specifies a FileSystem.
   * @param path to use
   */
  public @Tainted Path makeQualified(@Tainted FileSystem this, @Tainted Path path) {
    checkPath(path);
    return path.makeQualified(this.getUri(), this.getWorkingDirectory());
  }
    
  /**
   * Get a new delegation token for this file system.
   * This is an internal method that should have been declared protected
   * but wasn't historically.
   * Callers should use {@link #addDelegationTokens(String, Credentials)}
   * 
   * @param renewer the account name that is allowed to renew the token.
   * @return a new delegation token
   * @throws IOException
   */
  @InterfaceAudience.Private()
  public @Tainted Token<?> getDelegationToken(@Tainted FileSystem this, @Tainted String renewer) throws IOException {
    return null;
  }
  
  /**
   * Obtain all delegation tokens used by this FileSystem that are not
   * already present in the given Credentials.  Existing tokens will neither
   * be verified as valid nor having the given renewer.  Missing tokens will
   * be acquired and added to the given Credentials.
   * 
   * Default Impl: works for simple fs with its own token
   * and also for an embedded fs whose tokens are those of its
   * children file system (i.e. the embedded fs has not tokens of its
   * own).
   * 
   * @param renewer the user allowed to renew the delegation tokens
   * @param credentials cache in which to add new delegation tokens
   * @return list of new delegation tokens
   * @throws IOException
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
  public @Tainted Token<?> @Tainted [] addDelegationTokens(
      @Tainted FileSystem this, final @Tainted String renewer, @Tainted Credentials credentials) throws IOException {
    if (credentials == null) {
      credentials = new @Tainted Credentials();
    }
    final @Tainted List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> tokens = new @Tainted ArrayList<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>>();
    collectDelegationTokens(renewer, credentials, tokens);
    return tokens.toArray(new Token<?> @Tainted [tokens.size()]);
  }
  
  /**
   * Recursively obtain the tokens for this FileSystem and all descended
   * FileSystems as determined by getChildFileSystems().
   * @param renewer the user allowed to renew the delegation tokens
   * @param credentials cache in which to add the new delegation tokens
   * @param tokens list in which to add acquired tokens
   * @throws IOException
   */
  private void collectDelegationTokens(@Tainted FileSystem this, final @Tainted String renewer,
                                       final @Tainted Credentials credentials,
                                       final @Tainted List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> tokens)
                                           throws IOException {
    final @Tainted String serviceName = getCanonicalServiceName();
    // Collect token of the this filesystem and then of its embedded children
    if (serviceName != null) { // fs has token, grab it
      final @Tainted Text service = new @Tainted Text(serviceName);
      @Tainted
      Token<@Tainted ? extends java.lang.@Tainted Object> token = credentials.getToken(service);
      if (token == null) {
        token = getDelegationToken(renewer);
        if (token != null) {
          tokens.add(token);
          credentials.addToken(service, token);
        }
      }
    }
    // Now collect the tokens from the children
    final @Tainted FileSystem @Tainted [] children = getChildFileSystems();
    if (children != null) {
      for (final @Tainted FileSystem fs : children) {
        fs.collectDelegationTokens(renewer, credentials, tokens);
      }
    }
  }

  /**
   * Get all the immediate child FileSystems embedded in this FileSystem.
   * It does not recurse and get grand children.  If a FileSystem
   * has multiple child FileSystems, then it should return a unique list
   * of those FileSystems.  Default is to return null to signify no children.
   * 
   * @return FileSystems used by this FileSystem
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS" })
  @VisibleForTesting
  public @Tainted FileSystem @Tainted [] getChildFileSystems(@Tainted FileSystem this) {
    return null;
  }
  
  /** create a file with the provided permission
   * The permission of the file is set to be the provided permission as in
   * setPermission, not permission&~umask
   * 
   * It is implemented using two RPCs. It is understood that it is inefficient,
   * but the implementation is thread-safe. The other option is to change the
   * value of umask in configuration to be 0, but it is not thread-safe.
   * 
   * @param fs file system handle
   * @param file the name of the file to be created
   * @param permission the permission of the file
   * @return an output stream
   * @throws IOException
   */
  public static @Tainted FSDataOutputStream create(@Tainted FileSystem fs,
      @Tainted
      Path file, @Tainted FsPermission permission) throws IOException {
    // create the file with default permission
    @Tainted
    FSDataOutputStream out = fs.create(file);
    // set its permission to the supplied one
    fs.setPermission(file, permission);
    return out;
  }

  /** create a directory with the provided permission
   * The permission of the directory is set to be the provided permission as in
   * setPermission, not permission&~umask
   * 
   * @see #create(FileSystem, Path, FsPermission)
   * 
   * @param fs file system handle
   * @param dir the name of the directory to be created
   * @param permission the permission of the directory
   * @return true if the directory creation succeeds; false otherwise
   * @throws IOException
   */
  public static @Tainted boolean mkdirs(@Tainted FileSystem fs, @Tainted Path dir, @Tainted FsPermission permission)
  throws IOException {
    // create the directory using the default permission
    @Tainted
    boolean result = fs.mkdirs(dir);
    // set its permission to be the supplied one
    fs.setPermission(dir, permission);
    return result;
  }

  ///////////////////////////////////////////////////////////////
  // FileSystem
  ///////////////////////////////////////////////////////////////

  protected @Tainted FileSystem() {
    super(null);
  }

  /** 
   * Check that a Path belongs to this FileSystem.
   * @param path to check
   */
  protected void checkPath(@Tainted FileSystem this, @Tainted Path path) {
    @Tainted
    URI uri = path.toUri();
    @Tainted
    String thatScheme = uri.getScheme();
    if (thatScheme == null)                // fs is relative
      return;
    @Tainted
    URI thisUri = getCanonicalUri();
    @Tainted
    String thisScheme = thisUri.getScheme();
    //authority and scheme are not case sensitive
    if (thisScheme.equalsIgnoreCase(thatScheme)) {// schemes match
      @Tainted
      String thisAuthority = thisUri.getAuthority();
      @Tainted
      String thatAuthority = uri.getAuthority();
      if (thatAuthority == null &&                // path's authority is null
          thisAuthority != null) {                // fs has an authority
        @Tainted
        URI defaultUri = getDefaultUri(getConf());
        if (thisScheme.equalsIgnoreCase(defaultUri.getScheme())) {
          uri = defaultUri; // schemes match, so use this uri instead
        } else {
          uri = null; // can't determine auth of the path
        }
      }
      if (uri != null) {
        // canonicalize uri before comparing with this fs
        uri = canonicalizeUri(uri);
        thatAuthority = uri.getAuthority();
        if (thisAuthority == thatAuthority ||       // authorities match
            (thisAuthority != null &&
             thisAuthority.equalsIgnoreCase(thatAuthority)))
          return;
      }
    }
    throw new @Tainted IllegalArgumentException("Wrong FS: "+path+
                                       ", expected: "+this.getUri());
  }

  /**
   * Return an array containing hostnames, offset and size of 
   * portions of the given file.  For a nonexistent 
   * file or regions, null will be returned.
   *
   * This call is most helpful with DFS, where it returns 
   * hostnames of machines that contain the given file.
   *
   * The FileSystem will simply return an elt containing 'localhost'.
   *
   * @param file FilesStatus to get data from
   * @param start offset into the given file
   * @param len length for which to get locations for
   */
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted FileSystem this, @Tainted FileStatus file, 
      @Tainted
      long start, @Tainted long len) throws IOException {
    if (file == null) {
      return null;
    }

    if (start < 0 || len < 0) {
      throw new @Tainted IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() <= start) {
      return new @Tainted BlockLocation @Tainted [0];

    }
    @Tainted
    String @Tainted [] name = new String @Tainted [] { "localhost:50010" };
    @Tainted
    String @Tainted [] host = new String @Tainted [] { "localhost" };
    return new @Tainted BlockLocation @Tainted [] {
      new @Tainted BlockLocation(name, host, 0, file.getLen()) };
  }
 

  /**
   * Return an array containing hostnames, offset and size of 
   * portions of the given file.  For a nonexistent 
   * file or regions, null will be returned.
   *
   * This call is most helpful with DFS, where it returns 
   * hostnames of machines that contain the given file.
   *
   * The FileSystem will simply return an elt containing 'localhost'.
   *
   * @param p path is used to identify an FS since an FS could have
   *          another FS that it could be delegating the call to
   * @param start offset into the given file
   * @param len length for which to get locations for
   */
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted FileSystem this, @Tainted Path p, 
      @Tainted
      long start, @Tainted long len) throws IOException {
    if (p == null) {
      throw new @Tainted NullPointerException();
    }
    @Tainted
    FileStatus file = getFileStatus(p);
    return getFileBlockLocations(file, start, len);
  }
  
  /**
   * Return a set of server default configuration values
   * @return server default configuration values
   * @throws IOException
   * @deprecated use {@link #getServerDefaults(Path)} instead
   */
  @Deprecated
  public @Tainted FsServerDefaults getServerDefaults(@Tainted FileSystem this) throws IOException {
    @Tainted
    Configuration conf = getConf();
    // CRC32 is chosen as default as it is available in all 
    // releases that support checksum.
    // The client trash configuration is ignored.
    return new @Tainted FsServerDefaults(getDefaultBlockSize(), 
        conf.getInt("io.bytes.per.checksum", 512), 
        64 * 1024, 
        getDefaultReplication(),
        conf.getInt("io.file.buffer.size", 4096),
        false,
        CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT,
        DataChecksum.Type.CRC32);
  }

  /**
   * Return a set of server default configuration values
   * @param p path is used to identify an FS since an FS could have
   *          another FS that it could be delegating the call to
   * @return server default configuration values
   * @throws IOException
   */
  public @Tainted FsServerDefaults getServerDefaults(@Tainted FileSystem this, @Tainted Path p) throws IOException {
    return getServerDefaults();
  }

  /**
   * Return the fully-qualified path of path f resolving the path
   * through any symlinks or mount point
   * @param p path to be resolved
   * @return fully qualified path 
   * @throws FileNotFoundException
   */
   public @Tainted Path resolvePath(@Tainted FileSystem this, final @Tainted Path p) throws IOException {
     checkPath(p);
     return getFileStatus(p).getPath();
   }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public abstract @Tainted FSDataInputStream open(@Tainted FileSystem this, @Tainted Path f, @Tainted int bufferSize)
    throws IOException;
    
  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file to open
   */
  public @Tainted FSDataInputStream open(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return open(f, getConf().getInt("io.file.buffer.size", 4096));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return create(f, true);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file to create
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an exception will be thrown.
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f, @Tainted boolean overwrite)
      throws IOException {
    return create(f, overwrite, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   * @param f the file to create
   * @param progress to report progress
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f, @Tainted Progressable progress) 
      throws IOException {
    return create(f, true, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f), progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   * @param replication the replication factor
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f, @Tainted short replication)
      throws IOException {
    return create(f, true, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  replication,
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   * @param f the file to create
   * @param replication the replication factor
   * @param progress to report progress
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f, @Tainted short replication, 
      @Tainted
      Progressable progress) throws IOException {
    return create(f, true, 
                  getConf().getInt(
                      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
                  replication,
                  getDefaultBlockSize(f), progress);
  }

    
  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file name to create
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f, 
                                   @Tainted
                                   boolean overwrite,
                                   @Tainted
                                   int bufferSize
                                   ) throws IOException {
    return create(f, overwrite, bufferSize, 
                  getDefaultReplication(f),
                  getDefaultBlockSize(f));
  }
    
  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the path of the file to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f, 
                                   @Tainted
                                   boolean overwrite,
                                   @Tainted
                                   int bufferSize,
                                   @Tainted
                                   Progressable progress
                                   ) throws IOException {
    return create(f, overwrite, bufferSize, 
                  getDefaultReplication(f),
                  getDefaultBlockSize(f), progress);
  }
    
    
  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file. 
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f, 
                                   @Tainted
                                   boolean overwrite,
                                   @Tainted
                                   int bufferSize,
                                   @Tainted
                                   short replication,
                                   @Tainted
                                   long blockSize
                                   ) throws IOException {
    return create(f, overwrite, bufferSize, replication, blockSize, null);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file. 
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f,
                                            @Tainted
                                            boolean overwrite,
                                            @Tainted
                                            int bufferSize,
                                            @Tainted
                                            short replication,
                                            @Tainted
                                            long blockSize,
                                            @Tainted
                                            Progressable progress
                                            ) throws IOException {
    return this.create(f, FsPermission.getFileDefault().applyUMask(
        FsPermission.getUMask(getConf())), overwrite, bufferSize,
        replication, blockSize, progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public abstract @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f,
      @Tainted
      FsPermission permission,
      @Tainted
      boolean overwrite,
      @Tainted
      int bufferSize,
      @Tainted
      short replication,
      @Tainted
      long blockSize,
      @Tainted
      Progressable progress) throws IOException;
  
  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f,
      @Tainted
      FsPermission permission,
      @Tainted
      EnumSet<@Tainted CreateFlag> flags,
      @Tainted
      int bufferSize,
      @Tainted
      short replication,
      @Tainted
      long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    return create(f, permission, flags, bufferSize, replication,
        blockSize, progress, null);
  }
  
  /**
   * Create an FSDataOutputStream at the indicated Path with a custom
   * checksum option
   * @param f the file name to open
   * @param permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @param checksumOpt checksum parameter. If null, the values
   *        found in conf will be used.
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public @Tainted FSDataOutputStream create(@Tainted FileSystem this, @Tainted Path f,
      @Tainted
      FsPermission permission,
      @Tainted
      EnumSet<@Tainted CreateFlag> flags,
      @Tainted
      int bufferSize,
      @Tainted
      short replication,
      @Tainted
      long blockSize,
      @Tainted
      Progressable progress,
      @Tainted
      ChecksumOpt checksumOpt) throws IOException {
    // Checksum options are ignored by default. The file systems that
    // implement checksum need to override this method. The full
    // support is currently only available in DFS.
    return create(f, permission, flags.contains(CreateFlag.OVERWRITE), 
        bufferSize, replication, blockSize, progress);
  }

  /*.
   * This create has been added to support the FileContext that processes
   * the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected @Tainted FSDataOutputStream primitiveCreate(@Tainted FileSystem this, @Tainted Path f,
     @Tainted
     FsPermission absolutePermission, @Tainted EnumSet<@Tainted CreateFlag> flag, @Tainted int bufferSize,
     @Tainted
     short replication, @Tainted long blockSize, @Tainted Progressable progress,
     @Tainted
     ChecksumOpt checksumOpt) throws IOException {

    @Tainted
    boolean pathExists = exists(f);
    CreateFlag.validate(f, pathExists, flag);
    
    // Default impl  assumes that permissions do not matter and 
    // nor does the bytesPerChecksum  hence
    // calling the regular create is good enough.
    // FSs that implement permissions should override this.

    if (pathExists && flag.contains(CreateFlag.APPEND)) {
      return append(f, bufferSize, progress);
    }
    
    return this.create(f, absolutePermission,
        flag.contains(CreateFlag.OVERWRITE), bufferSize, replication,
        blockSize, progress);
  }
  
  /**
   * This version of the mkdirs method assumes that the permission is absolute.
   * It has been added to support the FileContext that processes the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected @Tainted boolean primitiveMkdir(@Tainted FileSystem this, @Tainted Path f, @Tainted FsPermission absolutePermission)
    throws IOException {
    // Default impl is to assume that permissions do not matter and hence
    // calling the regular mkdirs is good enough.
    // FSs that implement permissions should override this.
   return this.mkdirs(f, absolutePermission);
  }


  /**
   * This version of the mkdirs method assumes that the permission is absolute.
   * It has been added to support the FileContext that processes the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected void primitiveMkdir(@Tainted FileSystem this, @Tainted Path f, @Tainted FsPermission absolutePermission, 
                    @Tainted
                    boolean createParent)
    throws IOException {
    
    if (!createParent) { // parent must exist.
      // since the this.mkdirs makes parent dirs automatically
      // we must throw exception if parent does not exist.
      final @Tainted FileStatus stat = getFileStatus(f.getParent());
      if (stat == null) {
        throw new @Tainted FileNotFoundException("Missing parent:" + f);
      }
      if (!stat.isDirectory()) {
        throw new @Tainted ParentNotDirectoryException("parent is not a dir");
      }
      // parent does exist - go ahead with mkdir of leaf
    }
    // Default impl is to assume that permissions do not matter and hence
    // calling the regular mkdirs is good enough.
    // FSs that implement permissions should override this.
    if (!this.mkdirs(f, absolutePermission)) {
      throw new @Tainted IOException("mkdir of "+ f + " failed");
    }
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting. Same as create(), except fails if parent directory doesn't
   * already exist.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   * the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   * @deprecated API only for 0.20-append
   */
  @Deprecated
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted FileSystem this, @Tainted Path f,
      @Tainted
      boolean overwrite,
      @Tainted
      int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    return this.createNonRecursive(f, FsPermission.getFileDefault(),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting. Same as create(), except fails if parent directory doesn't
   * already exist.
   * @param f the file name to open
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   * the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   * @deprecated API only for 0.20-append
   */
   @Deprecated
   public @Tainted FSDataOutputStream createNonRecursive(@Tainted FileSystem this, @Tainted Path f, @Tainted FsPermission permission,
       @Tainted
       boolean overwrite, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
       @Tainted
       Progressable progress) throws IOException {
     return createNonRecursive(f, permission,
         overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
             : EnumSet.of(CreateFlag.CREATE), bufferSize,
             replication, blockSize, progress);
   }

   /**
    * Opens an FSDataOutputStream at the indicated Path with write-progress
    * reporting. Same as create(), except fails if parent directory doesn't
    * already exist.
    * @param f the file name to open
    * @param permission
    * @param flags {@link CreateFlag}s to use for this stream.
    * @param bufferSize the size of the buffer to be used.
    * @param replication required block replication for the file.
    * @param blockSize
    * @param progress
    * @throws IOException
    * @see #setPermission(Path, FsPermission)
    * @deprecated API only for 0.20-append
    */
    @Deprecated
    public @Tainted FSDataOutputStream createNonRecursive(@Tainted FileSystem this, @Tainted Path f, @Tainted FsPermission permission,
        @Tainted
        EnumSet<@Tainted CreateFlag> flags, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
        @Tainted
        Progressable progress) throws IOException {
      throw new @Tainted IOException("createNonRecursive unsupported for this filesystem "
          + this.getClass());
    }

  /**
   * Creates the given Path as a brand-new zero-length file.  If
   * create fails, or if it already existed, return false.
   *
   * @param f path to use for create
   */
  public @Tainted boolean createNewFile(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    if (exists(f)) {
      return false;
    } else {
      create(f, false, getConf().getInt("io.file.buffer.size", 4096)).close();
      return true;
    }
  }

  /**
   * Append to an existing file (optional operation).
   * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
   * @param f the existing file to be appended.
   * @throws IOException
   */
  public @Tainted FSDataOutputStream append(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return append(f, getConf().getInt("io.file.buffer.size", 4096), null);
  }
  /**
   * Append to an existing file (optional operation).
   * Same as append(f, bufferSize, null).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException
   */
  public @Tainted FSDataOutputStream append(@Tainted FileSystem this, @Tainted Path f, @Tainted int bufferSize) throws IOException {
    return append(f, bufferSize, null);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException
   */
  public abstract @Tainted FSDataOutputStream append(@Tainted FileSystem this, @Tainted Path f, @Tainted int bufferSize,
      @Tainted
      Progressable progress) throws IOException;

  /**
   * Concat existing files together.
   * @param trg the path to the target destination.
   * @param psrcs the paths to the sources to use for the concatenation.
   * @throws IOException
   */
  public void concat(@Tainted FileSystem this, final @Tainted Path trg, final @Tainted Path @Tainted [] psrcs) throws IOException {
    throw new @Tainted UnsupportedOperationException("Not implemented by the " + 
        getClass().getSimpleName() + " FileSystem implementation");
  }

 /**
   * Get replication.
   * 
   * @deprecated Use getFileStatus() instead
   * @param src file name
   * @return file replication
   * @throws IOException
   */ 
  @Deprecated
  public @Tainted short getReplication(@Tainted FileSystem this, @Tainted Path src) throws IOException {
    return getFileStatus(src).getReplication();
  }

  /**
   * Set replication for an existing file.
   * 
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public @Tainted boolean setReplication(@Tainted FileSystem this, @Tainted Path src, @Tainted short replication)
    throws IOException {
    return true;
  }

  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   * @return true if rename is successful
   */
  public abstract @Tainted boolean rename(@Tainted FileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException;

  /**
   * Renames Path src to Path dst
   * <ul>
   * <li
   * <li>Fails if src is a file and dst is a directory.
   * <li>Fails if src is a directory and dst is a file.
   * <li>Fails if the parent of dst does not exist or is a file.
   * </ul>
   * <p>
   * If OVERWRITE option is not passed as an argument, rename fails
   * if the dst already exists.
   * <p>
   * If OVERWRITE option is passed as an argument, rename overwrites
   * the dst if it is a file or an empty directory. Rename fails if dst is
   * a non-empty directory.
   * <p>
   * Note that atomicity of rename is dependent on the file system
   * implementation. Please refer to the file system documentation for
   * details. This default implementation is non atomic.
   * <p>
   * This method is deprecated since it is a temporary method added to 
   * support the transition from FileSystem to FileContext for user 
   * applications.
   * 
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   */
  @Deprecated
  protected void rename(@Tainted FileSystem this, final @Tainted Path src, final @Tainted Path dst,
      final @Tainted Rename @Tainted ... options) throws IOException {
    // Default implementation
    final @Tainted FileStatus srcStatus = getFileLinkStatus(src);
    if (srcStatus == null) {
      throw new @Tainted FileNotFoundException("rename source " + src + " not found.");
    }

    @Tainted
    boolean overwrite = false;
    if (null != options) {
      for (@Tainted Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }

    @Tainted
    FileStatus dstStatus;
    try {
      dstStatus = getFileLinkStatus(dst);
    } catch (@Tainted IOException e) {
      dstStatus = null;
    }
    if (dstStatus != null) {
      if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
        throw new @Tainted IOException("Source " + src + " Destination " + dst
            + " both should be either file or directory");
      }
      if (!overwrite) {
        throw new @Tainted FileAlreadyExistsException("rename destination " + dst
            + " already exists.");
      }
      // Delete the destination that is a file or an empty directory
      if (dstStatus.isDirectory()) {
        @Tainted
        FileStatus @Tainted [] list = listStatus(dst);
        if (list != null && list.length != 0) {
          throw new @Tainted IOException(
              "rename cannot overwrite non empty destination directory " + dst);
        }
      }
      delete(dst, false);
    } else {
      final @Tainted Path parent = dst.getParent();
      final @Tainted FileStatus parentStatus = getFileStatus(parent);
      if (parentStatus == null) {
        throw new @Tainted FileNotFoundException("rename destination parent " + parent
            + " not found.");
      }
      if (!parentStatus.isDirectory()) {
        throw new @Tainted ParentNotDirectoryException("rename destination parent " + parent
            + " is a file.");
      }
    }
    if (!rename(src, dst)) {
      throw new @Tainted IOException("rename from " + src + " to " + dst + " failed.");
    }
  }
  
  /**
   * Delete a file 
   * @deprecated Use {@link #delete(Path, boolean)} instead.
   */
  @Deprecated
  public @Tainted boolean delete(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return delete(f, true);
  }
  
  /** Delete a file.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to 
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false. 
   * @return  true if delete is successful else false. 
   * @throws IOException
   */
  public abstract @Tainted boolean delete(@Tainted FileSystem this, @Tainted Path f, @Tainted boolean recursive) throws IOException;

  /**
   * Mark a path to be deleted when FileSystem is closed.
   * When the JVM shuts down,
   * all FileSystem objects will be closed automatically.
   * Then,
   * the marked path will be deleted as a result of closing the FileSystem.
   *
   * The path has to exist in the file system.
   * 
   * @param f the path to delete.
   * @return  true if deleteOnExit is successful, otherwise false.
   * @throws IOException
   */
  public @Tainted boolean deleteOnExit(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    if (!exists(f)) {
      return false;
    }
    synchronized (deleteOnExit) {
      deleteOnExit.add(f);
    }
    return true;
  }
  
  /**
   * Cancel the deletion of the path when the FileSystem is closed
   * @param f the path to cancel deletion
   */
  public @Tainted boolean cancelDeleteOnExit(@Tainted FileSystem this, @Tainted Path f) {
    synchronized (deleteOnExit) {
      return deleteOnExit.remove(f);
    }
  }

  /**
   * Delete all files that were marked as delete-on-exit. This recursively
   * deletes all files in the specified paths.
   */
  protected void processDeleteOnExit(@Tainted FileSystem this) {
    synchronized (deleteOnExit) {
      for (@Tainted Iterator<@Tainted Path> iter = deleteOnExit.iterator(); iter.hasNext();) {
        @Tainted
        Path path = iter.next();
        try {
          if (exists(path)) {
            delete(path, true);
          }
        }
        catch (@Tainted IOException e) {
          LOG.info("Ignoring failure to deleteOnExit for path " + path);
        }
        iter.remove();
      }
    }
  }
  
  /** Check if exists.
   * @param f source file
   */
  public @Tainted boolean exists(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    try {
      return getFileStatus(f) != null;
    } catch (@Tainted FileNotFoundException e) {
      return false;
    }
  }

  /** True iff the named path is a directory.
   * Note: Avoid using this method. Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   * @param f path to check
   */
  public @Tainted boolean isDirectory(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    try {
      return getFileStatus(f).isDirectory();
    } catch (@Tainted FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /** True iff the named path is a regular file.
   * Note: Avoid using this method. Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   * @param f path to check
   */
  public @Tainted boolean isFile(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    try {
      return getFileStatus(f).isFile();
    } catch (@Tainted FileNotFoundException e) {
      return false;               // f does not exist
    }
  }
  
  /** The number of bytes in a file. */
  /** @deprecated Use getFileStatus() instead */
  @Deprecated
  public @Tainted long getLength(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return getFileStatus(f).getLen();
  }
    
  /** Return the {@link ContentSummary} of a given {@link Path}.
  * @param f path to use
  */
  public @Tainted ContentSummary getContentSummary(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    @Tainted
    FileStatus status = getFileStatus(f);
    if (status.isFile()) {
      // f is a file
      return new @Tainted ContentSummary(status.getLen(), 1, 0);
    }
    // f is a directory
    @Tainted
    long @Tainted [] summary = new long @Tainted [] {0, 0, 1};
    for(@Tainted FileStatus s : listStatus(f)) {
      @Tainted
      ContentSummary c = s.isDirectory() ? getContentSummary(s.getPath()) :
                                     new @Tainted ContentSummary(s.getLen(), 1, 0);
      summary[0] += c.getLength();
      summary[1] += c.getFileCount();
      summary[2] += c.getDirectoryCount();
    }
    return new @Tainted ContentSummary(summary[0], summary[1], summary[2]);
  }

  final private static @Tainted PathFilter DEFAULT_FILTER = new @Tainted PathFilter() {
    @Override
    public @Tainted boolean accept(@Tainted Path file) {
      return true;
    }
  };
    
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public abstract @Tainted FileStatus @Tainted [] listStatus(@Tainted FileSystem this, @Tainted Path f) throws FileNotFoundException, 
                                                         IOException;
    
  /*
   * Filter files/directories in the given path using the user-supplied path
   * filter. Results are added to the given array <code>results</code>.
   */
  private void listStatus(@Tainted FileSystem this, @Tainted ArrayList<@Tainted FileStatus> results, @Tainted Path f,
      @Tainted
      PathFilter filter) throws FileNotFoundException, IOException {
    @Tainted
    FileStatus listing @Tainted [] = listStatus(f);
    if (listing == null) {
      throw new @Tainted IOException("Error accessing " + f);
    }

    for (@Tainted int i = 0; i < listing.length; i++) {
      if (filter.accept(listing[i].getPath())) {
        results.add(listing[i]);
      }
    }
  }

  /**
   * @return an iterator over the corrupt files under the given path
   * (may contain duplicates if a file has more than one corrupt block)
   * @throws IOException
   */
  public @Tainted RemoteIterator<@Tainted Path> listCorruptFileBlocks(@Tainted FileSystem this, @Tainted Path path)
    throws IOException {
    throw new @Tainted UnsupportedOperationException(getClass().getCanonicalName() +
                                            " does not support" +
                                            " listCorruptFileBlocks");
  }

  /**
   * Filter files/directories in the given path using the user-supplied path
   * filter.
   * 
   * @param f
   *          a path name
   * @param filter
   *          the user-supplied path filter
   * @return an array of FileStatus objects for the files under the given path
   *         after applying the filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation   
   */
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted FileSystem this, @Tainted Path f, @Tainted PathFilter filter) 
                                   throws FileNotFoundException, IOException {
    @Tainted
    ArrayList<@Tainted FileStatus> results = new @Tainted ArrayList<@Tainted FileStatus>();
    listStatus(results, f, filter);
    return results.toArray(new @Tainted FileStatus @Tainted [results.size()]);
  }

  /**
   * Filter files/directories in the given list of paths using default
   * path filter.
   * 
   * @param files
   *          a list of paths
   * @return a list of statuses for the files under the given paths after
   *         applying the filter default Path filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted FileSystem this, @Tainted Path @Tainted [] files)
      throws FileNotFoundException, IOException {
    return listStatus(files, DEFAULT_FILTER);
  }

  /**
   * Filter files/directories in the given list of paths using user-supplied
   * path filter.
   * 
   * @param files
   *          a list of paths
   * @param filter
   *          the user-supplied path filter
   * @return a list of statuses for the files under the given paths after
   *         applying the filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted FileSystem this, @Tainted Path @Tainted [] files, @Tainted PathFilter filter)
      throws FileNotFoundException, IOException {
    @Tainted
    ArrayList<@Tainted FileStatus> results = new @Tainted ArrayList<@Tainted FileStatus>();
    for (@Tainted int i = 0; i < files.length; i++) {
      listStatus(results, files[i], filter);
    }
    return results.toArray(new @Tainted FileStatus @Tainted [results.size()]);
  }

  /**
   * <p>Return all the files that match filePattern and are not checksum
   * files. Results are sorted by their names.
   * 
   * <p>
   * A filename pattern is composed of <i>regular</i> characters and
   * <i>special pattern matching</i> characters, which are:
   *
   * <dl>
   *  <dd>
   *   <dl>
   *    <p>
   *    <dt> <tt> ? </tt>
   *    <dd> Matches any single character.
   *
   *    <p>
   *    <dt> <tt> * </tt>
   *    <dd> Matches zero or more characters.
   *
   *    <p>
   *    <dt> <tt> [<i>abc</i>] </tt>
   *    <dd> Matches a single character from character set
   *     <tt>{<i>a,b,c</i>}</tt>.
   *
   *    <p>
   *    <dt> <tt> [<i>a</i>-<i>b</i>] </tt>
   *    <dd> Matches a single character from the character range
   *     <tt>{<i>a...b</i>}</tt>.  Note that character <tt><i>a</i></tt> must be
   *     lexicographically less than or equal to character <tt><i>b</i></tt>.
   *
   *    <p>
   *    <dt> <tt> [^<i>a</i>] </tt>
   *    <dd> Matches a single character that is not from character set or range
   *     <tt>{<i>a</i>}</tt>.  Note that the <tt>^</tt> character must occur
   *     immediately to the right of the opening bracket.
   *
   *    <p>
   *    <dt> <tt> \<i>c</i> </tt>
   *    <dd> Removes (escapes) any special meaning of character <i>c</i>.
   *
   *    <p>
   *    <dt> <tt> {ab,cd} </tt>
   *    <dd> Matches a string from the string set <tt>{<i>ab, cd</i>} </tt>
   *    
   *    <p>
   *    <dt> <tt> {ab,c{de,fh}} </tt>
   *    <dd> Matches a string from the string set <tt>{<i>ab, cde, cfh</i>}</tt>
   *
   *   </dl>
   *  </dd>
   * </dl>
   *
   * @param pathPattern a regular expression specifying a pth pattern

   * @return an array of paths that match the path pattern
   * @throws IOException
   */
  public @Tainted FileStatus @Tainted [] globStatus(@Tainted FileSystem this, @Tainted Path pathPattern) throws IOException {
    return new @Tainted Globber(this, pathPattern, DEFAULT_FILTER).glob();
  }
  
  /**
   * Return an array of FileStatus objects whose path names match
   * {@code pathPattern} and is accepted by the user-supplied path filter.
   * Results are sorted by their path names.
   * 
   * @param pathPattern a regular expression specifying the path pattern
   * @param filter a user-supplied path filter
   * @return null if {@code pathPattern} has no glob and the path does not exist
   *         an empty array if {@code pathPattern} has a glob and no path
   *         matches it else an array of {@link FileStatus} objects matching the
   *         pattern
   * @throws IOException if any I/O error occurs when fetching file status
   */
  public @Tainted FileStatus @Tainted [] globStatus(@Tainted FileSystem this, @Tainted Path pathPattern, @Tainted PathFilter filter)
      throws IOException {
    return new @Tainted Globber(this, pathPattern, filter).glob();
  }
  
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory. 
   * Return the file's status and block locations If the path is a file.
   * 
   * If a returned status is a file, it contains the file's block locations.
   * 
   * @param f is the path
   *
   * @return an iterator that traverses statuses of the files/directories 
   *         in the given path
   *
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws IOException If an I/O error occurred
   */
  public @Tainted RemoteIterator<@Tainted LocatedFileStatus> listLocatedStatus(@Tainted FileSystem this, final @Tainted Path f)
  throws FileNotFoundException, IOException {
    return listLocatedStatus(f, DEFAULT_FILTER);
  }

  /**
   * Listing a directory
   * The returned results include its block location if it is a file
   * The results are filtered by the given path filter
   * @param f a path
   * @param filter a path filter
   * @return an iterator that traverses statuses of the files/directories 
   *         in the given path
   * @throws FileNotFoundException if <code>f</code> does not exist
   * @throws IOException if any I/O error occurred
   */
  protected @Tainted RemoteIterator<@Tainted LocatedFileStatus> listLocatedStatus(@Tainted FileSystem this, final @Tainted Path f,
      final @Tainted PathFilter filter)
  /*throws FileNotFoundException, IOException*/ {
    return null;
    /*return new RemoteIterator<LocatedFileStatus>() {
      private final FileStatus[] stats = listStatus(f, filter);
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i<stats.length;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new NoSuchElementException("No more entry in " + f);
        }
        FileStatus result = stats[i++];
        BlockLocation[] locs = result.isFile() ?
            getFileBlockLocations(result.getPath(), 0, result.getLen()) :
            null;
        return new LocatedFileStatus(result, locs);
      }
    };*/
  }

  /**
   * List the statuses and block locations of the files in the given path.
   * 
   * If the path is a directory, 
   *   if recursive is false, returns files in the directory;
   *   if recursive is true, return files in the subtree rooted at the path.
   * If the path is a file, return the file's status and block locations.
   * 
   * @param f is the path
   * @param recursive if the subdirectories need to be traversed recursively
   *
   * @return an iterator that traverses statuses of the files
   *
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public @Tainted RemoteIterator<@Tainted LocatedFileStatus> listFiles(
      @Tainted FileSystem this, final @Tainted Path f, final @Tainted boolean recursive)
  throws FileNotFoundException, IOException {
    return new  @Tainted  RemoteIterator< @Tainted  LocatedFileStatus>() {
      private @Tainted Stack<@Tainted RemoteIterator< @Tainted  LocatedFileStatus>> itors =
        new Stack<RemoteIterator<LocatedFileStatus>>();
      private @Tainted RemoteIterator<@Tainted LocatedFileStatus> curItor =
        listLocatedStatus(f);
      private @Tainted LocatedFileStatus curFile;
     
      @Override
      public @Tainted boolean hasNext() throws IOException {
        while (curFile == null) {
          if (curItor.hasNext()) {
            handleFileStat(curItor.next());
          } else if (!itors.empty()) {
            curItor = itors.pop();
          } else {
            return false;
          }
        }
        return true;
      }

      /**
       * Process the input stat.
       * If it is a file, return the file stat.
       * If it is a directory, traverse the directory if recursive is true;
       * ignore it if recursive is false.
       * @param stat input status
       * @throws IOException if any IO error occurs
       */
      private void handleFileStat(@Tainted LocatedFileStatus stat) throws IOException {
        if (stat.isFile()) { // file
          curFile = stat;
        } else if (recursive) { // directory
          itors.push(curItor);
          curItor = listLocatedStatus(stat.getPath());
        }
      }

      @Override
      public @Tainted LocatedFileStatus next() throws IOException {
        if (hasNext()) {
          @Tainted
          LocatedFileStatus result = curFile;
          curFile = null;
          return result;
        } 
        throw new java.util.NoSuchElementException("No more entry in " + f);
      }
    };
  }
  
  /** Return the current user's home directory in this filesystem.
   * The default implementation returns "/user/$USER/".
   */
  public @Tainted Path getHomeDirectory(@Tainted FileSystem this) {
    return this.makeQualified(
        new @Tainted Path("/user/"+System.getProperty("user.name")));
  }


  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   * 
   * @param new_dir
   */
  public abstract void setWorkingDirectory(@Tainted FileSystem this, @Tainted Path new_dir);
    
  /**
   * Get the current working directory for the given file system
   * @return the directory pathname
   */
  public abstract @Tainted Path getWorkingDirectory(@Tainted FileSystem this);
  
  
  /**
   * Note: with the new FilesContext class, getWorkingDirectory()
   * will be removed. 
   * The working directory is implemented in FilesContext.
   * 
   * Some file systems like LocalFileSystem have an initial workingDir
   * that we use as the starting workingDir. For other file systems
   * like HDFS there is no built in notion of an initial workingDir.
   * 
   * @return if there is built in notion of workingDir then it
   * is returned; else a null is returned.
   */
  protected @Tainted Path getInitialWorkingDirectory(@Tainted FileSystem this) {
    return null;
  }

  /**
   * Call {@link #mkdirs(Path, FsPermission)} with default permission.
   */
  public @Tainted boolean mkdirs(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return mkdirs(f, FsPermission.getDirDefault());
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has the semantics of Unix 'mkdir -p'.
   * Existence of the directory hierarchy is not an error.
   * @param f path to create
   * @param permission to apply to f
   */
  public abstract @Tainted boolean mkdirs(@Tainted FileSystem this, @Tainted Path f, @Tainted FsPermission permission
      ) throws IOException;

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name and the source is kept intact afterwards
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(@Tainted FileSystem this, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    copyFromLocalFile(false, src, dst);
  }

  /**
   * The src files is on the local disk.  Add it to FS at
   * the given dst name, removing the source afterwards.
   * @param srcs path
   * @param dst path
   */
  public void moveFromLocalFile(@Tainted FileSystem this, @Tainted Path @Tainted [] srcs, @Tainted Path dst)
    throws IOException {
    copyFromLocalFile(true, true, srcs, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name, removing the source afterwards.
   * @param src path
   * @param dst path
   */
  public void moveFromLocalFile(@Tainted FileSystem this, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    copyFromLocalFile(true, src, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(@Tainted FileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    copyFromLocalFile(delSrc, true, src, dst);
  }
  
  /**
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param srcs array of paths which are source
   * @param dst path
   */
  public void copyFromLocalFile(@Tainted FileSystem this, @Tainted boolean delSrc, @Tainted boolean overwrite, 
                                @Tainted
                                Path @Tainted [] srcs, @Tainted Path dst)
    throws IOException {
    @Tainted
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), srcs, this, dst, delSrc, overwrite, conf);
  }
  
  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(@Tainted FileSystem this, @Tainted boolean delSrc, @Tainted boolean overwrite, 
                                @Tainted
                                Path src, @Tainted Path dst)
    throws IOException {
    @Tainted
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, overwrite, conf);
  }
    
  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * @param src path
   * @param dst path
   */
  public void copyToLocalFile(@Tainted FileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    copyToLocalFile(false, src, dst);
  }
    
  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * Remove the source afterwards
   * @param src path
   * @param dst path
   */
  public void moveToLocalFile(@Tainted FileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    copyToLocalFile(true, src, dst);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * delSrc indicates if the src will be removed or not.
   * @param delSrc whether to delete the src
   * @param src path
   * @param dst path
   */   
  public void copyToLocalFile(@Tainted FileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    copyToLocalFile(delSrc, src, dst, false);
  }
  
    /**
   * The src file is under FS, and the dst is on the local disk. Copy it from FS
   * control to the local dst name. delSrc indicates if the src will be removed
   * or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem
   * as local file system or not. RawLocalFileSystem is non crc file system.So,
   * It will not create any crc files at local.
   * 
   * @param delSrc
   *          whether to delete the src
   * @param src
   *          path
   * @param dst
   *          path
   * @param useRawLocalFileSystem
   *          whether to use RawLocalFileSystem as local file system or not.
   * 
   * @throws IOException
   *           - if any IO error
   */
  public void copyToLocalFile(@Tainted FileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst,
      @Tainted
      boolean useRawLocalFileSystem) throws IOException {
    @Tainted
    Configuration conf = getConf();
    @Tainted
    FileSystem local = null;
    if (useRawLocalFileSystem) {
      local = getLocal(conf).getRawFileSystem();
    } else {
      local = getLocal(conf);
    }
    FileUtil.copy(this, src, local, dst, delSrc, conf);
  }

  /**
   * Returns a local File that the user can write output to.  The caller
   * provides both the eventual FS target name and the local working
   * file.  If the FS is local, we write directly into the target.  If
   * the FS is remote, we write into the tmp local area.
   * @param fsOutputFile path of output file
   * @param tmpLocalFile path of local tmp file
   */
  public @Tainted Path startLocalOutput(@Tainted FileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  /**
   * Called when we're all done writing to the target.  A local FS will
   * do nothing, because we've written to exactly the right place.  A remote
   * FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   * @param fsOutputFile path of output file
   * @param tmpLocalFile path to local tmp file
   */
  public void completeLocalOutput(@Tainted FileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * No more filesystem operations are needed.  Will
   * release any held locks.
   */
  @Override
  public void close(@Tainted FileSystem this) throws IOException {
    // delete all files that were marked as delete-on-exit.
    processDeleteOnExit();
    CACHE.remove(this.key, this);
  }

  /** Return the total size of all files in the filesystem.*/
  public @Tainted long getUsed(@Tainted FileSystem this) throws IOException{
    @Tainted
    long used = 0;
    @Tainted
    FileStatus @Tainted [] files = listStatus(new @Tainted Path("/"));
    for(@Tainted FileStatus file:files){
      used += file.getLen();
    }
    return used;
  }
  
  /**
   * Get the block size for a particular file.
   * @param f the filename
   * @return the number of bytes in a block
   */
  /** @deprecated Use getFileStatus() instead */
  @Deprecated
  public @Tainted long getBlockSize(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return getFileStatus(f).getBlockSize();
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize i/o time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public @Tainted long getDefaultBlockSize(@Tainted FileSystem this) {
    // default to 32MB: large enough to minimize the impact of seeks
    return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
  }
    
  /** Return the number of bytes that large input files should be optimally
   * be split into to minimize i/o time.  The given path will be used to
   * locate the actual filesystem.  The full path does not have to exist.
   * @param f path of file
   * @return the default block size for the path's filesystem
   */
  public @Tainted long getDefaultBlockSize(@Tainted FileSystem this, @Tainted Path f) {
    return getDefaultBlockSize();
  }

  /**
   * Get the default replication.
   * @deprecated use {@link #getDefaultReplication(Path)} instead
   */
  @Deprecated
  public @Tainted short getDefaultReplication(@Tainted FileSystem this) { return 1; }

  /**
   * Get the default replication for a path.   The given path will be used to
   * locate the actual filesystem.  The full path does not have to exist.
   * @param path of the file
   * @return default replication for the path's filesystem 
   */
  public @Tainted short getDefaultReplication(@Tainted FileSystem this, @Tainted Path path) {
    return getDefaultReplication();
  }
  
  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public abstract @Tainted FileStatus getFileStatus(@Tainted FileSystem this, @Tainted Path f) throws IOException;

  /**
   * See {@link FileContext#fixRelativePart}
   */
  protected @Tainted Path fixRelativePart(@Tainted FileSystem this, @Tainted Path p) {
    if (p.isUriPathAbsolute()) {
      return p;
    } else {
      return new @Tainted Path(getWorkingDirectory(), p);
    }
  }

  /**
   * See {@link FileContext#createSymlink(Path, Path, boolean)}
   */
  public void createSymlink(@Tainted FileSystem this, final @Tainted Path target, final @Tainted Path link,
      final @Tainted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException {
    // Supporting filesystems should override this method
    throw new @Tainted UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * See {@link FileContext#getFileLinkStatus(Path)}
   */
  public @Tainted FileStatus getFileLinkStatus(@Tainted FileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    // Supporting filesystems should override this method
    return getFileStatus(f);
  }

  /**
   * See {@link AbstractFileSystem#supportsSymlinks()}
   */
  public @Tainted boolean supportsSymlinks(@Tainted FileSystem this) {
    return false;
  }

  /**
   * See {@link FileContext#getLinkTarget(Path)}
   */
  public @Tainted Path getLinkTarget(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    // Supporting filesystems should override this method
    throw new @Tainted UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * See {@link AbstractFileSystem#getLinkTarget(Path)}
   */
  protected @Tainted Path resolveLink(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    // Supporting filesystems should override this method
    throw new @Tainted UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * Get the checksum of a file.
   *
   * @param f The file path
   * @return The file checksum.  The default return value is null,
   *  which indicates that no checksum algorithm is implemented
   *  in the corresponding FileSystem.
   */
  public @Tainted FileChecksum getFileChecksum(@Tainted FileSystem this, @Tainted Path f) throws IOException {
    return null;
  }
  
  /**
   * Set the verify checksum flag. This is only applicable if the 
   * corresponding FileSystem supports checksum. By default doesn't do anything.
   * @param verifyChecksum
   */
  public void setVerifyChecksum(@Tainted FileSystem this, @Tainted boolean verifyChecksum) {
    //doesn't do anything
  }

  /**
   * Set the write checksum flag. This is only applicable if the 
   * corresponding FileSystem supports checksum. By default doesn't do anything.
   * @param writeChecksum
   */
  public void setWriteChecksum(@Tainted FileSystem this, @Tainted boolean writeChecksum) {
    //doesn't do anything
  }

  /**
   * Returns a status object describing the use and capacity of the
   * file system. If the file system has multiple partitions, the
   * use and capacity of the root partition is reflected.
   * 
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  public @Tainted FsStatus getStatus(@Tainted FileSystem this) throws IOException {
    return getStatus(null);
  }

  /**
   * Returns a status object describing the use and capacity of the
   * file system. If the file system has multiple partitions, the
   * use and capacity of the partition pointed to by the specified
   * path is reflected.
   * @param p Path for which status should be obtained. null means
   * the default partition. 
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  public @Tainted FsStatus getStatus(@Tainted FileSystem this, @Tainted Path p) throws IOException {
    return new @Tainted FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
  }

  /**
   * Set permission of a path.
   * @param p
   * @param permission
   */
  public void setPermission(@Tainted FileSystem this, @Tainted Path p, @Tainted FsPermission permission
      ) throws IOException {
  }

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  public void setOwner(@Tainted FileSystem this, @Tainted Path p, @Untainted String username, @Untainted String groupname
      ) throws IOException {
  }

  /**
   * Set access time of a file
   * @param p The path
   * @param mtime Set the modification time of this file.
   *              The number of milliseconds since Jan 1, 1970. 
   *              A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   *              The number of milliseconds since Jan 1, 1970. 
   *              A value of -1 means that this call should not set access time.
   */
  public void setTimes(@Tainted FileSystem this, @Tainted Path p, @Tainted long mtime, @Tainted long atime
      ) throws IOException {
  }

  /**
   * Create a snapshot with a default name.
   * @param path The directory where snapshots will be taken.
   * @return the snapshot path.
   */
  public final @Tainted Path createSnapshot(@Tainted FileSystem this, @Tainted Path path) throws IOException {
    return createSnapshot(path, null);
  }

  /**
   * Create a snapshot
   * @param path The directory where snapshots will be taken.
   * @param snapshotName The name of the snapshot
   * @return the snapshot path.
   */
  public @Tainted Path createSnapshot(@Tainted FileSystem this, @Tainted Path path, @Tainted String snapshotName)
      throws IOException {
    throw new @Tainted UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support createSnapshot");
  }
  
  /**
   * Rename a snapshot
   * @param path The directory path where the snapshot was taken
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   * @throws IOException
   */
  public void renameSnapshot(@Tainted FileSystem this, @Tainted Path path, @Tainted String snapshotOldName,
      @Tainted
      String snapshotNewName) throws IOException {
    throw new @Tainted UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support renameSnapshot");
  }
  
  /**
   * Delete a snapshot of a directory
   * @param path  The directory that the to-be-deleted snapshot belongs to
   * @param snapshotName The name of the snapshot
   */
  public void deleteSnapshot(@Tainted FileSystem this, @Tainted Path path, @Tainted String snapshotName)
      throws IOException {
    throw new @Tainted UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support deleteSnapshot");
  }
  
  // making it volatile to be able to do a double checked locking
  private volatile static @Tainted boolean FILE_SYSTEMS_LOADED = false;

  private static final @Tainted Map<@Tainted String, @Tainted Class<@Tainted ? extends @Tainted FileSystem>>
    SERVICE_FILE_SYSTEMS = new @Tainted HashMap<@Tainted String, @Tainted Class<@Tainted ? extends @Tainted FileSystem>>();

  private static void loadFileSystems() {
    synchronized (FileSystem.class) {
      if (!FILE_SYSTEMS_LOADED) {
        @Tainted
        ServiceLoader<@Tainted FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
        for (@Tainted FileSystem fs : serviceLoader) {
          SERVICE_FILE_SYSTEMS.put(fs.getScheme(), fs.getClass());
        }
        FILE_SYSTEMS_LOADED = true;
      }
    }
  }

  public static @Tainted Class<@Tainted ? extends @Tainted FileSystem> getFileSystemClass(@Tainted String scheme,
      @Tainted
      Configuration conf) throws IOException {
    if (!FILE_SYSTEMS_LOADED) {
      loadFileSystems();
    }
    @Tainted
    Class<@Tainted ? extends @Tainted FileSystem> clazz = null;
    if (conf != null) {
      clazz = (@Tainted Class<@Tainted ? extends @Tainted FileSystem>) conf.getClass("fs." + scheme + ".impl", null);
    }
    if (clazz == null) {
      clazz = SERVICE_FILE_SYSTEMS.get(scheme);
    }
    if (clazz == null) {
      throw new @Tainted IOException("No FileSystem for scheme: " + scheme);
    }
    return clazz;
  }

  private static @Tainted FileSystem createFileSystem(@Tainted URI uri, @Tainted Configuration conf
      ) throws IOException {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> clazz = getFileSystemClass(uri.getScheme(), conf);
    if (clazz == null) {
      throw new @Tainted IOException("No FileSystem for scheme: " + uri.getScheme());
    }
    @Tainted
    FileSystem fs = (@Tainted FileSystem)ReflectionUtils.newInstance(clazz, conf);
    fs.initialize(uri, conf);
    return fs;
  }

  /** Caching FileSystem objects */
  static class Cache {
    private final @Tainted ClientFinalizer clientFinalizer = new @Tainted ClientFinalizer();

    private final @Tainted Map<@Tainted Key, @Tainted FileSystem> map = new @Tainted HashMap<@Tainted Key, @Tainted FileSystem>();
    private final @Tainted Set<@Tainted Key> toAutoClose = new @Tainted HashSet<@Tainted Key>();

    /** A variable that makes all objects in the cache unique */
    private static @Tainted AtomicLong unique = new @Tainted AtomicLong(1);

    @Tainted
    FileSystem get(FileSystem.@Tainted Cache this, @Tainted URI uri, @Tainted Configuration conf) throws IOException{
      @Tainted
      Key key = new @Tainted Key(uri, conf);
      return getInternal(uri, conf, key);
    }

    /** The objects inserted into the cache using this method are all unique */
    @Tainted
    FileSystem getUnique(FileSystem.@Tainted Cache this, @Tainted URI uri, @Tainted Configuration conf) throws IOException{
      @Tainted
      Key key = new @Tainted Key(uri, conf, unique.getAndIncrement());
      return getInternal(uri, conf, key);
    }

    private @Tainted FileSystem getInternal(FileSystem.@Tainted Cache this, @Tainted URI uri, @Tainted Configuration conf, @Tainted Key key) throws IOException{
      @Tainted
      FileSystem fs;
      synchronized (this) {
        fs = map.get(key);
      }
      if (fs != null) {
        return fs;
      }

      fs = createFileSystem(uri, conf);
      synchronized (this) { // refetch the lock again
        @Tainted
        FileSystem oldfs = map.get(key);
        if (oldfs != null) { // a file system is created while lock is releasing
          fs.close(); // close the new file system
          return oldfs;  // return the old file system
        }
        
        // now insert the new file system into the map
        if (map.isEmpty()
                && !ShutdownHookManager.get().isShutdownInProgress()) {
          ShutdownHookManager.get().addShutdownHook(clientFinalizer, SHUTDOWN_HOOK_PRIORITY);
        }
        fs.key = key;
        map.put(key, fs);
        if (conf.getBoolean("fs.automatic.close", true)) {
          toAutoClose.add(key);
        }
        return fs;
      }
    }

    synchronized void remove(FileSystem.@Tainted Cache this, @Tainted Key key, @Tainted FileSystem fs) {
      if (map.containsKey(key) && fs == map.get(key)) {
        map.remove(key);
        toAutoClose.remove(key);
        }
    }

    synchronized void closeAll(FileSystem.@Tainted Cache this) throws IOException {
      closeAll(false);
    }

    /**
     * Close all FileSystem instances in the Cache.
     * @param onlyAutomatic only close those that are marked for automatic closing
     */
    synchronized void closeAll(FileSystem.@Tainted Cache this, @Tainted boolean onlyAutomatic) throws IOException {
      @Tainted
      List<@Tainted IOException> exceptions = new @Tainted ArrayList<@Tainted IOException>();

      // Make a copy of the keys in the map since we'll be modifying
      // the map while iterating over it, which isn't safe.
      @Tainted
      List<@Tainted Key> keys = new @Tainted ArrayList<@Tainted Key>();
      keys.addAll(map.keySet());

      for (@Tainted Key key : keys) {
        final @Tainted FileSystem fs = map.get(key);

        if (onlyAutomatic && !toAutoClose.contains(key)) {
          continue;
        }

        //remove from cache
        remove(key, fs);

        if (fs != null) {
          try {
            fs.close();
          }
          catch(@Tainted IOException ioe) {
            exceptions.add(ioe);
          }
        }
      }

      if (!exceptions.isEmpty()) {
        throw MultipleIOException.createIOException(exceptions);
      }
    }

    private class ClientFinalizer implements @Tainted Runnable {
      @Override
      public synchronized void run(FileSystem.@Tainted Cache.ClientFinalizer this) {
        try {
          closeAll(true);
        } catch (@Tainted IOException e) {
          LOG.info("FileSystem.Cache.closeAll() threw an exception:\n" + e);
        }
      }
    }

    synchronized void closeAll(FileSystem.@Tainted Cache this, @Tainted UserGroupInformation ugi) throws IOException {
      @Tainted
      List<@Tainted FileSystem> targetFSList = new @Tainted ArrayList<@Tainted FileSystem>();
      //Make a pass over the list and collect the filesystems to close
      //we cannot close inline since close() removes the entry from the Map
      for (Map.@Tainted Entry<@Tainted Key, @Tainted FileSystem> entry : map.entrySet()) {
        final @Tainted Key key = entry.getKey();
        final @Tainted FileSystem fs = entry.getValue();
        if (ugi.equals(key.ugi) && fs != null) {
          targetFSList.add(fs);   
        }
      }
      @Tainted
      List<@Tainted IOException> exceptions = new @Tainted ArrayList<@Tainted IOException>();
      //now make a pass over the target list and close each
      for (@Tainted FileSystem fs : targetFSList) {
        try {
          fs.close();
        }
        catch(@Tainted IOException ioe) {
          exceptions.add(ioe);
        }
      }
      if (!exceptions.isEmpty()) {
        throw MultipleIOException.createIOException(exceptions);
      }
    }

    /** FileSystem.Cache.Key */
    static class Key {
      final @Tainted String scheme;
      final @Tainted String authority;
      final @Tainted UserGroupInformation ugi;
      final @Tainted long unique;   // an artificial way to make a key unique

      @Tainted
      Key(@Tainted URI uri, @Tainted Configuration conf) throws IOException {
        this(uri, conf, 0);
      }

      @Tainted
      Key(@Tainted URI uri, @Tainted Configuration conf, @Tainted long unique) throws IOException {
        scheme = uri.getScheme()==null?"":uri.getScheme().toLowerCase();
        authority = uri.getAuthority()==null?"":uri.getAuthority().toLowerCase();
        this.unique = unique;
        
        this.ugi = UserGroupInformation.getCurrentUser();
      }

      @Override
      public @Tainted int hashCode(FileSystem.Cache.@Tainted Key this) {
        return (scheme + authority).hashCode() + ugi.hashCode() + (@Tainted int)unique;
      }

      static @Tainted boolean isEqual(@Tainted Object a, @Tainted Object b) {
        return a == b || (a != null && a.equals(b));        
      }

      @Override
      public @Tainted boolean equals(FileSystem.Cache.@Tainted Key this, @Tainted Object obj) {
        if (obj == this) {
          return true;
        }
        if (obj != null && obj instanceof @Tainted Key) {
          @Tainted
          Key that = (@Tainted Key)obj;
          return isEqual(this.scheme, that.scheme)
                 && isEqual(this.authority, that.authority)
                 && isEqual(this.ugi, that.ugi)
                 && (this.unique == that.unique);
        }
        return false;        
      }

      @Override
      public @Tainted String toString(FileSystem.Cache.@Tainted Key this) {
        return "("+ugi.toString() + ")@" + scheme + "://" + authority;        
      }
    }
  }
  
  public static final class Statistics {
    private final @Tainted String scheme;
    private @Tainted AtomicLong bytesRead = new @Tainted AtomicLong();
    private @Tainted AtomicLong bytesWritten = new @Tainted AtomicLong();
    private @Tainted AtomicInteger readOps = new @Tainted AtomicInteger();
    private @Tainted AtomicInteger largeReadOps = new @Tainted AtomicInteger();
    private @Tainted AtomicInteger writeOps = new @Tainted AtomicInteger();
    
    public @Tainted Statistics(@Tainted String scheme) {
      this.scheme = scheme;
    }

    /**
     * Copy constructor.
     * 
     * @param st
     *          The input Statistics object which is cloned.
     */
    public @Tainted Statistics(@Tainted Statistics st) {
      this.scheme = st.scheme;
      this.bytesRead = new @Tainted AtomicLong(st.bytesRead.longValue());
      this.bytesWritten = new @Tainted AtomicLong(st.bytesWritten.longValue());
    }

    /**
     * Increment the bytes read in the statistics
     * @param newBytes the additional bytes read
     */
    public void incrementBytesRead(FileSystem.@Tainted Statistics this, @Tainted long newBytes) {
      bytesRead.getAndAdd(newBytes);
    }
    
    /**
     * Increment the bytes written in the statistics
     * @param newBytes the additional bytes written
     */
    public void incrementBytesWritten(FileSystem.@Tainted Statistics this, @Tainted long newBytes) {
      bytesWritten.getAndAdd(newBytes);
    }
    
    /**
     * Increment the number of read operations
     * @param count number of read operations
     */
    public void incrementReadOps(FileSystem.@Tainted Statistics this, @Tainted int count) {
      readOps.getAndAdd(count);
    }

    /**
     * Increment the number of large read operations
     * @param count number of large read operations
     */
    public void incrementLargeReadOps(FileSystem.@Tainted Statistics this, @Tainted int count) {
      largeReadOps.getAndAdd(count);
    }

    /**
     * Increment the number of write operations
     * @param count number of write operations
     */
    public void incrementWriteOps(FileSystem.@Tainted Statistics this, @Tainted int count) {
      writeOps.getAndAdd(count);
    }

    /**
     * Get the total number of bytes read
     * @return the number of bytes
     */
    public @Tainted long getBytesRead(FileSystem.@Tainted Statistics this) {
      return bytesRead.get();
    }
    
    /**
     * Get the total number of bytes written
     * @return the number of bytes
     */
    public @Tainted long getBytesWritten(FileSystem.@Tainted Statistics this) {
      return bytesWritten.get();
    }
    
    /**
     * Get the number of file system read operations such as list files
     * @return number of read operations
     */
    public @Tainted int getReadOps(FileSystem.@Tainted Statistics this) {
      return readOps.get() + largeReadOps.get();
    }

    /**
     * Get the number of large file system read operations such as list files
     * under a large directory
     * @return number of large read operations
     */
    public @Tainted int getLargeReadOps(FileSystem.@Tainted Statistics this) {
      return largeReadOps.get();
    }

    /**
     * Get the number of file system write operations such as create, append 
     * rename etc.
     * @return number of write operations
     */
    public @Tainted int getWriteOps(FileSystem.@Tainted Statistics this) {
      return writeOps.get();
    }

    @Override
    public @Tainted String toString(FileSystem.@Tainted Statistics this) {
      return bytesRead + " bytes read, " + bytesWritten + " bytes written, "
          + readOps + " read ops, " + largeReadOps + " large read ops, "
          + writeOps + " write ops";
    }
    
    /**
     * Reset the counts of bytes to 0.
     */
    public void reset(FileSystem.@Tainted Statistics this) {
      bytesWritten.set(0);
      bytesRead.set(0);
    }
    
    /**
     * Get the uri scheme associated with this statistics object.
     * @return the schema associated with this set of statistics
     */
    public @Tainted String getScheme(FileSystem.@Tainted Statistics this) {
      return scheme;
    }
  }
  
  /**
   * Get the Map of Statistics object indexed by URI Scheme.
   * @return a Map having a key as URI scheme and value as Statistics object
   * @deprecated use {@link #getAllStatistics} instead
   */
  @Deprecated
  public static synchronized @Tainted Map<@Tainted String, @Tainted Statistics> getStatistics() {
    @Tainted
    Map<@Tainted String, @Tainted Statistics> result = new @Tainted HashMap<@Tainted String, @Tainted Statistics>();
    for(@Tainted Statistics stat: statisticsTable.values()) {
      result.put(stat.getScheme(), stat);
    }
    return result;
  }

  /**
   * Return the FileSystem classes that have Statistics
   */
  public static synchronized @Tainted List<@Tainted Statistics> getAllStatistics() {
    return new @Tainted ArrayList<@Tainted Statistics>(statisticsTable.values());
  }
  
  /**
   * Get the statistics for a particular file system
   * @param cls the class to lookup
   * @return a statistics object
   */
  public static synchronized 
  @Tainted
  Statistics getStatistics(@Tainted String scheme, @Tainted Class<@Tainted ? extends @Tainted FileSystem> cls) {
    @Tainted
    Statistics result = statisticsTable.get(cls);
    if (result == null) {
      result = new @Tainted Statistics(scheme);
      statisticsTable.put(cls, result);
    }
    return result;
  }
  
  /**
   * Reset all statistics for all file systems
   */
  public static synchronized void clearStatistics() {
    for(@Tainted Statistics stat: statisticsTable.values()) {
      stat.reset();
    }
  }

  /**
   * Print all statistics for all file systems
   */
  public static synchronized
  void printStatistics() throws IOException {
    for (Map.@Tainted Entry<@Tainted Class<@Tainted ? extends @Tainted FileSystem>, @Tainted Statistics> pair: 
            statisticsTable.entrySet()) {
      System.out.println("  FileSystem " + pair.getKey().getName() + 
                         ": " + pair.getValue());
    }
  }
}
