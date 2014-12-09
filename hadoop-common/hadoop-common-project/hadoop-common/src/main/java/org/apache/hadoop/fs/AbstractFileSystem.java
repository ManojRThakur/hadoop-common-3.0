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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * This class provides an interface for implementors of a Hadoop file system
 * (analogous to the VFS of Unix). Applications do not access this class;
 * instead they access files across all file systems using {@link FileContext}.
 * 
 * Pathnames passed to AbstractFileSystem can be fully qualified URI that
 * matches the "this" file system (ie same scheme and authority) 
 * or a Slash-relative name that is assumed to be relative
 * to the root of the "this" file system .
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class AbstractFileSystem {
  static final @Tainted Log LOG = LogFactory.getLog(AbstractFileSystem.class);

  /** Recording statistics per a file system class. */
  private static final @Tainted Map<@Tainted URI, @Tainted Statistics> 
      STATISTICS_TABLE = new @Tainted HashMap<@Tainted URI, @Tainted Statistics>();
  
  /** Cache of constructors for each file system class. */
  private static final @Tainted Map<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted Constructor<@Tainted ? extends java.lang.@Tainted Object>> CONSTRUCTOR_CACHE = 
    new @Tainted ConcurrentHashMap<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted Constructor<@Tainted ? extends java.lang.@Tainted Object>>();
  
  private static final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] URI_CONFIG_ARGS = 
    new @Tainted Class @Tainted []{URI.class, Configuration.class};
  
  /** The statistics for this file system. */
  protected @Tainted Statistics statistics;
  
  private final @Tainted URI myUri;
  
  public @Tainted Statistics getStatistics(@Tainted AbstractFileSystem this) {
    return statistics;
  }
  
  /**
   * Returns true if the specified string is considered valid in the path part
   * of a URI by this file system.  The default implementation enforces the rules
   * of HDFS, but subclasses may override this method to implement specific
   * validation rules for specific file systems.
   * 
   * @param src String source filename to check, path part of the URI
   * @return boolean true if the specified string is considered valid
   */
  public @Tainted boolean isValidName(@Tainted AbstractFileSystem this, @Tainted String src) {
    // Prohibit ".." "." and anything containing ":"
    @Tainted
    StringTokenizer tokens = new @Tainted StringTokenizer(src, Path.SEPARATOR);
    while(tokens.hasMoreTokens()) {
      @Tainted
      String element = tokens.nextToken();
      if (element.equals("..") ||
          element.equals(".")  ||
          (element.indexOf(":") >= 0)) {
        return false;
      }
    }
    return true;
  }
  
  /** 
   * Create an object for the given class and initialize it from conf.
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  static <@Tainted T extends java.lang.@Tainted Object> @Tainted T newInstance(@Tainted Class<@Tainted T> theClass,
    @Tainted
    URI uri, @Tainted Configuration conf) {
    @Tainted
    T result;
    try {
      @Tainted
      Constructor<@Tainted T> meth = (@Tainted Constructor<@Tainted T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(URI_CONFIG_ARGS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(uri, conf);
    } catch (@Tainted Exception e) {
      throw new @Tainted RuntimeException(e);
    }
    return result;
  }
  
  /**
   * Create a file system instance for the specified uri using the conf. The
   * conf is used to find the class name that implements the file system. The
   * conf is also passed to the file system for its configuration.
   *
   * @param uri URI of the file system
   * @param conf Configuration for the file system
   * 
   * @return Returns the file system for the given URI
   *
   * @throws UnsupportedFileSystemException file system for <code>uri</code> is
   *           not found
   */
  public static @Tainted AbstractFileSystem createFileSystem(@Tainted URI uri, @Tainted Configuration conf)
      throws UnsupportedFileSystemException {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> clazz = conf.getClass("fs.AbstractFileSystem." + 
                                uri.getScheme() + ".impl", null);
    if (clazz == null) {
      throw new @Tainted UnsupportedFileSystemException(
          "No AbstractFileSystem for scheme: " + uri.getScheme());
    }
    return (@Tainted AbstractFileSystem) newInstance(clazz, uri, conf);
  }

  /**
   * Get the statistics for a particular file system.
   * 
   * @param uri
   *          used as key to lookup STATISTICS_TABLE. Only scheme and authority
   *          part of the uri are used.
   * @return a statistics object
   */
  protected static synchronized @Tainted Statistics getStatistics(@Tainted URI uri) {
    @Tainted
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new @Tainted IllegalArgumentException("Scheme not defined in the uri: "
          + uri);
    }
    @Tainted
    URI baseUri = getBaseUri(uri);
    @Tainted
    Statistics result = STATISTICS_TABLE.get(baseUri);
    if (result == null) {
      result = new @Tainted Statistics(scheme);
      STATISTICS_TABLE.put(baseUri, result);
    }
    return result;
  }
  
  private static @Tainted URI getBaseUri(@Tainted URI uri) {
    @Tainted
    String scheme = uri.getScheme();
    @Tainted
    String authority = uri.getAuthority();
    @Tainted
    String baseUriString = scheme + "://";
    if (authority != null) {
      baseUriString = baseUriString + authority;
    } else {
      baseUriString = baseUriString + "/";
    }
    return URI.create(baseUriString);
  }
  
  public static synchronized void clearStatistics() {
    for(@Tainted Statistics stat: STATISTICS_TABLE.values()) {
      stat.reset();
    }
  }

  /**
   * Prints statistics for all file systems.
   */
  public static synchronized void printStatistics() {
    for (Map.@Tainted Entry<@Tainted URI, @Tainted Statistics> pair : STATISTICS_TABLE.entrySet()) {
      System.out.println("  FileSystem " + pair.getKey().getScheme() + "://"
          + pair.getKey().getAuthority() + ": " + pair.getValue());
    }
  }
  
  protected static synchronized @Tainted Map<@Tainted URI, @Tainted Statistics> getAllStatistics() {
    @Tainted
    Map<@Tainted URI, @Tainted Statistics> statsMap = new @Tainted HashMap<@Tainted URI, @Tainted Statistics>(
        STATISTICS_TABLE.size());
    for (Map.@Tainted Entry<@Tainted URI, @Tainted Statistics> pair : STATISTICS_TABLE.entrySet()) {
      @Tainted
      URI key = pair.getKey();
      @Tainted
      Statistics value = pair.getValue();
      @Tainted
      Statistics newStatsObj = new @Tainted Statistics(value);
      statsMap.put(URI.create(key.toString()), newStatsObj);
    }
    return statsMap;
  }

  /**
   * The main factory method for creating a file system. Get a file system for
   * the URI's scheme and authority. The scheme of the <code>uri</code>
   * determines a configuration property name,
   * <tt>fs.AbstractFileSystem.<i>scheme</i>.impl</tt> whose value names the
   * AbstractFileSystem class.
   * 
   * The entire URI and conf is passed to the AbstractFileSystem factory method.
   * 
   * @param uri for the file system to be created.
   * @param conf which is passed to the file system impl.
   * 
   * @return file system for the given URI.
   * 
   * @throws UnsupportedFileSystemException if the file system for
   *           <code>uri</code> is not supported.
   */
  public static @Tainted AbstractFileSystem get(final @Tainted URI uri, final @Tainted Configuration conf)
      throws UnsupportedFileSystemException {
    return createFileSystem(uri, conf);
  }

  /**
   * Constructor to be called by subclasses.
   * 
   * @param uri for this file system.
   * @param supportedScheme the scheme supported by the implementor
   * @param authorityNeeded if true then theURI must have authority, if false
   *          then the URI must have null authority.
   *
   * @throws URISyntaxException <code>uri</code> has syntax error
   */
  public @Tainted AbstractFileSystem(final @Tainted URI uri, final @Tainted String supportedScheme,
      final @Tainted boolean authorityNeeded, final @Tainted int defaultPort)
      throws URISyntaxException {
    myUri = getUri(uri, supportedScheme, authorityNeeded, defaultPort);
    statistics = getStatistics(uri); 
  }
  
  /**
   * Check that the Uri's scheme matches
   * @param uri
   * @param supportedScheme
   */
  public void checkScheme(@Tainted AbstractFileSystem this, @Tainted URI uri, @Tainted String supportedScheme) {
    @Tainted
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new @Tainted HadoopIllegalArgumentException("Uri without scheme: " + uri);
    }
    if (!scheme.equals(supportedScheme)) {
      throw new @Tainted HadoopIllegalArgumentException("Uri scheme " + uri
          + " does not match the scheme " + supportedScheme);
    }
  }

  /**
   * Get the URI for the file system based on the given URI. The path, query
   * part of the given URI is stripped out and default file system port is used
   * to form the URI.
   * 
   * @param uri FileSystem URI.
   * @param authorityNeeded if true authority cannot be null in the URI. If
   *          false authority must be null.
   * @param defaultPort default port to use if port is not specified in the URI.
   * 
   * @return URI of the file system
   * 
   * @throws URISyntaxException <code>uri</code> has syntax error
   */
  private @Tainted URI getUri(@Tainted AbstractFileSystem this, @Tainted URI uri, @Tainted String supportedScheme,
      @Tainted
      boolean authorityNeeded, @Tainted int defaultPort) throws URISyntaxException {
    checkScheme(uri, supportedScheme);
    // A file system implementation that requires authority must always
    // specify default port
    if (defaultPort < 0 && authorityNeeded) {
      throw new @Tainted HadoopIllegalArgumentException(
          "FileSystem implementation error -  default port " + defaultPort
              + " is not valid");
    }
    @Tainted
    String authority = uri.getAuthority();
    if (authority == null) {
       if (authorityNeeded) {
         throw new @Tainted HadoopIllegalArgumentException("Uri without authority: " + uri);
       } else {
         return new @Tainted URI(supportedScheme + ":///");
       }   
    }
    // authority is non null  - AuthorityNeeded may be true or false.
    @Tainted
    int port = uri.getPort();
    port = (port == -1 ? defaultPort : port);
    if (port == -1) { // no port supplied and default port is not specified
      return new @Tainted URI(supportedScheme, authority, "/", null);
    }
    return new @Tainted URI(supportedScheme + "://" + uri.getHost() + ":" + port);
  }
  
  /**
   * The default port of this file system.
   * 
   * @return default port of this file system's Uri scheme
   *         A uri with a port of -1 => default port;
   */
  public abstract @Tainted int getUriDefaultPort(@Tainted AbstractFileSystem this);

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   * 
   * @return the uri of this file system.
   */
  public @Tainted URI getUri(@Tainted AbstractFileSystem this) {
    return myUri;
  }
  
  /**
   * Check that a Path belongs to this FileSystem.
   * 
   * If the path is fully qualified URI, then its scheme and authority
   * matches that of this file system. Otherwise the path must be 
   * slash-relative name.
   * 
   * @throws InvalidPathException if the path is invalid
   */
  public void checkPath(@Tainted AbstractFileSystem this, @Tainted Path path) {
    @Tainted
    URI uri = path.toUri();
    @Tainted
    String thatScheme = uri.getScheme();
    @Tainted
    String thatAuthority = uri.getAuthority();
    if (thatScheme == null) {
      if (thatAuthority == null) {
        if (path.isUriPathAbsolute()) {
          return;
        }
        throw new @Tainted InvalidPathException("relative paths not allowed:" + 
            path);
      } else {
        throw new @Tainted InvalidPathException(
            "Path without scheme with non-null authority:" + path);
      }
    }
    @Tainted
    String thisScheme = this.getUri().getScheme();
    @Tainted
    String thisHost = this.getUri().getHost();
    @Tainted
    String thatHost = uri.getHost();
    
    // Schemes and hosts must match.
    // Allow for null Authority for file:///
    if (!thisScheme.equalsIgnoreCase(thatScheme) ||
       (thisHost != null && 
            !thisHost.equalsIgnoreCase(thatHost)) ||
       (thisHost == null && thatHost != null)) {
      throw new @Tainted InvalidPathException("Wrong FS: " + path + ", expected: "
          + this.getUri());
    }
    
    // Ports must match, unless this FS instance is using the default port, in
    // which case the port may be omitted from the given URI
    @Tainted
    int thisPort = this.getUri().getPort();
    @Tainted
    int thatPort = uri.getPort();
    if (thatPort == -1) { // -1 => defaultPort of Uri scheme
      thatPort = this.getUriDefaultPort();
    }
    if (thisPort != thatPort) {
      throw new @Tainted InvalidPathException("Wrong FS: " + path + ", expected: "
          + this.getUri());
    }
  }
  
  /**
   * Get the path-part of a pathname. Checks that URI matches this file system
   * and that the path-part is a valid name.
   * 
   * @param p path
   * 
   * @return path-part of the Path p
   */
  public @Tainted String getUriPath(@Tainted AbstractFileSystem this, final @Tainted Path p) {
    checkPath(p);
    @Tainted
    String s = p.toUri().getPath();
    if (!isValidName(s)) {
      throw new @Tainted InvalidPathException("Path part " + s + " from URI " + p
          + " is not a valid filename.");
    }
    return s;
  }
  
  /**
   * Make the path fully qualified to this file system
   * @param path
   * @return the qualified path
   */
  public @Tainted Path makeQualified(@Tainted AbstractFileSystem this, @Tainted Path path) {
    checkPath(path);
    return path.makeQualified(this.getUri(), null);
  }
  
  /**
   * Some file systems like LocalFileSystem have an initial workingDir
   * that is used as the starting workingDir. For other file systems
   * like HDFS there is no built in notion of an initial workingDir.
   * 
   * @return the initial workingDir if the file system has such a notion
   *         otherwise return a null.
   */
  public @Tainted Path getInitialWorkingDirectory(@Tainted AbstractFileSystem this) {
    return null;
  }
  
  /** 
   * Return the current user's home directory in this file system.
   * The default implementation returns "/user/$USER/".
   * 
   * @return current user's home directory.
   */
  public @Tainted Path getHomeDirectory(@Tainted AbstractFileSystem this) {
    return new @Tainted Path("/user/"+System.getProperty("user.name")).makeQualified(
                                                                getUri(), null);
  }
  
  /**
   * Return a set of server default configuration values.
   * 
   * @return server default configuration values
   * 
   * @throws IOException an I/O error occurred
   */
  public abstract @Tainted FsServerDefaults getServerDefaults(@Tainted AbstractFileSystem this) throws IOException; 

  /**
   * Return the fully-qualified path of path f resolving the path
   * through any internal symlinks or mount point
   * @param p path to be resolved
   * @return fully qualified path 
   * @throws FileNotFoundException, AccessControlException, IOException
   *         UnresolvedLinkException if symbolic link on path cannot be resolved
   *          internally
   */
   public @Tainted Path resolvePath(@Tainted AbstractFileSystem this, final @Tainted Path p) throws FileNotFoundException,
           UnresolvedLinkException, AccessControlException, IOException {
     checkPath(p);
     return getFileStatus(p).getPath(); // default impl is to return the path
   }
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#create(Path, EnumSet, Options.CreateOpts...)} except
   * that the Path f must be fully qualified and the permission is absolute
   * (i.e. umask has been applied).
   */
  public final @Tainted FSDataOutputStream create(@Tainted AbstractFileSystem this, final @Tainted Path f,
      final @Tainted EnumSet<@Tainted CreateFlag> createFlag, Options.@Tainted CreateOpts @Tainted ... opts)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, UnresolvedLinkException, IOException {
    checkPath(f);
    @Tainted
    int bufferSize = -1;
    @Tainted
    short replication = -1;
    @Tainted
    long blockSize = -1;
    @Tainted
    int bytesPerChecksum = -1;
    @Tainted
    ChecksumOpt checksumOpt = null;
    @Tainted
    FsPermission permission = null;
    @Tainted
    Progressable progress = null;
    @Tainted
    Boolean createParent = null;
 
    for (@Tainted CreateOpts iOpt : opts) {
      if (CreateOpts.BlockSize.class.isInstance(iOpt)) {
        if (blockSize != -1) {
          throw new @Tainted HadoopIllegalArgumentException(
              "BlockSize option is set multiple times");
        }
        blockSize = ((CreateOpts.@Tainted BlockSize) iOpt).getValue();
      } else if (CreateOpts.BufferSize.class.isInstance(iOpt)) {
        if (bufferSize != -1) {
          throw new @Tainted HadoopIllegalArgumentException(
              "BufferSize option is set multiple times");
        }
        bufferSize = ((CreateOpts.@Tainted BufferSize) iOpt).getValue();
      } else if (CreateOpts.ReplicationFactor.class.isInstance(iOpt)) {
        if (replication != -1) {
          throw new @Tainted HadoopIllegalArgumentException(
              "ReplicationFactor option is set multiple times");
        }
        replication = ((CreateOpts.@Tainted ReplicationFactor) iOpt).getValue();
      } else if (CreateOpts.BytesPerChecksum.class.isInstance(iOpt)) {
        if (bytesPerChecksum != -1) {
          throw new @Tainted HadoopIllegalArgumentException(
              "BytesPerChecksum option is set multiple times");
        }
        bytesPerChecksum = ((CreateOpts.@Tainted BytesPerChecksum) iOpt).getValue();
      } else if (CreateOpts.ChecksumParam.class.isInstance(iOpt)) {
        if (checksumOpt != null) {
          throw new  @Tainted HadoopIllegalArgumentException(
              "CreateChecksumType option is set multiple times");
        }
        checksumOpt = ((CreateOpts.@Tainted ChecksumParam) iOpt).getValue();
      } else if (CreateOpts.Perms.class.isInstance(iOpt)) {
        if (permission != null) {
          throw new @Tainted HadoopIllegalArgumentException(
              "Perms option is set multiple times");
        }
        permission = ((CreateOpts.@Tainted Perms) iOpt).getValue();
      } else if (CreateOpts.Progress.class.isInstance(iOpt)) {
        if (progress != null) {
          throw new @Tainted HadoopIllegalArgumentException(
              "Progress option is set multiple times");
        }
        progress = ((CreateOpts.@Tainted Progress) iOpt).getValue();
      } else if (CreateOpts.CreateParent.class.isInstance(iOpt)) {
        if (createParent != null) {
          throw new @Tainted HadoopIllegalArgumentException(
              "CreateParent option is set multiple times");
        }
        createParent = ((CreateOpts.@Tainted CreateParent) iOpt).getValue();
      } else {
        throw new @Tainted HadoopIllegalArgumentException("Unkown CreateOpts of type " +
            iOpt.getClass().getName());
      }
    }
    if (permission == null) {
      throw new @Tainted HadoopIllegalArgumentException("no permission supplied");
    }


    @Tainted
    FsServerDefaults ssDef = getServerDefaults();
    if (ssDef.getBlockSize() % ssDef.getBytesPerChecksum() != 0) {
      throw new @Tainted IOException("Internal error: default blockSize is" + 
          " not a multiple of default bytesPerChecksum ");
    }
    
    if (blockSize == -1) {
      blockSize = ssDef.getBlockSize();
    }

    // Create a checksum option honoring user input as much as possible.
    // If bytesPerChecksum is specified, it will override the one set in
    // checksumOpt. Any missing value will be filled in using the default.
    @Tainted
    ChecksumOpt defaultOpt = new @Tainted ChecksumOpt(
        ssDef.getChecksumType(),
        ssDef.getBytesPerChecksum());
    checksumOpt = ChecksumOpt.processChecksumOpt(defaultOpt,
        checksumOpt, bytesPerChecksum);

    if (bufferSize == -1) {
      bufferSize = ssDef.getFileBufferSize();
    }
    if (replication == -1) {
      replication = ssDef.getReplication();
    }
    if (createParent == null) {
      createParent = false;
    }

    if (blockSize % bytesPerChecksum != 0) {
      throw new @Tainted HadoopIllegalArgumentException(
             "blockSize should be a multiple of checksumsize");
    }

    return this.createInternal(f, createFlag, permission, bufferSize,
      replication, blockSize, progress, checksumOpt, createParent);
  }

  /**
   * The specification of this method matches that of
   * {@link #create(Path, EnumSet, Options.CreateOpts...)} except that the opts
   * have been declared explicitly.
   */
  public abstract @Tainted FSDataOutputStream createInternal(@Tainted AbstractFileSystem this, @Tainted Path f,
      @Tainted
      EnumSet<@Tainted CreateFlag> flag, @Tainted FsPermission absolutePermission,
      @Tainted
      int bufferSize, @Tainted short replication, @Tainted long blockSize, @Tainted Progressable progress,
      @Tainted
      ChecksumOpt checksumOpt, @Tainted boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#mkdir(Path, FsPermission, boolean)} except that the Path
   * f must be fully qualified and the permission is absolute (i.e. 
   * umask has been applied).
   */
  public abstract void mkdir(@Tainted AbstractFileSystem this, final @Tainted Path dir, final @Tainted FsPermission permission,
      final @Tainted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#delete(Path, boolean)} except that Path f must be for
   * this file system.
   */
  public abstract @Tainted boolean delete(@Tainted AbstractFileSystem this, final @Tainted Path f, final @Tainted boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#open(Path)} except that Path f must be for this
   * file system.
   */
  public @Tainted FSDataInputStream open(@Tainted AbstractFileSystem this, final @Tainted Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    return open(f, getServerDefaults().getFileBufferSize());
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext#open(Path, int)} except that Path f must be for this
   * file system.
   */
  public abstract @Tainted FSDataInputStream open(@Tainted AbstractFileSystem this, final @Tainted Path f, @Tainted int bufferSize)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setReplication(Path, short)} except that Path f must be
   * for this file system.
   */
  public abstract @Tainted boolean setReplication(@Tainted AbstractFileSystem this, final @Tainted Path f,
      final @Tainted short replication) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this file system.
   */
  public final void rename(@Tainted AbstractFileSystem this, final @Tainted Path src, final @Tainted Path dst,
      final Options.@Tainted Rename @Tainted ... options) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    @Tainted
    boolean overwrite = false;
    if (null != options) {
      for (@Tainted Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    renameInternal(src, dst, overwrite);
  }
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this file system and NO OVERWRITE is performed.
   * 
   * File systems that do not have a built in overwrite need implement only this
   * method and can take advantage of the default impl of the other
   * {@link #renameInternal(Path, Path, boolean)}
   */
  public abstract void renameInternal(@Tainted AbstractFileSystem this, final @Tainted Path src, final @Tainted Path dst)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnresolvedLinkException, IOException;
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this file system.
   */
  public void renameInternal(@Tainted AbstractFileSystem this, final @Tainted Path src, final @Tainted Path dst,
      @Tainted
      boolean overwrite) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    // Default implementation deals with overwrite in a non-atomic way
    final @Tainted FileStatus srcStatus = getFileLinkStatus(src);

    @Tainted
    FileStatus dstStatus;
    try {
      dstStatus = getFileLinkStatus(dst);
    } catch (@Tainted IOException e) {
      dstStatus = null;
    }
    if (dstStatus != null) {
      if (dst.equals(src)) {
        throw new @Tainted FileAlreadyExistsException(
            "The source "+src+" and destination "+dst+" are the same");
      }
      if (srcStatus.isSymlink() && dst.equals(srcStatus.getSymlink())) {
        throw new @Tainted FileAlreadyExistsException(
            "Cannot rename symlink "+src+" to its target "+dst);
      }
      // It's OK to rename a file to a symlink and vice versa
      if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
        throw new @Tainted IOException("Source " + src + " and destination " + dst
            + " must both be directories");
      }
      if (!overwrite) {
        throw new @Tainted FileAlreadyExistsException("Rename destination " + dst
            + " already exists.");
      }
      // Delete the destination that is a file or an empty directory
      if (dstStatus.isDirectory()) {
        @Tainted
        RemoteIterator<@Tainted FileStatus> list = listStatusIterator(dst);
        if (list != null && list.hasNext()) {
          throw new @Tainted IOException(
              "Rename cannot overwrite non empty destination directory " + dst);
        }
      }
      delete(dst, false);
    } else {
      final @Tainted Path parent = dst.getParent();
      final @Tainted FileStatus parentStatus = getFileStatus(parent);
      if (parentStatus.isFile()) {
        throw new @Tainted ParentNotDirectoryException("Rename destination parent "
            + parent + " is a file.");
      }
    }
    renameInternal(src, dst);
  }
  
  /**
   * Returns true if the file system supports symlinks, false otherwise.
   * @return true if filesystem supports symlinks
   */
  public @Tainted boolean supportsSymlinks(@Tainted AbstractFileSystem this) {
    return false;
  }
  
  /**
   * The specification of this method matches that of  
   * {@link FileContext#createSymlink(Path, Path, boolean)};
   */
  public void createSymlink(@Tainted AbstractFileSystem this, final @Tainted Path target, final @Tainted Path link,
      final @Tainted boolean createParent) throws IOException, UnresolvedLinkException {
    throw new @Tainted IOException("File system does not support symlinks");    
  }

  /**
   * Partially resolves the path. This is used during symlink resolution in
   * {@link FSLinkResolver}, and differs from the similarly named method
   * {@link FileContext#getLinkTarget(Path)}.
   */
  public @Tainted Path getLinkTarget(@Tainted AbstractFileSystem this, final @Tainted Path f) throws IOException {
    /* We should never get here. Any file system that threw an
     * UnresolvedLinkException, causing this function to be called,
     * needs to override this method.
     */
    throw new @Tainted AssertionError();
  }
    
  /**
   * The specification of this method matches that of
   * {@link FileContext#setPermission(Path, FsPermission)} except that Path f
   * must be for this file system.
   */
  public abstract void setPermission(@Tainted AbstractFileSystem this, final @Tainted Path f,
      final @Tainted FsPermission permission) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setOwner(Path, String, String)} except that Path f must
   * be for this file system.
   */
  public abstract void setOwner(@Tainted AbstractFileSystem this, final @Tainted Path f, final @Untainted String username,
      final @Untainted String groupname) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setTimes(Path, long, long)} except that Path f must be
   * for this file system.
   */
  public abstract void setTimes(@Tainted AbstractFileSystem this, final @Tainted Path f, final @Tainted long mtime,
    final @Tainted long atime) throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileChecksum(Path)} except that Path f must be for
   * this file system.
   */
  public abstract @Tainted FileChecksum getFileChecksum(@Tainted AbstractFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileStatus(Path)} 
   * except that an UnresolvedLinkException may be thrown if a symlink is 
   * encountered in the path.
   */
  public abstract @Tainted FileStatus getFileStatus(@Tainted AbstractFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileLinkStatus(Path)}
   * except that an UnresolvedLinkException may be thrown if a symlink is  
   * encountered in the path leading up to the final path component.
   * If the file system does not support symlinks then the behavior is
   * equivalent to {@link AbstractFileSystem#getFileStatus(Path)}.
   */
  public @Tainted FileStatus getFileLinkStatus(@Tainted AbstractFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return getFileStatus(f);
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileBlockLocations(Path, long, long)} except that
   * Path f must be for this file system.
   */
  public abstract @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted AbstractFileSystem this, final @Tainted Path f,
      final @Tainted long start, final @Tainted long len) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFsStatus(Path)} except that Path f must be for this
   * file system.
   */
  public @Tainted FsStatus getFsStatus(@Tainted AbstractFileSystem this, final @Tainted Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    // default impl gets FsStatus of root
    return getFsStatus();
  }
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#getFsStatus(Path)}.
   */
  public abstract @Tainted FsStatus getFsStatus(@Tainted AbstractFileSystem this) throws AccessControlException,
      FileNotFoundException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#listStatus(Path)} except that Path f must be for this
   * file system.
   */
  public @Tainted RemoteIterator<@Tainted FileStatus> listStatusIterator(@Tainted AbstractFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return new @Tainted RemoteIterator<@Tainted FileStatus>() {
      private @Tainted int i = 0;
      private @Tainted FileStatus @Tainted [] statusList = listStatus(f);
      
      @Override
      public @Tainted boolean hasNext() {
        return i < statusList.length;
      }
      
      @Override
      public @Tainted FileStatus next() {
        if (!hasNext()) {
          throw new @Tainted NoSuchElementException();
        }
        return statusList[i++];
      }
    };
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext#listLocatedStatus(Path)} except that Path f 
   * must be for this file system.
   */
  public @Tainted RemoteIterator<@Tainted LocatedFileStatus> listLocatedStatus(@Tainted AbstractFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return new @Tainted RemoteIterator<@Tainted LocatedFileStatus>() {
      private @Tainted RemoteIterator<@Tainted FileStatus> itor = listStatusIterator(f);
      
      @Override
      public @Tainted boolean hasNext() throws IOException {
        return itor.hasNext();
      }
      
      @Override
      public @Tainted LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new @Tainted NoSuchElementException("No more entry in " + f);
        }
        @Tainted
        FileStatus result = itor.next();
        @Tainted
        BlockLocation @Tainted [] locs = null;
        if (result.isFile()) {
          locs = getFileBlockLocations(
              result.getPath(), 0, result.getLen());
        }
        return new @Tainted LocatedFileStatus(result, locs);
      }
    };
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext.Util#listStatus(Path)} except that Path f must be 
   * for this file system.
   */
  public abstract @Tainted FileStatus @Tainted [] listStatus(@Tainted AbstractFileSystem this, final @Tainted Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * @return an iterator over the corrupt files under the given path
   * (may contain duplicates if a file has more than one corrupt block)
   * @throws IOException
   */
  public @Tainted RemoteIterator<@Tainted Path> listCorruptFileBlocks(@Tainted AbstractFileSystem this, @Tainted Path path)
    throws IOException {
    throw new @Tainted UnsupportedOperationException(getClass().getCanonicalName() +
                                            " does not support" +
                                            " listCorruptFileBlocks");
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext#setVerifyChecksum(boolean, Path)} except that Path f
   * must be for this file system.
   */
  public abstract void setVerifyChecksum(@Tainted AbstractFileSystem this, final @Tainted boolean verifyChecksum)
      throws AccessControlException, IOException;
  
  /**
   * Get a canonical name for this file system.
   * @return a URI string that uniquely identifies this file system
   */
  public @Tainted String getCanonicalServiceName(@Tainted AbstractFileSystem this) {
    return SecurityUtil.buildDTServiceName(getUri(), getUriDefaultPort());
  }
  
  /**
   * Get one or more delegation tokens associated with the filesystem. Normally
   * a file system returns a single delegation token. A file system that manages
   * multiple file systems underneath, could return set of delegation tokens for
   * all the file systems it manages
   * 
   * @param renewer the account name that is allowed to renew the token.
   * @return List of delegation tokens.
   *   If delegation tokens not supported then return a list of size zero.
   * @throws IOException
   */
  @InterfaceAudience.LimitedPrivate( { "HDFS", "MapReduce" })
  public @Tainted List<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>> getDelegationTokens(@Tainted AbstractFileSystem this, @Tainted String renewer) throws IOException {
    return new @Tainted ArrayList<@Tainted Token<@Tainted ? extends java.lang.@Tainted Object>>(0);
  }
  
  @Override //Object
  public @Tainted int hashCode(@Tainted AbstractFileSystem this) {
    return myUri.hashCode();
  }
  
  @Override //Object
  public @Tainted boolean equals(@Tainted AbstractFileSystem this, @Tainted Object other) {
    if (other == null || !(other instanceof @Tainted AbstractFileSystem)) {
      return false;
    }
    return myUri.equals(((@Tainted AbstractFileSystem) other).myUri);
  }
}
