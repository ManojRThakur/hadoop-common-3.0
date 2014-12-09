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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;

/**
 * This is an implementation of the Hadoop Archive 
 * Filesystem. This archive Filesystem has index files
 * of the form _index* and has contents of the form
 * part-*. The index files store the indexes of the 
 * real files. The index files are of the form _masterindex
 * and _index. The master index is a level of indirection 
 * in to the index file to make the look ups faster. the index
 * file is sorted with hash code of the paths that it contains 
 * and the master index contains pointers to the positions in 
 * index for ranges of hashcodes.
 */

public class HarFileSystem extends @Tainted FileSystem {

  private static final @Tainted Log LOG = LogFactory.getLog(HarFileSystem.class);

  public static final @Tainted String METADATA_CACHE_ENTRIES_KEY = "fs.har.metadatacache.entries";
  public static final @Tainted int METADATA_CACHE_ENTRIES_DEFAULT = 10;

  public static final @Tainted int VERSION = 3;

  private static @Tainted Map<@Tainted URI, @Tainted HarMetaData> harMetaCache;

  // uri representation of this Har filesystem
  private @Tainted URI uri;
  // the top level path of the archive
  // in the underlying file system
  private @Tainted Path archivePath;
  // the har auth
  private @Tainted String harAuth;

  // pointer into the static metadata cache
  private @Tainted HarMetaData metadata;

  private @Tainted FileSystem fs;

  /**
   * public construction of harfilesystem
   */
  public @Tainted HarFileSystem() {
    // Must call #initialize() method to set the underlying file system
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>har</code>
   */
  @Override
  public @Tainted String getScheme(@Tainted HarFileSystem this) {
    return "har";
  }

  /**
   * Constructor to create a HarFileSystem with an
   * underlying filesystem.
   * @param fs underlying file system
   */
  public @Tainted HarFileSystem(@Tainted FileSystem fs) {
    this.fs = fs;
    this.statistics = fs.statistics;
  }
 
  private synchronized void initializeMetadataCache(@Tainted HarFileSystem this, @Tainted Configuration conf) {
    if (harMetaCache == null) {
      @Tainted
      int cacheSize = conf.getInt(METADATA_CACHE_ENTRIES_KEY, METADATA_CACHE_ENTRIES_DEFAULT);
      harMetaCache = Collections.synchronizedMap(new @Tainted LruCache<@Tainted URI, @Tainted HarMetaData>(cacheSize));
    }
  }
 
  /**
   * Initialize a Har filesystem per har archive. The 
   * archive home directory is the top level directory
   * in the filesystem that contains the HAR archive.
   * Be careful with this method, you do not want to go 
   * on creating new Filesystem instances per call to 
   * path.getFileSystem().
   * the uri of Har is 
   * har://underlyingfsscheme-host:port/archivepath.
   * or 
   * har:///archivepath. This assumes the underlying filesystem
   * to be used in case not specified.
   */
  @Override
  public void initialize(@Tainted HarFileSystem this, @Tainted URI name, @Tainted Configuration conf) throws IOException {
    // initialize the metadata cache, if needed
    initializeMetadataCache(conf);

    // decode the name
    @Tainted
    URI underLyingURI = decodeHarURI(name, conf);
    // we got the right har Path- now check if this is 
    // truly a har filesystem
    @Tainted
    Path harPath = archivePath(
      new @Tainted Path(name.getScheme(), name.getAuthority(), name.getPath()));
    if (harPath == null) { 
      throw new @Tainted IOException("Invalid path for the Har Filesystem. " + 
                           name.toString());
    }
    if (fs == null) {
      fs = FileSystem.get(underLyingURI, conf);
    }
    uri = harPath.toUri();
    archivePath = new @Tainted Path(uri.getPath());
    harAuth = getHarAuth(underLyingURI);
    //check for the underlying fs containing
    // the index file
    @Tainted
    Path masterIndexPath = new @Tainted Path(archivePath, "_masterindex");
    @Tainted
    Path archiveIndexPath = new @Tainted Path(archivePath, "_index");
    if (!fs.exists(masterIndexPath) || !fs.exists(archiveIndexPath)) {
      throw new @Tainted IOException("Invalid path for the Har Filesystem. " +
          "No index file in " + harPath);
    }

    metadata = harMetaCache.get(uri);
    if (metadata != null) {
      @Tainted
      FileStatus mStat = fs.getFileStatus(masterIndexPath);
      @Tainted
      FileStatus aStat = fs.getFileStatus(archiveIndexPath);
      if (mStat.getModificationTime() != metadata.getMasterIndexTimestamp() ||
          aStat.getModificationTime() != metadata.getArchiveIndexTimestamp()) {
        // the archive has been overwritten since we last read it
        // remove the entry from the meta data cache
        metadata = null;
        harMetaCache.remove(uri);
      }
    }
    if (metadata == null) {
      metadata = new @Tainted HarMetaData(fs, masterIndexPath, archiveIndexPath);
      metadata.parseMetaData();
      harMetaCache.put(uri, metadata);
    }
  }

  @Override
  public @Tainted Configuration getConf(@Tainted HarFileSystem this) {
    return fs.getConf();
  }

  // get the version of the filesystem from the masterindex file
  // the version is currently not useful since its the first version
  // of archives
  public @Tainted int getHarVersion(@Tainted HarFileSystem this) throws IOException {
    if (metadata != null) {
      return metadata.getVersion();
    }
    else {
      throw new @Tainted IOException("Invalid meta data for the Har Filesystem");
    }
  }

  /*
   * find the parent path that is the 
   * archive path in the path. The last
   * path segment that ends with .har is 
   * the path that will be returned.
   */
  private @Tainted Path archivePath(@Tainted HarFileSystem this, @Tainted Path p) {
    @Tainted
    Path retPath = null;
    @Tainted
    Path tmp = p;
    for (@Tainted int i=0; i< p.depth(); i++) {
      if (tmp.toString().endsWith(".har")) {
        retPath = tmp;
        break;
      }
      tmp = tmp.getParent();
    }
    return retPath;
  }

  /**
   * decode the raw URI to get the underlying URI
   * @param rawURI raw Har URI
   * @return filtered URI of the underlying fileSystem
   */
  private @Tainted URI decodeHarURI(@Tainted HarFileSystem this, @Tainted URI rawURI, @Tainted Configuration conf) throws IOException {
    @Tainted
    String tmpAuth = rawURI.getAuthority();
    //we are using the default file
    //system in the config 
    //so create a underlying uri and 
    //return it
    if (tmpAuth == null) {
      //create a path 
      return FileSystem.getDefaultUri(conf);
    }
    @Tainted
    String authority = rawURI.getAuthority();
    if (authority == null) {
      throw new @Tainted IOException("URI: " + rawURI
          + " is an invalid Har URI since authority==null."
          + "  Expecting har://<scheme>-<host>/<path>.");
    }
 
    @Tainted
    int i = authority.indexOf('-');
    if (i < 0) {
      throw new @Tainted IOException("URI: " + rawURI
          + " is an invalid Har URI since '-' not found."
          + "  Expecting har://<scheme>-<host>/<path>.");
    }
 
    if (rawURI.getQuery() != null) {
      // query component not allowed
      throw new @Tainted IOException("query component in Path not supported  " + rawURI);
    }
 
    @Tainted
    URI tmp;
    try {
      // convert <scheme>-<host> to <scheme>://<host>
      @Tainted
      URI baseUri = new @Tainted URI(authority.replaceFirst("-", "://"));
 
      tmp = new @Tainted URI(baseUri.getScheme(), baseUri.getAuthority(),
            rawURI.getPath(), rawURI.getQuery(), rawURI.getFragment());
    } catch (@Tainted URISyntaxException e) {
      throw new @Tainted IOException("URI: " + rawURI
          + " is an invalid Har URI. Expecting har://<scheme>-<host>/<path>.");
    }
    return tmp;
  }

  private static @Tainted String decodeString(@Tainted String str)
    throws UnsupportedEncodingException {
    return URLDecoder.decode(str, "UTF-8");
  }

  private @Tainted String decodeFileName(@Tainted HarFileSystem this, @Tainted String fname)
    throws UnsupportedEncodingException {
    @Tainted
    int version = metadata.getVersion();
    if (version == 2 || version == 3){
      return decodeString(fname);
    }
    return fname;
  }

  /**
   * return the top level archive.
   */
  @Override
  public @Tainted Path getWorkingDirectory(@Tainted HarFileSystem this) {
    return new @Tainted Path(uri.toString());
  }

  @Override
  public @Tainted Path getInitialWorkingDirectory(@Tainted HarFileSystem this) {
    return getWorkingDirectory();
  }

  @Override
  public @Tainted FsStatus getStatus(@Tainted HarFileSystem this, @Tainted Path p) throws IOException {
    return fs.getStatus(p);
  }

  /**
   * Create a har specific auth 
   * har-underlyingfs:port
   * @param underLyingUri the uri of underlying
   * filesystem
   * @return har specific auth
   */
  private @Tainted String getHarAuth(@Tainted HarFileSystem this, @Tainted URI underLyingUri) {
    @Tainted
    String auth = underLyingUri.getScheme() + "-";
    if (underLyingUri.getHost() != null) {
      auth += underLyingUri.getHost();
      if (underLyingUri.getPort() != -1) {
        auth += ":";
        auth +=  underLyingUri.getPort();
      }
    }
    else {
      auth += ":";
    }
    return auth;
  }

  /**
   * Used for delegation token related functionality. Must delegate to
   * underlying file system.
   */
  @Override
  protected @Tainted URI getCanonicalUri(@Tainted HarFileSystem this) {
    return fs.getCanonicalUri();
  }

  @Override
  protected @PolyTainted URI canonicalizeUri(@Tainted HarFileSystem this, @PolyTainted URI uri) {
    return fs.canonicalizeUri(uri);
  }

  /**
   * Returns the uri of this filesystem.
   * The uri is of the form 
   * har://underlyingfsschema-host:port/pathintheunderlyingfs
   */
  @Override
  public @Tainted URI getUri(@Tainted HarFileSystem this) {
    return this.uri;
  }
  
  @Override
  protected void checkPath(@Tainted HarFileSystem this, @Tainted Path path) {
    fs.checkPath(path);
  }

  @Override
  public @Tainted Path resolvePath(@Tainted HarFileSystem this, @Tainted Path p) throws IOException {
    return fs.resolvePath(p);
  }

  /**
   * this method returns the path 
   * inside the har filesystem.
   * this is relative path inside 
   * the har filesystem.
   * @param path the fully qualified path in the har filesystem.
   * @return relative path in the filesystem.
   */
  private @Tainted Path getPathInHar(@Tainted HarFileSystem this, @Tainted Path path) {
    @Tainted
    Path harPath = new @Tainted Path(path.toUri().getPath());
    if (archivePath.compareTo(harPath) == 0)
      return new @Tainted Path(Path.SEPARATOR);
    @Tainted
    Path tmp = new @Tainted Path(harPath.getName());
    @Tainted
    Path parent = harPath.getParent();
    while (!(parent.compareTo(archivePath) == 0)) {
      if (parent.toString().equals(Path.SEPARATOR)) {
        tmp = null;
        break;
      }
      tmp = new @Tainted Path(parent.getName(), tmp);
      parent = parent.getParent();
    }
    if (tmp != null) 
      tmp = new @Tainted Path(Path.SEPARATOR, tmp);
    return tmp;
  }
  
  //the relative path of p. basically 
  // getting rid of /. Parsing and doing 
  // string manipulation is not good - so
  // just use the path api to do it.
  private @Tainted Path makeRelative(@Tainted HarFileSystem this, @Tainted String initial, @Tainted Path p) {
    @Tainted
    String scheme = this.uri.getScheme();
    @Tainted
    String authority = this.uri.getAuthority();
    @Tainted
    Path root = new @Tainted Path(Path.SEPARATOR);
    if (root.compareTo(p) == 0)
      return new @Tainted Path(scheme, authority, initial);
    @Tainted
    Path retPath = new @Tainted Path(p.getName());
    @Tainted
    Path parent = p.getParent();
    for (@Tainted int i=0; i < p.depth()-1; i++) {
      retPath = new @Tainted Path(parent.getName(), retPath);
      parent = parent.getParent();
    }
    return new @Tainted Path(new @Tainted Path(scheme, authority, initial),
      retPath.toString());
  }
  
  /* this makes a path qualified in the har filesystem
   * (non-Javadoc)
   * @see org.apache.hadoop.fs.FilterFileSystem#makeQualified(
   * org.apache.hadoop.fs.Path)
   */
  @Override
  public @Tainted Path makeQualified(@Tainted HarFileSystem this, @Tainted Path path) {
    // make sure that we just get the 
    // path component 
    @Tainted
    Path fsPath = path;
    if (!path.isAbsolute()) {
      fsPath = new @Tainted Path(archivePath, path);
    }

    @Tainted
    URI tmpURI = fsPath.toUri();
    //change this to Har uri 
    return new @Tainted Path(uri.getScheme(), harAuth, tmpURI.getPath());
  }

  /**
   * Fix offset and length of block locations.
   * Note that this method modifies the original array.
   * @param locations block locations of har part file
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @param fileOffsetInHar the offset of the desired file in the har part file
   * @return block locations with fixed offset and length
   */  
  static @Tainted BlockLocation @Tainted [] fixBlockLocations(@Tainted BlockLocation @Tainted [] locations,
                                          @Tainted
                                          long start,
                                          @Tainted
                                          long len,
                                          @Tainted
                                          long fileOffsetInHar) {
    // offset 1 past last byte of desired range
    @Tainted
    long end = start + len;

    for (@Tainted BlockLocation location : locations) {
      // offset of part block relative to beginning of desired file
      // (may be negative if file starts in this part block)
      @Tainted
      long harBlockStart = location.getOffset() - fileOffsetInHar;
      // offset 1 past last byte of har block relative to beginning of
      // desired file
      @Tainted
      long harBlockEnd = harBlockStart + location.getLength();
      
      if (start > harBlockStart) {
        // desired range starts after beginning of this har block
        // fix offset to beginning of relevant range (relative to desired file)
        location.setOffset(start);
        // fix length to relevant portion of har block
        location.setLength(location.getLength() - (start - harBlockStart));
      } else {
        // desired range includes beginning of this har block
        location.setOffset(harBlockStart);
      }
      
      if (harBlockEnd > end) {
        // range ends before end of this har block
        // fix length to remove irrelevant portion at the end
        location.setLength(location.getLength() - (harBlockEnd - end));
      }
    }
    
    return locations;
  }
  
  /**
   * Get block locations from the underlying fs and fix their
   * offsets and lengths.
   * @param file the input file status to get block locations
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @return block locations for this segment of file
   * @throws IOException
   */
  @Override
  public @Tainted BlockLocation @Tainted [] getFileBlockLocations(@Tainted HarFileSystem this, @Tainted FileStatus file, @Tainted long start,
                                               @Tainted
                                               long len) throws IOException {
    @Tainted
    HarStatus hstatus = getFileHarStatus(file.getPath());
    @Tainted
    Path partPath = new @Tainted Path(archivePath, hstatus.getPartName());
    @Tainted
    FileStatus partStatus = metadata.getPartFileStatus(partPath);

    // get all part blocks that overlap with the desired file blocks
    @Tainted
    BlockLocation @Tainted [] locations = 
      fs.getFileBlockLocations(partStatus,
                               hstatus.getStartIndex() + start, len);

    return fixBlockLocations(locations, start, len, hstatus.getStartIndex());
  }
  
  /**
   * the hash of the path p inside  the filesystem
   * @param p the path in the harfilesystem
   * @return the hash code of the path.
   */
  public static @Tainted int getHarHash(@Tainted Path p) {
    return (p.toString().hashCode() & 0x7fffffff);
  }
  
  static class Store {
    public @Tainted Store() {
      begin = end = startHash = endHash = 0;
    }
    public @Tainted Store(@Tainted long begin, @Tainted long end, @Tainted int startHash, @Tainted int endHash) {
      this.begin = begin;
      this.end = end;
      this.startHash = startHash;
      this.endHash = endHash;
    }
    public @Tainted long begin;
    public @Tainted long end;
    public @Tainted int startHash;
    public @Tainted int endHash;
  }
  
  /**
   * Get filestatuses of all the children of a given directory. This just reads
   * through index file and reads line by line to get all statuses for children
   * of a directory. Its a brute force way of getting all such filestatuses
   * 
   * @param parent
   *          the parent path directory
   * @param statuses
   *          the list to add the children filestatuses to
   */
  private void fileStatusesInIndex(@Tainted HarFileSystem this, @Tainted HarStatus parent, @Tainted List<@Tainted FileStatus> statuses)
          throws IOException {
    @Tainted
    String parentString = parent.getName();
    if (!parentString.endsWith(Path.SEPARATOR)){
        parentString += Path.SEPARATOR;
    }
    @Tainted
    Path harPath = new @Tainted Path(parentString);
    @Tainted
    int harlen = harPath.depth();
    final @Tainted Map<@Tainted String, @Tainted FileStatus> cache = new @Tainted TreeMap<@Tainted String, @Tainted FileStatus>();

    for (@Tainted HarStatus hstatus : metadata.archive.values()) {
      @Tainted
      String child = hstatus.getName();
      if ((child.startsWith(parentString))) {
        @Tainted
        Path thisPath = new @Tainted Path(child);
        if (thisPath.depth() == harlen + 1) {
          statuses.add(toFileStatus(hstatus, cache));
        }
      }
    }
  }

  /**
   * Combine the status stored in the index and the underlying status. 
   * @param h status stored in the index
   * @param cache caching the underlying file statuses
   * @return the combined file status
   * @throws IOException
   */
  private @Tainted FileStatus toFileStatus(@Tainted HarFileSystem this, @Tainted HarStatus h,
      @Tainted
      Map<@Tainted String, @Tainted FileStatus> cache) throws IOException {
    @Tainted
    FileStatus underlying = null;
    if (cache != null) {
      underlying = cache.get(h.partName);
    }
    if (underlying == null) {
      final @Tainted Path p = h.isDir? archivePath: new @Tainted Path(archivePath, h.partName);
      underlying = fs.getFileStatus(p);
      if (cache != null) {
        cache.put(h.partName, underlying);
      }
    }

    @Tainted
    long modTime = 0;
    @Tainted
    int version = metadata.getVersion();
    if (version < 3) {
      modTime = underlying.getModificationTime();
    } else if (version == 3) {
      modTime = h.getModificationTime();
    }

    return new @Tainted FileStatus(
        h.isDir()? 0L: h.getLength(),
        h.isDir(),
        underlying.getReplication(),
        underlying.getBlockSize(),
        modTime,
        underlying.getAccessTime(),
        underlying.getPermission(),
        underlying.getOwner(),
        underlying.getGroup(),
        makeRelative(this.uri.getPath(), new @Tainted Path(h.name)));
  }

  // a single line parser for hadoop archives status 
  // stored in a single line in the index files 
  // the format is of the form 
  // filename "dir"/"file" partFileName startIndex length 
  // <space separated children>
  private class HarStatus {
    @Tainted
    boolean isDir;
    @Tainted
    String name;
    @Tainted
    List<@Tainted String> children;
    @Tainted
    String partName;
    @Tainted
    long startIndex;
    @Tainted
    long length;
    @Tainted
    long modificationTime = 0;

    public @Tainted HarStatus(@Tainted String harString) throws UnsupportedEncodingException {
      @Tainted
      String @Tainted [] splits = harString.split(" ");
      this.name = decodeFileName(splits[0]);
      this.isDir = "dir".equals(splits[1]) ? true: false;
      // this is equal to "none" if its a directory
      this.partName = splits[2];
      this.startIndex = Long.parseLong(splits[3]);
      this.length = Long.parseLong(splits[4]);

      @Tainted
      int version = metadata.getVersion();
      @Tainted
      String @Tainted [] propSplits = null;
      // propSplits is used to retrieve the metainformation that Har versions
      // 1 & 2 missed (modification time, permission, owner group).
      // These fields are stored in an encoded string placed in different
      // locations depending on whether it's a file or directory entry.
      // If it's a directory, the string will be placed at the partName
      // location (directories have no partName because they don't have data
      // to be stored). This is done because the number of fields in a
      // directory entry is unbounded (all children are listed at the end)
      // If it's a file, the string will be the last field.
      if (isDir) {
        if (version == 3){
          propSplits = decodeString(this.partName).split(" ");
        }
        children = new @Tainted ArrayList<@Tainted String>();
        for (@Tainted int i = 5; i < splits.length; i++) {
          children.add(decodeFileName(splits[i]));
        }
      } else if (version == 3) {
        propSplits = decodeString(splits[5]).split(" ");
      }

      if (propSplits != null && propSplits.length >= 4) {
        modificationTime = Long.parseLong(propSplits[0]);
        // the fields below are stored in the file but are currently not used
        // by HarFileSystem
        // permission = new FsPermission(Short.parseShort(propSplits[1]));
        // owner = decodeString(propSplits[2]);
        // group = decodeString(propSplits[3]);
      }
    }
    public @Tainted boolean isDir(@Tainted HarFileSystem.HarStatus this) {
      return isDir;
    }
    
    public @Tainted String getName(@Tainted HarFileSystem.HarStatus this) {
      return name;
    }
    public @Tainted String getPartName(@Tainted HarFileSystem.HarStatus this) {
      return partName;
    }
    public @Tainted long getStartIndex(@Tainted HarFileSystem.HarStatus this) {
      return startIndex;
    }
    public @Tainted long getLength(@Tainted HarFileSystem.HarStatus this) {
      return length;
    }
    public @Tainted long getModificationTime(@Tainted HarFileSystem.HarStatus this) {
      return modificationTime;
    }
  }
  
  /**
   * return the filestatus of files in har archive.
   * The permission returned are that of the archive
   * index files. The permissions are not persisted 
   * while creating a hadoop archive.
   * @param f the path in har filesystem
   * @return filestatus.
   * @throws IOException
   */
  @Override
  public @Tainted FileStatus getFileStatus(@Tainted HarFileSystem this, @Tainted Path f) throws IOException {
    @Tainted
    HarStatus hstatus = getFileHarStatus(f);
    return toFileStatus(hstatus, null);
  }

  private @Tainted HarStatus getFileHarStatus(@Tainted HarFileSystem this, @Tainted Path f) throws IOException {
    // get the fs DataInputStream for the underlying file
    // look up the index.
    @Tainted
    Path p = makeQualified(f);
    @Tainted
    Path harPath = getPathInHar(p);
    if (harPath == null) {
      throw new @Tainted IOException("Invalid file name: " + f + " in " + uri);
    }
    @Tainted
    HarStatus hstatus = metadata.archive.get(harPath);
    if (hstatus == null) {
      throw new @Tainted FileNotFoundException("File: " +  f + " does not exist in " + uri);
    }
    return hstatus;
  }

  /**
   * @return null since no checksum algorithm is implemented.
   */
  @Override
  public @Tainted FileChecksum getFileChecksum(@Tainted HarFileSystem this, @Tainted Path f) {
    return null;
  }

  /**
   * Returns a har input stream which fakes end of 
   * file. It reads the index files to get the part 
   * file name and the size and start of the file.
   */
  @Override
  public @Tainted FSDataInputStream open(@Tainted HarFileSystem this, @Tainted Path f, @Tainted int bufferSize) throws IOException {
    // get the fs DataInputStream for the underlying file
    @Tainted
    HarStatus hstatus = getFileHarStatus(f);
    if (hstatus.isDir()) {
      throw new @Tainted FileNotFoundException(f + " : not a file in " +
                archivePath);
    }
    return new @Tainted HarFSDataInputStream(fs, new @Tainted Path(archivePath, 
        hstatus.getPartName()),
        hstatus.getStartIndex(), hstatus.getLength(), bufferSize);
  }

  /**
   * Used for delegation token related functionality. Must delegate to
   * underlying file system.
   */
  @Override
  public @Tainted FileSystem @Tainted [] getChildFileSystems(@Tainted HarFileSystem this) {
    return new @Tainted FileSystem @Tainted []{fs};
  }

  @Override
  public @Tainted FSDataOutputStream create(@Tainted HarFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    throw new @Tainted IOException("Har: create not allowed.");
  }

  @SuppressWarnings("deprecation")
  @Override
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted HarFileSystem this, @Tainted Path f, @Tainted boolean overwrite,
      @Tainted
      int bufferSize, @Tainted short replication, @Tainted long blockSize, @Tainted Progressable progress)
      throws IOException {
    throw new @Tainted IOException("Har: create not allowed.");
  }

  @Override
  public @Tainted FSDataOutputStream append(@Tainted HarFileSystem this, @Tainted Path f, @Tainted int bufferSize, @Tainted Progressable progress) throws IOException {
    throw new @Tainted IOException("Har: append not allowed.");
  }

  @Override
  public void close(@Tainted HarFileSystem this) throws IOException {
    super.close();
    if (fs != null) {
      try {
        fs.close();
      } catch(@Tainted IOException ie) {
        //this might already be closed
        // ignore
      }
    }
  }
  
  /**
   * Not implemented.
   */
  @Override
  public @Tainted boolean setReplication(@Tainted HarFileSystem this, @Tainted Path src, @Tainted short replication) throws IOException{
    throw new @Tainted IOException("Har: setReplication not allowed");
  }

  @Override
  public @Tainted boolean rename(@Tainted HarFileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    throw new @Tainted IOException("Har: rename not allowed");
  }

  @Override
  public @Tainted FSDataOutputStream append(@Tainted HarFileSystem this, @Tainted Path f) throws IOException {
    throw new @Tainted IOException("Har: append not allowed");
  }

  /**
   * Not implemented.
   */
  @Override
  public @Tainted boolean delete(@Tainted HarFileSystem this, @Tainted Path f, @Tainted boolean recursive) throws IOException { 
    throw new @Tainted IOException("Har: delete not allowed");
  }

  /**
   * liststatus returns the children of a directory 
   * after looking up the index files.
   */
  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted HarFileSystem this, @Tainted Path f) throws IOException {
    //need to see if the file is an index in file
    //get the filestatus of the archive directory
    // we will create fake filestatuses to return
    // to the client
    @Tainted
    List<@Tainted FileStatus> statuses = new @Tainted ArrayList<@Tainted FileStatus>();
    @Tainted
    Path tmpPath = makeQualified(f);
    @Tainted
    Path harPath = getPathInHar(tmpPath);
    @Tainted
    HarStatus hstatus = metadata.archive.get(harPath);
    if (hstatus == null) {
      throw new @Tainted FileNotFoundException("File " + f + " not found in " + archivePath);
    }
    if (hstatus.isDir()) {
      fileStatusesInIndex(hstatus, statuses);
    } else {
      statuses.add(toFileStatus(hstatus, null));
    }
    
    return statuses.toArray(new @Tainted FileStatus @Tainted [statuses.size()]);
  }
  
  /**
   * return the top level archive path.
   */
  @Override
  public @Tainted Path getHomeDirectory(@Tainted HarFileSystem this) {
    return new @Tainted Path(uri.toString());
  }

  @Override
  public void setWorkingDirectory(@Tainted HarFileSystem this, @Tainted Path newDir) {
    //does nothing.
  }
  
  /**
   * not implemented.
   */
  @Override
  public @Tainted boolean mkdirs(@Tainted HarFileSystem this, @Tainted Path f, @Tainted FsPermission permission) throws IOException {
    throw new @Tainted IOException("Har: mkdirs not allowed");
  }
  
  /**
   * not implemented.
   */
  @Override
  public void copyFromLocalFile(@Tainted HarFileSystem this, @Tainted boolean delSrc, @Tainted boolean overwrite,
      @Tainted
      Path src, @Tainted Path dst) throws IOException {
    throw new @Tainted IOException("Har: copyfromlocalfile not allowed");
  }

  @Override
  public void copyFromLocalFile(@Tainted HarFileSystem this, @Tainted boolean delSrc, @Tainted boolean overwrite,
      @Tainted
      Path @Tainted [] srcs, @Tainted Path dst) throws IOException {
    throw new @Tainted IOException("Har: copyfromlocalfile not allowed");
  }

  /**
   * copies the file in the har filesystem to a local file.
   */
  @Override
  public void copyToLocalFile(@Tainted HarFileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst) 
    throws IOException {
    FileUtil.copy(this, src, getLocal(getConf()), dst, false, getConf());
  }
  
  /**
   * not implemented.
   */
  @Override
  public @Tainted Path startLocalOutput(@Tainted HarFileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile) 
    throws IOException {
    throw new @Tainted IOException("Har: startLocalOutput not allowed");
  }
  
  /**
   * not implemented.
   */
  @Override
  public void completeLocalOutput(@Tainted HarFileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile) 
    throws IOException {
    throw new @Tainted IOException("Har: completeLocalOutput not allowed");
  }
  
  /**
   * not implemented.
   */
  @Override
  public void setOwner(@Tainted HarFileSystem this, @Tainted Path p, @Tainted String username, @Tainted String groupname)
    throws IOException {
    throw new @Tainted IOException("Har: setowner not allowed");
  }

  @Override
  public void setTimes(@Tainted HarFileSystem this, @Tainted Path p, @Tainted long mtime, @Tainted long atime) throws IOException {
    throw new @Tainted IOException("Har: setTimes not allowed");
  }

  /**
   * Not implemented.
   */
  @Override
  public void setPermission(@Tainted HarFileSystem this, @Tainted Path p, @Tainted FsPermission permission)
    throws IOException {
    throw new @Tainted IOException("Har: setPermission not allowed");
  }
  
  /**
   * Hadoop archives input stream. This input stream fakes EOF 
   * since archive files are part of bigger part files.
   */
  private static class HarFSDataInputStream extends @Tainted FSDataInputStream {
    /**
     * Create an input stream that fakes all the reads/positions/seeking.
     */
    private static class HarFsInputStream extends @Tainted FSInputStream
        implements @Tainted CanSetDropBehind, @Tainted CanSetReadahead {
      private @Tainted long position;
      private @Tainted long start;
      private @Tainted long end;
      //The underlying data input stream that the
      // underlying filesystem will return.
      private @Tainted FSDataInputStream underLyingStream;
      //one byte buffer
      private @Tainted byte @Tainted [] oneBytebuff = new @Tainted byte @Tainted [1];
      @Tainted
      HarFsInputStream(@Tainted FileSystem fs, @Tainted Path path, @Tainted long start,
          @Tainted
          long length, @Tainted int bufferSize) throws IOException {
        underLyingStream = fs.open(path, bufferSize);
        underLyingStream.seek(start);
        // the start of this file in the part file
        this.start = start;
        // the position pointer in the part file
        this.position = start;
        // the end pointer in the part file
        this.end = start + length;
      }
      
      @Override
      public synchronized @Tainted int available(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this) throws IOException {
        @Tainted
        long remaining = end - underLyingStream.getPos();
        if (remaining > (@Tainted long)Integer.MAX_VALUE) {
          return Integer.MAX_VALUE;
        }
        return (@Tainted int) remaining;
      }
      
      @Override
      public synchronized  void close(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this) throws IOException {
        underLyingStream.close();
        super.close();
      }
      
      //not implemented
      @Override
      public void mark(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted int readLimit) {
        // do nothing 
      }
      
      /**
       * reset is not implemented
       */
      @Override
      public void reset(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this) throws IOException {
        throw new @Tainted IOException("reset not implemented.");
      }
      
      @Override
      public synchronized @Tainted int read(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this) throws IOException {
        @Tainted
        int ret = read(oneBytebuff, 0, 1);
        return (ret <= 0) ? -1: (oneBytebuff[0] & 0xff);
      }
      
      @Override
      public synchronized @Tainted int read(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted byte @Tainted [] b) throws IOException {
        @Tainted
        int ret = read(b, 0, b.length);
        if (ret != -1) {
          position += ret;
        }
        return ret;
      }
      
      /**
       * 
       */
      @Override
      public synchronized @Tainted int read(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted byte @Tainted [] b, @Tainted int offset, @Tainted int len) 
        throws IOException {
        @Tainted
        int newlen = len;
        @Tainted
        int ret = -1;
        if (position + len > end) {
          newlen = (@Tainted int) (end - position);
        }
        // end case
        if (newlen == 0)
          return ret;
        ret = underLyingStream.read(b, offset, newlen);
        position += ret;
        return ret;
      }
      
      @Override
      public synchronized @Tainted long skip(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted long n) throws IOException {
        @Tainted
        long tmpN = n;
        if (tmpN > 0) {
          if (position + tmpN > end) {
            tmpN = end - position;
          }
          underLyingStream.seek(tmpN + position);
          position += tmpN;
          return tmpN;
        }
        return (tmpN < 0)? -1 : 0;
      }
      
      @Override
      public synchronized @Tainted long getPos(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this) throws IOException {
        return (position - start);
      }
      
      @Override
      public synchronized void seek(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted long pos) throws IOException {
        if (pos < 0 || (start + pos > end)) {
          throw new @Tainted IOException("Failed to seek: EOF");
        }
        position = start + pos;
        underLyingStream.seek(position);
      }

      @Override
      public @Tainted boolean seekToNewSource(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted long targetPos) throws IOException {
        // do not need to implement this
        // hdfs in itself does seektonewsource
        // while reading.
        return false;
      }
      
      /**
       * implementing position readable. 
       */
      @Override
      public @Tainted int read(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted long pos, @Tainted byte @Tainted [] b, @Tainted int offset, @Tainted int length) 
      throws IOException {
        @Tainted
        int nlength = length;
        if (start + nlength + pos > end) {
          nlength = (@Tainted int) (end - (start + pos));
        }
        return underLyingStream.read(pos + start , b, offset, nlength);
      }
      
      /**
       * position readable again.
       */
      @Override
      public void readFully(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted long pos, @Tainted byte @Tainted [] b, @Tainted int offset, @Tainted int length) 
      throws IOException {
        if (start + length + pos > end) {
          throw new @Tainted IOException("Not enough bytes to read.");
        }
        underLyingStream.readFully(pos + start, b, offset, length);
      }
      
      @Override
      public void readFully(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted long pos, @Tainted byte @Tainted [] b) throws IOException {
          readFully(pos, b, 0, b.length);
      }

      @Override
      public void setReadahead(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted Long readahead) throws IOException {
        underLyingStream.setReadahead(readahead);
      }

      @Override
      public void setDropBehind(HarFileSystem.HarFSDataInputStream.@Tainted HarFsInputStream this, @Tainted Boolean dropBehind) throws IOException {
        underLyingStream.setDropBehind(dropBehind);
      }
    }
  
    /**
     * constructors for har input stream.
     * @param fs the underlying filesystem
     * @param p The path in the underlying filesystem
     * @param start the start position in the part file
     * @param length the length of valid data in the part file
     * @param bufsize the buffer size
     * @throws IOException
     */
    public @Tainted HarFSDataInputStream(@Tainted FileSystem fs, @Tainted Path  p, @Tainted long start, 
        @Tainted
        long length, @Tainted int bufsize) throws IOException {
        super(new @Tainted HarFsInputStream(fs, p, start, length, bufsize));
    }
  }

  private class HarMetaData {
    private @Tainted FileSystem fs;
    private @Tainted int version;
    // the masterIndex of the archive
    private @Tainted Path masterIndexPath;
    // the index file 
    private @Tainted Path archiveIndexPath;

    private @Tainted long masterIndexTimestamp;
    private @Tainted long archiveIndexTimestamp;

    @Tainted
    List<@Tainted Store> stores = new @Tainted ArrayList<@Tainted Store>();
    @Tainted
    Map<@Tainted Path, @Tainted HarStatus> archive = new @Tainted HashMap<@Tainted Path, @Tainted HarStatus>();
    private @Tainted Map<@Tainted Path, @Tainted FileStatus> partFileStatuses = new @Tainted HashMap<@Tainted Path, @Tainted FileStatus>();

    public @Tainted HarMetaData(@Tainted FileSystem fs, @Tainted Path masterIndexPath, @Tainted Path archiveIndexPath) {
      this.fs = fs;
      this.masterIndexPath = masterIndexPath;
      this.archiveIndexPath = archiveIndexPath;
    }

    public @Tainted FileStatus getPartFileStatus(@Tainted HarFileSystem.HarMetaData this, @Tainted Path partPath) throws IOException {
      @Tainted
      FileStatus status;
      status = partFileStatuses.get(partPath);
      if (status == null) {
        status = fs.getFileStatus(partPath);
        partFileStatuses.put(partPath, status);
      }
      return status;
    }

    public @Tainted long getMasterIndexTimestamp(@Tainted HarFileSystem.HarMetaData this) {
      return masterIndexTimestamp;
    }

    public @Tainted long getArchiveIndexTimestamp(@Tainted HarFileSystem.HarMetaData this) {
      return archiveIndexTimestamp;
    }

    private @Tainted int getVersion(@Tainted HarFileSystem.HarMetaData this) {
      return version;
    }

    private void parseMetaData(@Tainted HarFileSystem.HarMetaData this) throws IOException {
      @Tainted
      Text line = new @Tainted Text();
      @Tainted
      long read;
      @Tainted
      FSDataInputStream in = null;
      @Tainted
      LineReader lin = null;

      try {
        in = fs.open(masterIndexPath);
        @Tainted
        FileStatus masterStat = fs.getFileStatus(masterIndexPath);
        masterIndexTimestamp = masterStat.getModificationTime();
        lin = new @Tainted LineReader(in, getConf());
        read = lin.readLine(line);

        // the first line contains the version of the index file
        @Tainted
        String versionLine = line.toString();
        @Tainted
        String @Tainted [] arr = versionLine.split(" ");
        version = Integer.parseInt(arr[0]);
        // make it always backwards-compatible
        if (this.version > HarFileSystem.VERSION) {
          throw new @Tainted IOException("Invalid version " + 
              this.version + " expected " + HarFileSystem.VERSION);
        }

        // each line contains a hashcode range and the index file name
        @Tainted
        String @Tainted [] readStr;
        while(read < masterStat.getLen()) {
          @Tainted
          int b = lin.readLine(line);
          read += b;
          readStr = line.toString().split(" ");
          @Tainted
          int startHash = Integer.parseInt(readStr[0]);
          @Tainted
          int endHash  = Integer.parseInt(readStr[1]);
          stores.add(new @Tainted Store(Long.parseLong(readStr[2]), 
              Long.parseLong(readStr[3]), startHash,
              endHash));
          line.clear();
        }
      } catch (@Tainted IOException ioe) {
        LOG.warn("Encountered exception ", ioe);
        throw ioe;
      } finally {
        IOUtils.cleanup(LOG, lin, in);
      }

      @Tainted
      FSDataInputStream aIn = fs.open(archiveIndexPath);
      try {
        @Tainted
        FileStatus archiveStat = fs.getFileStatus(archiveIndexPath);
        archiveIndexTimestamp = archiveStat.getModificationTime();
        @Tainted
        LineReader aLin;

        // now start reading the real index file
        for (@Tainted Store s: stores) {
          read = 0;
          aIn.seek(s.begin);
          aLin = new @Tainted LineReader(aIn, getConf());
          while (read + s.begin < s.end) {
            @Tainted
            int tmp = aLin.readLine(line);
            read += tmp;
            @Tainted
            String lineFeed = line.toString();
            @Tainted
            String @Tainted [] parsed = lineFeed.split(" ");
            parsed[0] = decodeFileName(parsed[0]);
            archive.put(new @Tainted Path(parsed[0]), new @Tainted HarStatus(lineFeed));
            line.clear();
          }
        }
      } finally {
        IOUtils.cleanup(LOG, aIn);
      }
    }
  }
  
  /*
   * testing purposes only:
   */
  @Tainted
  HarMetaData getMetadata(@Tainted HarFileSystem this) {
    return metadata;
  }

  private static class LruCache<@Tainted K extends java.lang.@Tainted Object, @Tainted V extends java.lang.@Tainted Object> extends @Tainted LinkedHashMap<K, V> {
    private final @Tainted int MAX_ENTRIES;

    public @Tainted LruCache(@Tainted int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        MAX_ENTRIES = maxEntries;
    }

    @Override
    protected @Tainted boolean removeEldestEntry(HarFileSystem.@Tainted LruCache<K, V> this, Map.@Tainted Entry<@Tainted K, @Tainted V> eldest) {
        return size() > MAX_ENTRIES;
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted HarFileSystem this) throws IOException {
    return fs.getServerDefaults();
  }

  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted HarFileSystem this, @Tainted Path f) throws IOException {
    return fs.getServerDefaults(f);
  }

  @Override
  public @Tainted long getUsed(@Tainted HarFileSystem this) throws IOException{
    return fs.getUsed();
  }

  @SuppressWarnings("deprecation")
  @Override
  public @Tainted long getDefaultBlockSize(@Tainted HarFileSystem this) {
    return fs.getDefaultBlockSize();
  }

  @SuppressWarnings("deprecation")
  @Override
  public @Tainted long getDefaultBlockSize(@Tainted HarFileSystem this, @Tainted Path f) {
    return fs.getDefaultBlockSize(f);
  }

  @SuppressWarnings("deprecation")
  @Override
  public @Tainted short getDefaultReplication(@Tainted HarFileSystem this) {
    return fs.getDefaultReplication();
  }

  @Override
  public @Tainted short getDefaultReplication(@Tainted HarFileSystem this, @Tainted Path f) {
    return fs.getDefaultReplication(f);
  }
}
