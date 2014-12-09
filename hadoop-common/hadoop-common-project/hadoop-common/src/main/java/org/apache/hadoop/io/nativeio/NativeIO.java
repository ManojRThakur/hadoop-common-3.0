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
package org.apache.hadoop.io.nativeio;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Shell;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * JNI wrappers for various native IO-related calls not available in Java.
 * These functions should generally be used alongside a fallback to another
 * more portable mechanism.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativeIO {
  public static class POSIX {
    // Flags for open() call from bits/fcntl.h
    public static final @Tainted int O_RDONLY   =    00;
    public static final @Tainted int O_WRONLY   =    01;
    public static final @Tainted int O_RDWR     =    02;
    public static final @Tainted int O_CREAT    =  0100;
    public static final @Tainted int O_EXCL     =  0200;
    public static final @Tainted int O_NOCTTY   =  0400;
    public static final @Tainted int O_TRUNC    = 01000;
    public static final @Tainted int O_APPEND   = 02000;
    public static final @Tainted int O_NONBLOCK = 04000;
    public static final @Tainted int O_SYNC   =  010000;
    public static final @Tainted int O_ASYNC  =  020000;
    public static final @Tainted int O_FSYNC = O_SYNC;
    public static final @Tainted int O_NDELAY = O_NONBLOCK;

    // Flags for posix_fadvise() from bits/fcntl.h
    /* No further special treatment.  */
    public static final @Tainted int POSIX_FADV_NORMAL = 0;
    /* Expect random page references.  */
    public static final @Tainted int POSIX_FADV_RANDOM = 1;
    /* Expect sequential page references.  */
    public static final @Tainted int POSIX_FADV_SEQUENTIAL = 2;
    /* Will need these pages.  */
    public static final @Tainted int POSIX_FADV_WILLNEED = 3;
    /* Don't need these pages.  */
    public static final @Tainted int POSIX_FADV_DONTNEED = 4;
    /* Data will be accessed once.  */
    public static final @Tainted int POSIX_FADV_NOREUSE = 5;


    /* Wait upon writeout of all pages
       in the range before performing the
       write.  */
    public static final @Tainted int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
    /* Initiate writeout of all those
       dirty pages in the range which are
       not presently under writeback.  */
    public static final @Tainted int SYNC_FILE_RANGE_WRITE = 2;

    /* Wait upon writeout of all pages in
       the range after performing the
       write.  */
    public static final @Tainted int SYNC_FILE_RANGE_WAIT_AFTER = 4;

    private static final @Tainted Log LOG = LogFactory.getLog(NativeIO.class);

    @VisibleForTesting
    public static @Tainted CacheTracker cacheTracker = null;
    
    private static @Tainted boolean nativeLoaded = false;
    private static @Tainted boolean fadvisePossible = true;
    private static @Tainted boolean syncFileRangePossible = true;

    static final @Tainted String WORKAROUND_NON_THREADSAFE_CALLS_KEY =
      "hadoop.workaround.non.threadsafe.getpwuid";
    static final @Tainted boolean WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT = true;

    private static @Tainted long cacheTimeout = -1;

    public static interface CacheTracker {
      public void fadvise(NativeIO.POSIX.@Tainted CacheTracker this, @Tainted String identifier, @Tainted long offset, @Tainted long len, @Tainted int flags);
    }
    
    static {
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        try {
          @Tainted
          Configuration conf = new @Tainted Configuration();
          workaroundNonThreadSafePasswdCalls = conf.getBoolean(
            WORKAROUND_NON_THREADSAFE_CALLS_KEY,
            WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT);

          initNative();
          nativeLoaded = true;

          cacheTimeout = conf.getLong(
            CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
            CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) *
            1000;
          LOG.debug("Initialized cache for IDs to User/Group mapping with a " +
            " cache timeout of " + cacheTimeout/1000 + " seconds.");

        } catch (@Tainted Throwable t) {
          // This can happen if the user has an older version of libhadoop.so
          // installed - in this case we can continue without native IO
          // after warning
          LOG.error("Unable to initialize NativeIO libraries", t);
        }
      }
    }

    /**
     * Return true if the JNI-based native IO extensions are available.
     */
    public static @Tainted boolean isAvailable() {
      return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
    }

    /** Wrapper around open(2) */
    public static native @Tainted FileDescriptor open(@Tainted String path, @Tainted int flags, @Tainted int mode) throws IOException;
    /** Wrapper around fstat(2) */
    private static native @Tainted Stat fstat(@Tainted FileDescriptor fd) throws IOException;

    /** Native chmod implementation. On UNIX, it is a wrapper around chmod(2) */
    private static native void chmodImpl(@Tainted String path, @Tainted int mode) throws IOException;

    public static void chmod(@Tainted String path, @Tainted int mode) throws IOException {
      if (!Shell.WINDOWS) {
        chmodImpl(path, mode);
      } else {
        try {
          chmodImpl(path, mode);
        } catch (@Tainted NativeIOException nioe) {
          if (nioe.getErrorCode() == 3) {
            throw new @Tainted NativeIOException("No such file or directory",
                Errno.ENOENT);
          } else {
            LOG.warn(String.format("NativeIO.chmod error (%d): %s",
                nioe.getErrorCode(), nioe.getMessage()));
            throw new @Tainted NativeIOException("Unknown error", Errno.UNKNOWN);
          }
        }
      }
    }

    /** Wrapper around posix_fadvise(2) */
    static native void posix_fadvise(
      @Tainted
      FileDescriptor fd, @Tainted long offset, @Tainted long len, @Tainted int flags) throws NativeIOException;

    /** Wrapper around sync_file_range(2) */
    static native void sync_file_range(
      @Tainted
      FileDescriptor fd, @Tainted long offset, @Tainted long nbytes, @Tainted int flags) throws NativeIOException;

    /**
     * Call posix_fadvise on the given file descriptor. See the manpage
     * for this syscall for more information. On systems where this
     * call is not available, does nothing.
     *
     * @throws NativeIOException if there is an error with the syscall
     */
    public static void posixFadviseIfPossible(@Tainted String identifier,
        @Tainted
        FileDescriptor fd, @Tainted long offset, @Tainted long len, @Tainted int flags)
        throws NativeIOException {
      if (cacheTracker != null) {
        cacheTracker.fadvise(identifier, offset, len, flags);
      }
      if (nativeLoaded && fadvisePossible) {
        try {
          posix_fadvise(fd, offset, len, flags);
        } catch (@Tainted UnsupportedOperationException uoe) {
          fadvisePossible = false;
        } catch (@Tainted UnsatisfiedLinkError ule) {
          fadvisePossible = false;
        }
      }
    }

    /**
     * Call sync_file_range on the given file descriptor. See the manpage
     * for this syscall for more information. On systems where this
     * call is not available, does nothing.
     *
     * @throws NativeIOException if there is an error with the syscall
     */
    public static void syncFileRangeIfPossible(
        @Tainted
        FileDescriptor fd, @Tainted long offset, @Tainted long nbytes, @Tainted int flags)
        throws NativeIOException {
      if (nativeLoaded && syncFileRangePossible) {
        try {
          sync_file_range(fd, offset, nbytes, flags);
        } catch (@Tainted UnsupportedOperationException uoe) {
          syncFileRangePossible = false;
        } catch (@Tainted UnsatisfiedLinkError ule) {
          syncFileRangePossible = false;
        }
      }
    }

    /** Linux only methods used for getOwner() implementation */
    private static native @Tainted long getUIDforFDOwnerforOwner(@Tainted FileDescriptor fd) throws IOException;
    private static native @Tainted String getUserName(@Tainted long uid) throws IOException;

    /**
     * Result type of the fstat call
     */
    public static class Stat {
      private @Tainted int ownerId, groupId;
      private @Tainted String owner, group;
      private @Tainted int mode;

      // Mode constants
      public static final @Tainted int S_IFMT = 0170000;      /* type of file */
      public static final @Tainted int   S_IFIFO  = 0010000;  /* named pipe (fifo) */
      public static final @Tainted int   S_IFCHR  = 0020000;  /* character special */
      public static final @Tainted int   S_IFDIR  = 0040000;  /* directory */
      public static final @Tainted int   S_IFBLK  = 0060000;  /* block special */
      public static final @Tainted int   S_IFREG  = 0100000;  /* regular */
      public static final @Tainted int   S_IFLNK  = 0120000;  /* symbolic link */
      public static final @Tainted int   S_IFSOCK = 0140000;  /* socket */
      public static final @Tainted int   S_IFWHT  = 0160000;  /* whiteout */
      public static final @Tainted int S_ISUID = 0004000;  /* set user id on execution */
      public static final @Tainted int S_ISGID = 0002000;  /* set group id on execution */
      public static final @Tainted int S_ISVTX = 0001000;  /* save swapped text even after use */
      public static final @Tainted int S_IRUSR = 0000400;  /* read permission, owner */
      public static final @Tainted int S_IWUSR = 0000200;  /* write permission, owner */
      public static final @Tainted int S_IXUSR = 0000100;  /* execute/search permission, owner */

      @Tainted
      Stat(@Tainted int ownerId, @Tainted int groupId, @Tainted int mode) {
        this.ownerId = ownerId;
        this.groupId = groupId;
        this.mode = mode;
      }
      
      @Tainted
      Stat(@Tainted String owner, @Tainted String group, @Tainted int mode) {
        if (!Shell.WINDOWS) {
          this.owner = owner;
        } else {
          this.owner = stripDomain(owner);
        }
        if (!Shell.WINDOWS) {
          this.group = group;
        } else {
          this.group = stripDomain(group);
        }
        this.mode = mode;
      }
      
      @Override
      public @Tainted String toString(NativeIO.POSIX.@Tainted Stat this) {
        return "Stat(owner='" + owner + "', group='" + group + "'" +
          ", mode=" + mode + ")";
      }

      public @Tainted String getOwner(NativeIO.POSIX.@Tainted Stat this) {
        return owner;
      }
      public @Tainted String getGroup(NativeIO.POSIX.@Tainted Stat this) {
        return group;
      }
      public @Tainted int getMode(NativeIO.POSIX.@Tainted Stat this) {
        return mode;
      }
    }

    /**
     * Returns the file stat for a file descriptor.
     *
     * @param fd file descriptor.
     * @return the file descriptor file stat.
     * @throws IOException thrown if there was an IO error while obtaining the file stat.
     */
    public static @Tainted Stat getFstat(@Tainted FileDescriptor fd) throws IOException {
      @Tainted
      Stat stat = null;
      if (!Shell.WINDOWS) {
        stat = fstat(fd); 
        stat.owner = getName(IdCache.USER, stat.ownerId);
        stat.group = getName(IdCache.GROUP, stat.groupId);
      } else {
        try {
          stat = fstat(fd);
        } catch (@Tainted NativeIOException nioe) {
          if (nioe.getErrorCode() == 6) {
            throw new @Tainted NativeIOException("The handle is invalid.",
                Errno.EBADF);
          } else {
            LOG.warn(String.format("NativeIO.getFstat error (%d): %s",
                nioe.getErrorCode(), nioe.getMessage()));
            throw new @Tainted NativeIOException("Unknown error", Errno.UNKNOWN);
          }
        }
      }
      return stat;
    }

    private static @Tainted String getName(@Tainted IdCache domain, @Tainted int id) throws IOException {
      @Tainted
      Map<@Tainted Integer, @Tainted CachedName> idNameCache = (domain == IdCache.USER)
        ? USER_ID_NAME_CACHE : GROUP_ID_NAME_CACHE;
      @Tainted
      String name;
      @Tainted
      CachedName cachedName = idNameCache.get(id);
      @Tainted
      long now = System.currentTimeMillis();
      if (cachedName != null && (cachedName.timestamp + cacheTimeout) > now) {
        name = cachedName.name;
      } else {
        name = (domain == IdCache.USER) ? getUserName(id) : getGroupName(id);
        if (LOG.isDebugEnabled()) {
          @Tainted
          String type = (domain == IdCache.USER) ? "UserName" : "GroupName";
          LOG.debug("Got " + type + " " + name + " for ID " + id +
            " from the native implementation");
        }
        cachedName = new @Tainted CachedName(name, now);
        idNameCache.put(id, cachedName);
      }
      return name;
    }

    static native @Tainted String getUserName(@Tainted int uid) throws IOException;
    static native @Tainted String getGroupName(@Tainted int uid) throws IOException;

    private static class CachedName {
      final @Tainted long timestamp;
      final @Tainted String name;

      public @Tainted CachedName(@Tainted String name, @Tainted long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
      }
    }

    private static final @Tainted Map<@Tainted Integer, @Tainted CachedName> USER_ID_NAME_CACHE =
      new @Tainted ConcurrentHashMap<@Tainted Integer, @Tainted CachedName>();

    private static final @Tainted Map<@Tainted Integer, @Tainted CachedName> GROUP_ID_NAME_CACHE =
      new @Tainted ConcurrentHashMap<@Tainted Integer, @Tainted CachedName>();

    private enum IdCache {  @Tainted  USER,  @Tainted  GROUP }
  }

  private static @Tainted boolean workaroundNonThreadSafePasswdCalls = false;


  public static class Windows {
    // Flags for CreateFile() call on Windows
    public static final @Tainted long GENERIC_READ = 0x80000000L;
    public static final @Tainted long GENERIC_WRITE = 0x40000000L;

    public static final @Tainted long FILE_SHARE_READ = 0x00000001L;
    public static final @Tainted long FILE_SHARE_WRITE = 0x00000002L;
    public static final @Tainted long FILE_SHARE_DELETE = 0x00000004L;

    public static final @Tainted long CREATE_NEW = 1;
    public static final @Tainted long CREATE_ALWAYS = 2;
    public static final @Tainted long OPEN_EXISTING = 3;
    public static final @Tainted long OPEN_ALWAYS = 4;
    public static final @Tainted long TRUNCATE_EXISTING = 5;

    public static final @Tainted long FILE_BEGIN = 0;
    public static final @Tainted long FILE_CURRENT = 1;
    public static final @Tainted long FILE_END = 2;

    /** Wrapper around CreateFile() on Windows */
    public static native @Tainted FileDescriptor createFile(@Tainted String path,
        @Tainted
        long desiredAccess, @Tainted long shareMode, @Tainted long creationDisposition)
        throws IOException;

    /** Wrapper around SetFilePointer() on Windows */
    public static native @Tainted long setFilePointer(@Tainted FileDescriptor fd,
        @Tainted
        long distanceToMove, @Tainted long moveMethod) throws IOException;

    /** Windows only methods used for getOwner() implementation */
    private static native @Tainted String getOwner(@Tainted FileDescriptor fd) throws IOException;

    /** Supported list of Windows access right flags */
    public static enum AccessRight {

@Tainted  ACCESS_READ (0x0001),      // FILE_READ_DATA

@Tainted  ACCESS_WRITE (0x0002),     // FILE_WRITE_DATA

@Tainted  ACCESS_EXECUTE (0x0020);   // FILE_EXECUTE

      private final @Tainted int accessRight;
      @Tainted
      AccessRight(@Tainted int access) {
        accessRight = access;
      }

      public @Tainted int accessRight(NativeIO.Windows.@Tainted AccessRight this) {
        return accessRight;
      }
    };

    /** Windows only method used to check if the current process has requested
     *  access rights on the given path. */
    private static native @Tainted boolean access0(@Tainted String path, @Tainted int requestedAccess);

    /**
     * Checks whether the current process has desired access rights on
     * the given path.
     * 
     * Longer term this native function can be substituted with JDK7
     * function Files#isReadable, isWritable, isExecutable.
     *
     * @param path input path
     * @param desiredAccess ACCESS_READ, ACCESS_WRITE or ACCESS_EXECUTE
     * @return true if access is allowed
     * @throws IOException I/O exception on error
     */
    public static @Tainted boolean access(@Tainted String path, @Tainted AccessRight desiredAccess)
        throws IOException {
      return access0(path, desiredAccess.accessRight());
    }

    static {
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        try {
          initNative();
          nativeLoaded = true;
        } catch (@Tainted Throwable t) {
          // This can happen if the user has an older version of libhadoop.so
          // installed - in this case we can continue without native IO
          // after warning
          LOG.error("Unable to initialize NativeIO libraries", t);
        }
      }
    }
  }

  private static final @Tainted Log LOG = LogFactory.getLog(NativeIO.class);

  private static @Tainted boolean nativeLoaded = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        initNative();
        nativeLoaded = true;
      } catch (@Tainted Throwable t) {
        // This can happen if the user has an older version of libhadoop.so
        // installed - in this case we can continue without native IO
        // after warning
        LOG.error("Unable to initialize NativeIO libraries", t);
      }
    }
  }

  /**
   * Return true if the JNI-based native IO extensions are available.
   */
  public static @Tainted boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
  }

  /** Initialize the JNI method ID and class ID cache */
  private static native void initNative();

  private static class CachedUid {
    final @Tainted long timestamp;
    final @Tainted String username;
    public @Tainted CachedUid(@Tainted String username, @Tainted long timestamp) {
      this.timestamp = timestamp;
      this.username = username;
    }
  }
  private static final @Tainted Map<@Tainted Long, @Tainted CachedUid> uidCache =
      new @Tainted ConcurrentHashMap<@Tainted Long, @Tainted CachedUid>();
  private static @Tainted long cacheTimeout;
  private static @Tainted boolean initialized = false;
  
  /**
   * The Windows logon name has two part, NetBIOS domain name and
   * user account name, of the format DOMAIN\UserName. This method
   * will remove the domain part of the full logon name.
   *
   * @param the full principal name containing the domain
   * @return name with domain removed
   */
  private static @Tainted String stripDomain(@Tainted String name) {
    @Tainted
    int i = name.indexOf('\\');
    if (i != -1)
      name = name.substring(i + 1);
    return name;
  }

  public static @Tainted String getOwner(@Tainted FileDescriptor fd) throws IOException {
    ensureInitialized();
    if (Shell.WINDOWS) {
      @Tainted
      String owner = Windows.getOwner(fd);
      owner = stripDomain(owner);
      return owner;
    } else {
      @Tainted
      long uid = POSIX.getUIDforFDOwnerforOwner(fd);
      @Tainted
      CachedUid cUid = uidCache.get(uid);
      @Tainted
      long now = System.currentTimeMillis();
      if (cUid != null && (cUid.timestamp + cacheTimeout) > now) {
        return cUid.username;
      }
      @Tainted
      String user = POSIX.getUserName(uid);
      LOG.info("Got UserName " + user + " for UID " + uid
          + " from the native implementation");
      cUid = new @Tainted CachedUid(user, now);
      uidCache.put(uid, cUid);
      return user;
    }
  }

  /**
   * Create a FileInputStream that shares delete permission on the
   * file opened, i.e. other process can delete the file the
   * FileInputStream is reading. Only Windows implementation uses
   * the native interface.
   */
  public static @Tainted FileInputStream getShareDeleteFileInputStream(@Tainted File f)
      throws IOException {
    if (!Shell.WINDOWS) {
      // On Linux the default FileInputStream shares delete permission
      // on the file opened.
      //
      return new @Tainted FileInputStream(f);
    } else {
      // Use Windows native interface to create a FileInputStream that
      // shares delete permission on the file opened.
      //
      @Tainted
      FileDescriptor fd = Windows.createFile(
          f.getAbsolutePath(),
          Windows.GENERIC_READ,
          Windows.FILE_SHARE_READ |
              Windows.FILE_SHARE_WRITE |
              Windows.FILE_SHARE_DELETE,
          Windows.OPEN_EXISTING);
      return new @Tainted FileInputStream(fd);
    }
  }

  /**
   * Create a FileInputStream that shares delete permission on the
   * file opened at a given offset, i.e. other process can delete
   * the file the FileInputStream is reading. Only Windows implementation
   * uses the native interface.
   */
  public static @Tainted FileInputStream getShareDeleteFileInputStream(@Tainted File f, @Tainted long seekOffset)
      throws IOException {
    if (!Shell.WINDOWS) {
      @Tainted
      RandomAccessFile rf = new @Tainted RandomAccessFile(f, "r");
      if (seekOffset > 0) {
        rf.seek(seekOffset);
      }
      return new @Tainted FileInputStream(rf.getFD());
    } else {
      // Use Windows native interface to create a FileInputStream that
      // shares delete permission on the file opened, and set it to the
      // given offset.
      //
      @Tainted
      FileDescriptor fd = NativeIO.Windows.createFile(
          f.getAbsolutePath(),
          NativeIO.Windows.GENERIC_READ,
          NativeIO.Windows.FILE_SHARE_READ |
              NativeIO.Windows.FILE_SHARE_WRITE |
              NativeIO.Windows.FILE_SHARE_DELETE,
          NativeIO.Windows.OPEN_EXISTING);
      if (seekOffset > 0)
        NativeIO.Windows.setFilePointer(fd, seekOffset, NativeIO.Windows.FILE_BEGIN);
      return new @Tainted FileInputStream(fd);
    }
  }

  /**
   * Create the specified File for write access, ensuring that it does not exist.
   * @param f the file that we want to create
   * @param permissions we want to have on the file (if security is enabled)
   *
   * @throws AlreadyExistsException if the file already exists
   * @throws IOException if any other error occurred
   */
  public static @Tainted FileOutputStream getCreateForWriteFileOutputStream(@Tainted File f, @Tainted int permissions)
      throws IOException {
    if (!Shell.WINDOWS) {
      // Use the native wrapper around open(2)
      try {
        @Tainted
        FileDescriptor fd = NativeIO.POSIX.open(f.getAbsolutePath(),
            NativeIO.POSIX.O_WRONLY | NativeIO.POSIX.O_CREAT
                | NativeIO.POSIX.O_EXCL, permissions);
        return new @Tainted FileOutputStream(fd);
      } catch (@Tainted NativeIOException nioe) {
        if (nioe.getErrno() == Errno.EEXIST) {
          throw new @Tainted AlreadyExistsException(nioe);
        }
        throw nioe;
      }
    } else {
      // Use the Windows native APIs to create equivalent FileOutputStream
      try {
        @Tainted
        FileDescriptor fd = NativeIO.Windows.createFile(f.getCanonicalPath(),
            NativeIO.Windows.GENERIC_WRITE,
            NativeIO.Windows.FILE_SHARE_DELETE
                | NativeIO.Windows.FILE_SHARE_READ
                | NativeIO.Windows.FILE_SHARE_WRITE,
            NativeIO.Windows.CREATE_NEW);
        NativeIO.POSIX.chmod(f.getCanonicalPath(), permissions);
        return new @Tainted FileOutputStream(fd);
      } catch (@Tainted NativeIOException nioe) {
        if (nioe.getErrorCode() == 80) {
          // ERROR_FILE_EXISTS
          // 80 (0x50)
          // The file exists
          throw new @Tainted AlreadyExistsException(nioe);
        }
        throw nioe;
      }
    }
  }

  private synchronized static void ensureInitialized() {
    if (!initialized) {
      cacheTimeout =
          new @Tainted Configuration().getLong("hadoop.security.uid.cache.secs",
              4*60*60) * 1000;
      LOG.info("Initialized cache for UID to User mapping with a cache" +
          " timeout of " + cacheTimeout/1000 + " seconds.");
      initialized = true;
    }
  }
  
  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  public static void renameTo(@Tainted File src, @Tainted File dst)
      throws IOException {
    if (!nativeLoaded) {
      if (!src.renameTo(dst)) {
        throw new @Tainted IOException("renameTo(src=" + src + ", dst=" +
          dst + ") failed.");
      }
    } else {
      renameTo0(src.getAbsolutePath(), dst.getAbsolutePath());
    }
  }

  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  private static native void renameTo0(@Tainted String src, @Tainted String dst)
      throws NativeIOException;
}
