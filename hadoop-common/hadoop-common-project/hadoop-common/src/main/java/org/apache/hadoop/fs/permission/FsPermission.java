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
package org.apache.hadoop.fs.permission;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * A class for file/directory permissions.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FsPermission implements @Tainted Writable {
  private static final @Tainted Log LOG = LogFactory.getLog(FsPermission.class);

  static final @Tainted WritableFactory FACTORY = new @Tainted WritableFactory() {
    @Override
    public @Tainted Writable newInstance() { return new @Tainted FsPermission(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(FsPermission.class, FACTORY);
    WritableFactories.setFactory(ImmutableFsPermission.class, FACTORY);
  }

  /** Create an immutable {@link FsPermission} object. */
  public static @Tainted FsPermission createImmutable(@Tainted short permission) {
    return new @Tainted ImmutableFsPermission(permission);
  }

  //POSIX permission style
  private @Tainted FsAction useraction = null;
  private @Tainted FsAction groupaction = null;
  private @Tainted FsAction otheraction = null;
  private @Tainted boolean stickyBit = false;

  private @Tainted FsPermission() {}

  /**
   * Construct by the given {@link FsAction}.
   * @param u user action
   * @param g group action
   * @param o other action
   */
  public @Tainted FsPermission(@Tainted FsAction u, @Tainted FsAction g, @Tainted FsAction o) {
    this(u, g, o, false);
  }

  public @Tainted FsPermission(@Tainted FsAction u, @Tainted FsAction g, @Tainted FsAction o, @Tainted boolean sb) {
    set(u, g, o, sb);
  }

  /**
   * Construct by the given mode.
   * @param mode
   * @see #toShort()
   */
  public @Tainted FsPermission(@Tainted short mode) { fromShort(mode); }

  /**
   * Copy constructor
   * 
   * @param other other permission
   */
  public @Tainted FsPermission(@Tainted FsPermission other) {
    this.useraction = other.useraction;
    this.groupaction = other.groupaction;
    this.otheraction = other.otheraction;
    this.stickyBit = other.stickyBit;
  }
  
  /**
   * Construct by given mode, either in octal or symbolic format.
   * @param mode mode as a string, either in octal or symbolic format
   * @throws IllegalArgumentException if <code>mode</code> is invalid
   */
  public @Tainted FsPermission(@Tainted String mode) {
    this(new @Tainted UmaskParser(mode).getUMask());
  }

  /** Return user {@link FsAction}. */
  public @Tainted FsAction getUserAction(@Tainted FsPermission this) {return useraction;}

  /** Return group {@link FsAction}. */
  public @Tainted FsAction getGroupAction(@Tainted FsPermission this) {return groupaction;}

  /** Return other {@link FsAction}. */
  public @Tainted FsAction getOtherAction(@Tainted FsPermission this) {return otheraction;}

  private void set(@Tainted FsPermission this, @Tainted FsAction u, @Tainted FsAction g, @Tainted FsAction o, @Tainted boolean sb) {
    useraction = u;
    groupaction = g;
    otheraction = o;
    stickyBit = sb;
  }

  public void fromShort(@Tainted FsPermission this, @Tainted short n) {
    @Tainted
    FsAction @Tainted [] v = FsAction.values();

    set(v[(n >>> 6) & 7], v[(n >>> 3) & 7], v[n & 7], (((n >>> 9) & 1) == 1) );
  }

  @Override
  public void write(@Tainted FsPermission this, @Tainted DataOutput out) throws IOException {
    out.writeShort(toShort());
  }

  @Override
  public void readFields(@Tainted FsPermission this, @Tainted DataInput in) throws IOException {
    fromShort(in.readShort());
  }

  /**
   * Create and initialize a {@link FsPermission} from {@link DataInput}.
   */
  public static @Tainted FsPermission read(@Tainted DataInput in) throws IOException {
    @Tainted
    FsPermission p = new @Tainted FsPermission();
    p.readFields(in);
    return p;
  }

  /**
   * Encode the object to a short.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  //Short values are considered trusted
  public @Untainted short toShort(@Tainted FsPermission this) {
    @Tainted
    int s =  (stickyBit ? 1 << 9 : 0)     |
             (useraction.ordinal() << 6)  |
             (groupaction.ordinal() << 3) |
             otheraction.ordinal();

    return (@Untainted short)s;
  }

  @Override
  public @Tainted boolean equals(@Tainted FsPermission this, @Tainted Object obj) {
    if (obj instanceof @Tainted FsPermission) {
      @Tainted
      FsPermission that = (@Tainted FsPermission)obj;
      return this.useraction == that.useraction
          && this.groupaction == that.groupaction
          && this.otheraction == that.otheraction
          && this.stickyBit == that.stickyBit;
    }
    return false;
  }

  @Override
  public @Tainted int hashCode(@Tainted FsPermission this) {return toShort();}

  @Override
  public @Tainted String toString(@Tainted FsPermission this) {
    @Tainted
    String str = useraction.SYMBOL + groupaction.SYMBOL + otheraction.SYMBOL;
    if(stickyBit) {
      @Tainted
      StringBuilder str2 = new @Tainted StringBuilder(str);
      str2.replace(str2.length() - 1, str2.length(),
           otheraction.implies(FsAction.EXECUTE) ? "t" : "T");
      str = str2.toString();
    }

    return str;
  }

  /**
   * Apply a umask to this permission and return a new one.
   *
   * The umask is used by create, mkdir, and other Hadoop filesystem operations.
   * The mode argument for these operations is modified by removing the bits
   * which are set in the umask.  Thus, the umask limits the permissions which
   * newly created files and directories get.
   *
   * @param umask              The umask to use
   * 
   * @return                   The effective permission
   */
  public @Tainted FsPermission applyUMask(@Tainted FsPermission this, @Tainted FsPermission umask) {
    return new @Tainted FsPermission(useraction.and(umask.useraction.not()),
        groupaction.and(umask.groupaction.not()),
        otheraction.and(umask.otheraction.not()));
  }

  /** umask property label deprecated key and code in getUMask method
   *  to accommodate it may be removed in version .23 */
  public static final @Tainted String DEPRECATED_UMASK_LABEL = "dfs.umask"; 
  public static final @Tainted String UMASK_LABEL = 
                  CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY;
  public static final @Tainted int DEFAULT_UMASK = 
                  CommonConfigurationKeys.FS_PERMISSIONS_UMASK_DEFAULT;

  /** 
   * Get the user file creation mask (umask)
   * 
   * {@code UMASK_LABEL} config param has umask value that is either symbolic 
   * or octal.
   * 
   * Symbolic umask is applied relative to file mode creation mask; 
   * the permission op characters '+' clears the corresponding bit in the mask, 
   * '-' sets bits in the mask.
   * 
   * Octal umask, the specified bits are set in the file mode creation mask.
   * 
   * {@code DEPRECATED_UMASK_LABEL} config param has umask value set to decimal.
   */
  public static @Tainted FsPermission getUMask(@Tainted Configuration conf) {
    @Tainted
    int umask = DEFAULT_UMASK;
    
    // To ensure backward compatibility first use the deprecated key.
    // If the deprecated key is not present then check for the new key
    if(conf != null) {
      @Tainted
      String confUmask = conf.get(UMASK_LABEL);
      @Tainted
      int oldUmask = conf.getInt(DEPRECATED_UMASK_LABEL, Integer.MIN_VALUE);
      try {
        if(confUmask != null) {
          umask = new @Tainted UmaskParser(confUmask).getUMask();
        }
      } catch(@Tainted IllegalArgumentException iae) {
        // Provide more explanation for user-facing message
        @Tainted
        String type = iae instanceof @Tainted NumberFormatException ? "decimal"
            : "octal or symbolic";
        @Tainted
        String error = "Unable to parse configuration " + UMASK_LABEL
            + " with value " + confUmask + " as " + type + " umask.";
        LOG.warn(error);
        
        // If oldUmask is not set, then throw the exception
        if (oldUmask == Integer.MIN_VALUE) {
          throw new @Tainted IllegalArgumentException(error);
        }
      }
        
      if(oldUmask != Integer.MIN_VALUE) { // Property was set with old key
        if (umask != oldUmask) {
          LOG.warn(DEPRECATED_UMASK_LABEL
              + " configuration key is deprecated. " + "Convert to "
              + UMASK_LABEL + ", using octal or symbolic umask "
              + "specifications.");
          // Old and new umask values do not match - Use old umask
          umask = oldUmask;
        }
      }
    }
    
    return new @Tainted FsPermission((@Tainted short)umask);
  }

  public @Tainted boolean getStickyBit(@Tainted FsPermission this) {
    return stickyBit;
  }

  /** Set the user file creation mask (umask) */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void setUMask(@Tainted Configuration conf, @Tainted FsPermission umask) {
    //Integer/short strings are considered trusted
    conf.set(UMASK_LABEL, (@Untainted String) String.format("%1$03o", umask.toShort()));
    conf.setInt(DEPRECATED_UMASK_LABEL, (@Untainted Short) umask.toShort());
  }

  /**
   * Get the default permission for directory and symlink.
   * In previous versions, this default permission was also used to
   * create files, so files created end up with ugo+x permission.
   * See HADOOP-9155 for detail. 
   * Two new methods are added to solve this, please use 
   * {@link FsPermission#getDirDefault()} for directory, and use
   * {@link FsPermission#getFileDefault()} for file.
   * This method is kept for compatibility.
   */
  public static @Tainted FsPermission getDefault() {
    return new @Tainted FsPermission((@Tainted short)00777);
  }

  /**
   * Get the default permission for directory.
   */
  public static @Tainted FsPermission getDirDefault() {
    return new @Tainted FsPermission((@Tainted short)00777);
  }

  /**
   * Get the default permission for file.
   */
  public static @Tainted FsPermission getFileDefault() {
    return new @Tainted FsPermission((@Tainted short)00666);
  }

  /**
   * Create a FsPermission from a Unix symbolic permission string
   * @param unixSymbolicPermission e.g. "-rw-rw-rw-"
   */
  public static @Tainted FsPermission valueOf(@Tainted String unixSymbolicPermission) {
    if (unixSymbolicPermission == null) {
      return null;
    }
    else if (unixSymbolicPermission.length() != 10) {
      throw new @Tainted IllegalArgumentException("length != 10(unixSymbolicPermission="
          + unixSymbolicPermission + ")");
    }

    @Untainted
    int n = 0;
    for(@Tainted int i = 1; i < unixSymbolicPermission.length(); i++) {
      n = n << 1;
      @Tainted
      char c = unixSymbolicPermission.charAt(i);
      n += (c == '-' || c == 'T' || c == 'S') ? 0: 1;
    }

    // Add sticky bit value if set
    if(unixSymbolicPermission.charAt(9) == 't' ||
        unixSymbolicPermission.charAt(9) == 'T')
      n += 01000;

    return new @Tainted FsPermission((@Tainted short)n);
  }
  
  private static class ImmutableFsPermission extends @Tainted FsPermission {
    public @Tainted ImmutableFsPermission(@Tainted short permission) {
      super(permission);
    }
    @Override
    public @Tainted FsPermission applyUMask(FsPermission.@Tainted ImmutableFsPermission this, @Tainted FsPermission umask) {
      throw new @Tainted UnsupportedOperationException();
    }
    @Override
    public void readFields(FsPermission.@Tainted ImmutableFsPermission this, @Tainted DataInput in) throws IOException {
      throw new @Tainted UnsupportedOperationException();
    }    
  }
}
