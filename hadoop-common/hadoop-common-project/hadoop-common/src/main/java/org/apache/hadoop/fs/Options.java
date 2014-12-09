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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/**
 * This class contains options related to file system operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Options {
  /**
   * Class to support the varargs for create() options.
   *
   */
  public static class CreateOpts {
    private @Tainted CreateOpts() { };
    public static @Tainted BlockSize blockSize(@Tainted long bs) { 
      return new @Tainted BlockSize(bs);
    }
    public static @Tainted BufferSize bufferSize(@Tainted int bs) { 
      return new @Tainted BufferSize(bs);
    }
    public static @Tainted ReplicationFactor repFac(@Tainted short rf) { 
      return new @Tainted ReplicationFactor(rf);
    }
    public static @Tainted BytesPerChecksum bytesPerChecksum(@Tainted short crc) {
      return new @Tainted BytesPerChecksum(crc);
    }
    public static @Tainted ChecksumParam checksumParam(
        @Tainted
        ChecksumOpt csumOpt) {
      return new @Tainted ChecksumParam(csumOpt);
    }
    public static @Tainted Perms perms(@Tainted FsPermission perm) {
      return new @Tainted Perms(perm);
    }
    public static @Tainted CreateParent createParent() {
      return new @Tainted CreateParent(true);
    }
    public static @Tainted CreateParent donotCreateParent() {
      return new @Tainted CreateParent(false);
    }
    
    public static class BlockSize extends @Tainted CreateOpts {
      private final @Tainted long blockSize;
      protected @Tainted BlockSize(@Tainted long bs) {
        if (bs <= 0) {
          throw new @Tainted IllegalArgumentException(
                        "Block size must be greater than 0");
        }
        blockSize = bs; 
      }
      public @Tainted long getValue(Options.CreateOpts.@Tainted BlockSize this) { return blockSize; }
    }
    
    public static class ReplicationFactor extends @Tainted CreateOpts {
      private final @Tainted short replication;
      protected @Tainted ReplicationFactor(@Tainted short rf) { 
        if (rf <= 0) {
          throw new @Tainted IllegalArgumentException(
                      "Replication must be greater than 0");
        }
        replication = rf;
      }
      public @Tainted short getValue(Options.CreateOpts.@Tainted ReplicationFactor this) { return replication; }
    }
    
    public static class BufferSize extends @Tainted CreateOpts {
      private final @Tainted int bufferSize;
      protected @Tainted BufferSize(@Tainted int bs) {
        if (bs <= 0) {
          throw new @Tainted IllegalArgumentException(
                        "Buffer size must be greater than 0");
        }
        bufferSize = bs; 
      }
      public @Tainted int getValue(Options.CreateOpts.@Tainted BufferSize this) { return bufferSize; }
    }

    /** This is not needed if ChecksumParam is specified. **/
    public static class BytesPerChecksum extends @Tainted CreateOpts {
      private final @Tainted int bytesPerChecksum;
      protected @Tainted BytesPerChecksum(@Tainted short bpc) { 
        if (bpc <= 0) {
          throw new @Tainted IllegalArgumentException(
                        "Bytes per checksum must be greater than 0");
        }
        bytesPerChecksum = bpc; 
      }
      public @Tainted int getValue(Options.CreateOpts.@Tainted BytesPerChecksum this) { return bytesPerChecksum; }
    }

    public static class ChecksumParam extends @Tainted CreateOpts {
      private final @Tainted ChecksumOpt checksumOpt;
      protected @Tainted ChecksumParam(@Tainted ChecksumOpt csumOpt) {
        checksumOpt = csumOpt;
      }
      public @Tainted ChecksumOpt getValue(Options.CreateOpts.@Tainted ChecksumParam this) { return checksumOpt; }
    }
    
    public static class Perms extends @Tainted CreateOpts {
      private final @Tainted FsPermission permissions;
      protected @Tainted Perms(@Tainted FsPermission perm) { 
        if(perm == null) {
          throw new @Tainted IllegalArgumentException("Permissions must not be null");
        }
        permissions = perm; 
      }
      public @Tainted FsPermission getValue(Options.CreateOpts.@Tainted Perms this) { return permissions; }
    }
    
    public static class Progress extends @Tainted CreateOpts {
      private final @Tainted Progressable progress;
      protected @Tainted Progress(@Tainted Progressable prog) { 
        if(prog == null) {
          throw new @Tainted IllegalArgumentException("Progress must not be null");
        }
        progress = prog;
      }
      public @Tainted Progressable getValue(Options.CreateOpts.@Tainted Progress this) { return progress; }
    }
    
    public static class CreateParent extends @Tainted CreateOpts {
      private final @Tainted boolean createParent;
      protected @Tainted CreateParent(@Tainted boolean createPar) {
        createParent = createPar;}
      public @Tainted boolean getValue(Options.CreateOpts.@Tainted CreateParent this) { return createParent; }
    }

    
    /**
     * Get an option of desired type
     * @param theClass is the desired class of the opt
     * @param opts - not null - at least one opt must be passed
     * @return an opt from one of the opts of type theClass.
     *   returns null if there isn't any
     */
    protected static @Tainted CreateOpts getOpt(@Tainted Class<@Tainted ? extends @Tainted CreateOpts> theClass,  @Tainted CreateOpts @Tainted ...opts) {
      if (opts == null) {
        throw new @Tainted IllegalArgumentException("Null opt");
      }
      @Tainted
      CreateOpts result = null;
      for (@Tainted int i = 0; i < opts.length; ++i) {
        if (opts[i].getClass() == theClass) {
          if (result != null) 
            throw new @Tainted IllegalArgumentException("multiple blocksize varargs");
          result = opts[i];
        }
      }
      return result;
    }
    /**
     * set an option
     * @param newValue  the option to be set
     * @param opts  - the option is set into this array of opts
     * @return updated CreateOpts[] == opts + newValue
     */
    protected static <@Tainted T extends @Tainted CreateOpts> @Tainted CreateOpts @Tainted [] setOpt(@Tainted T newValue,
        @Tainted
        CreateOpts @Tainted ...opts) {
      @Tainted
      boolean alreadyInOpts = false;
      if (opts != null) {
        for (@Tainted int i = 0; i < opts.length; ++i) {
          if (opts[i].getClass() == newValue.getClass()) {
            if (alreadyInOpts) 
              throw new @Tainted IllegalArgumentException("multiple opts varargs");
            alreadyInOpts = true;
            opts[i] = newValue;
          }
        }
      }
      @Tainted
      CreateOpts @Tainted [] resultOpt = opts;
      if (!alreadyInOpts) { // no newValue in opt
        @Tainted
        CreateOpts @Tainted [] newOpts = new @Tainted CreateOpts @Tainted [opts.length + 1];
        System.arraycopy(opts, 0, newOpts, 0, opts.length);
        newOpts[opts.length] = newValue;
        resultOpt = newOpts;
      }
      return resultOpt;
    }
  }

  /**
   * Enum to support the varargs for rename() options
   */
  public static enum Rename {

@Tainted  NONE((@Tainted byte) 0), // No options

@Tainted  OVERWRITE((@Tainted byte) 1); // Overwrite the rename destination

    private final @Tainted byte code;
    
    private @Tainted Rename(@Tainted byte code) {
      this.code = code;
    }

    public static @Tainted Rename valueOf(@Tainted byte code) {
      return code < 0 || code >= values().length ? null : values()[code];
    }

    public @Tainted byte value(Options.@Tainted Rename this) {
      return code;
    }
  }

  /**
   * This is used in FileSystem and FileContext to specify checksum options.
   */
  public static class ChecksumOpt {
    private final @Tainted int crcBlockSize;
    private final DataChecksum.@Tainted Type crcType;

    /**
     * Create a uninitialized one
     */
    public @Tainted ChecksumOpt() {
      crcBlockSize = -1;
      crcType = DataChecksum.Type.DEFAULT;
    }

    /**
     * Normal ctor
     * @param type checksum type
     * @param size bytes per checksum
     */
    public @Tainted ChecksumOpt(DataChecksum.@Tainted Type type, @Tainted int size) {
      crcBlockSize = size;
      crcType = type;
    }

    public @Tainted int getBytesPerChecksum(Options.@Tainted ChecksumOpt this) {
      return crcBlockSize;
    }

    public DataChecksum.@Tainted Type getChecksumType(Options.@Tainted ChecksumOpt this) {
      return crcType;
    }

    /**
     * Create a ChecksumOpts that disables checksum
     */
    public static @Tainted ChecksumOpt createDisabled() {
      return new @Tainted ChecksumOpt(DataChecksum.Type.NULL, -1);
    }

    /**
     * A helper method for processing user input and default value to 
     * create a combined checksum option. This is a bit complicated because
     * bytesPerChecksum is kept for backward compatibility.
     *
     * @param defaultOpt Default checksum option
     * @param userOpt User-specified checksum option. Ignored if null.
     * @param userBytesPerChecksum User-specified bytesPerChecksum
     *                Ignored if < 0.
     */
    public static @Tainted ChecksumOpt processChecksumOpt(@Tainted ChecksumOpt defaultOpt, 
        @Tainted
        ChecksumOpt userOpt, @Tainted int userBytesPerChecksum) {
      // The following is done to avoid unnecessary creation of new objects.
      // tri-state variable: 0 default, 1 userBytesPerChecksum, 2 userOpt
      @Tainted
      short whichSize;
      // true default, false userOpt
      @Tainted
      boolean useDefaultType;
      
      //  bytesPerChecksum - order of preference
      //    user specified value in bytesPerChecksum
      //    user specified value in checksumOpt
      //    default.
      if (userBytesPerChecksum > 0) {
        whichSize = 1; // userBytesPerChecksum
      } else if (userOpt != null && userOpt.getBytesPerChecksum() > 0) {
        whichSize = 2; // userOpt
      } else {
        whichSize = 0; // default
      }

      // checksum type - order of preference
      //   user specified value in checksumOpt
      //   default.
      if (userOpt != null &&
            userOpt.getChecksumType() != DataChecksum.Type.DEFAULT) {
        useDefaultType = false;
      } else {
        useDefaultType = true;
      }

      // Short out the common and easy cases
      if (whichSize == 0 && useDefaultType) {
        return defaultOpt;
      } else if (whichSize == 2 && !useDefaultType) {
        return userOpt;
      }

      // Take care of the rest of combinations
      DataChecksum.@Tainted Type type = useDefaultType ? defaultOpt.getChecksumType() :
          userOpt.getChecksumType();
      if (whichSize == 0) {
        return new @Tainted ChecksumOpt(type, defaultOpt.getBytesPerChecksum());
      } else if (whichSize == 1) {
        return new @Tainted ChecksumOpt(type, userBytesPerChecksum);
      } else {
        return new @Tainted ChecksumOpt(type, userOpt.getBytesPerChecksum());
      }
    }

    /**
     * A helper method for processing user input and default value to 
     * create a combined checksum option. 
     *
     * @param defaultOpt Default checksum option
     * @param userOpt User-specified checksum option
     */
    public static @Tainted ChecksumOpt processChecksumOpt(@Tainted ChecksumOpt defaultOpt,
        @Tainted
        ChecksumOpt userOpt) {
      return processChecksumOpt(defaultOpt, userOpt, -1);
    }
  }
}
