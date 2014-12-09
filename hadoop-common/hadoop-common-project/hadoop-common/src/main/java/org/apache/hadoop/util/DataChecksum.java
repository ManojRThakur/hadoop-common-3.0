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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;

/**
 * This class provides inteface and utilities for processing checksums for
 * DFS data transfers.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DataChecksum implements @Tainted Checksum {
  
  // Misc constants
  public static final @Tainted int HEADER_LEN = 5; /// 1 byte type and 4 byte len
  
  // checksum types
  public static final @Tainted int CHECKSUM_NULL    = 0;
  public static final @Tainted int CHECKSUM_CRC32   = 1;
  public static final @Tainted int CHECKSUM_CRC32C  = 2;
  public static final @Tainted int CHECKSUM_DEFAULT = 3; 
  public static final @Tainted int CHECKSUM_MIXED   = 4;
 
  /** The checksum types */
  public static enum Type {

@Tainted  NULL  (CHECKSUM_NULL, 0),

@Tainted  CRC32 (CHECKSUM_CRC32, 4),

@Tainted  CRC32C(CHECKSUM_CRC32C, 4),

@Tainted  DEFAULT(CHECKSUM_DEFAULT, 0), // This cannot be used to create DataChecksum

@Tainted  MIXED (CHECKSUM_MIXED, 0); // This cannot be used to create DataChecksum

    public final @Tainted int id;
    public final @Tainted int size;
    
    private @Tainted Type(@Tainted int id, @Tainted int size) {
      this.id = id;
      this.size = size;
    }

    /** @return the type corresponding to the id. */
    public static @Tainted Type valueOf(@Tainted int id) {
      if (id < 0 || id >= values().length) {
        throw new @Tainted IllegalArgumentException("id=" + id
            + " out of range [0, " + values().length + ")");
      }
      return values()[id];
    }
  }


  public static @Tainted DataChecksum newDataChecksum(@Tainted Type type, @Tainted int bytesPerChecksum ) {
    if ( bytesPerChecksum <= 0 ) {
      return null;
    }
    
    switch ( type ) {
    case NULL :
      return new @Tainted DataChecksum(type, new @Tainted ChecksumNull(), bytesPerChecksum );
    case CRC32 :
      return new @Tainted DataChecksum(type, new @Tainted PureJavaCrc32(), bytesPerChecksum );
    case CRC32C:
      return new @Tainted DataChecksum(type, new @Tainted PureJavaCrc32C(), bytesPerChecksum);
    default:
      return null;  
    }
  }
  
  /**
   * Creates a DataChecksum from HEADER_LEN bytes from arr[offset].
   * @return DataChecksum of the type in the array or null in case of an error.
   */
  public static @Tainted DataChecksum newDataChecksum( @Tainted byte bytes @Tainted [], @Tainted int offset ) {
    if ( offset < 0 || bytes.length < offset + HEADER_LEN ) {
      return null;
    }
    
    // like readInt():
    @Tainted
    int bytesPerChecksum = ( (bytes[offset+1] & 0xff) << 24 ) | 
                           ( (bytes[offset+2] & 0xff) << 16 ) |
                           ( (bytes[offset+3] & 0xff) << 8 )  |
                           ( (bytes[offset+4] & 0xff) );
    return newDataChecksum( Type.valueOf(bytes[offset]), bytesPerChecksum );
  }
  
  /**
   * This constructucts a DataChecksum by reading HEADER_LEN bytes from
   * input stream <i>in</i>
   */
  public static @Tainted DataChecksum newDataChecksum( @Tainted DataInputStream in )
                                 throws IOException {
    @Tainted
    int type = in.readByte();
    @Tainted
    int bpc = in.readInt();
    @Tainted
    DataChecksum summer = newDataChecksum(Type.valueOf(type), bpc );
    if ( summer == null ) {
      throw new @Tainted IOException( "Could not create DataChecksum of type " +
                             type + " with bytesPerChecksum " + bpc );
    }
    return summer;
  }
  
  /**
   * Writes the checksum header to the output stream <i>out</i>.
   */
  public void writeHeader( @Tainted DataChecksum this, @Tainted DataOutputStream out ) 
                           throws IOException { 
    out.writeByte( type.id );
    out.writeInt( bytesPerChecksum );
  }

  public @Tainted byte @Tainted [] getHeader(@Tainted DataChecksum this) {
    @Tainted
    byte @Tainted [] header = new @Tainted byte @Tainted [DataChecksum.HEADER_LEN];
    header[0] = (@Tainted byte) (type.id & 0xff);
    // Writing in buffer just like DataOutput.WriteInt()
    header[1+0] = (@Tainted byte) ((bytesPerChecksum >>> 24) & 0xff);
    header[1+1] = (@Tainted byte) ((bytesPerChecksum >>> 16) & 0xff);
    header[1+2] = (@Tainted byte) ((bytesPerChecksum >>> 8) & 0xff);
    header[1+3] = (@Tainted byte) (bytesPerChecksum & 0xff);
    return header;
  }
  
  /**
   * Writes the current checksum to the stream.
   * If <i>reset</i> is true, then resets the checksum.
   * @return number of bytes written. Will be equal to getChecksumSize();
   */
   public @Tainted int writeValue( @Tainted DataChecksum this, @Tainted DataOutputStream out, @Tainted boolean reset )
                          throws IOException {
     if ( type.size <= 0 ) {
       return 0;
     }

     if ( type.size == 4 ) {
       out.writeInt( (@Tainted int) summer.getValue() );
     } else {
       throw new @Tainted IOException( "Unknown Checksum " + type );
     }
     
     if ( reset ) {
       reset();
     }
     
     return type.size;
   }
   
   /**
    * Writes the current checksum to a buffer.
    * If <i>reset</i> is true, then resets the checksum.
    * @return number of bytes written. Will be equal to getChecksumSize();
    */
    public @Tainted int writeValue( @Tainted DataChecksum this, @Tainted byte @Tainted [] buf, @Tainted int offset, @Tainted boolean reset )
                           throws IOException {
      if ( type.size <= 0 ) {
        return 0;
      }

      if ( type.size == 4 ) {
        @Tainted
        int checksum = (@Tainted int) summer.getValue();
        buf[offset+0] = (@Tainted byte) ((checksum >>> 24) & 0xff);
        buf[offset+1] = (@Tainted byte) ((checksum >>> 16) & 0xff);
        buf[offset+2] = (@Tainted byte) ((checksum >>> 8) & 0xff);
        buf[offset+3] = (@Tainted byte) (checksum & 0xff);
      } else {
        throw new @Tainted IOException( "Unknown Checksum " + type );
      }
      
      if ( reset ) {
        reset();
      }
      
      return type.size;
    }
   
   /**
    * Compares the checksum located at buf[offset] with the current checksum.
    * @return true if the checksum matches and false otherwise.
    */
   public @Tainted boolean compare( @Tainted DataChecksum this, @Tainted byte buf @Tainted [], @Tainted int offset ) {
     if ( type.size == 4 ) {
       @Tainted
       int checksum = ( (buf[offset+0] & 0xff) << 24 ) | 
                      ( (buf[offset+1] & 0xff) << 16 ) |
                      ( (buf[offset+2] & 0xff) << 8 )  |
                      ( (buf[offset+3] & 0xff) );
       return checksum == (@Tainted int) summer.getValue();
     }
     return type.size == 0;
   }
   
  private final @Tainted Type type;
  private final @Tainted Checksum summer;
  private final @Tainted int bytesPerChecksum;
  private @Tainted int inSum = 0;
  
  private @Tainted DataChecksum( @Tainted Type type, @Tainted Checksum checksum, @Tainted int chunkSize ) {
    this.type = type;
    summer = checksum;
    bytesPerChecksum = chunkSize;
  }
  
  // Accessors
  public @Tainted Type getChecksumType(@Tainted DataChecksum this) {
    return type;
  }
  public @Tainted int getChecksumSize(@Tainted DataChecksum this) {
    return type.size;
  }
  public @Tainted int getBytesPerChecksum(@Tainted DataChecksum this) {
    return bytesPerChecksum;
  }
  public @Tainted int getNumBytesInSum(@Tainted DataChecksum this) {
    return inSum;
  }
  
  public static final @Tainted int SIZE_OF_INTEGER = Integer.SIZE / Byte.SIZE;
  static public @Tainted int getChecksumHeaderSize() {
    return 1 + SIZE_OF_INTEGER; // type byte, bytesPerChecksum int
  }
  //Checksum Interface. Just a wrapper around member summer.
  @Override
  public @Tainted long getValue(@Tainted DataChecksum this) {
    return summer.getValue();
  }
  @Override
  public void reset(@Tainted DataChecksum this) {
    summer.reset();
    inSum = 0;
  }
  @Override
  public void update( @Tainted DataChecksum this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len ) {
    if ( len > 0 ) {
      summer.update( b, off, len );
      inSum += len;
    }
  }
  @Override
  public void update( @Tainted DataChecksum this, @Tainted int b ) {
    summer.update( b );
    inSum += 1;
  }
  
  /**
   * Verify that the given checksums match the given data.
   * 
   * The 'mark' of the ByteBuffer parameters may be modified by this function,.
   * but the position is maintained.
   *  
   * @param data the DirectByteBuffer pointing to the data to verify.
   * @param checksums the DirectByteBuffer pointing to a series of stored
   *                  checksums
   * @param fileName the name of the file being read, for error-reporting
   * @param basePos the file position to which the start of 'data' corresponds
   * @throws ChecksumException if the checksums do not match
   */
  public void verifyChunkedSums(@Tainted DataChecksum this, @Tainted ByteBuffer data, @Tainted ByteBuffer checksums,
      @Tainted
      String fileName, @Tainted long basePos)
  throws ChecksumException {
    if (type.size == 0) return;
    
    if (data.hasArray() && checksums.hasArray()) {
      verifyChunkedSums(
          data.array(), data.arrayOffset() + data.position(), data.remaining(),
          checksums.array(), checksums.arrayOffset() + checksums.position(),
          fileName, basePos);
      return;
    }
    if (NativeCrc32.isAvailable()) {
      NativeCrc32.verifyChunkedSums(bytesPerChecksum, type.id, checksums, data,
          fileName, basePos);
      return;
    }
    
    @Tainted
    int startDataPos = data.position();
    data.mark();
    checksums.mark();
    try {
      @Tainted
      byte @Tainted [] buf = new @Tainted byte @Tainted [bytesPerChecksum];
      @Tainted
      byte @Tainted [] sum = new @Tainted byte @Tainted [type.size];
      while (data.remaining() > 0) {
        @Tainted
        int n = Math.min(data.remaining(), bytesPerChecksum);
        checksums.get(sum);
        data.get(buf, 0, n);
        summer.reset();
        summer.update(buf, 0, n);
        @Tainted
        int calculated = (@Tainted int)summer.getValue();
        @Tainted
        int stored = (sum[0] << 24 & 0xff000000) |
          (sum[1] << 16 & 0xff0000) |
          (sum[2] << 8 & 0xff00) |
          sum[3] & 0xff;
        if (calculated != stored) {
          @Tainted
          long errPos = basePos + data.position() - startDataPos - n;
          throw new @Tainted ChecksumException(
              "Checksum error: "+ fileName + " at "+ errPos +
              " exp: " + stored + " got: " + calculated, errPos);
        }
      }
    } finally {
      data.reset();
      checksums.reset();
    }
  }
  
  /**
   * Implementation of chunked verification specifically on byte arrays. This
   * is to avoid the copy when dealing with ByteBuffers that have array backing.
   */
  private void verifyChunkedSums(
      @Tainted DataChecksum this, @Tainted
      byte @Tainted [] data, @Tainted int dataOff, @Tainted int dataLen,
      @Tainted
      byte @Tainted [] checksums, @Tainted int checksumsOff, @Tainted String fileName,
      @Tainted
      long basePos) throws ChecksumException {
    
    @Tainted
    int remaining = dataLen;
    @Tainted
    int dataPos = 0;
    while (remaining > 0) {
      @Tainted
      int n = Math.min(remaining, bytesPerChecksum);
      
      summer.reset();
      summer.update(data, dataOff + dataPos, n);
      dataPos += n;
      remaining -= n;
      
      @Tainted
      int calculated = (@Tainted int)summer.getValue();
      @Tainted
      int stored = (checksums[checksumsOff] << 24 & 0xff000000) |
        (checksums[checksumsOff + 1] << 16 & 0xff0000) |
        (checksums[checksumsOff + 2] << 8 & 0xff00) |
        checksums[checksumsOff + 3] & 0xff;
      checksumsOff += 4;
      if (calculated != stored) {
        @Tainted
        long errPos = basePos + dataPos - n;
        throw new @Tainted ChecksumException(
            "Checksum error: "+ fileName + " at "+ errPos +
            " exp: " + stored + " got: " + calculated, errPos);
      }
    }
  }

  /**
   * Calculate checksums for the given data.
   * 
   * The 'mark' of the ByteBuffer parameters may be modified by this function,
   * but the position is maintained.
   * 
   * @param data the DirectByteBuffer pointing to the data to checksum.
   * @param checksums the DirectByteBuffer into which checksums will be
   *                  stored. Enough space must be available in this
   *                  buffer to put the checksums.
   */
  public void calculateChunkedSums(@Tainted DataChecksum this, @Tainted ByteBuffer data, @Tainted ByteBuffer checksums) {
    if (type.size == 0) return;
    
    if (data.hasArray() && checksums.hasArray()) {
      calculateChunkedSums(data.array(), data.arrayOffset() + data.position(), data.remaining(),
          checksums.array(), checksums.arrayOffset() + checksums.position());
      return;
    }
    
    data.mark();
    checksums.mark();
    try {
      @Tainted
      byte @Tainted [] buf = new @Tainted byte @Tainted [bytesPerChecksum];
      while (data.remaining() > 0) {
        @Tainted
        int n = Math.min(data.remaining(), bytesPerChecksum);
        data.get(buf, 0, n);
        summer.reset();
        summer.update(buf, 0, n);
        checksums.putInt((@Tainted int)summer.getValue());
      }
    } finally {
      data.reset();
      checksums.reset();
    }
  }

  /**
   * Implementation of chunked calculation specifically on byte arrays. This
   * is to avoid the copy when dealing with ByteBuffers that have array backing.
   */
  private void calculateChunkedSums(
      @Tainted DataChecksum this, @Tainted
      byte @Tainted [] data, @Tainted int dataOffset, @Tainted int dataLength,
      @Tainted
      byte @Tainted [] sums, @Tainted int sumsOffset) {

    @Tainted
    int remaining = dataLength;
    while (remaining > 0) {
      @Tainted
      int n = Math.min(remaining, bytesPerChecksum);
      summer.reset();
      summer.update(data, dataOffset, n);
      dataOffset += n;
      remaining -= n;
      @Tainted
      long calculated = summer.getValue();
      sums[sumsOffset++] = (@Tainted byte) (calculated >> 24);
      sums[sumsOffset++] = (@Tainted byte) (calculated >> 16);
      sums[sumsOffset++] = (@Tainted byte) (calculated >> 8);
      sums[sumsOffset++] = (@Tainted byte) (calculated);
    }
  }

  @Override
  public @Tainted boolean equals(@Tainted DataChecksum this, @Tainted Object other) {
    if (!(other instanceof @Tainted DataChecksum)) {
      return false;
    }
    @Tainted
    DataChecksum o = (@Tainted DataChecksum)other;
    return o.bytesPerChecksum == this.bytesPerChecksum &&
      o.type == this.type;
  }
  
  @Override
  public @Tainted int hashCode(@Tainted DataChecksum this) {
    return (this.type.id + 31) * this.bytesPerChecksum;
  }
  
  @Override
  public @Tainted String toString(@Tainted DataChecksum this) {
    return "DataChecksum(type=" + type +
      ", chunkSize=" + bytesPerChecksum + ")";
  }
  
  /**
   * This just provides a dummy implimentation for Checksum class
   * This is used when there is no checksum available or required for 
   * data
   */
  static class ChecksumNull implements @Tainted Checksum {
    
    public @Tainted ChecksumNull() {}
    
    //Dummy interface
    @Override
    public @Tainted long getValue(DataChecksum.@Tainted ChecksumNull this) { return 0; }
    @Override
    public void reset(DataChecksum.@Tainted ChecksumNull this) {}
    @Override
    public void update(DataChecksum.@Tainted ChecksumNull this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {}
    @Override
    public void update(DataChecksum.@Tainted ChecksumNull this, @Tainted int b) {}
  };
}
