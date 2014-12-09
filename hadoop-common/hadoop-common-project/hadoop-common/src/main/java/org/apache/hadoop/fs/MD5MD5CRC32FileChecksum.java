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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.znerd.xmlenc.XMLOutputter;

import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;

/** MD5 of MD5 of CRC32. */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class MD5MD5CRC32FileChecksum extends @Tainted FileChecksum {
  public static final @Tainted int LENGTH = MD5Hash.MD5_LEN
      + (Integer.SIZE + Long.SIZE)/Byte.SIZE;

  private @Tainted int bytesPerCRC;
  private @Tainted long crcPerBlock;
  private @Tainted MD5Hash md5;

  /** Same as this(0, 0, null) */
  public @Tainted MD5MD5CRC32FileChecksum() {
    this(0, 0, null);
  }

  /** Create a MD5FileChecksum */
  public @Tainted MD5MD5CRC32FileChecksum(@Tainted int bytesPerCRC, @Tainted long crcPerBlock, @Tainted MD5Hash md5) {
    this.bytesPerCRC = bytesPerCRC;
    this.crcPerBlock = crcPerBlock;
    this.md5 = md5;
  }
  
  @Override
  public @Tainted String getAlgorithmName(@Tainted MD5MD5CRC32FileChecksum this) {
    return "MD5-of-" + crcPerBlock + "MD5-of-" + bytesPerCRC +
        getCrcType().name();
  }

  public static DataChecksum.@Tainted Type getCrcTypeFromAlgorithmName(@Tainted String algorithm)
      throws IOException {
    if (algorithm.endsWith(DataChecksum.Type.CRC32.name())) {
      return DataChecksum.Type.CRC32;
    } else if (algorithm.endsWith(DataChecksum.Type.CRC32C.name())) {
      return DataChecksum.Type.CRC32C;
    }

    throw new @Tainted IOException("Unknown checksum type in " + algorithm);
  }
 
  @Override
  public @Tainted int getLength(@Tainted MD5MD5CRC32FileChecksum this) {return LENGTH;}
 
  @Override
  public @Tainted byte @Tainted [] getBytes(@Tainted MD5MD5CRC32FileChecksum this) {
    return WritableUtils.toByteArray(this);
  }

  /** returns the CRC type */
  public DataChecksum.@Tainted Type getCrcType(@Tainted MD5MD5CRC32FileChecksum this) {
    // default to the one that is understood by all releases.
    return DataChecksum.Type.CRC32;
  }

  public @Tainted ChecksumOpt getChecksumOpt(@Tainted MD5MD5CRC32FileChecksum this) {
    return new @Tainted ChecksumOpt(getCrcType(), bytesPerCRC);
  }

  @Override
  public void readFields(@Tainted MD5MD5CRC32FileChecksum this, @Tainted DataInput in) throws IOException {
    bytesPerCRC = in.readInt();
    crcPerBlock = in.readLong();
    md5 = MD5Hash.read(in);
  }
 
  @Override
  public void write(@Tainted MD5MD5CRC32FileChecksum this, @Tainted DataOutput out) throws IOException {
    out.writeInt(bytesPerCRC);
    out.writeLong(crcPerBlock);
    md5.write(out);    
  }

  /** Write that object to xml output. */
  public static void write(@Tainted XMLOutputter xml, @Tainted MD5MD5CRC32FileChecksum that
      ) throws IOException {
    xml.startTag(MD5MD5CRC32FileChecksum.class.getName());
    if (that != null) {
      xml.attribute("bytesPerCRC", "" + that.bytesPerCRC);
      xml.attribute("crcPerBlock", "" + that.crcPerBlock);
      xml.attribute("crcType", ""+ that.getCrcType().name());
      xml.attribute("md5", "" + that.md5);
    }
    xml.endTag();
  }

  /** Return the object represented in the attributes. */
  public static @Tainted MD5MD5CRC32FileChecksum valueOf(@Tainted Attributes attrs
      ) throws SAXException {
    final @Tainted String bytesPerCRC = attrs.getValue("bytesPerCRC");
    final @Tainted String crcPerBlock = attrs.getValue("crcPerBlock");
    final @Tainted String md5 = attrs.getValue("md5");
    @Tainted
    String crcType = attrs.getValue("crcType");
    DataChecksum.@Tainted Type finalCrcType;
    if (bytesPerCRC == null || crcPerBlock == null || md5 == null) {
      return null;
    }

    try {
      // old versions don't support crcType.
      if (crcType == null || crcType.equals("")) {
        finalCrcType = DataChecksum.Type.CRC32;
      } else {
        finalCrcType = DataChecksum.Type.valueOf(crcType);
      }

      switch (finalCrcType) {
        case CRC32:
          return new @Tainted MD5MD5CRC32GzipFileChecksum(
              Integer.valueOf(bytesPerCRC),
              Integer.valueOf(crcPerBlock),
              new @Tainted MD5Hash(md5));
        case CRC32C:
          return new @Tainted MD5MD5CRC32CastagnoliFileChecksum(
              Integer.valueOf(bytesPerCRC),
              Integer.valueOf(crcPerBlock),
              new @Tainted MD5Hash(md5));
        default:
          // we should never get here since finalCrcType will
          // hold a valid type or we should have got an exception.
          return null;
      }
    } catch (@Tainted Exception e) {
      throw new @Tainted SAXException("Invalid attributes: bytesPerCRC=" + bytesPerCRC
          + ", crcPerBlock=" + crcPerBlock + ", crcType=" + crcType 
          + ", md5=" + md5, e);
    }
  }
 
  @Override
  public @Tainted String toString(@Tainted MD5MD5CRC32FileChecksum this) {
    return getAlgorithmName() + ":" + md5;
  }
}
