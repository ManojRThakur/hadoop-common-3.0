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

package org.apache.hadoop.io;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@InterfaceAudience.Public
@InterfaceStability.Stable
public final class WritableUtils  {

  public static @Tainted byte @Tainted [] readCompressedByteArray(@Tainted DataInput in) throws IOException {
    @Tainted
    int length = in.readInt();
    if (length == -1) return null;
    @Tainted
    byte @Tainted [] buffer = new @Tainted byte @Tainted [length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    @Tainted
    GZIPInputStream gzi = new @Tainted GZIPInputStream(new @Tainted ByteArrayInputStream(buffer, 0, buffer.length));
    @Tainted
    byte @Tainted [] outbuf = new @Tainted byte @Tainted [length];
    @Tainted
    ByteArrayOutputStream bos =  new @Tainted ByteArrayOutputStream();
    @Tainted
    int len;
    while((len=gzi.read(outbuf, 0, outbuf.length)) != -1){
      bos.write(outbuf, 0, len);
    }
    @Tainted
    byte @Tainted [] decompressed =  bos.toByteArray();
    bos.close();
    gzi.close();
    return decompressed;
  }

  public static void skipCompressedByteArray(@Tainted DataInput in) throws IOException {
    @Tainted
    int length = in.readInt();
    if (length != -1) {
      skipFully(in, length);
    }
  }

  public static @Tainted int  writeCompressedByteArray(@Tainted DataOutput out, 
                                              @Tainted
                                              byte @Tainted [] bytes) throws IOException {
    if (bytes != null) {
      @Tainted
      ByteArrayOutputStream bos =  new @Tainted ByteArrayOutputStream();
      @Tainted
      GZIPOutputStream gzout = new @Tainted GZIPOutputStream(bos);
      try {
        gzout.write(bytes, 0, bytes.length);
        gzout.close();
        gzout = null;
      } finally {
        IOUtils.closeStream(gzout);
      }
      @Tainted
      byte @Tainted [] buffer = bos.toByteArray();
      @Tainted
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
      /* debug only! Once we have confidence, can lose this. */
      return ((bytes.length != 0) ? (100*buffer.length)/bytes.length : 0);
    } else {
      out.writeInt(-1);
      return -1;
    }
  }


  /* Ugly utility, maybe someone else can do this better  */
  public static @Tainted String readCompressedString(@Tainted DataInput in) throws IOException {
    @Tainted
    byte @Tainted [] bytes = readCompressedByteArray(in);
    if (bytes == null) return null;
    return new @Tainted String(bytes, "UTF-8");
  }


  public static @Tainted int  writeCompressedString(@Tainted DataOutput out, @Tainted String s) throws IOException {
    return writeCompressedByteArray(out, (s != null) ? s.getBytes("UTF-8") : null);
  }

  /*
   *
   * Write a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   * 
   */
  public static void writeString(@Tainted DataOutput out, @Tainted String s) throws IOException {
    if (s != null) {
      @Tainted
      byte @Tainted [] buffer = s.getBytes("UTF-8");
      @Tainted
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
    } else {
      out.writeInt(-1);
    }
  }

  /*
   * Read a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   *
   */
  public static @Tainted String readString(@Tainted DataInput in) throws IOException{
    @Tainted
    int length = in.readInt();
    if (length == -1) return null;
    @Tainted
    byte @Tainted [] buffer = new @Tainted byte @Tainted [length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    return new @Tainted String(buffer,"UTF-8");  
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection.
   *
   */
  public static void writeStringArray(@Tainted DataOutput out, @Tainted String @Tainted [] s) throws IOException{
    out.writeInt(s.length);
    for(@Tainted int i = 0; i < s.length; i++) {
      writeString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array of
   * compressed Strings. Handles also null arrays and null values.
   * Could be generalised using introspection.
   *
   */
  public static void writeCompressedStringArray(@Tainted DataOutput out, @Tainted String @Tainted [] s) throws IOException{
    if (s == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(s.length);
    for(@Tainted int i = 0; i < s.length; i++) {
      writeCompressedString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Actually this bit couldn't...
   *
   */
  public static @Tainted String @Tainted [] readStringArray(@Tainted DataInput in) throws IOException {
    @Tainted
    int len = in.readInt();
    if (len == -1) return null;
    @Tainted
    String @Tainted [] s = new @Tainted String @Tainted [len];
    for(@Tainted int i = 0; i < len; i++) {
      s[i] = readString(in);
    }
    return s;
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Handles null arrays and null values.
   *
   */
  public static  @Tainted String @Tainted [] readCompressedStringArray(@Tainted DataInput in) throws IOException {
    @Tainted
    int len = in.readInt();
    if (len == -1) return null;
    @Tainted
    String @Tainted [] s = new @Tainted String @Tainted [len];
    for(@Tainted int i = 0; i < len; i++) {
      s[i] = readCompressedString(in);
    }
    return s;
  }


  /*
   *
   * Test Utility Method Display Byte Array. 
   *
   */
  public static void displayByteArray(@Tainted byte @Tainted [] record){
    @Tainted
    int i;
    for(i=0;i < record.length -1; i++){
      if (i % 16 == 0) { System.out.println(); }
      System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
      System.out.print(Integer.toHexString(record[i] & 0x0F));
      System.out.print(",");
    }
    System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
    System.out.print(Integer.toHexString(record[i] & 0x0F));
    System.out.println();
  }

  /**
   * Make a copy of a writable object using serialization to a buffer.
   * @param orig The object to copy
   * @return The copied object
   */
  public static <@Tainted T extends @Tainted Writable> @Tainted T clone(@Tainted T orig, @Tainted Configuration conf) {
    try {
      @SuppressWarnings("unchecked") // Unchecked cast from Class to Class<T>
      @Tainted
      T newInst = ReflectionUtils.newInstance((@Tainted Class<@Tainted T>) orig.getClass(), conf);
      ReflectionUtils.copy(conf, orig, newInst);
      return newInst;
    } catch (@Tainted IOException e) {
      throw new @Tainted RuntimeException("Error writing/reading clone buffer", e);
    }
  }

  /**
   * Make a copy of the writable object using serialiation to a buffer
   * @param dst the object to copy from
   * @param src the object to copy into, which is destroyed
   * @throws IOException
   * @deprecated use ReflectionUtils.cloneInto instead.
   */
  @Deprecated
  public static void cloneInto(@Tainted Writable dst, @Tainted Writable src) throws IOException {
    ReflectionUtils.cloneWritableInto(dst, src);
  }

  /**
   * Serializes an integer to a binary stream with zero-compressed encoding.
   * For -120 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * integer is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -121 and -124, the following integer
   * is positive, with number of bytes that follow are -(v+120).
   * If the first byte value v is between -125 and -128, the following integer
   * is negative, with number of bytes that follow are -(v+124). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param stream Binary output stream
   * @param i Integer to be serialized
   * @throws java.io.IOException 
   */
  public static void writeVInt(@Tainted DataOutput stream, @Tainted int i) throws IOException {
    writeVLong(stream, i);
  }
  
  /**
   * Serializes a long to a binary stream with zero-compressed encoding.
   * For -112 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * long is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -113 and -120, the following long
   * is positive, with number of bytes that follow are -(v+112).
   * If the first byte value v is between -121 and -128, the following long
   * is negative, with number of bytes that follow are -(v+120). Bytes are
   * stored in the high-non-zero-byte-first order.
   * 
   * @param stream Binary output stream
   * @param i Long to be serialized
   * @throws java.io.IOException 
   */
  public static void writeVLong(@Tainted DataOutput stream, @Tainted long i) throws IOException {
    if (i >= -112 && i <= 127) {
      stream.writeByte((@Tainted byte)i);
      return;
    }
      
    @Tainted
    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }
      
    @Tainted
    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }
      
    stream.writeByte((@Tainted byte)len);
      
    len = (len < -120) ? -(len + 120) : -(len + 112);
      
    for (@Tainted int idx = len; idx != 0; idx--) {
      @Tainted
      int shiftbits = (idx - 1) * 8;
      @Tainted
      long mask = 0xFFL << shiftbits;
      stream.writeByte((@Tainted byte)((i & mask) >> shiftbits));
    }
  }
  

  /**
   * Reads a zero-compressed encoded long from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException 
   * @return deserialized long from stream.
   */
  public static @Tainted long readVLong(@Tainted DataInput stream) throws IOException {
    @Tainted
    byte firstByte = stream.readByte();
    @Tainted
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    @Tainted
    long i = 0;
    for (@Tainted int idx = 0; idx < len-1; idx++) {
      @Tainted
      byte b = stream.readByte();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    @SuppressWarnings("ostrusted") // TypeChecking error none of the results need to be trusted
    long result = (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    return result;
  }

  /**
   * Reads a zero-compressed encoded integer from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException 
   * @return deserialized integer from stream.
   */
  public static @Tainted int readVInt(@Tainted DataInput stream) throws IOException {
    @Tainted
    long n = readVLong(stream);
    if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
      throw new @Tainted IOException("value too long to fit in integer");
    }
    return (@Tainted int)n;
  }

  /**
   * Reads an integer from the input stream and returns it.
   *
   * This function validates that the integer is between [lower, upper],
   * inclusive.
   *
   * @param stream Binary input stream
   * @throws java.io.IOException
   * @return deserialized integer from stream
   */
  public static @Tainted int readVIntInRange(@Tainted DataInput stream, @Tainted int lower, @Tainted int upper)
      throws IOException {
    @Tainted
    long n = readVLong(stream);
    if (n < lower) {
      if (lower == 0) {
        throw new @Tainted IOException("expected non-negative integer, got " + n);
      } else {
        throw new @Tainted IOException("expected integer greater than or equal to " +
            lower + ", got " + n);
      }
    }
    if (n > upper) {
      throw new @Tainted IOException("expected integer less or equal to " + upper +
          ", got " + n);
    }
    return (@Tainted int)n;
  }

  /**
   * Given the first byte of a vint/vlong, determine the sign
   * @param value the first byte
   * @return is the value negative
   */
  public static @Tainted boolean isNegativeVInt(@Tainted byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * Parse the first byte of a vint/vlong to determine the number of bytes
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static @Tainted int decodeVIntSize(@Tainted byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * Get the encoded length if an integer is stored in a variable-length format
   * @return the encoded length 
   */
  public static @Tainted int getVIntSize(@Tainted long i) {
    if (i >= -112 && i <= 127) {
      return 1;
    }
      
    if (i < 0) {
      i ^= -1L; // take one's complement'
    }
    // find the number of bytes with non-leading zeros
    @Tainted
    int dataBits = Long.SIZE - Long.numberOfLeadingZeros(i);
    // find the number of data bytes + length byte
    return (dataBits + 7) / 8 + 1;
  }
  /**
   * Read an Enum value from DataInput, Enums are read and written 
   * using String values. 
   * @param <T> Enum type
   * @param in DataInput to read from 
   * @param enumType Class type of Enum
   * @return Enum represented by String read from DataInput
   * @throws IOException
   */
  public static <@Tainted T extends @Tainted Enum<@Tainted T>> @Tainted T readEnum(@Tainted DataInput in, @Tainted Class<@Tainted T> enumType)
    throws IOException{
    return T.valueOf(enumType, Text.readString(in));
  }
  /**
   * writes String value of enum to DataOutput. 
   * @param out Dataoutput stream
   * @param enumVal enum value
   * @throws IOException
   */
  public static void writeEnum(@Tainted DataOutput out,  @Tainted Enum<@Tainted ? extends java.lang.@Tainted Object> enumVal) 
    throws IOException{
    Text.writeString(out, enumVal.name()); 
  }
  /**
   * Skip <i>len</i> number of bytes in input stream<i>in</i>
   * @param in input stream
   * @param len number of bytes to skip
   * @throws IOException when skipped less number of bytes
   */
  public static void skipFully(@Tainted DataInput in, @Tainted int len) throws IOException {
    @Tainted
    int total = 0;
    @Tainted
    int cur = 0;

    while ((total<len) && ((cur = in.skipBytes(len-total)) > 0)) {
        total += cur;
    }

    if (total<len) {
      throw new @Tainted IOException("Not able to skip " + len + " bytes, possibly " +
                            "due to end of input.");
    }
  }

  /** Convert writables to a byte array */
  public static @Tainted byte @Tainted [] toByteArray(@Tainted Writable @Tainted ... writables) {
    final @Tainted DataOutputBuffer out = new @Tainted DataOutputBuffer();
    try {
      for(@Tainted Writable w : writables) {
        w.write(out);
      }
      out.close();
    } catch (@Tainted IOException e) {
      throw new @Tainted RuntimeException("Fail to convert writables to a byte array",e);
    }
    return out.getData();
  }

  /**
   * Read a string, but check it for sanity. The format consists of a vint
   * followed by the given number of bytes.
   * @param in the stream to read from
   * @param maxLength the largest acceptable length of the encoded string
   * @return the bytes as a string
   * @throws IOException if reading from the DataInput fails
   * @throws IllegalArgumentException if the encoded byte size for string 
             is negative or larger than maxSize. Only the vint is read.
   */
  public static @Tainted String readStringSafely(@Tainted DataInput in,
                                        @Tainted
                                        int maxLength
                                        ) throws IOException, 
                                                 IllegalArgumentException {
    @Tainted
    int length = readVInt(in);
    if (length < 0 || length > maxLength) {
      throw new @Tainted IllegalArgumentException("Encoded byte size for String was " + length + 
                                         ", which is outside of 0.." +
                                         maxLength + " range.");
    }
    @Tainted
    byte @Tainted [] bytes = new @Tainted byte @Tainted [length];
    in.readFully(bytes, 0, length);
    return Text.decode(bytes);
  }
}
