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
import org.checkerframework.checker.tainting.qual.Untainted;
import org.checkerframework.checker.tainting.qual.PolyTainted;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Arrays;

import org.apache.avro.reflect.Stringable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** This class stores text using standard UTF8 encoding.  It provides methods
 * to serialize, deserialize, and compare texts at byte level.  The type of
 * length is integer and is serialized using zero-compressed format.  <p>In
 * addition, it provides methods for string traversal without converting the
 * byte array to a string.  <p>Also includes utilities for
 * serializing/deserialing a string, coding/decoding a string, checking if a
 * byte array contains valid UTF8 code, calculating the length of an encoded
 * string.
 */
@Stringable
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Text extends @Tainted BinaryComparable
    implements @Tainted WritableComparable<@Tainted BinaryComparable> {
  
  private static @Tainted ThreadLocal<@Tainted CharsetEncoder> ENCODER_FACTORY =
    new @Tainted ThreadLocal<@Tainted CharsetEncoder>() {
      @Override
      protected @Tainted CharsetEncoder initialValue() {
        return Charset.forName("UTF-8").newEncoder().
               onMalformedInput(CodingErrorAction.REPORT).
               onUnmappableCharacter(CodingErrorAction.REPORT);
    }
  };
  
  private static @Tainted ThreadLocal<@Tainted CharsetDecoder> DECODER_FACTORY =
    new @Tainted ThreadLocal<@Tainted CharsetDecoder>() {
    @Override
    protected @Tainted CharsetDecoder initialValue() {
      return Charset.forName("UTF-8").newDecoder().
             onMalformedInput(CodingErrorAction.REPORT).
             onUnmappableCharacter(CodingErrorAction.REPORT);
    }
  };
  
  private static final @Tainted byte @Tainted [] EMPTY_BYTES = new @Tainted byte @Tainted [0];
  
  private @Tainted byte @Tainted [] bytes;
  private @Tainted int length;

  public @Tainted Text() {
    bytes = EMPTY_BYTES;
  }

  /** Construct from a string. 
   */
  public @Tainted Text(@Tainted String string) {
    set(string);
  }

  /** Construct from another text. */
  public @Tainted Text(@Tainted Text utf8) {
    set(utf8);
  }

  /** Construct from a byte array.
   */
  public @Tainted Text(@Tainted byte @Tainted [] utf8)  {
    set(utf8);
  }
  
  /**
   * Get a copy of the bytes that is exactly the length of the data.
   * See {@link #getBytes()} for faster access to the underlying array.
   */
  public @Tainted byte @Tainted [] copyBytes(@Tainted Text this) {
    @Tainted
    byte @Tainted [] result = new @Tainted byte @Tainted [length];
    System.arraycopy(bytes, 0, result, 0, length);
    return result;
  }
  
  /**
   * Returns the raw bytes; however, only data up to {@link #getLength()} is
   * valid. Please use {@link #copyBytes()} if you
   * need the returned array to be precisely the length of the data.
   */
  @Override
  public @Tainted byte @Tainted [] getBytes(@Tainted Text this) {
    return bytes;
  }

  /** Returns the number of bytes in the byte array */ 
  @Override
  public @Tainted int getLength(@Tainted Text this) {
    return length;
  }
  
  /**
   * Returns the Unicode Scalar Value (32-bit integer value)
   * for the character at <code>position</code>. Note that this
   * method avoids using the converter or doing String instantiation
   * @return the Unicode scalar value at position or -1
   *          if the position is invalid or points to a
   *          trailing byte
   */
  public @Tainted int charAt(@Tainted Text this, @Tainted int position) {
    if (position > this.length) return -1; // too long
    if (position < 0) return -1; // duh.
      
    @Tainted
    ByteBuffer bb = (@Tainted ByteBuffer)ByteBuffer.wrap(bytes).position(position);
    return bytesToCodePoint(bb.slice());
  }
  
  public @Tainted int find(@Tainted Text this, @Tainted String what) {
    return find(what, 0);
  }
  
  /**
   * Finds any occurence of <code>what</code> in the backing
   * buffer, starting as position <code>start</code>. The starting
   * position is measured in bytes and the return value is in
   * terms of byte position in the buffer. The backing buffer is
   * not converted to a string for this operation.
   * @return byte position of the first occurence of the search
   *         string in the UTF-8 buffer or -1 if not found
   */
  public @Tainted int find(@Tainted Text this, @Tainted String what, @Tainted int start) {
    try {
      @Tainted
      ByteBuffer src = ByteBuffer.wrap(this.bytes,0,this.length);
      @Tainted
      ByteBuffer tgt = encode(what);
      @Tainted
      byte b = tgt.get();
      src.position(start);
          
      while (src.hasRemaining()) {
        if (b == src.get()) { // matching first byte
          src.mark(); // save position in loop
          tgt.mark(); // save position in target
          @Tainted
          boolean found = true;
          @Tainted
          int pos = src.position()-1;
          while (tgt.hasRemaining()) {
            if (!src.hasRemaining()) { // src expired first
              tgt.reset();
              src.reset();
              found = false;
              break;
            }
            if (!(tgt.get() == src.get())) {
              tgt.reset();
              src.reset();
              found = false;
              break; // no match
            }
          }
          if (found) return pos;
        }
      }
      return -1; // not found
    } catch (@Tainted CharacterCodingException e) {
      // can't get here
      e.printStackTrace();
      return -1;
    }
  }  
  /** Set to contain the contents of a string. 
   */
  public void set(@Tainted Text this, @Tainted String string) {
    try {
      @Tainted
      ByteBuffer bb = encode(string, true);
      bytes = bb.array();
      length = bb.limit();
    }catch(@Tainted CharacterCodingException e) {
      throw new @Tainted RuntimeException("Should not have happened ", e); 
    }
  }

  /** Set to a utf8 byte array
   */
  public void set(@Tainted Text this, @Tainted byte @Tainted [] utf8) {
    set(utf8, 0, utf8.length);
  }
  
  /** copy a text. */
  public void set(@Tainted Text this, @Tainted Text other) {
    set(other.getBytes(), 0, other.getLength());
  }

  /**
   * Set the Text to range of bytes
   * @param utf8 the data to copy from
   * @param start the first position of the new string
   * @param len the number of bytes of the new string
   */
  public void set(@Tainted Text this, @Tainted byte @Tainted [] utf8, @Tainted int start, @Tainted int len) {
    setCapacity(len, false);
    System.arraycopy(utf8, start, bytes, 0, len);
    this.length = len;
  }

  /**
   * Append a range of bytes to the end of the given text
   * @param utf8 the data to copy from
   * @param start the first position to append from utf8
   * @param len the number of bytes to append
   */
  public void append(@Tainted Text this, @Tainted byte @Tainted [] utf8, @Tainted int start, @Tainted int len) {
    setCapacity(length + len, true);
    System.arraycopy(utf8, start, bytes, length, len);
    length += len;
  }

  /**
   * Clear the string to empty.
   *
   * <em>Note</em>: For performance reasons, this call does not clear the
   * underlying byte array that is retrievable via {@link #getBytes()}.
   * In order to free the byte-array memory, call {@link #set(byte[])}
   * with an empty byte array (For example, <code>new byte[0]</code>).
   */
  public void clear(@Tainted Text this) {
    length = 0;
  }

  /*
   * Sets the capacity of this Text object to <em>at least</em>
   * <code>len</code> bytes. If the current buffer is longer,
   * then the capacity and existing content of the buffer are
   * unchanged. If <code>len</code> is larger
   * than the current capacity, the Text object's capacity is
   * increased to match.
   * @param len the number of bytes we need
   * @param keepData should the old data be kept
   */
  private void setCapacity(@Tainted Text this, @Tainted int len, @Tainted boolean keepData) {
    if (bytes == null || bytes.length < len) {
      if (bytes != null && keepData) {
        bytes = Arrays.copyOf(bytes, Math.max(len,length << 1));
      } else {
        bytes = new @Tainted byte @Tainted [len];
      }
    }
  }
   
  /** 
   * Convert text back to string
   * @see java.lang.Object#toString()
   */
  @Override
  public @Tainted String toString(@Tainted Text this) {
    try {
      return decode(bytes, 0, length);
    } catch (@Tainted CharacterCodingException e) { 
      throw new @Tainted RuntimeException("Should not have happened " , e); 
    }
  }
  
  /** deserialize 
   */
  @Override
  public void readFields(@Tainted Text this, @Tainted DataInput in) throws IOException {
    @Tainted
    int newLength = WritableUtils.readVInt(in);
    setCapacity(newLength, false);
    in.readFully(bytes, 0, newLength);
    length = newLength;
  }
  
  public void readFields(@Tainted Text this, @Tainted DataInput in, @Tainted int maxLength) throws IOException {
    @Tainted
    int newLength = WritableUtils.readVInt(in);
    if (newLength < 0) {
      throw new @Tainted IOException("tried to deserialize " + newLength +
          " bytes of data!  newLength must be non-negative.");
    } else if (newLength >= maxLength) {
      throw new @Tainted IOException("tried to deserialize " + newLength +
          " bytes of data, but maxLength = " + maxLength);
    }
    setCapacity(newLength, false);
    in.readFully(bytes, 0, newLength);
    length = newLength;
  }

  /** Skips over one Text in the input. */
  public static void skip(@Tainted DataInput in) throws IOException {
    @Tainted
    int length = WritableUtils.readVInt(in);
    WritableUtils.skipFully(in, length);
  }

  /** serialize
   * write this object to out
   * length uses zero-compressed encoding
   * @see Writable#write(DataOutput)
   */
  @Override
  public void write(@Tainted Text this, @Tainted DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(bytes, 0, length);
  }

  public void write(@Tainted Text this, @Tainted DataOutput out, @Tainted int maxLength) throws IOException {
    if (length > maxLength) {
      throw new @Tainted IOException("data was too long to write!  Expected " +
          "less than or equal to " + maxLength + " bytes, but got " +
          length + " bytes.");
    }
    WritableUtils.writeVInt(out, length);
    out.write(bytes, 0, length);
  }

  /** Returns true iff <code>o</code> is a Text with the same contents.  */
  @Override
  public @Tainted boolean equals(@Tainted Text this, @Tainted Object o) {
    if (o instanceof @Tainted Text)
      return super.equals(o);
    return false;
  }

  @Override
  public @Tainted int hashCode(@Tainted Text this) {
    return super.hashCode();
  }

  /** A WritableComparator optimized for Text keys. */
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(Text.class);
    }

    @Override
    public @Tainted int compare(Text.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      @Tainted
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      @Tainted
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
    }
  }

  static {
    // register this comparator
    WritableComparator.define(Text.class, new @Tainted Comparator());
  }

  /// STATIC UTILITIES FROM HERE DOWN
  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If the input is malformed,
   * replace by a default value.
   */
  public static @Tainted String decode(@Tainted byte @Tainted [] utf8) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8), true);
  }
  
  public static @Tainted String decode(@Tainted byte @Tainted [] utf8, @Tainted int start, @Tainted int length) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), true);
  }
  
  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   */
  public static @Tainted String decode(@Tainted byte @Tainted [] utf8, @Tainted int start, @Tainted int length, @Tainted boolean replace) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), replace);
  }
  
  private static @Tainted String decode(@Tainted ByteBuffer utf8, @Tainted boolean replace) 
    throws CharacterCodingException {
    @Tainted
    CharsetDecoder decoder = DECODER_FACTORY.get();
    if (replace) {
      decoder.onMalformedInput(
          java.nio.charset.CodingErrorAction.REPLACE);
      decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    @Tainted
    String str = decoder.decode(utf8).toString();
    // set decoder back to its default value: REPORT
    if (replace) {
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return str;
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If the input is malformed,
   * invalid chars are replaced by a default value.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */

  public static @Tainted ByteBuffer encode(@Tainted String string)
    throws CharacterCodingException {
    return encode(string, true);
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */
  public static @Tainted ByteBuffer encode(@Tainted String string, @Tainted boolean replace)
    throws CharacterCodingException {
    @Tainted
    CharsetEncoder encoder = ENCODER_FACTORY.get();
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPLACE);
      encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    @Tainted
    ByteBuffer bytes = 
      encoder.encode(CharBuffer.wrap(string.toCharArray()));
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPORT);
      encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return bytes;
  }

  static final public @Tainted int DEFAULT_MAX_LEN = 1024 * 1024;

  /** Read a UTF8 encoded string from in
   */
  @SuppressWarnings("ostrusted:return.type.incompatible")
  public static @PolyTainted String readString(@PolyTainted DataInput in) throws IOException {
    @Tainted
    int length = WritableUtils.readVInt(in);
    @Tainted
    byte @Tainted [] bytes = new @Tainted byte @Tainted [length];
    in.readFully(bytes, 0, length);
    return decode(bytes);
  }
  
  /** Read a UTF8 encoded string with a maximum size
   */
  public static @Tainted String readString(@Tainted DataInput in, @Tainted int maxLength)
      throws IOException {
    @Tainted
    int length = WritableUtils.readVIntInRange(in, 0, maxLength);
    @Tainted
    byte @Tainted [] bytes = new @Tainted byte @Tainted [length];
    in.readFully(bytes, 0, length);
    return decode(bytes);
  }
  
  /** Write a UTF8 encoded string to out
   */
  public static @Tainted int writeString(@Tainted DataOutput out, @Tainted String s) throws IOException {
    @Tainted
    ByteBuffer bytes = encode(s);
    @Tainted
    int length = bytes.limit();
    WritableUtils.writeVInt(out, length);
    out.write(bytes.array(), 0, length);
    return length;
  }

  /** Write a UTF8 encoded string with a maximum size to out
   */
  public static @Tainted int writeString(@Tainted DataOutput out, @Tainted String s, @Tainted int maxLength)
      throws IOException {
    @Tainted
    ByteBuffer bytes = encode(s);
    @Tainted
    int length = bytes.limit();
    if (length > maxLength) {
      throw new @Tainted IOException("string was too long to write!  Expected " +
          "less than or equal to " + maxLength + " bytes, but got " +
          length + " bytes.");
    }
    WritableUtils.writeVInt(out, length);
    out.write(bytes.array(), 0, length);
    return length;
  }

  ////// states for validateUTF8
  
  private static final @Tainted int LEAD_BYTE = 0;

  private static final @Tainted int TRAIL_BYTE_1 = 1;

  private static final @Tainted int TRAIL_BYTE = 2;

  /** 
   * Check if a byte array contains valid utf-8
   * @param utf8 byte array
   * @throws MalformedInputException if the byte array contains invalid utf-8
   */
  public static void validateUTF8(@Tainted byte @Tainted [] utf8) throws MalformedInputException {
    validateUTF8(utf8, 0, utf8.length);     
  }
  
  /**
   * Check to see if a byte array is valid utf-8
   * @param utf8 the array of bytes
   * @param start the offset of the first byte in the array
   * @param len the length of the byte sequence
   * @throws MalformedInputException if the byte array contains invalid bytes
   */
  public static void validateUTF8(@Tainted byte @Tainted [] utf8, @Tainted int start, @Tainted int len)
    throws MalformedInputException {
    @Tainted
    int count = start;
    @Tainted
    int leadByte = 0;
    @Tainted
    int length = 0;
    @Tainted
    int state = LEAD_BYTE;
    while (count < start+len) {
      @Tainted
      int aByte = utf8[count] & 0xFF;

      switch (state) {
      case LEAD_BYTE:
        leadByte = aByte;
        length = bytesFromUTF8[aByte];

        switch (length) {
        case 0: // check for ASCII
          if (leadByte > 0x7F)
            throw new @Tainted MalformedInputException(count);
          break;
        case 1:
          if (leadByte < 0xC2 || leadByte > 0xDF)
            throw new @Tainted MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        case 2:
          if (leadByte < 0xE0 || leadByte > 0xEF)
            throw new @Tainted MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        case 3:
          if (leadByte < 0xF0 || leadByte > 0xF4)
            throw new @Tainted MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        default:
          // too long! Longest valid UTF-8 is 4 bytes (lead + three)
          // or if < 0 we got a trail byte in the lead byte position
          throw new @Tainted MalformedInputException(count);
        } // switch (length)
        break;

      case TRAIL_BYTE_1:
        if (leadByte == 0xF0 && aByte < 0x90)
          throw new @Tainted MalformedInputException(count);
        if (leadByte == 0xF4 && aByte > 0x8F)
          throw new @Tainted MalformedInputException(count);
        if (leadByte == 0xE0 && aByte < 0xA0)
          throw new @Tainted MalformedInputException(count);
        if (leadByte == 0xED && aByte > 0x9F)
          throw new @Tainted MalformedInputException(count);
        // falls through to regular trail-byte test!!
      case TRAIL_BYTE:
        if (aByte < 0x80 || aByte > 0xBF)
          throw new @Tainted MalformedInputException(count);
        if (--length == 0) {
          state = LEAD_BYTE;
        } else {
          state = TRAIL_BYTE;
        }
        break;
      } // switch (state)
      count++;
    }
  }

  /**
   * Magic numbers for UTF-8. These are the number of bytes
   * that <em>follow</em> a given lead byte. Trailing bytes
   * have the value -1. The values 4 and 5 are presented in
   * this table, even though valid UTF-8 cannot include the
   * five and six byte sequences.
   */
  static final @Tainted int @Tainted [] bytesFromUTF8 =
  new int @Tainted [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    // trail bytes
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
    3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5 };

  /**
   * Returns the next code point at the current position in
   * the buffer. The buffer's position will be incremented.
   * Any mark set on this buffer will be changed by this method!
   */
  public static @Tainted int bytesToCodePoint(@Tainted ByteBuffer bytes) {
    bytes.mark();
    @Tainted
    byte b = bytes.get();
    bytes.reset();
    @Tainted
    int extraBytesToRead = bytesFromUTF8[(b & 0xFF)];
    if (extraBytesToRead < 0) return -1; // trailing byte!
    @Tainted
    int ch = 0;

    switch (extraBytesToRead) {
    case 5: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
    case 4: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
    case 3: ch += (bytes.get() & 0xFF); ch <<= 6;
    case 2: ch += (bytes.get() & 0xFF); ch <<= 6;
    case 1: ch += (bytes.get() & 0xFF); ch <<= 6;
    case 0: ch += (bytes.get() & 0xFF);
    }
    ch -= offsetsFromUTF8[extraBytesToRead];

    return ch;
  }

  
  static final @Tainted int offsetsFromUTF8 @Tainted [] =
  new int @Tainted [] { 0x00000000, 0x00003080,
    0x000E2080, 0x03C82080, 0xFA082080, 0x82082080 };

  /**
   * For the given string, returns the number of UTF-8 bytes
   * required to encode the string.
   * @param string text to encode
   * @return number of UTF-8 bytes required to encode
   */
  public static @Tainted int utf8Length(@Tainted String string) {
    @Tainted
    CharacterIterator iter = new @Tainted StringCharacterIterator(string);
    @Tainted
    char ch = iter.first();
    @Tainted
    int size = 0;
    while (ch != CharacterIterator.DONE) {
      if ((ch >= 0xD800) && (ch < 0xDC00)) {
        // surrogate pair?
        @Tainted
        char trail = iter.next();
        if ((trail > 0xDBFF) && (trail < 0xE000)) {
          // valid pair
          size += 4;
        } else {
          // invalid pair
          size += 3;
          iter.previous(); // rewind one
        }
      } else if (ch < 0x80) {
        size++;
      } else if (ch < 0x800) {
        size += 2;
      } else {
        // ch < 0x10000, that is, the largest char value
        size += 3;
      }
      ch = iter.next();
    }
    return size;
  }
}
