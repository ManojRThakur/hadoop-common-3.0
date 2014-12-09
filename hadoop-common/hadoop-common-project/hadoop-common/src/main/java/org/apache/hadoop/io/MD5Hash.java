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
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.Arrays;
import java.security.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A Writable for MD5 hash values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MD5Hash implements @Tainted WritableComparable<@Tainted MD5Hash> {
  public static final @Tainted int MD5_LEN = 16;

  private static @Tainted ThreadLocal<@Tainted MessageDigest> DIGESTER_FACTORY = new @Tainted ThreadLocal<@Tainted MessageDigest>() {
    @Override
    protected @Tainted MessageDigest initialValue() {
      try {
        return MessageDigest.getInstance("MD5");
      } catch (@Tainted NoSuchAlgorithmException e) {
        throw new @Tainted RuntimeException(e);
      }
    }
  };

  private @Tainted byte @Tainted [] digest;

  /** Constructs an MD5Hash. */
  public @Tainted MD5Hash() {
    this.digest = new @Tainted byte @Tainted [MD5_LEN];
  }

  /** Constructs an MD5Hash from a hex string. */
  public @Tainted MD5Hash(@Tainted String hex) {
    setDigest(hex);
  }
  
  /** Constructs an MD5Hash with a specified value. */
  public @Tainted MD5Hash(@Tainted byte @Tainted [] digest) {
    if (digest.length != MD5_LEN)
      throw new @Tainted IllegalArgumentException("Wrong length: " + digest.length);
    this.digest = digest;
  }
  
  // javadoc from Writable
  @Override
  public void readFields(@Tainted MD5Hash this, @Tainted DataInput in) throws IOException {
    in.readFully(digest);
  }

  /** Constructs, reads and returns an instance. */
  public static @Tainted MD5Hash read(@Tainted DataInput in) throws IOException {
    @Tainted
    MD5Hash result = new @Tainted MD5Hash();
    result.readFields(in);
    return result;
  }

  // javadoc from Writable
  @Override
  public void write(@Tainted MD5Hash this, @Tainted DataOutput out) throws IOException {
    out.write(digest);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(@Tainted MD5Hash this, @Tainted MD5Hash that) {
    System.arraycopy(that.digest, 0, this.digest, 0, MD5_LEN);
  }

  /** Returns the digest bytes. */
  public @Tainted byte @Tainted [] getDigest(@Tainted MD5Hash this) { return digest; }

  /** Construct a hash value for a byte array. */
  public static @Tainted MD5Hash digest(@Tainted byte @Tainted [] data) {
    return digest(data, 0, data.length);
  }

  /**
   * Create a thread local MD5 digester
   */
  public static @Tainted MessageDigest getDigester() {
    @Tainted
    MessageDigest digester = DIGESTER_FACTORY.get();
    digester.reset();
    return digester;
  }

  /** Construct a hash value for the content from the InputStream. */
  public static @Tainted MD5Hash digest(@Tainted InputStream in) throws IOException {
    final @Tainted byte @Tainted [] buffer = new @Tainted byte @Tainted [4*1024]; 

    final @Tainted MessageDigest digester = getDigester();
    for(@Tainted int n; (n = in.read(buffer)) != -1; ) {
      digester.update(buffer, 0, n);
    }

    return new @Tainted MD5Hash(digester.digest());
  }

  /** Construct a hash value for a byte array. */
  public static @Tainted MD5Hash digest(@Tainted byte @Tainted [] data, @Tainted int start, @Tainted int len) {
    @Tainted
    byte @Tainted [] digest;
    @Tainted
    MessageDigest digester = getDigester();
    digester.update(data, start, len);
    digest = digester.digest();
    return new @Tainted MD5Hash(digest);
  }

  /** Construct a hash value for a String. */
  public static @Tainted MD5Hash digest(@Tainted String string) {
    return digest(UTF8.getBytes(string));
  }

  /** Construct a hash value for a String. */
  public static @Tainted MD5Hash digest(@Tainted UTF8 utf8) {
    return digest(utf8.getBytes(), 0, utf8.getLength());
  }

  /** Construct a half-sized version of this MD5.  Fits in a long **/
  public @Tainted long halfDigest(@Tainted MD5Hash this) {
    @Tainted
    long value = 0;
    for (@Tainted int i = 0; i < 8; i++)
      value |= ((digest[i] & 0xffL) << (8*(7-i)));
    return value;
  }

  /**
   * Return a 32-bit digest of the MD5.
   * @return the first 4 bytes of the md5
   */
  public @Tainted int quarterDigest(@Tainted MD5Hash this) {
    @Tainted
    int value = 0;
    for (@Tainted int i = 0; i < 4; i++)
      value |= ((digest[i] & 0xff) << (8*(3-i)));
    return value;    
  }

  /** Returns true iff <code>o</code> is an MD5Hash whose digest contains the
   * same values.  */
  @Override
  public @Tainted boolean equals(@Tainted MD5Hash this, @Tainted Object o) {
    if (!(o instanceof @Tainted MD5Hash))
      return false;
    @Tainted
    MD5Hash other = (@Tainted MD5Hash)o;
    return Arrays.equals(this.digest, other.digest);
  }

  /** Returns a hash code value for this object.
   * Only uses the first 4 bytes, since md5s are evenly distributed.
   */
  @Override
  public @Tainted int hashCode(@Tainted MD5Hash this) {
    return quarterDigest();
  }


  /** Compares this object with the specified object for order.*/
  @Override
  public @Tainted int compareTo(@Tainted MD5Hash this, @Tainted MD5Hash that) {
    return WritableComparator.compareBytes(this.digest, 0, MD5_LEN,
                                           that.digest, 0, MD5_LEN);
  }

  /** A WritableComparator optimized for MD5Hash keys. */
  public static class Comparator extends @Tainted WritableComparator {
    public @Tainted Comparator() {
      super(MD5Hash.class);
    }

    @Override
    public @Tainted int compare(MD5Hash.@Tainted Comparator this, @Tainted byte @Tainted [] b1, @Tainted int s1, @Tainted int l1,
                       @Tainted
                       byte @Tainted [] b2, @Tainted int s2, @Tainted int l2) {
      return compareBytes(b1, s1, MD5_LEN, b2, s2, MD5_LEN);
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(MD5Hash.class, new @Tainted Comparator());
  }

  private static final @Tainted char @Tainted [] HEX_DIGITS =
  new char @Tainted [] {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /** Returns a string representation of this object. */
  @Override
  public @Tainted String toString(@Tainted MD5Hash this) {
    @Tainted
    StringBuilder buf = new @Tainted StringBuilder(MD5_LEN*2);
    for (@Tainted int i = 0; i < MD5_LEN; i++) {
      @Tainted
      int b = digest[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();
  }

  /** Sets the digest value from a hex string. */
  public void setDigest(@Tainted MD5Hash this, @Tainted String hex) {
    if (hex.length() != MD5_LEN*2)
      throw new @Tainted IllegalArgumentException("Wrong length: " + hex.length());
    @Tainted
    byte @Tainted [] digest = new @Tainted byte @Tainted [MD5_LEN];
    for (@Tainted int i = 0; i < MD5_LEN; i++) {
      @Tainted
      int j = i << 1;
      digest[i] = (@Tainted byte)(charToNibble(hex.charAt(j)) << 4 |
                         charToNibble(hex.charAt(j+1)));
    }
    this.digest = digest;
  }

  private static final @Tainted int charToNibble(@Tainted char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      throw new @Tainted RuntimeException("Not a hex character: " + c);
    }
  }


}
