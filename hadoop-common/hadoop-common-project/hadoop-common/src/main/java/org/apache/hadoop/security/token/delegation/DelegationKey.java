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

package org.apache.hadoop.security.token.delegation;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import javax.crypto.SecretKey;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Key used for generating and verifying delegation tokens
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DelegationKey implements @Tainted Writable {
  private @Tainted int keyId;
  private @Tainted long expiryDate;
  @Nullable
  private @Tainted byte @Tainted [] keyBytes = null;
  private static final @Tainted int MAX_KEY_LEN = 1024 * 1024;

  /** Default constructore required for Writable */
  public @Tainted DelegationKey() {
    this(0, 0L, (@Tainted SecretKey)null);
  }

  public @Tainted DelegationKey(@Tainted int keyId, @Tainted long expiryDate, @Tainted SecretKey key) {
    this(keyId, expiryDate, key != null ? key.getEncoded() : null);
  }
  
  public @Tainted DelegationKey(@Tainted int keyId, @Tainted long expiryDate, @Tainted byte @Tainted [] encodedKey) {
    this.keyId = keyId;
    this.expiryDate = expiryDate;
    if (encodedKey != null) {
      if (encodedKey.length > MAX_KEY_LEN) {
        throw new @Tainted RuntimeException("can't create " + encodedKey.length +
            " byte long DelegationKey.");
      }
      this.keyBytes = encodedKey;
    }
  }

  public @Tainted int getKeyId(@Tainted DelegationKey this) {
    return keyId;
  }

  public @Tainted long getExpiryDate(@Tainted DelegationKey this) {
    return expiryDate;
  }

  public @Tainted SecretKey getKey(@Tainted DelegationKey this) {
    if (keyBytes == null || keyBytes.length == 0) {
      return null;
    } else {
      @Tainted
      SecretKey key = AbstractDelegationTokenSecretManager.createSecretKey(keyBytes);
      return key;
    }
  }
  
  public @Tainted byte @Tainted [] getEncodedKey(@Tainted DelegationKey this) {
    return keyBytes;
  }

  public void setExpiryDate(@Tainted DelegationKey this, @Tainted long expiryDate) {
    this.expiryDate = expiryDate;
  }

  /**
   */
  @Override
  public void write(@Tainted DelegationKey this, @Tainted DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeVLong(out, expiryDate);
    if (keyBytes == null) {
      WritableUtils.writeVInt(out, -1);
    } else {
      WritableUtils.writeVInt(out, keyBytes.length);
      out.write(keyBytes);
    }
  }

  /**
   */
  @Override
  public void readFields(@Tainted DelegationKey this, @Tainted DataInput in) throws IOException {
    keyId = WritableUtils.readVInt(in);
    expiryDate = WritableUtils.readVLong(in);
    @Tainted
    int len = WritableUtils.readVIntInRange(in, -1, MAX_KEY_LEN);
    if (len == -1) {
      keyBytes = null;
    } else {
      keyBytes = new @Tainted byte @Tainted [len];
      in.readFully(keyBytes);
    }
  }

  @Override
  public @Tainted int hashCode(@Tainted DelegationKey this) {
    final @Tainted int prime = 31;
    @Tainted
    int result = 1;
    result = prime * result + (@Tainted int) (expiryDate ^ (expiryDate >>> 32));
    result = prime * result + Arrays.hashCode(keyBytes);
    result = prime * result + keyId;
    return result;
  }

  @Override
  public @Tainted boolean equals(@Tainted DelegationKey this, @Tainted Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      @Tainted
      DelegationKey r = (@Tainted DelegationKey) right;
      return keyId == r.keyId &&
             expiryDate == r.expiryDate &&
             Arrays.equals(keyBytes, r.keyBytes);
    }
  }

}
