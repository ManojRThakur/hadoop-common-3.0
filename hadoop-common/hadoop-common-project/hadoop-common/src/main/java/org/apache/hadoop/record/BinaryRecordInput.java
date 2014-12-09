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

package org.apache.hadoop.record;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.DataInput;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BinaryRecordInput implements @Tainted RecordInput {
    
  private @Tainted DataInput in;
    
  static private class BinaryIndex implements @Tainted Index {
    private @Tainted int nelems;
    private @Tainted BinaryIndex(@Tainted int nelems) {
      this.nelems = nelems;
    }
    @Override
    public @Tainted boolean done(BinaryRecordInput.@Tainted BinaryIndex this) {
      return (nelems <= 0);
    }
    @Override
    public void incr(BinaryRecordInput.@Tainted BinaryIndex this) {
      nelems--;
    }
  }
    
  private @Tainted BinaryRecordInput() {}
    
  private void setDataInput(@Tainted BinaryRecordInput this, @Tainted DataInput inp) {
    this.in = inp;
  }
    
  private static @Tainted ThreadLocal<BinaryRecordInput> bIn = new @Tainted ThreadLocal<BinaryRecordInput>() {
      @Override
      protected synchronized @Tainted BinaryRecordInput initialValue() {
        return new @Tainted BinaryRecordInput();
      }
    };
    
  /**
   * Get a thread-local record input for the supplied DataInput.
   * @param inp data input stream
   * @return binary record input corresponding to the supplied DataInput.
   */
  public static @Tainted BinaryRecordInput get(@Tainted DataInput inp) {
    @Tainted
    BinaryRecordInput bin = (@Tainted BinaryRecordInput) bIn.get();
    bin.setDataInput(inp);
    return bin;
  }
    
  /** Creates a new instance of BinaryRecordInput */
  public @Tainted BinaryRecordInput(@Tainted InputStream strm) {
    this.in = new @Tainted DataInputStream(strm);
  }
    
  /** Creates a new instance of BinaryRecordInput */
  public @Tainted BinaryRecordInput(@Tainted DataInput din) {
    this.in = din;
  }
    
  @Override
  public @Tainted byte readByte(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return in.readByte();
  }
    
  @Override
  public @Tainted boolean readBool(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return in.readBoolean();
  }
    
  @Override
  public @Tainted int readInt(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return Utils.readVInt(in);
  }
    
  @Override
  public @Tainted long readLong(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return Utils.readVLong(in);
  }
    
  @Override
  public @Tainted float readFloat(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return in.readFloat();
  }
    
  @Override
  public @Tainted double readDouble(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return in.readDouble();
  }
    
  @Override
  public @Tainted String readString(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return Utils.fromBinaryString(in);
  }
    
  @Override
  public @Tainted Buffer readBuffer(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    final @Tainted int len = Utils.readVInt(in);
    final @Tainted byte @Tainted [] barr = new @Tainted byte @Tainted [len];
    in.readFully(barr);
    return new @Tainted Buffer(barr);
  }
    
  @Override
  public void startRecord(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    // no-op
  }
    
  @Override
  public void endRecord(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    // no-op
  }
    
  @Override
  public @Tainted Index startVector(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return new @Tainted BinaryIndex(readInt(tag));
  }
    
  @Override
  public void endVector(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    // no-op
  }
    
  @Override
  public @Tainted Index startMap(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    return new @Tainted BinaryIndex(readInt(tag));
  }
    
  @Override
  public void endMap(@Tainted BinaryRecordInput this, final @Tainted String tag) throws IOException {
    // no-op
  }
}
