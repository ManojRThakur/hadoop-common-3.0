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
import java.io.IOException;
import java.util.TreeMap;
import java.util.ArrayList;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BinaryRecordOutput implements @Tainted RecordOutput {
    
  private @Tainted DataOutput out;
    
  private @Tainted BinaryRecordOutput() {}
    
  private void setDataOutput(@Tainted BinaryRecordOutput this, @Tainted DataOutput out) {
    this.out = out;
  }
    
  private static @Tainted ThreadLocal<BinaryRecordOutput> bOut = new @Tainted ThreadLocal<BinaryRecordOutput>() {
      @Override
      protected synchronized @Tainted BinaryRecordOutput initialValue() {
        return new @Tainted BinaryRecordOutput();
      }
    };
    
  /**
   * Get a thread-local record output for the supplied DataOutput.
   * @param out data output stream
   * @return binary record output corresponding to the supplied DataOutput.
   */
  public static @Tainted BinaryRecordOutput get(@Tainted DataOutput out) {
    @Tainted
    BinaryRecordOutput bout = (@Tainted BinaryRecordOutput) bOut.get();
    bout.setDataOutput(out);
    return bout;
  }
    
  /** Creates a new instance of BinaryRecordOutput */
  public @Tainted BinaryRecordOutput(@Tainted OutputStream out) {
    this.out = new @Tainted DataOutputStream(out);
  }
    
  /** Creates a new instance of BinaryRecordOutput */
  public @Tainted BinaryRecordOutput(@Tainted DataOutput out) {
    this.out = out;
  }
    
    
  @Override
  public void writeByte(@Tainted BinaryRecordOutput this, @Tainted byte b, @Tainted String tag) throws IOException {
    out.writeByte(b);
  }
    
  @Override
  public void writeBool(@Tainted BinaryRecordOutput this, @Tainted boolean b, @Tainted String tag) throws IOException {
    out.writeBoolean(b);
  }
    
  @Override
  public void writeInt(@Tainted BinaryRecordOutput this, @Tainted int i, @Tainted String tag) throws IOException {
    Utils.writeVInt(out, i);
  }
    
  @Override
  public void writeLong(@Tainted BinaryRecordOutput this, @Tainted long l, @Tainted String tag) throws IOException {
    Utils.writeVLong(out, l);
  }
    
  @Override
  public void writeFloat(@Tainted BinaryRecordOutput this, @Tainted float f, @Tainted String tag) throws IOException {
    out.writeFloat(f);
  }
    
  @Override
  public void writeDouble(@Tainted BinaryRecordOutput this, @Tainted double d, @Tainted String tag) throws IOException {
    out.writeDouble(d);
  }
    
  @Override
  public void writeString(@Tainted BinaryRecordOutput this, @Tainted String s, @Tainted String tag) throws IOException {
    Utils.toBinaryString(out, s);
  }
    
  @Override
  public void writeBuffer(@Tainted BinaryRecordOutput this, @Tainted Buffer buf, @Tainted String tag)
    throws IOException {
    @Tainted
    byte @Tainted [] barr = buf.get();
    @Tainted
    int len = buf.getCount();
    Utils.writeVInt(out, len);
    out.write(barr, 0, len);
  }
    
  @Override
  public void startRecord(@Tainted BinaryRecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException {}
    
  @Override
  public void endRecord(@Tainted BinaryRecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException {}
    
  @Override
  public void startVector(@Tainted BinaryRecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException {
    writeInt(v.size(), tag);
  }
    
  @Override
  public void endVector(@Tainted BinaryRecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException {}
    
  @Override
  public void startMap(@Tainted BinaryRecordOutput this, @Tainted TreeMap v, @Tainted String tag) throws IOException {
    writeInt(v.size(), tag);
  }
    
  @Override
  public void endMap(@Tainted BinaryRecordOutput this, @Tainted TreeMap v, @Tainted String tag) throws IOException {}
    
}
