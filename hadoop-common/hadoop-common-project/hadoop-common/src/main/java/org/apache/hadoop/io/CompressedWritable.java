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
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A base-class for Writables which store themselves compressed and lazily
 * inflate on field access.  This is useful for large objects whose fields are
 * not be altered during a map or reduce operation: leaving the field data
 * compressed makes copying the instance from one file to another much
 * faster. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CompressedWritable implements @Tainted Writable {
  // if non-null, the compressed field data of this instance.
  private @Tainted byte @Tainted [] compressed;

  public @Tainted CompressedWritable() {}

  @Override
  public final void readFields(@Tainted CompressedWritable this, @Tainted DataInput in) throws IOException {
    compressed = new @Tainted byte @Tainted [in.readInt()];
    in.readFully(compressed, 0, compressed.length);
  }

  /** Must be called by all methods which access fields to ensure that the data
   * has been uncompressed. */
  protected void ensureInflated(@Tainted CompressedWritable this) {
    if (compressed != null) {
      try {
        @Tainted
        ByteArrayInputStream deflated = new @Tainted ByteArrayInputStream(compressed);
        @Tainted
        DataInput inflater =
          new @Tainted DataInputStream(new @Tainted InflaterInputStream(deflated));
        readFieldsCompressed(inflater);
        compressed = null;
      } catch (@Tainted IOException e) {
        throw new @Tainted RuntimeException(e);
      }
    }
  }

  /** Subclasses implement this instead of {@link #readFields(DataInput)}. */
  protected abstract void readFieldsCompressed(@Tainted CompressedWritable this, @Tainted DataInput in)
    throws IOException;

  @Override
  public final void write(@Tainted CompressedWritable this, @Tainted DataOutput out) throws IOException {
    if (compressed == null) {
      @Tainted
      ByteArrayOutputStream deflated = new @Tainted ByteArrayOutputStream();
      @Tainted
      Deflater deflater = new @Tainted Deflater(Deflater.BEST_SPEED);
      @Tainted
      DataOutputStream dout =
        new @Tainted DataOutputStream(new @Tainted DeflaterOutputStream(deflated, deflater));
      writeCompressed(dout);
      dout.close();
      deflater.end();
      compressed = deflated.toByteArray();
    }
    out.writeInt(compressed.length);
    out.write(compressed);
  }

  /** Subclasses implement this instead of {@link #write(DataOutput)}. */
  protected abstract void writeCompressed(@Tainted CompressedWritable this, @Tainted DataOutput out) throws IOException;

}
