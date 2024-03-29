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
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CsvRecordOutput implements @Tainted RecordOutput {

  private @Tainted PrintStream stream;
  private @Tainted boolean isFirst = true;
    
  private void throwExceptionOnError(@Tainted CsvRecordOutput this, @Tainted String tag) throws IOException {
    if (stream.checkError()) {
      throw new @Tainted IOException("Error serializing "+tag);
    }
  }
 
  private void printCommaUnlessFirst(@Tainted CsvRecordOutput this) {
    if (!isFirst) {
      stream.print(",");
    }
    isFirst = false;
  }
    
  /** Creates a new instance of CsvRecordOutput */
  public @Tainted CsvRecordOutput(@Tainted OutputStream out) {
    try {
      stream = new @Tainted PrintStream(out, true, "UTF-8");
    } catch (@Tainted UnsupportedEncodingException ex) {
      throw new @Tainted RuntimeException(ex);
    }
  }
    
  @Override
  public void writeByte(@Tainted CsvRecordOutput this, @Tainted byte b, @Tainted String tag) throws IOException {
    writeLong((@Tainted long)b, tag);
  }
    
  @Override
  public void writeBool(@Tainted CsvRecordOutput this, @Tainted boolean b, @Tainted String tag) throws IOException {
    printCommaUnlessFirst();
    @Tainted
    String val = b ? "T" : "F";
    stream.print(val);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeInt(@Tainted CsvRecordOutput this, @Tainted int i, @Tainted String tag) throws IOException {
    writeLong((@Tainted long)i, tag);
  }
    
  @Override
  public void writeLong(@Tainted CsvRecordOutput this, @Tainted long l, @Tainted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(l);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeFloat(@Tainted CsvRecordOutput this, @Tainted float f, @Tainted String tag) throws IOException {
    writeDouble((@Tainted double)f, tag);
  }
    
  @Override
  public void writeDouble(@Tainted CsvRecordOutput this, @Tainted double d, @Tainted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(d);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeString(@Tainted CsvRecordOutput this, @Tainted String s, @Tainted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(Utils.toCSVString(s));
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeBuffer(@Tainted CsvRecordOutput this, @Tainted Buffer buf, @Tainted String tag)
    throws IOException {
    printCommaUnlessFirst();
    stream.print(Utils.toCSVBuffer(buf));
    throwExceptionOnError(tag);
  }
    
  @Override
  public void startRecord(@Tainted CsvRecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException {
    if (tag != null && ! tag.isEmpty()) {
      printCommaUnlessFirst();
      stream.print("s{");
      isFirst = true;
    }
  }
    
  @Override
  public void endRecord(@Tainted CsvRecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException {
    if (tag == null || tag.isEmpty()) {
      stream.print("\n");
      isFirst = true;
    } else {
      stream.print("}");
      isFirst = false;
    }
  }
    
  @Override
  public void startVector(@Tainted CsvRecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print("v{");
    isFirst = true;
  }
    
  @Override
  public void endVector(@Tainted CsvRecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException {
    stream.print("}");
    isFirst = false;
  }
    
  @Override
  public void startMap(@Tainted CsvRecordOutput this, @Tainted TreeMap v, @Tainted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print("m{");
    isFirst = true;
  }
    
  @Override
  public void endMap(@Tainted CsvRecordOutput this, @Tainted TreeMap v, @Tainted String tag) throws IOException {
    stream.print("}");
    isFirst = false;
  }
}
