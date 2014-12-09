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
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CsvRecordInput implements @Tainted RecordInput {
    
  private @Tainted PushbackReader stream;
    
  private class CsvIndex implements @Tainted Index {
    @Override
    public @Tainted boolean done(@Tainted CsvRecordInput.CsvIndex this) {
      @Tainted
      char c = '\0';
      try {
        c = (@Tainted char) stream.read();
        stream.unread(c);
      } catch (@Tainted IOException ex) {
      }
      return (c == '}') ? true : false;
    }
    @Override
    public void incr(@Tainted CsvRecordInput.CsvIndex this) {}
  }
    
  private void throwExceptionOnError(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    throw new @Tainted IOException("Error deserializing "+tag);
  }
    
  private @Tainted String readField(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    try {
      @Tainted
      StringBuilder buf = new @Tainted StringBuilder();
      while (true) {
        @Tainted
        char c = (@Tainted char) stream.read();
        switch (c) {
        case ',':
          return buf.toString();
        case '}':
        case '\n':
        case '\r':
          stream.unread(c);
          return buf.toString();
        default:
          buf.append(c);
        }
      }
    } catch (@Tainted IOException ex) {
      throw new @Tainted IOException("Error reading "+tag);
    }
  }
    
  /** Creates a new instance of CsvRecordInput */
  public @Tainted CsvRecordInput(@Tainted InputStream in) {
    try {
      stream = new @Tainted PushbackReader(new @Tainted InputStreamReader(in, "UTF-8"));
    } catch (@Tainted UnsupportedEncodingException ex) {
      throw new @Tainted RuntimeException(ex);
    }
  }
    
  @Override
  public @Tainted byte readByte(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    return (@Tainted byte) readLong(tag);
  }
    
  @Override
  public @Tainted boolean readBool(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    String sval = readField(tag);
    return "T".equals(sval) ? true : false;
  }
    
  @Override
  public @Tainted int readInt(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    return (@Tainted int) readLong(tag);
  }
    
  @Override
  public @Tainted long readLong(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    String sval = readField(tag);
    try {
      @Tainted
      long lval = Long.parseLong(sval);
      return lval;
    } catch (@Tainted NumberFormatException ex) {
      throw new @Tainted IOException("Error deserializing "+tag);
    }
  }
    
  @Override
  public @Tainted float readFloat(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    return (@Tainted float) readDouble(tag);
  }
    
  @Override
  public @Tainted double readDouble(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    String sval = readField(tag);
    try {
      @Tainted
      double dval = Double.parseDouble(sval);
      return dval;
    } catch (@Tainted NumberFormatException ex) {
      throw new @Tainted IOException("Error deserializing "+tag);
    }
  }
    
  @Override
  public @Tainted String readString(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    String sval = readField(tag);
    return Utils.fromCSVString(sval);
  }
    
  @Override
  public @Tainted Buffer readBuffer(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    String sval = readField(tag);
    return Utils.fromCSVBuffer(sval);
  }
    
  @Override
  public void startRecord(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    if (tag != null && !tag.isEmpty()) {
      @Tainted
      char c1 = (@Tainted char) stream.read();
      @Tainted
      char c2 = (@Tainted char) stream.read();
      if (c1 != 's' || c2 != '{') {
        throw new @Tainted IOException("Error deserializing "+tag);
      }
    }
  }
    
  @Override
  public void endRecord(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    char c = (@Tainted char) stream.read();
    if (tag == null || tag.isEmpty()) {
      if (c != '\n' && c != '\r') {
        throw new @Tainted IOException("Error deserializing record.");
      } else {
        return;
      }
    }
        
    if (c != '}') {
      throw new @Tainted IOException("Error deserializing "+tag);
    }
    c = (@Tainted char) stream.read();
    if (c != ',') {
      stream.unread(c);
    }
        
    return;
  }
    
  @Override
  public @Tainted Index startVector(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    char c1 = (@Tainted char) stream.read();
    @Tainted
    char c2 = (@Tainted char) stream.read();
    if (c1 != 'v' || c2 != '{') {
      throw new @Tainted IOException("Error deserializing "+tag);
    }
    return new @Tainted CsvIndex();
  }
    
  @Override
  public void endVector(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    char c = (@Tainted char) stream.read();
    if (c != '}') {
      throw new @Tainted IOException("Error deserializing "+tag);
    }
    c = (@Tainted char) stream.read();
    if (c != ',') {
      stream.unread(c);
    }
    return;
  }
    
  @Override
  public @Tainted Index startMap(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    char c1 = (@Tainted char) stream.read();
    @Tainted
    char c2 = (@Tainted char) stream.read();
    if (c1 != 'm' || c2 != '{') {
      throw new @Tainted IOException("Error deserializing "+tag);
    }
    return new @Tainted CsvIndex();
  }
    
  @Override
  public void endMap(@Tainted CsvRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    char c = (@Tainted char) stream.read();
    if (c != '}') {
      throw new @Tainted IOException("Error deserializing "+tag);
    }
    c = (@Tainted char) stream.read();
    if (c != ',') {
      stream.unread(c);
    }
    return;
  }
}
