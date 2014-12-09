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
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * XML Serializer.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class XmlRecordOutput implements @Tainted RecordOutput {

  private @Tainted PrintStream stream;
    
  private @Tainted int indent = 0;
    
  private @Tainted Stack<@Tainted String> compoundStack;
    
  private void putIndent(@Tainted XmlRecordOutput this) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder("");
    for (@Tainted int idx = 0; idx < indent; idx++) {
      sb.append("  ");
    }
    stream.print(sb.toString());
  }
    
  private void addIndent(@Tainted XmlRecordOutput this) {
    indent++;
  }
    
  private void closeIndent(@Tainted XmlRecordOutput this) {
    indent--;
  }
    
  private void printBeginEnvelope(@Tainted XmlRecordOutput this, @Tainted String tag) {
    if (!compoundStack.empty()) {
      @Tainted
      String s = compoundStack.peek();
      if ("struct".equals(s)) {
        putIndent();
        stream.print("<member>\n");
        addIndent();
        putIndent();
        stream.print("<name>"+tag+"</name>\n");
        putIndent();
        stream.print("<value>");
      } else if ("vector".equals(s)) {
        stream.print("<value>");
      } else if ("map".equals(s)) {
        stream.print("<value>");
      }
    } else {
      stream.print("<value>");
    }
  }
    
  private void printEndEnvelope(@Tainted XmlRecordOutput this, @Tainted String tag) {
    if (!compoundStack.empty()) {
      @Tainted
      String s = compoundStack.peek();
      if ("struct".equals(s)) {
        stream.print("</value>\n");
        closeIndent();
        putIndent();
        stream.print("</member>\n");
      } else if ("vector".equals(s)) {
        stream.print("</value>\n");
      } else if ("map".equals(s)) {
        stream.print("</value>\n");
      }
    } else {
      stream.print("</value>\n");
    }
  }
    
  private void insideVector(@Tainted XmlRecordOutput this, @Tainted String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("vector");
  }
    
  private void outsideVector(@Tainted XmlRecordOutput this, @Tainted String tag) throws IOException {
    @Tainted
    String s = compoundStack.pop();
    if (!"vector".equals(s)) {
      throw new @Tainted IOException("Error serializing vector.");
    }
    printEndEnvelope(tag);
  }
    
  private void insideMap(@Tainted XmlRecordOutput this, @Tainted String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("map");
  }
    
  private void outsideMap(@Tainted XmlRecordOutput this, @Tainted String tag) throws IOException {
    @Tainted
    String s = compoundStack.pop();
    if (!"map".equals(s)) {
      throw new @Tainted IOException("Error serializing map.");
    }
    printEndEnvelope(tag);
  }
    
  private void insideRecord(@Tainted XmlRecordOutput this, @Tainted String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("struct");
  }
    
  private void outsideRecord(@Tainted XmlRecordOutput this, @Tainted String tag) throws IOException {
    @Tainted
    String s = compoundStack.pop();
    if (!"struct".equals(s)) {
      throw new @Tainted IOException("Error serializing record.");
    }
    printEndEnvelope(tag);
  }
    
  /** Creates a new instance of XmlRecordOutput */
  public @Tainted XmlRecordOutput(@Tainted OutputStream out) {
    try {
      stream = new @Tainted PrintStream(out, true, "UTF-8");
      compoundStack = new @Tainted Stack<@Tainted String>();
    } catch (@Tainted UnsupportedEncodingException ex) {
      throw new @Tainted RuntimeException(ex);
    }
  }
    
  @Override
  public void writeByte(@Tainted XmlRecordOutput this, @Tainted byte b, @Tainted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:i1>");
    stream.print(Byte.toString(b));
    stream.print("</ex:i1>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeBool(@Tainted XmlRecordOutput this, @Tainted boolean b, @Tainted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<boolean>");
    stream.print(b ? "1" : "0");
    stream.print("</boolean>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeInt(@Tainted XmlRecordOutput this, @Tainted int i, @Tainted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<i4>");
    stream.print(Integer.toString(i));
    stream.print("</i4>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeLong(@Tainted XmlRecordOutput this, @Tainted long l, @Tainted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:i8>");
    stream.print(Long.toString(l));
    stream.print("</ex:i8>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeFloat(@Tainted XmlRecordOutput this, @Tainted float f, @Tainted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:float>");
    stream.print(Float.toString(f));
    stream.print("</ex:float>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeDouble(@Tainted XmlRecordOutput this, @Tainted double d, @Tainted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<double>");
    stream.print(Double.toString(d));
    stream.print("</double>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeString(@Tainted XmlRecordOutput this, @Tainted String s, @Tainted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<string>");
    stream.print(Utils.toXMLString(s));
    stream.print("</string>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeBuffer(@Tainted XmlRecordOutput this, @Tainted Buffer buf, @Tainted String tag)
    throws IOException {
    printBeginEnvelope(tag);
    stream.print("<string>");
    stream.print(Utils.toXMLBuffer(buf));
    stream.print("</string>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void startRecord(@Tainted XmlRecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException {
    insideRecord(tag);
    stream.print("<struct>\n");
    addIndent();
  }
    
  @Override
  public void endRecord(@Tainted XmlRecordOutput this, @Tainted Record r, @Tainted String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</struct>");
    outsideRecord(tag);
  }
    
  @Override
  public void startVector(@Tainted XmlRecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException {
    insideVector(tag);
    stream.print("<array>\n");
    addIndent();
  }
    
  @Override
  public void endVector(@Tainted XmlRecordOutput this, @Tainted ArrayList v, @Tainted String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</array>");
    outsideVector(tag);
  }
    
  @Override
  public void startMap(@Tainted XmlRecordOutput this, @Tainted TreeMap v, @Tainted String tag) throws IOException {
    insideMap(tag);
    stream.print("<array>\n");
    addIndent();
  }
    
  @Override
  public void endMap(@Tainted XmlRecordOutput this, @Tainted TreeMap v, @Tainted String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</array>");
    outsideMap(tag);
  }

}
