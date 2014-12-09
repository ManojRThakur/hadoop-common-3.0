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
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.SAXParser;

/**
 * XML Deserializer.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class XmlRecordInput implements @Tainted RecordInput {
    
  static private class Value {
    private @Tainted String type;
    private @Tainted StringBuffer sb;
        
    public @Tainted Value(@Tainted String t) {
      type = t;
      sb = new @Tainted StringBuffer();
    }
    public void addChars(XmlRecordInput.@Tainted Value this, @Tainted char @Tainted [] buf, @Tainted int offset, @Tainted int len) {
      sb.append(buf, offset, len);
    }
    public @Tainted String getValue(XmlRecordInput.@Tainted Value this) { return sb.toString(); }
    public @Tainted String getType(XmlRecordInput.@Tainted Value this) { return type; }
  }
    
  private static class XMLParser extends @Tainted DefaultHandler {
    private @Tainted boolean charsValid = false;
        
    private @Tainted ArrayList<@Tainted Value> valList;
        
    private @Tainted XMLParser(@Tainted ArrayList<@Tainted Value> vlist) {
      valList = vlist;
    }
        
    @Override
    public void startDocument(XmlRecordInput.@Tainted XMLParser this) throws SAXException {}
        
    @Override
    public void endDocument(XmlRecordInput.@Tainted XMLParser this) throws SAXException {}
        
    @Override
    public void startElement(XmlRecordInput.@Tainted XMLParser this, @Tainted String ns,
                             @Tainted
                             String sname,
                             @Tainted
                             String qname,
                             @Tainted
                             Attributes attrs) throws SAXException {
      charsValid = false;
      if ("boolean".equals(qname) ||
          "i4".equals(qname) ||
          "int".equals(qname) ||
          "string".equals(qname) ||
          "double".equals(qname) ||
          "ex:i1".equals(qname) ||
          "ex:i8".equals(qname) ||
          "ex:float".equals(qname)) {
        charsValid = true;
        valList.add(new @Tainted Value(qname));
      } else if ("struct".equals(qname) ||
                 "array".equals(qname)) {
        valList.add(new @Tainted Value(qname));
      }
    }
        
    @Override
    public void endElement(XmlRecordInput.@Tainted XMLParser this, @Tainted String ns,
                           @Tainted
                           String sname,
                           @Tainted
                           String qname) throws SAXException {
      charsValid = false;
      if ("struct".equals(qname) ||
          "array".equals(qname)) {
        valList.add(new @Tainted Value("/"+qname));
      }
    }
        
    @Override
    public void characters(XmlRecordInput.@Tainted XMLParser this, @Tainted char buf @Tainted [], @Tainted int offset, @Tainted int len)
      throws SAXException {
      if (charsValid) {
        @Tainted
        Value v = valList.get(valList.size()-1);
        v.addChars(buf, offset, len);
      }
    }
        
  }
    
  private class XmlIndex implements @Tainted Index {
    @Override
    public @Tainted boolean done(@Tainted XmlRecordInput.XmlIndex this) {
      @Tainted
      Value v = valList.get(vIdx);
      if ("/array".equals(v.getType())) {
        valList.set(vIdx, null);
        vIdx++;
        return true;
      } else {
        return false;
      }
    }
    @Override
    public void incr(@Tainted XmlRecordInput.XmlIndex this) {}
  }
    
  private @Tainted ArrayList<@Tainted Value> valList;
  private @Tainted int vLen;
  private @Tainted int vIdx;
    
  private @Tainted Value next(@Tainted XmlRecordInput this) throws IOException {
    if (vIdx < vLen) {
      @Tainted
      Value v = valList.get(vIdx);
      valList.set(vIdx, null);
      vIdx++;
      return v;
    } else {
      throw new @Tainted IOException("Error in deserialization.");
    }
  }
    
  /** Creates a new instance of XmlRecordInput */
  public @Tainted XmlRecordInput(@Tainted InputStream in) {
    try{
      valList = new @Tainted ArrayList<@Tainted Value>();
      @Tainted
      DefaultHandler handler = new @Tainted XMLParser(valList);
      @Tainted
      SAXParserFactory factory = SAXParserFactory.newInstance();
      @Tainted
      SAXParser parser = factory.newSAXParser();
      parser.parse(in, handler);
      vLen = valList.size();
      vIdx = 0;
    } catch (@Tainted Exception ex) {
      throw new @Tainted RuntimeException(ex);
    }
  }
    
  @Override
  public @Tainted byte readByte(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"ex:i1".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return Byte.parseByte(v.getValue());
  }
    
  @Override
  public @Tainted boolean readBool(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"boolean".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return "1".equals(v.getValue());
  }
    
  @Override
  public @Tainted int readInt(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"i4".equals(v.getType()) &&
        !"int".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return Integer.parseInt(v.getValue());
  }
    
  @Override
  public @Tainted long readLong(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"ex:i8".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return Long.parseLong(v.getValue());
  }
    
  @Override
  public @Tainted float readFloat(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"ex:float".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return Float.parseFloat(v.getValue());
  }
    
  @Override
  public @Tainted double readDouble(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"double".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return Double.parseDouble(v.getValue());
  }
    
  @Override
  public @Tainted String readString(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"string".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return Utils.fromXMLString(v.getValue());
  }
    
  @Override
  public @Tainted Buffer readBuffer(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"string".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return Utils.fromXMLBuffer(v.getValue());
  }
    
  @Override
  public void startRecord(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"struct".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
  }
    
  @Override
  public void endRecord(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"/struct".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
  }
    
  @Override
  public @Tainted Index startVector(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    @Tainted
    Value v = next();
    if (!"array".equals(v.getType())) {
      throw new @Tainted IOException("Error deserializing "+tag+".");
    }
    return new @Tainted XmlIndex();
  }
    
  @Override
  public void endVector(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {}
    
  @Override
  public @Tainted Index startMap(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException {
    return startVector(tag);
  }
    
  @Override
  public void endMap(@Tainted XmlRecordInput this, @Tainted String tag) throws IOException { endVector(tag); }

}
