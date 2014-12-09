/* Generated By:JavaCC: Do not edit this line. Rcc.java */
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

package org.apache.hadoop.record.compiler.generated;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.compiler.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.io.File;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Rcc implements @Tainted RccConstants {
  private static @Tainted String language = "java";
  private static @Tainted String destDir = ".";
  private static @Tainted ArrayList<@Tainted String> recFiles = new @Tainted ArrayList<@Tainted String>();
  private static @Tainted ArrayList<@Tainted String> cmdargs = new @Tainted ArrayList<@Tainted String>();
  private static @Tainted JFile curFile;
  private static @Tainted Hashtable<@Tainted String, @Tainted JRecord> recTab;
  private static @Tainted String curDir = ".";
  private static @Tainted String curFileName;
  private static @Tainted String curModuleName;

  public static void main(@Tainted String @Tainted [] args) {
    System.exit(driver(args));
  }

  public static void usage() {
    System.err.println("Usage: rcc --language [java|c++] ddl-files");
  }

  public static @Tainted int driver(@Tainted String @Tainted [] args) {
    for (@Tainted int i=0; i<args.length; i++) {
      if ("-l".equalsIgnoreCase(args[i]) ||
          "--language".equalsIgnoreCase(args[i])) {
        language = args[i+1].toLowerCase();
        i++;
      } else if ("-d".equalsIgnoreCase(args[i]) ||
                 "--destdir".equalsIgnoreCase(args[i])) {
        destDir = args[i+1];
        i++;
      } else if (args[i].startsWith("-")) {
        @Tainted
        String arg = args[i].substring(1);
        if (arg.startsWith("-")) {
          arg = arg.substring(1);
        }
        cmdargs.add(arg.toLowerCase());
      } else {
        recFiles.add(args[i]);
      }
    }
    if (recFiles.size() == 0) {
      usage();
      return 1;
    }
    for (@Tainted int i=0; i<recFiles.size(); i++) {
      curFileName = recFiles.get(i);
      @Tainted
      File file = new @Tainted File(curFileName);
      try {
        @Tainted
        FileReader reader = new @Tainted FileReader(file);
        @Tainted
        Rcc parser = new @Tainted Rcc(reader);
        try {
          recTab = new @Tainted Hashtable<@Tainted String, @Tainted JRecord>();
          curFile = parser.Input();
        } catch (@Tainted ParseException e) {
          System.err.println(e.toString());
          return 1;
        }
        try {
          reader.close();
        } catch (@Tainted IOException e) {
        }
      } catch (@Tainted FileNotFoundException e) {
        System.err.println("File " + recFiles.get(i) +
                           " Not found.");
        return 1;
      }
      try {
        @Tainted
        int retCode = curFile.genCode(language, destDir, cmdargs);
        if (retCode != 0) { return retCode; }
      } catch (@Tainted IOException e) {
        System.err.println(e.toString());
        return 1;
      }
    }
    return 0;
  }

  final public @Tainted JFile Input(@Tainted Rcc this) throws ParseException {
    @Tainted
    ArrayList<@Tainted JFile> ilist = new @Tainted ArrayList<@Tainted JFile>();
    @Tainted
    ArrayList<@Tainted JRecord> rlist = new @Tainted ArrayList<@Tainted JRecord>();
    @Tainted
    JFile i;
    @Tainted
    ArrayList<@Tainted JRecord> l;
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case INCLUDE_TKN:
        i = Include();
        ilist.add(i);
        break;
      case MODULE_TKN:
        l = Module();
        rlist.addAll(l);
        break;
      default:
        jj_la1[0] = jj_gen;
        jj_consume_token(-1);
        throw new @Tainted ParseException();
      }
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case MODULE_TKN:
      case INCLUDE_TKN:
        ;
        break;
      default:
        jj_la1[1] = jj_gen;
        break label_1;
      }
    }
    jj_consume_token(0);
    {if (true) return new @Tainted JFile(curFileName, ilist, rlist);}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted JFile Include(@Tainted Rcc this) throws ParseException {
    @Tainted
    String fname;
    @Tainted
    Token t;
    jj_consume_token(INCLUDE_TKN);
    t = jj_consume_token(CSTRING_TKN);
    @Tainted
    JFile ret = null;
    fname = t.image.replaceAll("^\"", "").replaceAll("\"$","");
    @Tainted
    File file = new @Tainted File(curDir, fname);
    @Tainted
    String tmpDir = curDir;
    @Tainted
    String tmpFile = curFileName;
    curDir = file.getParent();
    curFileName = file.getName();
    try {
      @Tainted
      FileReader reader = new @Tainted FileReader(file);
      @Tainted
      Rcc parser = new @Tainted Rcc(reader);
      try {
        ret = parser.Input();
        System.out.println(fname + " Parsed Successfully");
      } catch (@Tainted ParseException e) {
        System.out.println(e.toString());
        System.exit(1);
      }
      try {
        reader.close();
      } catch (@Tainted IOException e) {
      }
    } catch (@Tainted FileNotFoundException e) {
      System.out.println("File " + fname +
                         " Not found.");
      System.exit(1);
    }
    curDir = tmpDir;
    curFileName = tmpFile;
    {if (true) return ret;}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted ArrayList<@Tainted JRecord> Module(@Tainted Rcc this) throws ParseException {
    @Tainted
    String mName;
    @Tainted
    ArrayList<@Tainted JRecord> rlist;
    jj_consume_token(MODULE_TKN);
    mName = ModuleName();
    curModuleName = mName;
    jj_consume_token(LBRACE_TKN);
    rlist = RecordList();
    jj_consume_token(RBRACE_TKN);
    {if (true) return rlist;}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted String ModuleName(@Tainted Rcc this) throws ParseException {
    @Tainted
    String name = "";
    @Tainted
    Token t;
    t = jj_consume_token(IDENT_TKN);
    name += t.image;
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case DOT_TKN:
        ;
        break;
      default:
        jj_la1[2] = jj_gen;
        break label_2;
      }
      jj_consume_token(DOT_TKN);
      t = jj_consume_token(IDENT_TKN);
      name += "." + t.image;
    }
    {if (true) return name;}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted ArrayList<@Tainted JRecord> RecordList(@Tainted Rcc this) throws ParseException {
    @Tainted
    ArrayList<@Tainted JRecord> rlist = new @Tainted ArrayList<@Tainted JRecord>();
    @Tainted
    JRecord r;
    label_3:
    while (true) {
      r = Record();
      rlist.add(r);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case RECORD_TKN:
        ;
        break;
      default:
        jj_la1[3] = jj_gen;
        break label_3;
      }
    }
    {if (true) return rlist;}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted JRecord Record(@Tainted Rcc this) throws ParseException {
    @Tainted
    String rname;
    @Tainted
    ArrayList<@Tainted JField<@Tainted JType>> flist = new @Tainted ArrayList<@Tainted JField<@Tainted JType>>();
    @Tainted
    Token t;
    @Tainted
    JField<@Tainted JType> f;
    jj_consume_token(RECORD_TKN);
    t = jj_consume_token(IDENT_TKN);
    rname = t.image;
    jj_consume_token(LBRACE_TKN);
    label_4:
    while (true) {
      f = Field();
      flist.add(f);
      jj_consume_token(SEMICOLON_TKN);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case BYTE_TKN:
      case BOOLEAN_TKN:
      case INT_TKN:
      case LONG_TKN:
      case FLOAT_TKN:
      case DOUBLE_TKN:
      case USTRING_TKN:
      case BUFFER_TKN:
      case VECTOR_TKN:
      case MAP_TKN:
      case IDENT_TKN:
        ;
        break;
      default:
        jj_la1[4] = jj_gen;
        break label_4;
      }
    }
    jj_consume_token(RBRACE_TKN);
    @Tainted
    String fqn = curModuleName + "." + rname;
    @Tainted
    JRecord r = new @Tainted JRecord(fqn, flist);
    recTab.put(fqn, r);
    {if (true) return r;}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted JField<@Tainted JType> Field(@Tainted Rcc this) throws ParseException {
    @Tainted
    JType jt;
    @Tainted
    Token t;
    jt = Type();
    t = jj_consume_token(IDENT_TKN);
    {if (true) return new @Tainted JField<@Tainted JType>(t.image, jt);}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted JType Type(@Tainted Rcc this) throws ParseException {
    @Tainted
    JType jt;
    @Tainted
    Token t;
    @Tainted
    String rname;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case MAP_TKN:
      jt = Map();
      {if (true) return jt;}
      break;
    case VECTOR_TKN:
      jt = Vector();
      {if (true) return jt;}
      break;
    case BYTE_TKN:
      jj_consume_token(BYTE_TKN);
      {if (true) return new @Tainted JByte();}
      break;
    case BOOLEAN_TKN:
      jj_consume_token(BOOLEAN_TKN);
      {if (true) return new @Tainted JBoolean();}
      break;
    case INT_TKN:
      jj_consume_token(INT_TKN);
      {if (true) return new @Tainted JInt();}
      break;
    case LONG_TKN:
      jj_consume_token(LONG_TKN);
      {if (true) return new @Tainted JLong();}
      break;
    case FLOAT_TKN:
      jj_consume_token(FLOAT_TKN);
      {if (true) return new @Tainted JFloat();}
      break;
    case DOUBLE_TKN:
      jj_consume_token(DOUBLE_TKN);
      {if (true) return new @Tainted JDouble();}
      break;
    case USTRING_TKN:
      jj_consume_token(USTRING_TKN);
      {if (true) return new @Tainted JString();}
      break;
    case BUFFER_TKN:
      jj_consume_token(BUFFER_TKN);
      {if (true) return new @Tainted JBuffer();}
      break;
    case IDENT_TKN:
      rname = ModuleName();
      if (rname.indexOf('.', 0) < 0) {
        rname = curModuleName + "." + rname;
      }
      @Tainted
      JRecord r = recTab.get(rname);
      if (r == null) {
        System.out.println("Type " + rname + " not known. Exiting.");
        System.exit(1);
      }
      {if (true) return r;}
      break;
    default:
      jj_la1[5] = jj_gen;
      jj_consume_token(-1);
      throw new @Tainted ParseException();
    }
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted JMap Map(@Tainted Rcc this) throws ParseException {
    @Tainted
    JType jt1;
    @Tainted
    JType jt2;
    jj_consume_token(MAP_TKN);
    jj_consume_token(LT_TKN);
    jt1 = Type();
    jj_consume_token(COMMA_TKN);
    jt2 = Type();
    jj_consume_token(GT_TKN);
    {if (true) return new @Tainted JMap(jt1, jt2);}
    throw new @Tainted Error("Missing return statement in function");
  }

  final public @Tainted JVector Vector(@Tainted Rcc this) throws ParseException {
    @Tainted
    JType jt;
    jj_consume_token(VECTOR_TKN);
    jj_consume_token(LT_TKN);
    jt = Type();
    jj_consume_token(GT_TKN);
    {if (true) return new @Tainted JVector(jt);}
    throw new @Tainted Error("Missing return statement in function");
  }

  public @Tainted RccTokenManager token_source;
  @Tainted
  SimpleCharStream jj_input_stream;
  public @Tainted Token token;
  public @Tainted Token jj_nt;
  private @Tainted int jj_ntk;
  private @Tainted int jj_gen;
  final private @Tainted int @Tainted [] jj_la1 = new @Tainted int @Tainted [6];
  static private @Tainted int @Tainted [] jj_la1_0;
  static private @Tainted int @Tainted [] jj_la1_1;
  static {
    jj_la1_0();
    jj_la1_1();
  }
  private static void jj_la1_0() {
    jj_la1_0 = new @Tainted int @Tainted [] {0x2800, 0x2800, 0x40000000, 0x1000, 0xffc000, 0xffc000,};
  }
  private static void jj_la1_1() {
    jj_la1_1 = new @Tainted int @Tainted [] {0x0, 0x0, 0x0, 0x0, 0x1, 0x1,};
  }

  public @Tainted Rcc(java.io.InputStream stream) {
    this(stream, null);
  }
  public @Tainted Rcc(java.io.InputStream stream, @Tainted String encoding) {
    try { jj_input_stream = new @Tainted SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new @Tainted RuntimeException(e); }
    token_source = new @Tainted RccTokenManager(jj_input_stream);
    token = new @Tainted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@Tainted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public void ReInit(@Tainted Rcc this, java.io.InputStream stream) {
    ReInit(stream, null);
  }
  public void ReInit(@Tainted Rcc this, java.io.InputStream stream, @Tainted String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new @Tainted RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new @Tainted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@Tainted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public @Tainted Rcc(java.io.Reader stream) {
    jj_input_stream = new @Tainted SimpleCharStream(stream, 1, 1);
    token_source = new @Tainted RccTokenManager(jj_input_stream);
    token = new @Tainted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@Tainted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public void ReInit(@Tainted Rcc this, java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new @Tainted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@Tainted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public @Tainted Rcc(@Tainted RccTokenManager tm) {
    token_source = tm;
    token = new @Tainted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@Tainted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public void ReInit(@Tainted Rcc this, @Tainted RccTokenManager tm) {
    token_source = tm;
    token = new @Tainted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@Tainted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  final private @Tainted Token jj_consume_token(@Tainted Rcc this, @Tainted int kind) throws ParseException {
    @Tainted
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }

  final public @Tainted Token getNextToken(@Tainted Rcc this) {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

  final public @Tainted Token getToken(@Tainted Rcc this, @Tainted int index) {
    @Tainted
    Token t = token;
    for (@Tainted int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  final private @Tainted int jj_ntk(@Tainted Rcc this) {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  private java.util.Vector<@Tainted int @Tainted []> jj_expentries = new java.util.Vector<@Tainted int @Tainted []>();
  private @Tainted int @Tainted [] jj_expentry;
  private @Tainted int jj_kind = -1;

  public @Tainted ParseException generateParseException(@Tainted Rcc this) {
    jj_expentries.removeAllElements();
    @Tainted
    boolean @Tainted [] la1tokens = new @Tainted boolean @Tainted [33];
    for (@Tainted int i = 0; i < 33; i++) {
      la1tokens[i] = false;
    }
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (@Tainted int i = 0; i < 6; i++) {
      if (jj_la1[i] == jj_gen) {
        for (@Tainted int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
        }
      }
    }
    for (@Tainted int i = 0; i < 33; i++) {
      if (la1tokens[i]) {
        jj_expentry = new @Tainted int @Tainted [1];
        jj_expentry[0] = i;
        jj_expentries.addElement(jj_expentry);
      }
    }
    @Tainted
    int @Tainted [] @Tainted [] exptokseq = new @Tainted int @Tainted [jj_expentries.size()] @Tainted [];
    for (@Tainted int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = jj_expentries.elementAt(i);
    }
    return new @Tainted ParseException(token, exptokseq, tokenImage);
  }

  final public void enable_tracing(@Tainted Rcc this) {
  }

  final public void disable_tracing(@Tainted Rcc this) {
  }

}