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

package org.apache.hadoop.record.compiler;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JRecord extends @Tainted JCompType {
  
  class JavaRecord extends @Tainted JavaCompType {
    
    private @Tainted String fullName;
    private @Tainted String name;
    private @Tainted String module;
    private @Tainted ArrayList<@Tainted JField<@Tainted JavaType>> fields =
      new @Tainted ArrayList<@Tainted JField<@Tainted JavaType>>();
    
    @Tainted
    JavaRecord(@Tainted String name, @Tainted ArrayList<@Tainted JField<@Tainted JType>> flist) {
      super(name, "Record", name, "TypeID.RIOType.STRUCT");
      this.fullName = name;
      @Tainted
      int idx = name.lastIndexOf('.');
      this.name = name.substring(idx+1);
      this.module = name.substring(0, idx);
      for (@Tainted Iterator<@Tainted JField<@Tainted JType>> iter = flist.iterator(); iter.hasNext();) {
        @Tainted
        JField<@Tainted JType> f = iter.next();
        fields.add(new @Tainted JField<@Tainted JavaType>(f.getName(), f.getType().getJavaType()));
      }
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JRecord.JavaRecord this) {
      return "new org.apache.hadoop.record.meta.StructTypeID(" + 
      fullName + ".getTypeInfo())";
    }

    @Override
    void genSetRTIFilter(@Tainted JRecord.JavaRecord this, @Tainted CodeBuffer cb, @Tainted Map<@Tainted String, @Tainted Integer> nestedStructMap) {
      // ignore, if we'ev already set the type filter for this record
      if (!nestedStructMap.containsKey(fullName)) {
        // we set the RTI filter here
        cb.append(fullName + ".setTypeFilter(rti.getNestedStructTypeInfo(\""+
            name + "\"));\n");
        nestedStructMap.put(fullName, null);
      }
    }

    // for each typeInfo in the filter, we see if there's a similar one in the record. 
    // Since we store typeInfos in ArrayLists, thsi search is O(n squared). We do it faster
    // if we also store a map (of TypeInfo to index), but since setupRtiFields() is called
    // only once when deserializing, we're sticking with the former, as the code is easier.  
    void genSetupRtiFields(@Tainted JRecord.JavaRecord this, @Tainted CodeBuffer cb) {
      cb.append("private static void setupRtiFields()\n{\n");
      cb.append("if (null == " + Consts.RTI_FILTER + ") return;\n");
      cb.append("// we may already have done this\n");
      cb.append("if (null != " + Consts.RTI_FILTER_FIELDS + ") return;\n");
      cb.append("int " + Consts.RIO_PREFIX + "i, " + Consts.RIO_PREFIX + "j;\n");
      cb.append(Consts.RTI_FILTER_FIELDS + " = new int [" + 
          Consts.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().size()];\n");
      cb.append("for (" + Consts.RIO_PREFIX + "i=0; " + Consts.RIO_PREFIX + "i<"+
          Consts.RTI_FILTER_FIELDS + ".length; " + Consts.RIO_PREFIX + "i++) {\n");
      cb.append(Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + "i] = 0;\n");
      cb.append("}\n");
      cb.append("java.util.Iterator<org.apache.hadoop.record.meta." +
          "FieldTypeInfo> " + Consts.RIO_PREFIX + "itFilter = " + 
          Consts.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().iterator();\n");
      cb.append(Consts.RIO_PREFIX + "i=0;\n");
      cb.append("while (" + Consts.RIO_PREFIX + "itFilter.hasNext()) {\n");
      cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + 
          Consts.RIO_PREFIX + "tInfoFilter = " + 
          Consts.RIO_PREFIX + "itFilter.next();\n");
      cb.append("java.util.Iterator<org.apache.hadoop.record.meta." + 
          "FieldTypeInfo> " + Consts.RIO_PREFIX + "it = " + Consts.RTI_VAR + 
          ".getFieldTypeInfos().iterator();\n");
      cb.append(Consts.RIO_PREFIX + "j=1;\n");
      cb.append("while (" + Consts.RIO_PREFIX + "it.hasNext()) {\n");
      cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + 
          Consts.RIO_PREFIX + "tInfo = " + Consts.RIO_PREFIX + "it.next();\n");
      cb.append("if (" + Consts.RIO_PREFIX + "tInfo.equals(" +  
          Consts.RIO_PREFIX + "tInfoFilter)) {\n");
      cb.append(Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + "i] = " +
          Consts.RIO_PREFIX + "j;\n");
      cb.append("break;\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "j++;\n");
      cb.append("}\n");
      /*int ct = 0;
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        ct++;
        JField<JavaType> jf = i.next();
        JavaType type = jf.getType();
        String name = jf.getName();
        if (ct != 1) {
          cb.append("else ");
        }
        type.genRtiFieldCondition(cb, name, ct);
      }
      if (ct != 0) {
        cb.append("else {\n");
        cb.append("rtiFilterFields[i] = 0;\n");
        cb.append("}\n");
      }*/
      cb.append(Consts.RIO_PREFIX + "i++;\n");
      cb.append("}\n");
      cb.append("}\n");
    }

    @Override
    void genReadMethod(@Tainted JRecord.JavaRecord this, @Tainted CodeBuffer cb, @Tainted String fname, @Tainted String tag, @Tainted boolean decl) {
      if (decl) {
        cb.append(fullName+" "+fname+";\n");
      }
      cb.append(fname+"= new "+fullName+"();\n");
      cb.append(fname+".deserialize(" + Consts.RECORD_INPUT + ",\""+tag+"\");\n");
    }
    
    @Override
    void genWriteMethod(@Tainted JRecord.JavaRecord this, @Tainted CodeBuffer cb, @Tainted String fname, @Tainted String tag) {
      cb.append(fname+".serialize(" + Consts.RECORD_OUTPUT + ",\""+tag+"\");\n");
    }
    
    @Override
    void genSlurpBytes(@Tainted JRecord.JavaRecord this, @Tainted CodeBuffer cb, @Tainted String b, @Tainted String s, @Tainted String l) {
      cb.append("{\n");
      cb.append("int r = "+fullName+
                ".Comparator.slurpRaw("+b+","+s+","+l+");\n");
      cb.append(s+"+=r; "+l+"-=r;\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@Tainted JRecord.JavaRecord this, @Tainted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("int r1 = "+fullName+
                ".Comparator.compareRaw(b1,s1,l1,b2,s2,l2);\n");
      cb.append("if (r1 <= 0) { return r1; }\n");
      cb.append("s1+=r1; s2+=r1; l1-=r1; l2-=r1;\n");
      cb.append("}\n");
    }
    
    void genCode(@Tainted JRecord.JavaRecord this, @Tainted String destDir, @Tainted ArrayList<@Tainted String> options) throws IOException {
      @Tainted
      String pkg = module;
      @Tainted
      String pkgpath = pkg.replaceAll("\\.", "/");
      @Tainted
      File pkgdir = new @Tainted File(destDir, pkgpath);

      final @Tainted File jfile = new @Tainted File(pkgdir, name+".java");
      if (!pkgdir.exists()) {
        // create the pkg directory
        @Tainted
        boolean ret = pkgdir.mkdirs();
        if (!ret) {
          throw new @Tainted IOException("Cannnot create directory: "+pkgpath);
        }
      } else if (!pkgdir.isDirectory()) {
        // not a directory
        throw new @Tainted IOException(pkgpath+" is not a directory.");
      }

      @Tainted
      CodeBuffer cb = new @Tainted CodeBuffer();
      cb.append("// File generated by hadoop record compiler. Do not edit.\n");
      cb.append("package "+module+";\n\n");
      cb.append("public class "+name+
                " extends org.apache.hadoop.record.Record {\n");
      
      // type information declarations
      cb.append("private static final " + 
          "org.apache.hadoop.record.meta.RecordTypeInfo " + 
          Consts.RTI_VAR + ";\n");
      cb.append("private static " + 
          "org.apache.hadoop.record.meta.RecordTypeInfo " + 
          Consts.RTI_FILTER + ";\n");
      cb.append("private static int[] " + Consts.RTI_FILTER_FIELDS + ";\n");
      
      // static init for type information
      cb.append("static {\n");
      cb.append(Consts.RTI_VAR + " = " +
          "new org.apache.hadoop.record.meta.RecordTypeInfo(\"" +
          name + "\");\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genStaticTypeInfo(cb, name);
      }
      cb.append("}\n\n");

      // field definitions
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genDecl(cb, name);
      }

      // default constructor
      cb.append("public "+name+"() { }\n");
      
      // constructor
      cb.append("public "+name+"(\n");
      @Tainted
      int fIdx = 0;
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext(); fIdx++) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genConstructorParam(cb, name);
        cb.append((!i.hasNext())?"":",\n");
      }
      cb.append(") {\n");
      fIdx = 0;
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext(); fIdx++) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genConstructorSet(cb, name);
      }
      cb.append("}\n");

      // getter/setter for type info
      cb.append("public static org.apache.hadoop.record.meta.RecordTypeInfo"
              + " getTypeInfo() {\n");
      cb.append("return " + Consts.RTI_VAR + ";\n");
      cb.append("}\n");
      cb.append("public static void setTypeFilter("
          + "org.apache.hadoop.record.meta.RecordTypeInfo rti) {\n");
      cb.append("if (null == rti) return;\n");
      cb.append(Consts.RTI_FILTER + " = rti;\n");
      cb.append(Consts.RTI_FILTER_FIELDS + " = null;\n");
      // set RTIFilter for nested structs.
      // To prevent setting up the type filter for the same struct more than once, 
      // we use a hash map to keep track of what we've set. 
      @Tainted
      Map<@Tainted String, @Tainted Integer> nestedStructMap = new @Tainted HashMap<@Tainted String, @Tainted Integer>();
      for (@Tainted JField<@Tainted JavaType> jf : fields) {
        @Tainted
        JavaType type = jf.getType();
        type.genSetRTIFilter(cb, nestedStructMap);
      }
      cb.append("}\n");

      // setupRtiFields()
      genSetupRtiFields(cb);

      // getters/setters for member variables
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genGetSet(cb, name);
      }
      
      // serialize()
      cb.append("public void serialize("+ 
          "final org.apache.hadoop.record.RecordOutput " + 
          Consts.RECORD_OUTPUT + ", final String " + Consts.TAG + ")\n"+
                "throws java.io.IOException {\n");
      cb.append(Consts.RECORD_OUTPUT + ".startRecord(this," + Consts.TAG + ");\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genWriteMethod(cb, name, name);
      }
      cb.append(Consts.RECORD_OUTPUT + ".endRecord(this," + Consts.TAG+");\n");
      cb.append("}\n");

      // deserializeWithoutFilter()
      cb.append("private void deserializeWithoutFilter("+
                "final org.apache.hadoop.record.RecordInput " + 
                Consts.RECORD_INPUT + ", final String " + Consts.TAG + ")\n"+
                "throws java.io.IOException {\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(" + Consts.TAG + ");\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genReadMethod(cb, name, name, false);
      }
      cb.append(Consts.RECORD_INPUT + ".endRecord(" + Consts.TAG+");\n");
      cb.append("}\n");
      
      // deserialize()
      cb.append("public void deserialize(final " +
          "org.apache.hadoop.record.RecordInput " + 
          Consts.RECORD_INPUT + ", final String " + Consts.TAG + ")\n"+
          "throws java.io.IOException {\n");
      cb.append("if (null == " + Consts.RTI_FILTER + ") {\n");
      cb.append("deserializeWithoutFilter(" + Consts.RECORD_INPUT + ", " + 
          Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      cb.append("// if we're here, we need to read based on version info\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(" + Consts.TAG + ");\n");
      cb.append("setupRtiFields();\n");
      cb.append("for (int " + Consts.RIO_PREFIX + "i=0; " + Consts.RIO_PREFIX + 
          "i<" + Consts.RTI_FILTER + ".getFieldTypeInfos().size(); " + 
          Consts.RIO_PREFIX + "i++) {\n");
      @Tainted
      int ct = 0;
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        ct++;
        if (1 != ct) {
          cb.append("else ");
        }
        cb.append("if (" + ct + " == " + Consts.RTI_FILTER_FIELDS + "[" +
            Consts.RIO_PREFIX + "i]) {\n");
        type.genReadMethod(cb, name, name, false);
        cb.append("}\n");
      }
      if (0 != ct) {
        cb.append("else {\n");
        cb.append("java.util.ArrayList<"
                + "org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = "
                + "(java.util.ArrayList<"
                + "org.apache.hadoop.record.meta.FieldTypeInfo>)"
                + "(" + Consts.RTI_FILTER + ".getFieldTypeInfos());\n");
        cb.append("org.apache.hadoop.record.meta.Utils.skip(" + 
            Consts.RECORD_INPUT + ", " + "typeInfos.get(" + Consts.RIO_PREFIX + 
            "i).getFieldID(), typeInfos.get(" + 
            Consts.RIO_PREFIX + "i).getTypeID());\n");
        cb.append("}\n");
      }
      cb.append("}\n");
      cb.append(Consts.RECORD_INPUT + ".endRecord(" + Consts.TAG+");\n");
      cb.append("}\n");

      // compareTo()
      cb.append("public int compareTo (final Object " + Consts.RIO_PREFIX + 
          "peer_) throws ClassCastException {\n");
      cb.append("if (!(" + Consts.RIO_PREFIX + "peer_ instanceof "+name+")) {\n");
      cb.append("throw new ClassCastException(\"Comparing different types of records.\");\n");
      cb.append("}\n");
      cb.append(name+" " + Consts.RIO_PREFIX + "peer = ("+name+") " + 
          Consts.RIO_PREFIX + "peer_;\n");
      cb.append("int " + Consts.RIO_PREFIX + "ret = 0;\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genCompareTo(cb, name, Consts.RIO_PREFIX + "peer."+name);
        cb.append("if (" + Consts.RIO_PREFIX + "ret != 0) return " + 
            Consts.RIO_PREFIX + "ret;\n");
      }
      cb.append("return " + Consts.RIO_PREFIX + "ret;\n");
      cb.append("}\n");
      
      // equals()
      cb.append("public boolean equals(final Object " + Consts.RIO_PREFIX + 
          "peer_) {\n");
      cb.append("if (!(" + Consts.RIO_PREFIX + "peer_ instanceof "+name+")) {\n");
      cb.append("return false;\n");
      cb.append("}\n");
      cb.append("if (" + Consts.RIO_PREFIX + "peer_ == this) {\n");
      cb.append("return true;\n");
      cb.append("}\n");
      cb.append(name+" " + Consts.RIO_PREFIX + "peer = ("+name+") " + 
          Consts.RIO_PREFIX + "peer_;\n");
      cb.append("boolean " + Consts.RIO_PREFIX + "ret = false;\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genEquals(cb, name, Consts.RIO_PREFIX + "peer."+name);
        cb.append("if (!" + Consts.RIO_PREFIX + "ret) return " + 
            Consts.RIO_PREFIX + "ret;\n");
      }
      cb.append("return " + Consts.RIO_PREFIX + "ret;\n");
      cb.append("}\n");

      // clone()
      cb.append("public Object clone() throws CloneNotSupportedException {\n");
      cb.append(name+" " + Consts.RIO_PREFIX + "other = new "+name+"();\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genClone(cb, name);
      }
      cb.append("return " + Consts.RIO_PREFIX + "other;\n");
      cb.append("}\n");
      
      cb.append("public int hashCode() {\n");
      cb.append("int " + Consts.RIO_PREFIX + "result = 17;\n");
      cb.append("int " + Consts.RIO_PREFIX + "ret;\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genHashCode(cb, name);
        cb.append(Consts.RIO_PREFIX + "result = 37*" + Consts.RIO_PREFIX + 
            "result + " + Consts.RIO_PREFIX + "ret;\n");
      }
      cb.append("return " + Consts.RIO_PREFIX + "result;\n");
      cb.append("}\n");
      
      cb.append("public static String signature() {\n");
      cb.append("return \""+getSignature()+"\";\n");
      cb.append("}\n");
      
      cb.append("public static class Comparator extends"+
                " org.apache.hadoop.record.RecordComparator {\n");
      cb.append("public Comparator() {\n");
      cb.append("super("+name+".class);\n");
      cb.append("}\n");
      
      cb.append("static public int slurpRaw(byte[] b, int s, int l) {\n");
      cb.append("try {\n");
      cb.append("int os = s;\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genSlurpBytes(cb, "b","s","l");
      }
      cb.append("return (os - s);\n");
      cb.append("} catch(java.io.IOException e) {\n");
      cb.append("throw new RuntimeException(e);\n");
      cb.append("}\n");
      cb.append("}\n");
      
      cb.append("static public int compareRaw(byte[] b1, int s1, int l1,\n");
      cb.append("                             byte[] b2, int s2, int l2) {\n");
      cb.append("try {\n");
      cb.append("int os1 = s1;\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted JavaType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted JavaType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        JavaType type = jf.getType();
        type.genCompareBytes(cb);
      }
      cb.append("return (os1 - s1);\n");
      cb.append("} catch(java.io.IOException e) {\n");
      cb.append("throw new RuntimeException(e);\n");
      cb.append("}\n");
      cb.append("}\n");
      cb.append("public int compare(byte[] b1, int s1, int l1,\n");
      cb.append("                   byte[] b2, int s2, int l2) {\n");
      cb.append("int ret = compareRaw(b1,s1,l1,b2,s2,l2);\n");
      cb.append("return (ret == -1)? -1 : ((ret==0)? 1 : 0);");
      cb.append("}\n");
      cb.append("}\n\n");
      cb.append("static {\n");
      cb.append("org.apache.hadoop.record.RecordComparator.define("
                +name+".class, new Comparator());\n");
      cb.append("}\n");
      cb.append("}\n");

      @Tainted
      FileWriter jj = new @Tainted FileWriter(jfile);
      try {
        jj.write(cb.toString());
      } finally {
        jj.close();
      }
    }
  }
  
  class CppRecord extends @Tainted CppCompType {
    
    private @Tainted String fullName;
    private @Tainted String name;
    private @Tainted String module;
    private @Tainted ArrayList<@Tainted JField<@Tainted CppType>> fields = 
      new @Tainted ArrayList<@Tainted JField<@Tainted CppType>>();
    
    @Tainted
    CppRecord(@Tainted String name, @Tainted ArrayList<@Tainted JField<@Tainted JType>> flist) {
      super(name.replaceAll("\\.","::"));
      this.fullName = name.replaceAll("\\.", "::");
      @Tainted
      int idx = name.lastIndexOf('.');
      this.name = name.substring(idx+1);
      this.module = name.substring(0, idx).replaceAll("\\.", "::");
      for (@Tainted Iterator<@Tainted JField<@Tainted JType>> iter = flist.iterator(); iter.hasNext();) {
        @Tainted
        JField<@Tainted JType> f = iter.next();
        fields.add(new @Tainted JField<@Tainted CppType>(f.getName(), f.getType().getCppType()));
      }
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JRecord.CppRecord this) {
      return "new ::hadoop::StructTypeID(" + 
      fullName + "::getTypeInfo().getFieldTypeInfos())";
    }

    @Tainted
    String genDecl(@Tainted JRecord.CppRecord this, @Tainted String fname) {
      return "  "+name+" "+fname+";\n";
    }
    
    @Override
    void genSetRTIFilter(@Tainted JRecord.CppRecord this, @Tainted CodeBuffer cb) {
      // we set the RTI filter here
      cb.append(fullName + "::setTypeFilter(rti.getNestedStructTypeInfo(\""+
          name + "\"));\n");
    }

    void genSetupRTIFields(@Tainted JRecord.CppRecord this, @Tainted CodeBuffer cb) {
      cb.append("void " + fullName + "::setupRtiFields() {\n");
      cb.append("if (NULL == p" + Consts.RTI_FILTER + ") return;\n");
      cb.append("if (NULL != p" + Consts.RTI_FILTER_FIELDS + ") return;\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + " = new int[p" + 
          Consts.RTI_FILTER + "->getFieldTypeInfos().size()];\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "i=0; " + 
          Consts.RIO_PREFIX + "i<p" + Consts.RTI_FILTER + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "i++) {\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + 
          "i] = 0;\n");
      cb.append("}\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "i=0; " + 
          Consts.RIO_PREFIX + "i<p" + Consts.RTI_FILTER + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "i++) {\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "j=0; " + 
          Consts.RIO_PREFIX + "j<p" + Consts.RTI_VAR + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "j++) {\n");
      cb.append("if (*(p" + Consts.RTI_FILTER + "->getFieldTypeInfos()[" + 
          Consts.RIO_PREFIX + "i]) == *(p" + Consts.RTI_VAR + 
          "->getFieldTypeInfos()[" + Consts.RIO_PREFIX + "j])) {\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + 
          "i] = " + Consts.RIO_PREFIX + "j+1;\n");
      cb.append("break;\n");
      cb.append("}\n");
      cb.append("}\n");
      cb.append("}\n");
      cb.append("}\n");
    }
    
    void genCode(@Tainted JRecord.CppRecord this, @Tainted FileWriter hh, @Tainted FileWriter cc, @Tainted ArrayList<@Tainted String> options)
      throws IOException {
      @Tainted
      CodeBuffer hb = new @Tainted CodeBuffer();
      
      @Tainted
      String @Tainted [] ns = module.split("::");
      for (@Tainted int i = 0; i < ns.length; i++) {
        hb.append("namespace "+ns[i]+" {\n");
      }
      
      hb.append("class "+name+" : public ::hadoop::Record {\n");
      hb.append("private:\n");
      
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        CppType type = jf.getType();
        type.genDecl(hb, name);
      }
      
      // type info vars
      hb.append("static ::hadoop::RecordTypeInfo* p" + Consts.RTI_VAR + ";\n");
      hb.append("static ::hadoop::RecordTypeInfo* p" + Consts.RTI_FILTER + ";\n");
      hb.append("static int* p" + Consts.RTI_FILTER_FIELDS + ";\n");
      hb.append("static ::hadoop::RecordTypeInfo* setupTypeInfo();\n");
      hb.append("static void setupRtiFields();\n");
      hb.append("virtual void deserializeWithoutFilter(::hadoop::IArchive& " + 
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ");\n");
      hb.append("public:\n");
      hb.append("static const ::hadoop::RecordTypeInfo& getTypeInfo() " +
          "{return *p" + Consts.RTI_VAR + ";}\n");
      hb.append("static void setTypeFilter(const ::hadoop::RecordTypeInfo& rti);\n");
      hb.append("static void setTypeFilter(const ::hadoop::RecordTypeInfo* prti);\n");
      hb.append("virtual void serialize(::hadoop::OArchive& " + 
          Consts.RECORD_OUTPUT + ", const char* " + Consts.TAG + ") const;\n");
      hb.append("virtual void deserialize(::hadoop::IArchive& " + 
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ");\n");
      hb.append("virtual const ::std::string& type() const;\n");
      hb.append("virtual const ::std::string& signature() const;\n");
      hb.append("virtual bool operator<(const "+name+"& peer_) const;\n");
      hb.append("virtual bool operator==(const "+name+"& peer_) const;\n");
      hb.append("virtual ~"+name+"() {};\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        CppType type = jf.getType();
        type.genGetSet(hb, name);
      }
      hb.append("}; // end record "+name+"\n");
      for (@Tainted int i=ns.length-1; i>=0; i--) {
        hb.append("} // end namespace "+ns[i]+"\n");
      }
      
      hh.write(hb.toString());
      
      @Tainted
      CodeBuffer cb = new @Tainted CodeBuffer();

      // initialize type info vars
      cb.append("::hadoop::RecordTypeInfo* " + fullName + "::p" + 
          Consts.RTI_VAR + " = " + fullName + "::setupTypeInfo();\n");
      cb.append("::hadoop::RecordTypeInfo* " + fullName + "::p" + 
          Consts.RTI_FILTER + " = NULL;\n");
      cb.append("int* " + fullName + "::p" + 
          Consts.RTI_FILTER_FIELDS + " = NULL;\n\n");

      // setupTypeInfo()
      cb.append("::hadoop::RecordTypeInfo* "+fullName+"::setupTypeInfo() {\n");
      cb.append("::hadoop::RecordTypeInfo* p = new ::hadoop::RecordTypeInfo(\"" + 
          name + "\");\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        CppType type = jf.getType();
        type.genStaticTypeInfo(cb, name);
      }
      cb.append("return p;\n");
      cb.append("}\n");

      // setTypeFilter()
      cb.append("void "+fullName+"::setTypeFilter(const " +
          "::hadoop::RecordTypeInfo& rti) {\n");
      cb.append("if (NULL != p" + Consts.RTI_FILTER + ") {\n");
      cb.append("delete p" + Consts.RTI_FILTER + ";\n");
      cb.append("}\n");
      cb.append("p" + Consts.RTI_FILTER + " = new ::hadoop::RecordTypeInfo(rti);\n");
      cb.append("if (NULL != p" + Consts.RTI_FILTER_FIELDS + ") {\n");
      cb.append("delete p" + Consts.RTI_FILTER_FIELDS + ";\n");
      cb.append("}\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + " = NULL;\n");
      // set RTIFilter for nested structs. We may end up with multiple lines that 
      // do the same thing, if the same struct is nested in more than one field, 
      // but that's OK. 
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        CppType type = jf.getType();
        type.genSetRTIFilter(cb);
      }
      cb.append("}\n");
      
      // setTypeFilter()
      cb.append("void "+fullName+"::setTypeFilter(const " +
          "::hadoop::RecordTypeInfo* prti) {\n");
      cb.append("if (NULL != prti) {\n");
      cb.append("setTypeFilter(*prti);\n");
      cb.append("}\n");
      cb.append("}\n");

      // setupRtiFields()
      genSetupRTIFields(cb);

      // serialize()
      cb.append("void "+fullName+"::serialize(::hadoop::OArchive& " + 
          Consts.RECORD_OUTPUT + ", const char* " + Consts.TAG + ") const {\n");
      cb.append(Consts.RECORD_OUTPUT + ".startRecord(*this," + 
          Consts.TAG + ");\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        CppType type = jf.getType();
        if (type instanceof @Tainted JBuffer.@Tainted CppBuffer) {
          cb.append(Consts.RECORD_OUTPUT + ".serialize("+name+","+name+
              ".length(),\""+name+"\");\n");
        } else {
          cb.append(Consts.RECORD_OUTPUT + ".serialize("+name+",\""+
              name+"\");\n");
        }
      }
      cb.append(Consts.RECORD_OUTPUT + ".endRecord(*this," + Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      
      // deserializeWithoutFilter()
      cb.append("void "+fullName+"::deserializeWithoutFilter(::hadoop::IArchive& " +
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ") {\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(*this," + 
          Consts.TAG + ");\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        CppType type = jf.getType();
        if (type instanceof @Tainted JBuffer.@Tainted CppBuffer) {
          cb.append("{\nsize_t len=0; " + Consts.RECORD_INPUT + ".deserialize("+
              name+",len,\""+name+"\");\n}\n");
        } else {
          cb.append(Consts.RECORD_INPUT + ".deserialize("+name+",\""+
              name+"\");\n");
        }
      }
      cb.append(Consts.RECORD_INPUT + ".endRecord(*this," + Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      
      // deserialize()
      cb.append("void "+fullName+"::deserialize(::hadoop::IArchive& " +
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ") {\n");
      cb.append("if (NULL == p" + Consts.RTI_FILTER + ") {\n");
      cb.append("deserializeWithoutFilter(" + Consts.RECORD_INPUT + ", " + 
          Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      cb.append("// if we're here, we need to read based on version info\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(*this," + 
          Consts.TAG + ");\n");
      cb.append("setupRtiFields();\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "i=0; " + 
          Consts.RIO_PREFIX + "i<p" + Consts.RTI_FILTER + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "i++) {\n");
      @Tainted
      int ct = 0;
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        @Tainted
        CppType type = jf.getType();
        ct++;
        if (1 != ct) {
          cb.append("else ");
        }
        cb.append("if (" + ct + " == p" + Consts.RTI_FILTER_FIELDS + "[" +
            Consts.RIO_PREFIX + "i]) {\n");
        if (type instanceof @Tainted JBuffer.@Tainted CppBuffer) {
          cb.append("{\nsize_t len=0; " + Consts.RECORD_INPUT + ".deserialize("+
              name+",len,\""+name+"\");\n}\n");
        } else {
          cb.append(Consts.RECORD_INPUT + ".deserialize("+name+",\""+
              name+"\");\n");
        }
        cb.append("}\n");
      }
      if (0 != ct) {
        cb.append("else {\n");
        cb.append("const std::vector< ::hadoop::FieldTypeInfo* >& typeInfos = p" + 
            Consts.RTI_FILTER + "->getFieldTypeInfos();\n");
        cb.append("::hadoop::Utils::skip(" + Consts.RECORD_INPUT + 
            ", typeInfos[" + Consts.RIO_PREFIX + "i]->getFieldID()->c_str()" + 
            ", *(typeInfos[" + Consts.RIO_PREFIX + "i]->getTypeID()));\n");
        cb.append("}\n");
      }
      cb.append("}\n");
      cb.append(Consts.RECORD_INPUT + ".endRecord(*this, " + Consts.TAG+");\n");
      cb.append("}\n");

      // operator <
      cb.append("bool "+fullName+"::operator< (const "+fullName+"& peer_) const {\n");
      cb.append("return (1\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        cb.append("&& ("+name+" < peer_."+name+")\n");
      }
      cb.append(");\n");
      cb.append("}\n");
      
      cb.append("bool "+fullName+"::operator== (const "+fullName+"& peer_) const {\n");
      cb.append("return (1\n");
      for (@Tainted Iterator<@Tainted JField<@Tainted CppType>> i = fields.iterator(); i.hasNext();) {
        @Tainted
        JField<@Tainted CppType> jf = i.next();
        @Tainted
        String name = jf.getName();
        cb.append("&& ("+name+" == peer_."+name+")\n");
      }
      cb.append(");\n");
      cb.append("}\n");
      
      cb.append("const ::std::string&"+fullName+"::type() const {\n");
      cb.append("static const ::std::string type_(\""+name+"\");\n");
      cb.append("return type_;\n");
      cb.append("}\n");
      
      cb.append("const ::std::string&"+fullName+"::signature() const {\n");
      cb.append("static const ::std::string sig_(\""+getSignature()+"\");\n");
      cb.append("return sig_;\n");
      cb.append("}\n");
      
      cc.write(cb.toString());
    }
  }
  
  class CRecord extends @Tainted CCompType {
    
  }
  
  private @Tainted String signature;
  
  /**
   * Creates a new instance of JRecord
   */
  public @Tainted JRecord(@Tainted String name, @Tainted ArrayList<@Tainted JField<@Tainted JType>> flist) {
    setJavaType(new @Tainted JavaRecord(name, flist));
    setCppType(new @Tainted CppRecord(name, flist));
    setCType(new @Tainted CRecord());
    // precompute signature
    @Tainted
    int idx = name.lastIndexOf('.');
    @Tainted
    String recName = name.substring(idx+1);
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder();
    sb.append("L").append(recName).append("(");
    for (@Tainted Iterator<@Tainted JField<@Tainted JType>> i = flist.iterator(); i.hasNext();) {
      @Tainted
      String s = i.next().getType().getSignature();
      sb.append(s);
    }
    sb.append(")");
    signature = sb.toString();
  }
  
  @Override
  @Tainted
  String getSignature(@Tainted JRecord this) {
    return signature;
  }
  
  void genCppCode(@Tainted JRecord this, @Tainted FileWriter hh, @Tainted FileWriter cc, @Tainted ArrayList<@Tainted String> options)
    throws IOException {
    ((@Tainted CppRecord)getCppType()).genCode(hh, cc, options);
  }
  
  void genJavaCode(@Tainted JRecord this, @Tainted String destDir, @Tainted ArrayList<@Tainted String> options)
    throws IOException {
    ((@Tainted JavaRecord)getJavaType()).genCode(destDir, options);
  }
}
