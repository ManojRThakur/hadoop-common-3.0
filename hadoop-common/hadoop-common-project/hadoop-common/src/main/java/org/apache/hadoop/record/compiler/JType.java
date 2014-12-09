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
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * Abstract Base class for all types supported by Hadoop Record I/O.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
abstract public class JType {
  
  static @Tainted String toCamelCase(@Tainted String name) {
    @Tainted
    char firstChar = name.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + name.substring(1);
    }
    return name;
  }
  
  @Tainted
  JavaType javaType;
  @Tainted
  CppType cppType;
  @Tainted
  CType cType;
  
  abstract class JavaType {
    private @Tainted String name;
    private @Tainted String methodSuffix;
    private @Tainted String wrapper;
    private @Tainted String typeIDByteString; // points to TypeID.RIOType 
    
    @Tainted
    JavaType(@Tainted String javaname,
        @Tainted
        String suffix,
        @Tainted
        String wrapper, 
        @Tainted
        String typeIDByteString) { 
      this.name = javaname;
      this.methodSuffix = suffix;
      this.wrapper = wrapper;
      this.typeIDByteString = typeIDByteString;
    }

    void genDecl(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append("private "+name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append(Consts.RTI_VAR + ".addField(\"" + fname + "\", " +
          getTypeIDObjectString() + ");\n");
    }
    
    abstract @Tainted String getTypeIDObjectString(@Tainted JType.JavaType this);
    
    void genSetRTIFilter(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted Map<@Tainted String, @Tainted Integer> nestedStructMap) {
      // do nothing by default
      return;
    }

    /*void genRtiFieldCondition(CodeBuffer cb, String fname, int ct) {
      cb.append("if ((tInfo.fieldID.equals(\"" + fname + "\")) && (typeVal ==" +
          " org.apache.hadoop.record.meta." + getTypeIDByteString() + ")) {\n");
      cb.append("rtiFilterFields[i] = " + ct + ";\n");
      cb.append("}\n");
    }

    void genRtiNestedFieldCondition(CodeBuffer cb, String varName, int ct) {
      cb.append("if (" + varName + ".getElementTypeID().getTypeVal() == " +
          "org.apache.hadoop.record.meta." + getTypeIDByteString() + 
          ") {\n");
      cb.append("rtiFilterFields[i] = " + ct + ";\n");
      cb.append("}\n");  
    }*/

    void genConstructorParam(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append("final "+name+" "+fname);
    }
    
    void genGetSet(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append("public "+name+" get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("public void set"+toCamelCase(fname)+"(final "+name+" "+fname+") {\n");
      cb.append("this."+fname+"="+fname+";\n");
      cb.append("}\n");
    }
    
    @Tainted
    String getType(@Tainted JType.JavaType this) {
      return name;
    }
    
    @Tainted
    String getWrapperType(@Tainted JType.JavaType this) {
      return wrapper;
    }
    
    @Tainted
    String getMethodSuffix(@Tainted JType.JavaType this) {
      return methodSuffix;
    }
    
    @Tainted
    String getTypeIDByteString(@Tainted JType.JavaType this) {
      return typeIDByteString;
    }
    
    void genWriteMethod(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname, @Tainted String tag) {
      cb.append(Consts.RECORD_OUTPUT + ".write"+methodSuffix + 
          "("+fname+",\""+tag+"\");\n");
    }
    
    void genReadMethod(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname, @Tainted String tag, @Tainted boolean decl) {
      if (decl) {
        cb.append(name+" "+fname+";\n");
      }
      cb.append(fname+"=" + Consts.RECORD_INPUT + ".read" + 
          methodSuffix+"(\""+tag+"\");\n");
    }
    
    void genCompareTo(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname, @Tainted String other) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+" == "+other+")? 0 :(("+
          fname+"<"+other+")?-1:1);\n");
    }
    
    abstract void genCompareBytes(@Tainted JType.JavaType this, @Tainted CodeBuffer cb);
    
    abstract void genSlurpBytes(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String b, @Tainted String s, @Tainted String l);
    
    void genEquals(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname, @Tainted String peer) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+"=="+peer+");\n");
    }
    
    void genHashCode(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = (int)"+fname+";\n");
    }
    
    void genConstructorSet(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append("this."+fname+" = "+fname+";\n");
    }
    
    void genClone(@Tainted JType.JavaType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = this."+fname+";\n");
    }
  }
  
  abstract class CppType {
    private @Tainted String name;
    
    @Tainted
    CppType(@Tainted String cppname) {
      name = cppname;
    }
    
    void genDecl(@Tainted JType.CppType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append(name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(@Tainted JType.CppType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append("p->addField(new ::std::string(\"" + 
          fname + "\"), " + getTypeIDObjectString() + ");\n");
    }
    
    void genGetSet(@Tainted JType.CppType this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append("virtual "+name+" get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual void set"+toCamelCase(fname)+"("+name+" m_) {\n");
      cb.append(fname+"=m_;\n");
      cb.append("}\n");
    }
    
    abstract @Tainted String getTypeIDObjectString(@Tainted JType.CppType this);

    void genSetRTIFilter(@Tainted JType.CppType this, @Tainted CodeBuffer cb) {
      // do nothing by default
      return;
    }

    @Tainted
    String getType(@Tainted JType.CppType this) {
      return name;
    }
  }
  
  class CType {
    
  }
  
  abstract @Tainted String getSignature(@Tainted JType this);
  
  void setJavaType(@Tainted JType this, @Tainted JavaType jType) {
    this.javaType = jType;
  }
  
  @Tainted
  JavaType getJavaType(@Tainted JType this) {
    return javaType;
  }
  
  void setCppType(@Tainted JType this, @Tainted CppType cppType) {
    this.cppType = cppType;
  }
  
  @Tainted
  CppType getCppType(@Tainted JType this) {
    return cppType;
  }
  
  void setCType(@Tainted JType this, @Tainted CType cType) {
    this.cType = cType;
  }
  
  @Tainted
  CType getCType(@Tainted JType this) {
    return cType;
  }
}
