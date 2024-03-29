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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JDouble extends @Tainted JType {
  
  class JavaDouble extends @Tainted JavaType {
    
    @Tainted
    JavaDouble() {
      super("double", "Double", "Double", "TypeID.RIOType.DOUBLE");
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JDouble.JavaDouble this) {
      return "org.apache.hadoop.record.meta.TypeID.DoubleTypeID";
    }

    @Override
    void genHashCode(@Tainted JDouble.JavaDouble this, @Tainted CodeBuffer cb, @Tainted String fname) {
      @Tainted
      String tmp = "Double.doubleToLongBits("+fname+")";
      cb.append(Consts.RIO_PREFIX + "ret = (int)("+tmp+"^("+tmp+">>>32));\n");
    }
    
    @Override
    void genSlurpBytes(@Tainted JDouble.JavaDouble this, @Tainted CodeBuffer cb, @Tainted String b, @Tainted String s, @Tainted String l) {
      cb.append("{\n");
      cb.append("if ("+l+"<8) {\n");
      cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append(s+"+=8; "+l+"-=8;\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@Tainted JDouble.JavaDouble this, @Tainted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("if (l1<8 || l2<8) {\n");
      cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append("double d1 = org.apache.hadoop.record.Utils.readDouble(b1, s1);\n");
      cb.append("double d2 = org.apache.hadoop.record.Utils.readDouble(b2, s2);\n");
      cb.append("if (d1 != d2) {\n");
      cb.append("return ((d1-d2) < 0) ? -1 : 0;\n");
      cb.append("}\n");
      cb.append("s1+=8; s2+=8; l1-=8; l2-=8;\n");
      cb.append("}\n");
    }
  }

  class CppDouble extends @Tainted CppType {
    
    @Tainted
    CppDouble() {
      super("double");
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JDouble.CppDouble this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_DOUBLE)";
    }
  }

  
  /** Creates a new instance of JDouble */
  public @Tainted JDouble() {
    setJavaType(new @Tainted JavaDouble());
    setCppType(new @Tainted CppDouble());
    setCType(new @Tainted CType());
  }
  
  @Override
  @Tainted
  String getSignature(@Tainted JDouble this) {
    return "d";
  }
}
