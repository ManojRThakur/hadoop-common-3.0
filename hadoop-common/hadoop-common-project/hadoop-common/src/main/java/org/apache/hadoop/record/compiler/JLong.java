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
 * Code generator for "long" type
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JLong extends @Tainted JType {
  
  class JavaLong extends @Tainted JavaType {
    
    @Tainted
    JavaLong() {
      super("long", "Long", "Long", "TypeID.RIOType.LONG");
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JLong.JavaLong this) {
      return "org.apache.hadoop.record.meta.TypeID.LongTypeID";
    }

    @Override
    void genHashCode(@Tainted JLong.JavaLong this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = (int) ("+fname+"^("+
          fname+">>>32));\n");
    }
    
    @Override
    void genSlurpBytes(@Tainted JLong.JavaLong this, @Tainted CodeBuffer cb, @Tainted String b, @Tainted String s, @Tainted String l) {
      cb.append("{\n");
      cb.append("long i = org.apache.hadoop.record.Utils.readVLong("+b+", "+s+");\n");
      cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
      cb.append(s+"+=z; "+l+"-=z;\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@Tainted JLong.JavaLong this, @Tainted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);\n");
      cb.append("long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);\n");
      cb.append("if (i1 != i2) {\n");
      cb.append("return ((i1-i2) < 0) ? -1 : 0;\n");
      cb.append("}\n");
      cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
      cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
      cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
      cb.append("}\n");
    }
  }

  class CppLong extends @Tainted CppType {
    
    @Tainted
    CppLong() {
      super("int64_t");
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JLong.CppLong this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_LONG)";
    }
  }

  /** Creates a new instance of JLong */
  public @Tainted JLong() {
    setJavaType(new @Tainted JavaLong());
    setCppType(new @Tainted CppLong());
    setCType(new @Tainted CType());
  }
  
  @Override
  @Tainted
  String getSignature(@Tainted JLong this) {
    return "l";
  }
}
