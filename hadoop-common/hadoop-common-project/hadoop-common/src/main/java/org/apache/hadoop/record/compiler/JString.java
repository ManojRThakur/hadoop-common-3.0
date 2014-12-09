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
public class JString extends @Tainted JCompType {
    
  class JavaString extends @Tainted JavaCompType {
    
    @Tainted
    JavaString() {
      super("String", "String", "String", "TypeID.RIOType.STRING");
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JString.JavaString this) {
      return "org.apache.hadoop.record.meta.TypeID.StringTypeID";
    }

    @Override
    void genSlurpBytes(@Tainted JString.JavaString this, @Tainted CodeBuffer cb, @Tainted String b, @Tainted String s, @Tainted String l) {
      cb.append("{\n");
      cb.append("int i = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
      cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
      cb.append(s+"+=(z+i); "+l+"-= (z+i);\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@Tainted JString.JavaString this, @Tainted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
      cb.append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
      cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
      cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
      cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
      cb.append("int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);\n");
      cb.append("if (r1 != 0) { return (r1<0)?-1:0; }\n");
      cb.append("s1+=i1; s2+=i2; l1-=i1; l1-=i2;\n");
      cb.append("}\n");
    }
    
    @Override
    void genClone(@Tainted JString.JavaString this, @Tainted CodeBuffer cb, @Tainted String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = this."+fname+";\n");
    }
  }

  class CppString extends @Tainted CppCompType {
    
    @Tainted
    CppString() {
      super("::std::string");
    }
    
    @Override
    @Tainted
    String getTypeIDObjectString(@Tainted JString.CppString this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_STRING)";
    }
  }
  
  /** Creates a new instance of JString */
  public @Tainted JString() {
    setJavaType(new @Tainted JavaString());
    setCppType(new @Tainted CppString());
    setCType(new @Tainted CCompType());
  }
    
  @Override
  @Tainted
  String getSignature(@Tainted JString this) {
    return "s";
  }
}
