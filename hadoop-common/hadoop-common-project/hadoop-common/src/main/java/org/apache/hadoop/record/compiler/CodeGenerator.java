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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * CodeGenerator is a Factory and a base class for Hadoop Record I/O translators.
 * Different translators register creation methods with this factory.
 */
abstract class CodeGenerator {
  
  private static @Tainted HashMap<@Tainted String, @Tainted CodeGenerator> generators =
    new @Tainted HashMap<@Tainted String, @Tainted CodeGenerator>();
  
  static {
    register("c", new @Tainted CGenerator());
    register("c++", new @Tainted CppGenerator());
    register("java", new @Tainted JavaGenerator());
  }
  
  static void register(@Tainted String lang, @Tainted CodeGenerator gen) {
    generators.put(lang, gen);
  }
  
  static @Tainted CodeGenerator get(@Tainted String lang) {
    return generators.get(lang);
  }
  
  abstract void genCode(@Tainted CodeGenerator this, @Tainted String file,
                        @Tainted
                        ArrayList<@Tainted JFile> inclFiles,
                        @Tainted
                        ArrayList<@Tainted JRecord> records,
                        @Tainted
                        String destDir,
                        @Tainted
                        ArrayList<@Tainted String> options) throws IOException;
}
