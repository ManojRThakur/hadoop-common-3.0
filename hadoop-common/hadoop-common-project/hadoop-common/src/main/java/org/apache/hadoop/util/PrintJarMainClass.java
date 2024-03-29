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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.jar.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A micro-application that prints the main class name out of a jar file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrintJarMainClass {
  
  /**
   * @param args
   */
  public static void main(@Tainted String @Tainted [] args) {
    try {
      @Tainted
      JarFile jar_file = new @Tainted JarFile(args[0]);
      if (jar_file != null) {
        @Tainted
        Manifest manifest = jar_file.getManifest();
        if (manifest != null) {
          @Tainted
          String value = manifest.getMainAttributes().getValue("Main-Class");
          if (value != null) {
            System.out.println(value.replaceAll("/", "."));
            return;
          }
        }
      }
    } catch (@Tainted Throwable e) {
      // ignore it
    }
    System.out.println("UNKNOWN");
    System.exit(1);
  }
  
}
