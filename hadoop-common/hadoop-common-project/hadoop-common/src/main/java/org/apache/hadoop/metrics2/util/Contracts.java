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

package org.apache.hadoop.metrics2.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Additional helpers (besides guava Preconditions) for programming by contract
 */
@InterfaceAudience.Private
public class Contracts {

  private @Tainted Contracts() {}

  /**
   * Check an argument for false conditions
   * @param <T> type of the argument
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T checkArg(@Tainted T arg, @Tainted boolean expression, @Tainted Object msg) {
    if (!expression) {
      throw new @Tainted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @Tainted int checkArg(@Tainted int arg, @Tainted boolean expression, @Tainted Object msg) {
    if (!expression) {
      throw new @Tainted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @Tainted long checkArg(@Tainted long arg, @Tainted boolean expression, @Tainted Object msg) {
    if (!expression) {
      throw new @Tainted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @Tainted float checkArg(@Tainted float arg, @Tainted boolean expression, @Tainted Object msg) {
    if (!expression) {
      throw new @Tainted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @Tainted double checkArg(@Tainted double arg, @Tainted boolean expression, @Tainted Object msg) {
    if (!expression) {
      throw new @Tainted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }
}
