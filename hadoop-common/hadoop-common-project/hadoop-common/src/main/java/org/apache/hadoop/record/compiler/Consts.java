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
 * const definitions for Record I/O compiler
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Consts {
  
  /** Cannot create a new instance */
  private @Tainted Consts() {
  }
  
  // prefix to use for variables in generated classes
  public static final @Tainted String RIO_PREFIX = "_rio_";
  // other vars used in generated classes
  public static final @Tainted String RTI_VAR = RIO_PREFIX + "recTypeInfo";
  public static final @Tainted String RTI_FILTER = RIO_PREFIX + "rtiFilter";
  public static final @Tainted String RTI_FILTER_FIELDS = RIO_PREFIX + "rtiFilterFields";
  public static final @Tainted String RECORD_OUTPUT = RIO_PREFIX + "a";
  public static final @Tainted String RECORD_INPUT = RIO_PREFIX + "a";
  public static final @Tainted String TAG = RIO_PREFIX + "tag";
  
}
