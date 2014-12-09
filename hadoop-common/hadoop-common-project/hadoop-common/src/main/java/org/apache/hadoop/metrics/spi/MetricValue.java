/*
 * MetricValue.java
 *
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

package org.apache.hadoop.metrics.spi;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A Number that is either an absolute or an incremental amount.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricValue {
    
  public static final @Tainted boolean ABSOLUTE = false;
  public static final @Tainted boolean INCREMENT = true;
    
  private @Tainted boolean isIncrement;
  private @Tainted Number number;
    
  /** Creates a new instance of MetricValue */
  public @Tainted MetricValue(@Tainted Number number, @Tainted boolean isIncrement) {
    this.number = number;
    this.isIncrement = isIncrement;
  }

  public @Tainted boolean isIncrement(@Tainted MetricValue this) {
    return isIncrement;
  }
    
  public @Tainted boolean isAbsolute(@Tainted MetricValue this) {
    return !isIncrement;
  }

  public @Tainted Number getNumber(@Tainted MetricValue this) {
    return number;
  }
    
}
