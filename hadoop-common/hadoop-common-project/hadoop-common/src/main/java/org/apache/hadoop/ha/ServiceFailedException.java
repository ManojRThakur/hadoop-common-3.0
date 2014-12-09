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
package org.apache.hadoop.ha;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * Exception thrown to indicate that an operation performed
 * to modify the state of a service or application failed.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServiceFailedException extends @Tainted IOException {
  private static final @Tainted long serialVersionUID = 1L;

  public @Tainted ServiceFailedException(final @Tainted String message) {
    super(message);
  }
  
  public @Tainted ServiceFailedException(@Tainted String message, @Tainted Throwable cause) {
    super(message, cause);
  }
}