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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

@InterfaceAudience.Private
public class HAServiceStatus {
  private @Tainted HAServiceState state;
  private @Tainted boolean readyToBecomeActive;
  private @Tainted String notReadyReason;
  
  public @Tainted HAServiceStatus(@Tainted HAServiceState state) {
    this.state = state;
  }

  public @Tainted HAServiceState getState(@Tainted HAServiceStatus this) {
    return state;
  }

  public @Tainted HAServiceStatus setReadyToBecomeActive(@Tainted HAServiceStatus this) {
    this.readyToBecomeActive = true;
    this.notReadyReason = null;
    return this;
  }
  
  public @Tainted HAServiceStatus setNotReadyToBecomeActive(@Tainted HAServiceStatus this, @Tainted String reason) {
    this.readyToBecomeActive = false;
    this.notReadyReason = reason;
    return this;
  }

  public @Tainted boolean isReadyToBecomeActive(@Tainted HAServiceStatus this) {
    return readyToBecomeActive;
  }

  public @Tainted String getNotReadyReason(@Tainted HAServiceStatus this) {
    return notReadyReason;
  }
}