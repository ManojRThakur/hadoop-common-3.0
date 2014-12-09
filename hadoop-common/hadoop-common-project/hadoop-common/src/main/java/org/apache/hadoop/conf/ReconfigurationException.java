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

package org.apache.hadoop.conf;
import org.checkerframework.checker.tainting.qual.Tainted;


/**
 * Exception indicating that configuration property cannot be changed
 * at run time.
 */
public class ReconfigurationException extends @Tainted Exception {

  private static final @Tainted long serialVersionUID = 1L;

  private @Tainted String property;
  private @Tainted String newVal;
  private @Tainted String oldVal;

  /**
   * Construct the exception message.
   */
  private static @Tainted String constructMessage(@Tainted String property, 
                                         @Tainted
                                         String newVal, @Tainted String oldVal) {
    @Tainted
    String message = "Could not change property " + property;
    if (oldVal != null) {
      message += " from \'" + oldVal;
    }
    if (newVal != null) {
      message += "\' to \'" + newVal + "\'";
    }
    return message;
  }

  
  /**
   * Create a new instance of {@link ReconfigurationException}.
   */
  public @Tainted ReconfigurationException() {
    super("Could not change configuration.");
    this.property = null;
    this.newVal = null;
    this.oldVal = null;
  }

  /**
   * Create a new instance of {@link ReconfigurationException}.
   */
  public @Tainted ReconfigurationException(@Tainted String property, 
                                  @Tainted
                                  String newVal, @Tainted String oldVal,
                                  @Tainted
                                  Throwable cause) {
    super(constructMessage(property, newVal, oldVal), cause);
    this.property = property;
    this.newVal = newVal;
    this.oldVal = oldVal;
  }

  /**
   * Create a new instance of {@link ReconfigurationException}.
   */
  public @Tainted ReconfigurationException(@Tainted String property, 
                                  @Tainted
                                  String newVal, @Tainted String oldVal) {
    super(constructMessage(property, newVal, oldVal));
    this.property = property;
    this.newVal = newVal;
    this.oldVal = oldVal;
  }

  /**
   * Get property that cannot be changed.
   */
  public @Tainted String getProperty(@Tainted ReconfigurationException this) {
    return property;
  }

  /**
   * Get value to which property was supposed to be changed.
   */
  public @Tainted String getNewValue(@Tainted ReconfigurationException this) {
    return newVal;
  }

  /**
   * Get old value of property that cannot be changed.
   */
  public @Tainted String getOldValue(@Tainted ReconfigurationException this) {
    return oldVal;
  }

}
