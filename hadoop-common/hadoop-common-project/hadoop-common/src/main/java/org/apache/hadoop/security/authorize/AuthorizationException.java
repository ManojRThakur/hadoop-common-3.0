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
package org.apache.hadoop.security.authorize;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.AccessControlException;

/**
 * An exception class for authorization-related issues.
 * 
 * This class <em>does not</em> provide the stack trace for security purposes.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class AuthorizationException extends @Tainted AccessControlException {
  private static final @Tainted long serialVersionUID = 1L;

  public @Tainted AuthorizationException() {
    super();
  }

  public @Tainted AuthorizationException(@Tainted String message) {
    super(message);
  }
  
  /**
   * Constructs a new exception with the specified cause and a detail
   * message of <tt>(cause==null ? null : cause.toString())</tt> (which
   * typically contains the class and detail message of <tt>cause</tt>).
   * @param  cause the cause (which is saved for later retrieval by the
   *         {@link #getCause()} method).  (A <tt>null</tt> value is
   *         permitted, and indicates that the cause is nonexistent or
   *         unknown.)
   */
  public @Tainted AuthorizationException(@Tainted Throwable cause) {
    super(cause);
  }
  
  private static @Tainted StackTraceElement @Tainted [] stackTrace = new @Tainted StackTraceElement @Tainted [0];
  @Override
  public @Tainted StackTraceElement @Tainted [] getStackTrace(@Tainted AuthorizationException this) {
    // Do not provide the stack-trace
    return stackTrace;
  }

  @Override
  public void printStackTrace(@Tainted AuthorizationException this) {
    // Do not provide the stack-trace
  }

  @Override
  public void printStackTrace(@Tainted AuthorizationException this, @Tainted PrintStream s) {
    // Do not provide the stack-trace
  }

  @Override
  public void printStackTrace(@Tainted AuthorizationException this, @Tainted PrintWriter s) {
    // Do not provide the stack-trace
  }
  
}
