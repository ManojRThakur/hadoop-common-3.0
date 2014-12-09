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
package org.apache.hadoop.fs;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

/**
 * Exceptions based on standard posix/linux style exceptions for path related
 * errors. Returns an exception with the format "path: standard error string".
 * 
 * This exception corresponds to Error Input/ouput(EIO)
 */
public class PathIOException extends @Tainted IOException {
  static final @Tainted long serialVersionUID = 0L;
  private static final @Tainted String EIO = "Input/output error";
  // NOTE: this really should be a Path, but a Path is buggy and won't
  // return the exact string used to construct the path, and it mangles
  // uris with no authority
  private @Tainted String operation;
  private @Tainted String path;
  private @Tainted String targetPath;

  /**
   * Constructor a generic I/O error exception
   *  @param path for the exception
   */
  public @Tainted PathIOException(@Tainted String path) {
    this(path, EIO, null);
  }

  /**
   * Appends the text of a Throwable to the default error message
   * @param path for the exception
   * @param cause a throwable to extract the error message
   */
  public @Tainted PathIOException(@Tainted String path, @Tainted Throwable cause) {
    this(path, EIO, cause);
  }

  /**
   * Avoid using this method.  Use a subclass of PathIOException if
   * possible.
   * @param path for the exception
   * @param error custom string to use an the error text
   */
  public @Tainted PathIOException(@Tainted String path, @Tainted String error) {
    this(path, error, null);
  }

  protected @Tainted PathIOException(@Tainted String path, @Tainted String error, @Tainted Throwable cause) {
    super(error, cause);
    this.path = path;
  }

  /** Format:
   * cmd: {operation} `path' {to `target'}: error string
   */
  @Override
  public @Tainted String getMessage(@Tainted PathIOException this) {
    @Tainted
    StringBuilder message = new @Tainted StringBuilder();
    if (operation != null) {
      message.append(operation + " ");
    }
    message.append(formatPath(path));
    if (targetPath != null) {
      message.append(" to " + formatPath(targetPath));
    }
    message.append(": " + super.getMessage());
    if (getCause() != null) {
      message.append(": " + getCause().getMessage());
    }
    return message.toString();
  }

  /** @return Path that generated the exception */
  public @Tainted Path getPath(@Tainted PathIOException this)  { return new @Tainted Path(path); }

  /** @return Path if the operation involved copying or moving, else null */
  public @Tainted Path getTargetPath(@Tainted PathIOException this) {
    return (targetPath != null) ? new @Tainted Path(targetPath) : null;
  }    
  
  /**
   * Optional operation that will preface the path
   * @param operation a string
   */
  public void setOperation(@Tainted PathIOException this, @Tainted String operation) {
    this.operation = operation;
  }
  
  /**
   * Optional path if the exception involved two paths, ex. a copy operation
   * @param targetPath the of the operation
   */
  public void setTargetPath(@Tainted PathIOException this, @Tainted String targetPath) {
    this.targetPath = targetPath;
  }
  
  private @Tainted String formatPath(@Tainted PathIOException this, @Tainted String path) {
    return "`" + path + "'";
  }
}