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
import java.net.URLStreamHandlerFactory;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory for URL stream handlers.
 * 
 * There is only one handler whose job is to create UrlConnections. A
 * FsUrlConnection relies on FileSystem to choose the appropriate FS
 * implementation.
 * 
 * Before returning our handler, we make sure that FileSystem knows an
 * implementation for the requested scheme/protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsUrlStreamHandlerFactory implements
    @Tainted
    URLStreamHandlerFactory {

  // The configuration holds supported FS implementation class names.
  private @Tainted Configuration conf;

  // This map stores whether a protocol is know or not by FileSystem
  private @Tainted Map<@Tainted String, @Tainted Boolean> protocols = new @Tainted HashMap<@Tainted String, @Tainted Boolean>();

  // The URL Stream handler
  private java.net.URLStreamHandler handler;

  public @Tainted FsUrlStreamHandlerFactory() {
    this(new @Tainted Configuration());
  }

  public @Tainted FsUrlStreamHandlerFactory(@Tainted Configuration conf) {
    this.conf = new @Tainted Configuration(conf);
    // force init of FileSystem code to avoid HADOOP-9041
    try {
      FileSystem.getFileSystemClass("file", conf);
    } catch (@Tainted IOException io) {
      throw new @Tainted RuntimeException(io);
    }
    this.handler = new @Tainted FsUrlStreamHandler(this.conf);
  }

  @Override
  public java.net.URLStreamHandler createURLStreamHandler(@Tainted FsUrlStreamHandlerFactory this, @Tainted String protocol) {
    if (!protocols.containsKey(protocol)) {
      @Tainted
      boolean known = true;
      try {
        FileSystem.getFileSystemClass(protocol, conf);
      }
      catch (@Tainted IOException ex) {
        known = false;
      }
      protocols.put(protocol, known);
    }
    if (protocols.get(protocol)) {
      return handler;
    } else {
      // FileSystem does not know the protocol, let the VM handle this
      return null;
    }
  }

}
