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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;

/**
 * Class responsible for pumping the streams of the subprocess
 * out to log4j. stderr is pumped to WARN level and stdout is
 * pumped to INFO level
 */
class StreamPumper {
  enum StreamType {

@Tainted  STDOUT,  @Tainted  STDERR;
  }

  private final @Tainted Log log;
  
  final @Tainted Thread thread;
  final @Tainted String logPrefix;
  final StreamPumper.@Tainted StreamType type;
  private final @Tainted InputStream stream;
  private @Tainted boolean started = false;
  
  @Tainted
  StreamPumper(final @Tainted Log log, final @Tainted String logPrefix,
      final @Tainted InputStream stream, final @Tainted StreamType type) {
    this.log = log;
    this.logPrefix = logPrefix;
    this.stream = stream;
    this.type = type;
    
    thread = new @Tainted Thread(new @Tainted Runnable() {
      @Override
      public void run() {
        try {
          pump();
        } catch (Throwable t) {
          ShellCommandFencer.LOG.warn(logPrefix +
              ": Unable to pump output from " + type,
              t);
        }
      }
    }, logPrefix + ": StreamPumper for " + type);
    thread.setDaemon(true);
  }
  
  void join(@Tainted StreamPumper this) throws InterruptedException {
    assert started;
    thread.join();
  }

  void start(@Tainted StreamPumper this) {
    assert !started;
    thread.start();
    started = true;
  }

  protected void pump(@Tainted StreamPumper this) throws IOException {
    @Tainted
    InputStreamReader inputStreamReader = new @Tainted InputStreamReader(stream);
    @Tainted
    BufferedReader br = new @Tainted BufferedReader(inputStreamReader);
    @Tainted
    String line = null;
    while ((line = br.readLine()) != null) {
      if (type == StreamType.STDOUT) {
        log.info(logPrefix + ": " + line);
      } else {
        log.warn(logPrefix + ": " + line);          
      }
    }
  }
}
