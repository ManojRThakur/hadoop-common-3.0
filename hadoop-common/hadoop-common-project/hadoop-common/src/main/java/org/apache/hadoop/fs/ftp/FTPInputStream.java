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
package org.apache.hadoop.fs.ftp;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FTPInputStream extends @Tainted FSInputStream {

  @Tainted
  InputStream wrappedStream;
  @Tainted
  FTPClient client;
  FileSystem.@Tainted Statistics stats;
  @Tainted
  boolean closed;
  @Tainted
  long pos;

  public @Tainted FTPInputStream(@Tainted InputStream stream, @Tainted FTPClient client,
      FileSystem.@Tainted Statistics stats) {
    if (stream == null) {
      throw new @Tainted IllegalArgumentException("Null InputStream");
    }
    if (client == null || !client.isConnected()) {
      throw new @Tainted IllegalArgumentException("FTP client null or not connected");
    }
    this.wrappedStream = stream;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }

  @Override
  public @Tainted long getPos(@Tainted FTPInputStream this) throws IOException {
    return pos;
  }

  // We don't support seek.
  @Override
  public void seek(@Tainted FTPInputStream this, @Tainted long pos) throws IOException {
    throw new @Tainted IOException("Seek not supported");
  }

  @Override
  public @Tainted boolean seekToNewSource(@Tainted FTPInputStream this, @Tainted long targetPos) throws IOException {
    throw new @Tainted IOException("Seek not supported");
  }

  @Override
  public synchronized @Tainted int read(@Tainted FTPInputStream this) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }

    @Tainted
    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public synchronized @Tainted int read(@Tainted FTPInputStream this, @Tainted byte buf @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }

    @Tainted
    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null && result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  @Override
  public synchronized void close(@Tainted FTPInputStream this) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }
    super.close();
    closed = true;
    if (!client.isConnected()) {
      throw new @Tainted FTPException("Client not connected");
    }

    @Tainted
    boolean cmdCompleted = client.completePendingCommand();
    client.logout();
    client.disconnect();
    if (!cmdCompleted) {
      throw new @Tainted FTPException("Could not complete transfer, Reply Code - "
          + client.getReplyCode());
    }
  }

  // Not supported.

  @Override
  public @Tainted boolean markSupported(@Tainted FTPInputStream this) {
    return false;
  }

  @Override
  public void mark(@Tainted FTPInputStream this, @Tainted int readLimit) {
    // Do nothing
  }

  @Override
  public void reset(@Tainted FTPInputStream this) throws IOException {
    throw new @Tainted IOException("Mark not supported");
  }
}
