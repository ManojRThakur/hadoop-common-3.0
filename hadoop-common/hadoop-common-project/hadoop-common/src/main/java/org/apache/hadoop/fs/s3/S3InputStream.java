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

package org.apache.hadoop.fs.s3;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3InputStream extends @Tainted FSInputStream {

  private @Tainted FileSystemStore store;

  private @Tainted Block @Tainted [] blocks;

  private @Tainted boolean closed;

  private @Tainted long fileLength;

  private @Tainted long pos = 0;

  private @Tainted File blockFile;
  
  private @Tainted DataInputStream blockStream;

  private @Tainted long blockEnd = -1;
  
  private FileSystem.@Tainted Statistics stats;
  
  private static final @Tainted Log LOG = 
    LogFactory.getLog(S3InputStream.class.getName());


  @Deprecated
  public @Tainted S3InputStream(@Tainted Configuration conf, @Tainted FileSystemStore store,
                       @Tainted
                       INode inode) {
    this(conf, store, inode, null);
  }

  public @Tainted S3InputStream(@Tainted Configuration conf, @Tainted FileSystemStore store,
                       @Tainted
                       INode inode, FileSystem.@Tainted Statistics stats) {
    
    this.store = store;
    this.stats = stats;
    this.blocks = inode.getBlocks();
    for (@Tainted Block block : blocks) {
      this.fileLength += block.getLength();
    }
  }

  @Override
  public synchronized @Tainted long getPos(@Tainted S3InputStream this) throws IOException {
    return pos;
  }

  @Override
  public synchronized @Tainted int available(@Tainted S3InputStream this) throws IOException {
    return (@Tainted int) (fileLength - pos);
  }

  @Override
  public synchronized void seek(@Tainted S3InputStream this, @Tainted long targetPos) throws IOException {
    if (targetPos > fileLength) {
      throw new @Tainted IOException("Cannot seek after EOF");
    }
    pos = targetPos;
    blockEnd = -1;
  }

  @Override
  public synchronized @Tainted boolean seekToNewSource(@Tainted S3InputStream this, @Tainted long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized @Tainted int read(@Tainted S3InputStream this) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }
    @Tainted
    int result = -1;
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      result = blockStream.read();
      if (result >= 0) {
        pos++;
      }
    }
    if (stats != null && result >= 0) {
      stats.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized @Tainted int read(@Tainted S3InputStream this, @Tainted byte buf @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      @Tainted
      int realLen = (@Tainted int) Math.min((long) len, (blockEnd - pos + 1L));
      @Tainted
      int result = blockStream.read(buf, off, realLen);
      if (result >= 0) {
        pos += result;
      }
      if (stats != null && result > 0) {
        stats.incrementBytesRead(result);
      }
      return result;
    }
    return -1;
  }

  private synchronized void blockSeekTo(@Tainted S3InputStream this, @Tainted long target) throws IOException {
    //
    // Compute desired block
    //
    @Tainted
    int targetBlock = -1;
    @Tainted
    long targetBlockStart = 0;
    @Tainted
    long targetBlockEnd = 0;
    for (@Tainted int i = 0; i < blocks.length; i++) {
      @Tainted
      long blockLength = blocks[i].getLength();
      targetBlockEnd = targetBlockStart + blockLength - 1;

      if (target >= targetBlockStart && target <= targetBlockEnd) {
        targetBlock = i;
        break;
      } else {
        targetBlockStart = targetBlockEnd + 1;
      }
    }
    if (targetBlock < 0) {
      throw new @Tainted IOException(
                            "Impossible situation: could not find target position " + target);
    }
    @Tainted
    long offsetIntoBlock = target - targetBlockStart;

    // read block blocks[targetBlock] from position offsetIntoBlock

    this.blockFile = store.retrieveBlock(blocks[targetBlock], offsetIntoBlock);

    this.pos = target;
    this.blockEnd = targetBlockEnd;
    this.blockStream = new @Tainted DataInputStream(new @Tainted FileInputStream(blockFile));

  }

  @Override
  public void close(@Tainted S3InputStream this) throws IOException {
    if (closed) {
      return;
    }
    if (blockStream != null) {
      blockStream.close();
      blockStream = null;
    }
    if (blockFile != null) {
      @Tainted
      boolean b = blockFile.delete();
      if (!b) {
        LOG.warn("Ignoring failed delete");
      }
    }
    super.close();
    closed = true;
  }

  /**
   * We don't support marks.
   */
  @Override
  public @Tainted boolean markSupported(@Tainted S3InputStream this) {
    return false;
  }

  @Override
  public void mark(@Tainted S3InputStream this, @Tainted int readLimit) {
    // Do nothing
  }

  @Override
  public void reset(@Tainted S3InputStream this) throws IOException {
    throw new @Tainted IOException("Mark not supported");
  }

}
