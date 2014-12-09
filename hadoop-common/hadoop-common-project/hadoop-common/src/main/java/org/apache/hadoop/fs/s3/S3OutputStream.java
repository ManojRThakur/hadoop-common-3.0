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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.apache.hadoop.util.Progressable;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3OutputStream extends @Tainted OutputStream {

  private @Tainted Configuration conf;
  
  private @Tainted int bufferSize;

  private @Tainted FileSystemStore store;

  private @Tainted Path path;

  private @Tainted long blockSize;

  private @Tainted File backupFile;

  private @Tainted OutputStream backupStream;

  private @Tainted Random r = new @Tainted Random();

  private @Tainted boolean closed;

  private @Tainted int pos = 0;

  private @Tainted long filePos = 0;

  private @Tainted int bytesWrittenToBlock = 0;

  private @Tainted byte @Tainted [] outBuf;

  private @Tainted List<@Tainted Block> blocks = new @Tainted ArrayList<@Tainted Block>();

  private @Tainted Block nextBlock;
  
  private static final @Tainted Log LOG = 
    LogFactory.getLog(S3OutputStream.class.getName());


  public @Tainted S3OutputStream(@Tainted Configuration conf, @Tainted FileSystemStore store,
                        @Tainted
                        Path path, @Tainted long blockSize, @Tainted Progressable progress,
                        @Tainted
                        int buffersize) throws IOException {
    
    this.conf = conf;
    this.store = store;
    this.path = path;
    this.blockSize = blockSize;
    this.backupFile = newBackupFile();
    this.backupStream = new @Tainted FileOutputStream(backupFile);
    this.bufferSize = buffersize;
    this.outBuf = new @Tainted byte @Tainted [bufferSize];

  }

  private @Tainted File newBackupFile(@Tainted S3OutputStream this) throws IOException {
    @Tainted
    File dir = new @Tainted File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new @Tainted IOException("Cannot create S3 buffer directory: " + dir);
    }
    @Tainted
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  public @Tainted long getPos(@Tainted S3OutputStream this) throws IOException {
    return filePos;
  }

  @Override
  public synchronized void write(@Tainted S3OutputStream this, @Tainted int b) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }

    if ((bytesWrittenToBlock + pos == blockSize) || (pos >= bufferSize)) {
      flush();
    }
    outBuf[pos++] = (@Tainted byte) b;
    filePos++;
  }

  @Override
  public synchronized void write(@Tainted S3OutputStream this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }
    while (len > 0) {
      @Tainted
      int remaining = bufferSize - pos;
      @Tainted
      int toWrite = Math.min(remaining, len);
      System.arraycopy(b, off, outBuf, pos, toWrite);
      pos += toWrite;
      off += toWrite;
      len -= toWrite;
      filePos += toWrite;

      if ((bytesWrittenToBlock + pos >= blockSize) || (pos == bufferSize)) {
        flush();
      }
    }
  }

  @Override
  public synchronized void flush(@Tainted S3OutputStream this) throws IOException {
    if (closed) {
      throw new @Tainted IOException("Stream closed");
    }

    if (bytesWrittenToBlock + pos >= blockSize) {
      flushData((@Tainted int) blockSize - bytesWrittenToBlock);
    }
    if (bytesWrittenToBlock == blockSize) {
      endBlock();
    }
    flushData(pos);
  }

  private synchronized void flushData(@Tainted S3OutputStream this, @Tainted int maxPos) throws IOException {
    @Tainted
    int workingPos = Math.min(pos, maxPos);

    if (workingPos > 0) {
      //
      // To the local block backup, write just the bytes
      //
      backupStream.write(outBuf, 0, workingPos);

      //
      // Track position
      //
      bytesWrittenToBlock += workingPos;
      System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
      pos -= workingPos;
    }
  }

  private synchronized void endBlock(@Tainted S3OutputStream this) throws IOException {
    //
    // Done with local copy
    //
    backupStream.close();

    //
    // Send it to S3
    //
    // TODO: Use passed in Progressable to report progress.
    nextBlockOutputStream();
    store.storeBlock(nextBlock, backupFile);
    internalClose();

    //
    // Delete local backup, start new one
    //
    @Tainted
    boolean b = backupFile.delete();
    if (!b) {
      LOG.warn("Ignoring failed delete");
    }
    backupFile = newBackupFile();
    backupStream = new @Tainted FileOutputStream(backupFile);
    bytesWrittenToBlock = 0;
  }

  private synchronized void nextBlockOutputStream(@Tainted S3OutputStream this) throws IOException {
    @Tainted
    long blockId = r.nextLong();
    while (store.blockExists(blockId)) {
      blockId = r.nextLong();
    }
    nextBlock = new @Tainted Block(blockId, bytesWrittenToBlock);
    blocks.add(nextBlock);
    bytesWrittenToBlock = 0;
  }

  private synchronized void internalClose(@Tainted S3OutputStream this) throws IOException {
    @Tainted
    INode inode = new @Tainted INode(FileType.FILE, blocks.toArray(new @Tainted Block @Tainted [blocks
                                                                    .size()]));
    store.storeINode(path, inode);
  }

  @Override
  public synchronized void close(@Tainted S3OutputStream this) throws IOException {
    if (closed) {
      return;
    }

    flush();
    if (filePos == 0 || bytesWrittenToBlock != 0) {
      endBlock();
    }

    backupStream.close();
    @Tainted
    boolean b = backupFile.delete();
    if (!b) {
      LOG.warn("Ignoring failed delete");
    }

    super.close();

    closed = true;
  }

}
