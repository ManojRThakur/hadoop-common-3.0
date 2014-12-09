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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;

/****************************************************
 * Provides server default configuration values to clients.
 * 
 ****************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FsServerDefaults implements @Tainted Writable {

  static { // register a ctor
    WritableFactories.setFactory(FsServerDefaults.class, new @Tainted WritableFactory() {
      @Override
      public @Tainted Writable newInstance() {
        return new @Tainted FsServerDefaults();
      }
    });
  }

  private @Tainted long blockSize;
  private @Tainted int bytesPerChecksum;
  private @Tainted int writePacketSize;
  private @Tainted short replication;
  private @Tainted int fileBufferSize;
  private @Tainted boolean encryptDataTransfer;
  private @Tainted long trashInterval;
  private DataChecksum.@Tainted Type checksumType;

  public @Tainted FsServerDefaults() {
  }

  public @Tainted FsServerDefaults(@Tainted long blockSize, @Tainted int bytesPerChecksum,
      @Tainted
      int writePacketSize, @Tainted short replication, @Tainted int fileBufferSize,
      @Tainted
      boolean encryptDataTransfer, @Tainted long trashInterval,
      DataChecksum.@Tainted Type checksumType) {
    this.blockSize = blockSize;
    this.bytesPerChecksum = bytesPerChecksum;
    this.writePacketSize = writePacketSize;
    this.replication = replication;
    this.fileBufferSize = fileBufferSize;
    this.encryptDataTransfer = encryptDataTransfer;
    this.trashInterval = trashInterval;
    this.checksumType = checksumType;
  }

  public @Tainted long getBlockSize(@Tainted FsServerDefaults this) {
    return blockSize;
  }

  public @Tainted int getBytesPerChecksum(@Tainted FsServerDefaults this) {
    return bytesPerChecksum;
  }

  public @Tainted int getWritePacketSize(@Tainted FsServerDefaults this) {
    return writePacketSize;
  }

  public @Tainted short getReplication(@Tainted FsServerDefaults this) {
    return replication;
  }

  public @Tainted int getFileBufferSize(@Tainted FsServerDefaults this) {
    return fileBufferSize;
  }
  
  public @Tainted boolean getEncryptDataTransfer(@Tainted FsServerDefaults this) {
    return encryptDataTransfer;
  }

  public @Tainted long getTrashInterval(@Tainted FsServerDefaults this) {
    return trashInterval;
  }

  public DataChecksum.@Tainted Type getChecksumType(@Tainted FsServerDefaults this) {
    return checksumType;
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  @Override
  @InterfaceAudience.Private
  public void write(@Tainted FsServerDefaults this, @Tainted DataOutput out) throws IOException {
    out.writeLong(blockSize);
    out.writeInt(bytesPerChecksum);
    out.writeInt(writePacketSize);
    out.writeShort(replication);
    out.writeInt(fileBufferSize);
    WritableUtils.writeEnum(out, checksumType);
  }

  @Override
  @InterfaceAudience.Private
  public void readFields(@Tainted FsServerDefaults this, @Tainted DataInput in) throws IOException {
    blockSize = in.readLong();
    bytesPerChecksum = in.readInt();
    writePacketSize = in.readInt();
    replication = in.readShort();
    fileBufferSize = in.readInt();
    checksumType = WritableUtils.readEnum(in, DataChecksum.Type.class);
  }
}
