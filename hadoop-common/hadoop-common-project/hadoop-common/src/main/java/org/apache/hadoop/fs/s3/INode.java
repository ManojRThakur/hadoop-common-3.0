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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;

/**
 * Holds file metadata including type (regular file, or directory),
 * and the list of blocks that are pointers to the data.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class INode {
	
  enum FileType {

@Tainted  DIRECTORY,  @Tainted  FILE
  }
  
  public static final @Tainted FileType @Tainted [] FILE_TYPES = new FileType @Tainted [] {
    FileType.DIRECTORY,
    FileType.FILE
  };

  public static final @Tainted INode DIRECTORY_INODE = new @Tainted INode(FileType.DIRECTORY, null);
  
  private @Tainted FileType fileType;
  private @Tainted Block @Tainted [] blocks;

  public @Tainted INode(@Tainted FileType fileType, @Tainted Block @Tainted [] blocks) {
    this.fileType = fileType;
    if (isDirectory() && blocks != null) {
      throw new @Tainted IllegalArgumentException("A directory cannot contain blocks.");
    }
    this.blocks = blocks;
  }

  public @Tainted Block @Tainted [] getBlocks(@Tainted INode this) {
    return blocks;
  }
  
  public @Tainted FileType getFileType(@Tainted INode this) {
    return fileType;
  }

  public @Tainted boolean isDirectory(@Tainted INode this) {
    return fileType == FileType.DIRECTORY;
  }  

  public @Tainted boolean isFile(@Tainted INode this) {
    return fileType == FileType.FILE;
  }
  
  public @Tainted long getSerializedLength(@Tainted INode this) {
    return 1L + (blocks == null ? 0 : 4 + blocks.length * 16);
  }
  

  public @Tainted InputStream serialize(@Tainted INode this) throws IOException {
    @Tainted
    ByteArrayOutputStream bytes = new @Tainted ByteArrayOutputStream();
    @Tainted
    DataOutputStream out = new @Tainted DataOutputStream(bytes);
    try {
      out.writeByte(fileType.ordinal());
      if (isFile()) {
        out.writeInt(blocks.length);
        for (@Tainted int i = 0; i < blocks.length; i++) {
          out.writeLong(blocks[i].getId());
          out.writeLong(blocks[i].getLength());
        }
      }
      out.close();
      out = null;
    } finally {
      IOUtils.closeStream(out);
    }
    return new @Tainted ByteArrayInputStream(bytes.toByteArray());
  }
  
  public static @Tainted INode deserialize(@Tainted InputStream in) throws IOException {
    if (in == null) {
      return null;
    }
    @Tainted
    DataInputStream dataIn = new @Tainted DataInputStream(in);
    @Tainted
    FileType fileType = INode.FILE_TYPES[dataIn.readByte()];
    switch (fileType) {
    case DIRECTORY:
      in.close();
      return INode.DIRECTORY_INODE;
    case FILE:
      @Tainted
      int numBlocks = dataIn.readInt();
      @Tainted
      Block @Tainted [] blocks = new @Tainted Block @Tainted [numBlocks];
      for (@Tainted int i = 0; i < numBlocks; i++) {
        @Tainted
        long id = dataIn.readLong();
        @Tainted
        long length = dataIn.readLong();
        blocks[i] = new @Tainted Block(id, length);
      }
      in.close();
      return new @Tainted INode(fileType, blocks);
    default:
      throw new @Tainted IllegalArgumentException("Cannot deserialize inode.");
    }    
  }  
  
}
