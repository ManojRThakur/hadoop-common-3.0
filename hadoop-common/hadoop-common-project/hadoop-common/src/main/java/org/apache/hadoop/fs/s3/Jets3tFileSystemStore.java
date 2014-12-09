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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Jets3tFileSystemStore implements @Tainted FileSystemStore {
  
  private static final @Tainted String FILE_SYSTEM_NAME = "fs";
  private static final @Tainted String FILE_SYSTEM_VALUE = "Hadoop";

  private static final @Tainted String FILE_SYSTEM_TYPE_NAME = "fs-type";
  private static final @Tainted String FILE_SYSTEM_TYPE_VALUE = "block";

  private static final @Tainted String FILE_SYSTEM_VERSION_NAME = "fs-version";
  private static final @Tainted String FILE_SYSTEM_VERSION_VALUE = "1";
  
  private static final @Tainted Map<@Tainted String, @Tainted Object> METADATA =
    new @Tainted HashMap<@Tainted String, @Tainted Object>();
  
  static {
    METADATA.put(FILE_SYSTEM_NAME, FILE_SYSTEM_VALUE);
    METADATA.put(FILE_SYSTEM_TYPE_NAME, FILE_SYSTEM_TYPE_VALUE);
    METADATA.put(FILE_SYSTEM_VERSION_NAME, FILE_SYSTEM_VERSION_VALUE);
  }

  private static final @Tainted String PATH_DELIMITER = Path.SEPARATOR;
  private static final @Tainted String BLOCK_PREFIX = "block_";

  private @Tainted Configuration conf;
  
  private @Tainted S3Service s3Service;

  private @Tainted S3Bucket bucket;
  
  private @Tainted int bufferSize;
  
  private static final @Tainted Log LOG = 
    LogFactory.getLog(Jets3tFileSystemStore.class.getName());
  
  @Override
  public void initialize(@Tainted Jets3tFileSystemStore this, @Tainted URI uri, @Tainted Configuration conf) throws IOException {
    
    this.conf = conf;
    
    @Tainted
    S3Credentials s3Credentials = new @Tainted S3Credentials();
    s3Credentials.initialize(uri, conf);
    try {
      @Tainted
      AWSCredentials awsCredentials =
        new @Tainted AWSCredentials(s3Credentials.getAccessKey(),
            s3Credentials.getSecretAccessKey());
      this.s3Service = new @Tainted RestS3Service(awsCredentials);
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }
    bucket = new @Tainted S3Bucket(uri.getHost());

    this.bufferSize = conf.getInt(
                       S3FileSystemConfigKeys.S3_STREAM_BUFFER_SIZE_KEY,
                       S3FileSystemConfigKeys.S3_STREAM_BUFFER_SIZE_DEFAULT
		      );
  }

  @Override
  public @Tainted String getVersion(@Tainted Jets3tFileSystemStore this) throws IOException {
    return FILE_SYSTEM_VERSION_VALUE;
  }

  private void delete(@Tainted Jets3tFileSystemStore this, @Tainted String key) throws IOException {
    try {
      s3Service.deleteObject(bucket, key);
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }
  }

  @Override
  public void deleteINode(@Tainted Jets3tFileSystemStore this, @Tainted Path path) throws IOException {
    delete(pathToKey(path));
  }

  @Override
  public void deleteBlock(@Tainted Jets3tFileSystemStore this, @Tainted Block block) throws IOException {
    delete(blockToKey(block));
  }

  @Override
  public @Tainted boolean inodeExists(@Tainted Jets3tFileSystemStore this, @Tainted Path path) throws IOException {
    @Tainted
    String key = pathToKey(path);
    @Tainted
    InputStream in = get(key, true);
    if (in == null) {
      if (isRoot(key)) {
        storeINode(path, INode.DIRECTORY_INODE);
        return true;
      } else {
        return false;
      }
    }
    in.close();
    return true;
  }
  
  @Override
  public @Tainted boolean blockExists(@Tainted Jets3tFileSystemStore this, @Tainted long blockId) throws IOException {
    @Tainted
    InputStream in = get(blockToKey(blockId), false);
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }

  private @Tainted InputStream get(@Tainted Jets3tFileSystemStore this, @Tainted String key, @Tainted boolean checkMetadata)
      throws IOException {
    
    try {
      @Tainted
      S3Object object = s3Service.getObject(bucket, key);
      if (checkMetadata) {
        checkMetadata(object);
      }
      return object.getDataInputStream();
    } catch (@Tainted S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    } catch (ServiceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	}
  }

  private @Tainted InputStream get(@Tainted Jets3tFileSystemStore this, @Tainted String key, @Tainted long byteRangeStart) throws IOException {
    try {
      @Tainted
      S3Object object = s3Service.getObject(bucket, key, null, null, null,
                                            null, byteRangeStart, null);
      return object.getDataInputStream();
    } catch (@Tainted S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    } catch (ServiceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	}
  }

  private void checkMetadata(@Tainted Jets3tFileSystemStore this, @Tainted S3Object object) throws S3FileSystemException,
      S3ServiceException {
    
    @Tainted
    String name = (@Tainted String) object.getMetadata(FILE_SYSTEM_NAME);
    if (!FILE_SYSTEM_VALUE.equals(name)) {
      throw new @Tainted S3FileSystemException("Not a Hadoop S3 file.");
    }
    @Tainted
    String type = (@Tainted String) object.getMetadata(FILE_SYSTEM_TYPE_NAME);
    if (!FILE_SYSTEM_TYPE_VALUE.equals(type)) {
      throw new @Tainted S3FileSystemException("Not a block file.");
    }
    @Tainted
    String dataVersion = (@Tainted String) object.getMetadata(FILE_SYSTEM_VERSION_NAME);
    if (!FILE_SYSTEM_VERSION_VALUE.equals(dataVersion)) {
      throw new @Tainted VersionMismatchException(FILE_SYSTEM_VERSION_VALUE,
          dataVersion);
    }
  }

  @Override
  public @Tainted INode retrieveINode(@Tainted Jets3tFileSystemStore this, @Tainted Path path) throws IOException {
    @Tainted
    String key = pathToKey(path);
    @Tainted
    InputStream in = get(key, true);
    if (in == null && isRoot(key)) {
      storeINode(path, INode.DIRECTORY_INODE);
      return INode.DIRECTORY_INODE;
    }
    return INode.deserialize(in);
  }

  @Override
  public @Tainted File retrieveBlock(@Tainted Jets3tFileSystemStore this, @Tainted Block block, @Tainted long byteRangeStart)
    throws IOException {
    @Tainted
    File fileBlock = null;
    @Tainted
    InputStream in = null;
    @Tainted
    OutputStream out = null;
    try {
      fileBlock = newBackupFile();
      in = get(blockToKey(block), byteRangeStart);
      out = new @Tainted BufferedOutputStream(new @Tainted FileOutputStream(fileBlock));
      @Tainted
      byte @Tainted [] buf = new @Tainted byte @Tainted [bufferSize];
      @Tainted
      int numRead;
      while ((numRead = in.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
      }
      return fileBlock;
    } catch (@Tainted IOException e) {
      // close output stream to file then delete file
      closeQuietly(out);
      out = null; // to prevent a second close
      if (fileBlock != null) {
        @Tainted
        boolean b = fileBlock.delete();
        if (!b) {
          LOG.warn("Ignoring failed delete");
        }
      }
      throw e;
    } finally {
      closeQuietly(out);
      closeQuietly(in);
    }
  }
  
  private @Tainted File newBackupFile(@Tainted Jets3tFileSystemStore this) throws IOException {
    @Tainted
    File dir = new @Tainted File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new @Tainted IOException("Cannot create S3 buffer directory: " + dir);
    }
    @Tainted
    File result = File.createTempFile("input-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  @Override
  public @Tainted Set<@Tainted Path> listSubPaths(@Tainted Jets3tFileSystemStore this, @Tainted Path path) throws IOException {
    try {
      @Tainted
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      @Tainted
      S3Object @Tainted [] objects = s3Service.listObjects(bucket, prefix, PATH_DELIMITER);
      @Tainted
      Set<@Tainted Path> prefixes = new @Tainted TreeSet<@Tainted Path>();
      for (@Tainted int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }
  }
  
  @Override
  public @Tainted Set<@Tainted Path> listDeepSubPaths(@Tainted Jets3tFileSystemStore this, @Tainted Path path) throws IOException {
    try {
      @Tainted
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      @Tainted
      S3Object @Tainted [] objects = s3Service.listObjects(bucket, prefix, null);
      @Tainted
      Set<@Tainted Path> prefixes = new @Tainted TreeSet<@Tainted Path>();
      for (@Tainted int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }    
  }

  private void put(@Tainted Jets3tFileSystemStore this, @Tainted String key, @Tainted InputStream in, @Tainted long length, @Tainted boolean storeMetadata)
      throws IOException {
    
    try {
      @Tainted
      S3Object object = new @Tainted S3Object(key);
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(length);
      if (storeMetadata) {
        object.addAllMetadata(METADATA);
      }
      s3Service.putObject(bucket, object);
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }
  }

  @Override
  public void storeINode(@Tainted Jets3tFileSystemStore this, @Tainted Path path, @Tainted INode inode) throws IOException {
    put(pathToKey(path), inode.serialize(), inode.getSerializedLength(), true);
  }

  @Override
  public void storeBlock(@Tainted Jets3tFileSystemStore this, @Tainted Block block, @Tainted File file) throws IOException {
    @Tainted
    BufferedInputStream in = null;
    try {
      in = new @Tainted BufferedInputStream(new @Tainted FileInputStream(file));
      put(blockToKey(block), in, block.getLength(), false);
    } finally {
      closeQuietly(in);
    }    
  }

  private void closeQuietly(@Tainted Jets3tFileSystemStore this, @Tainted Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (@Tainted IOException e) {
        // ignore
      }
    }
  }

  private @Tainted String pathToKey(@Tainted Jets3tFileSystemStore this, @Tainted Path path) {
    if (!path.isAbsolute()) {
      throw new @Tainted IllegalArgumentException("Path must be absolute: " + path);
    }
    return path.toUri().getPath();
  }

  private @Tainted Path keyToPath(@Tainted Jets3tFileSystemStore this, @Tainted String key) {
    return new @Tainted Path(key);
  }
  
  private @Tainted String blockToKey(@Tainted Jets3tFileSystemStore this, @Tainted long blockId) {
    return BLOCK_PREFIX + blockId;
  }

  private @Tainted String blockToKey(@Tainted Jets3tFileSystemStore this, @Tainted Block block) {
    return blockToKey(block.getId());
  }

  private @Tainted boolean isRoot(@Tainted Jets3tFileSystemStore this, @Tainted String key) {
    return key.isEmpty() || key.equals("/");
  }

  @Override
  public void purge(@Tainted Jets3tFileSystemStore this) throws IOException {
    try {
      @Tainted
      S3Object @Tainted [] objects = s3Service.listObjects(bucket);
      for (@Tainted int i = 0; i < objects.length; i++) {
        s3Service.deleteObject(bucket, objects[i].getKey());
      }
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }
  }

  @Override
  public void dump(@Tainted Jets3tFileSystemStore this) throws IOException {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder("S3 Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      @Tainted
      S3Object @Tainted [] objects = s3Service.listObjects(bucket, PATH_DELIMITER, null);
      for (@Tainted int i = 0; i < objects.length; i++) {
        @Tainted
        Path path = keyToPath(objects[i].getKey());
        sb.append(path).append("\n");
        @Tainted
        INode m = retrieveINode(path);
        sb.append("\t").append(m.getFileType()).append("\n");
        if (m.getFileType() == FileType.DIRECTORY) {
          continue;
        }
        for (@Tainted int j = 0; j < m.getBlocks().length; j++) {
          sb.append("\t").append(m.getBlocks()[j]).append("\n");
        }
      }
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }
    System.out.println(sb);
  }

}
