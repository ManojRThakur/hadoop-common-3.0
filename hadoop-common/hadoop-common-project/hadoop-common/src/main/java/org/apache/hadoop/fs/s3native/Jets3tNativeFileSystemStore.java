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

package org.apache.hadoop.fs.s3native;

import org.checkerframework.checker.tainting.qual.Tainted;

import static org.apache.hadoop.fs.s3native.NativeS3FileSystem.PATH_DELIMITER;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.fs.s3.S3Exception;
import org.jets3t.service.S3ObjectsChunk;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Jets3tNativeFileSystemStore implements @Tainted NativeFileSystemStore {
  
  private @Tainted S3Service s3Service;
  private @Tainted S3Bucket bucket;
  
  @Override
  public void initialize(@Tainted Jets3tNativeFileSystemStore this, @Tainted URI uri, @Tainted Configuration conf) throws IOException {
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
      handleServiceException(e);
    }
    bucket = new @Tainted S3Bucket(uri.getHost());
  }
  
  @Override
  public void storeFile(@Tainted Jets3tNativeFileSystemStore this, @Tainted String key, @Tainted File file, @Tainted byte @Tainted [] md5Hash)
    throws IOException {
    
    @Tainted
    BufferedInputStream in = null;
    try {
      in = new @Tainted BufferedInputStream(new @Tainted FileInputStream(file));
      @Tainted
      S3Object object = new @Tainted S3Object(key);
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(file.length());
      if (md5Hash != null) {
        object.setMd5Hash(md5Hash);
      }
      s3Service.putObject(bucket, object);
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (@Tainted IOException e) {
          // ignore
        }
      }
    }
  }

  @Override
  public void storeEmptyFile(@Tainted Jets3tNativeFileSystemStore this, @Tainted String key) throws IOException {
    try {
      @Tainted
      S3Object object = new @Tainted S3Object(key);
      object.setDataInputStream(new @Tainted ByteArrayInputStream(new @Tainted byte @Tainted [0]));
      object.setContentType("binary/octet-stream");
      object.setContentLength(0);
      s3Service.putObject(bucket, object);
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(e);
    }
  }
  
  @Override
  public @Tainted FileMetadata retrieveMetadata(@Tainted Jets3tNativeFileSystemStore this, @Tainted String key) throws IOException {
    try {
      @Tainted
      S3Object object = s3Service.getObjectDetails(bucket, key);
      return new @Tainted FileMetadata(key, object.getContentLength(),
          object.getLastModifiedDate().getTime());
    } catch (@Tainted S3ServiceException e) {
      // Following is brittle. Is there a better way?
      if (e.getMessage().contains("ResponseCode=404")) {
        return null;
      }
      handleServiceException(e);
      return null; //never returned - keep compiler happy
    }
  }
  
  @Override
  public @Tainted InputStream retrieve(@Tainted Jets3tNativeFileSystemStore this, @Tainted String key) throws IOException {
    try {
      @Tainted
      S3Object object = s3Service.getObject(bucket, key);
      return object.getDataInputStream();
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(key, e);
      return null; //never returned - keep compiler happy
    } catch (ServiceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	}
  }
  
  @Override
  public @Tainted InputStream retrieve(@Tainted Jets3tNativeFileSystemStore this, @Tainted String key, @Tainted long byteRangeStart)
    throws IOException {
    try {
      @Tainted
      S3Object object = s3Service.getObject(bucket, key, null, null, null,
                                            null, byteRangeStart, null);
      return object.getDataInputStream();
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(key, e);
      return null; //never returned - keep compiler happy
    } catch (ServiceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	}
  }

  @Override
  public @Tainted PartialListing list(@Tainted Jets3tNativeFileSystemStore this, @Tainted String prefix, @Tainted int maxListingLength)
    throws IOException {
    return list(prefix, maxListingLength, null, false);
  }
  
  @Override
  public @Tainted PartialListing list(@Tainted Jets3tNativeFileSystemStore this, @Tainted String prefix, @Tainted int maxListingLength, @Tainted String priorLastKey,
      @Tainted
      boolean recurse) throws IOException {

    return list(prefix, recurse ? null : PATH_DELIMITER, maxListingLength, priorLastKey);
  }


  private @Tainted PartialListing list(@Tainted Jets3tNativeFileSystemStore this, @Tainted String prefix, @Tainted String delimiter,
      @Tainted
      int maxListingLength, @Tainted String priorLastKey) throws IOException {
    try {
      if (prefix.length() > 0 && !prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      @Tainted
      S3ObjectsChunk chunk = (@Tainted S3ObjectsChunk) s3Service.listObjectsChunked(bucket.getName(),
          prefix, delimiter, maxListingLength, priorLastKey);
      
      @Tainted
      FileMetadata @Tainted [] fileMetadata =
        new @Tainted FileMetadata @Tainted [chunk.getObjects().length];
      for (@Tainted int i = 0; i < fileMetadata.length; i++) {
        @Tainted
        S3Object object = chunk.getObjects()[i];
        fileMetadata[i] = new @Tainted FileMetadata(object.getKey(),
            object.getContentLength(), object.getLastModifiedDate().getTime());
      }
      return new @Tainted PartialListing(chunk.getPriorLastKey(), fileMetadata,
          chunk.getCommonPrefixes());
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(e);
      return null; //never returned - keep compiler happy
    } catch (ServiceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	}
  }

  @Override
  public void delete(@Tainted Jets3tNativeFileSystemStore this, @Tainted String key) throws IOException {
    try {
      s3Service.deleteObject(bucket, key);
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(key, e);
    }
  }
  
  @Override
  public void copy(@Tainted Jets3tNativeFileSystemStore this, @Tainted String srcKey, @Tainted String dstKey) throws IOException {
    try {
      s3Service.copyObject(bucket.getName(), srcKey, bucket.getName(),
          new @Tainted S3Object(dstKey), false);
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(srcKey, e);
    } catch (ServiceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  @Override
  public void purge(@Tainted Jets3tNativeFileSystemStore this, @Tainted String prefix) throws IOException {
    try {
      @Tainted
      S3Object @Tainted [] objects = s3Service.listObjects(bucket, prefix, null);
      for (@Tainted S3Object object : objects) {
        s3Service.deleteObject(bucket, object.getKey());
      }
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(e);
    }
  }

  @Override
  public void dump(@Tainted Jets3tNativeFileSystemStore this) throws IOException {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder("S3 Native Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      @Tainted
      S3Object @Tainted [] objects = s3Service.listObjects(bucket);
      for (@Tainted S3Object object : objects) {
        sb.append(object.getKey()).append("\n");
      }
    } catch (@Tainted S3ServiceException e) {
      handleServiceException(e);
    }
    System.out.println(sb);
  }

  private void handleServiceException(@Tainted Jets3tNativeFileSystemStore this, @Tainted String key, @Tainted S3ServiceException e) throws IOException {
    if ("NoSuchKey".equals(e.getS3ErrorCode())) {
      throw new @Tainted FileNotFoundException("Key '" + key + "' does not exist in S3");
    } else {
      handleServiceException(e);
    }
  }

  private void handleServiceException(@Tainted Jets3tNativeFileSystemStore this, @Tainted S3ServiceException e) throws IOException {
    if (e.getCause() instanceof @Tainted IOException) {
      throw (@Tainted IOException) e.getCause();
    }
    else {
      throw new @Tainted S3Exception(e);
    }
  }
}
