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
import org.checkerframework.checker.tainting.qual.Untainted;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 * <p>
 * This class is a tool for migrating data from an older to a newer version
 * of an S3 filesystem.
 * </p>
 * <p>
 * All files in the filesystem are migrated by re-writing the block metadata
 * - no datafiles are touched.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MigrationTool extends @Tainted Configured implements @Tainted Tool {
  
  private @Tainted S3Service s3Service;
  private @Tainted S3Bucket bucket;

  // @SuppressWarnings("ostrusted") // cli args are trusted.
  public static void main(@Untainted String @Tainted [] args) throws Exception {
    @Tainted
    int res = ToolRunner.run(new @Tainted MigrationTool(), args);
    System.exit(res);
  }
  
  @Override
  public @Tainted int run(@Tainted MigrationTool this, @Tainted String @Tainted [] args) throws Exception {
    
    if (args.length == 0) {
      System.err.println("Usage: MigrationTool <S3 file system URI>");
      System.err.println("\t<S3 file system URI>\tfilesystem to migrate");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    @Tainted
    URI uri = URI.create(args[0]);
    
    initialize(uri);
    
    @Tainted
    FileSystemStore newStore = new @Tainted Jets3tFileSystemStore();
    newStore.initialize(uri, getConf());
    
    if (get("%2F") != null) { 
      System.err.println("Current version number is [unversioned].");
      System.err.println("Target version number is " +
          newStore.getVersion() + ".");
      @Tainted
      Store oldStore = new @Tainted UnversionedStore();
      migrate(oldStore, newStore);
      return 0;
    } else {
      @Tainted
      S3Object root = get("/");
      if (root != null) {
        @Tainted
        String version = (@Tainted String) root.getMetadata("fs-version");
        if (version == null) {
          System.err.println("Can't detect version - exiting.");
        } else {
          @Tainted
          String newVersion = newStore.getVersion();
          System.err.println("Current version number is " + version + ".");
          System.err.println("Target version number is " + newVersion + ".");
          if (version.equals(newStore.getVersion())) {
            System.err.println("No migration required.");
            return 0;
          }
          // use version number to create Store
          //Store oldStore = ... 
          //migrate(oldStore, newStore);
          System.err.println("Not currently implemented.");
          return 0;
        }
      }
      System.err.println("Can't detect version - exiting.");
      return 0;
    }
    
  }
  
  public void initialize(@Tainted MigrationTool this, @Tainted URI uri) throws IOException {
    
    
    
    try {
      @Tainted
      String accessKey = null;
      @Tainted
      String secretAccessKey = null;
      @Tainted
      String userInfo = uri.getUserInfo();
      if (userInfo != null) {
        @Tainted
        int index = userInfo.indexOf(':');
        if (index != -1) {
          accessKey = userInfo.substring(0, index);
          secretAccessKey = userInfo.substring(index + 1);
        } else {
          accessKey = userInfo;
        }
      }
      if (accessKey == null) {
        accessKey = getConf().get("fs.s3.awsAccessKeyId");
      }
      if (secretAccessKey == null) {
        secretAccessKey = getConf().get("fs.s3.awsSecretAccessKey");
      }
      if (accessKey == null && secretAccessKey == null) {
        throw new @Tainted IllegalArgumentException("AWS " +
                                           "Access Key ID and Secret Access Key " +
                                           "must be specified as the username " +
                                           "or password (respectively) of a s3 URL, " +
                                           "or by setting the " +
                                           "fs.s3.awsAccessKeyId or " +                         
                                           "fs.s3.awsSecretAccessKey properties (respectively).");
      } else if (accessKey == null) {
        throw new @Tainted IllegalArgumentException("AWS " +
                                           "Access Key ID must be specified " +
                                           "as the username of a s3 URL, or by setting the " +
                                           "fs.s3.awsAccessKeyId property.");
      } else if (secretAccessKey == null) {
        throw new @Tainted IllegalArgumentException("AWS " +
                                           "Secret Access Key must be specified " +
                                           "as the password of a s3 URL, or by setting the " +
                                           "fs.s3.awsSecretAccessKey property.");         
      }
      @Tainted
      AWSCredentials awsCredentials =
        new @Tainted AWSCredentials(accessKey, secretAccessKey);
      this.s3Service = new @Tainted RestS3Service(awsCredentials);
    } catch (@Tainted S3ServiceException e) {
      if (e.getCause() instanceof @Tainted IOException) {
        throw (@Tainted IOException) e.getCause();
      }
      throw new @Tainted S3Exception(e);
    }
    bucket = new @Tainted S3Bucket(uri.getHost());
  }
  
  private void migrate(@Tainted MigrationTool this, @Tainted Store oldStore, @Tainted FileSystemStore newStore)
      throws IOException {
    for (@Tainted Path path : oldStore.listAllPaths()) {
      @Tainted
      INode inode = oldStore.retrieveINode(path);
      oldStore.deleteINode(path);
      newStore.storeINode(path, inode);
    }
  }
  
  private @Tainted S3Object get(@Tainted MigrationTool this, @Tainted String key) {
    try {
      return s3Service.getObject(bucket, key);
    } catch (@Tainted S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
    }
    return null;
  }
  
  interface Store {

    @Tainted
    Set<@Tainted Path> listAllPaths(MigrationTool.@Tainted Store this) throws IOException;
    @Tainted
    INode retrieveINode(MigrationTool.@Tainted Store this, @Tainted Path path) throws IOException;
    void deleteINode(MigrationTool.@Tainted Store this, @Tainted Path path) throws IOException;
    
  }
  
  class UnversionedStore implements @Tainted Store {

    @Override
    public @Tainted Set<@Tainted Path> listAllPaths(@Tainted MigrationTool.UnversionedStore this) throws IOException {
      try {
        @Tainted
        String prefix = urlEncode(Path.SEPARATOR);
        @Tainted
        S3Object @Tainted [] objects = s3Service.listObjects(bucket, prefix, null);
        @Tainted
        Set<@Tainted Path> prefixes = new @Tainted TreeSet<@Tainted Path>();
        for (@Tainted int i = 0; i < objects.length; i++) {
          prefixes.add(keyToPath(objects[i].getKey()));
        }
        return prefixes;
      } catch (@Tainted S3ServiceException e) {
        if (e.getCause() instanceof @Tainted IOException) {
          throw (@Tainted IOException) e.getCause();
        }
        throw new @Tainted S3Exception(e);
      }   
    }

    @Override
    public void deleteINode(@Tainted MigrationTool.UnversionedStore this, @Tainted Path path) throws IOException {
      delete(pathToKey(path));
    }
    
    private void delete(@Tainted MigrationTool.UnversionedStore this, @Tainted String key) throws IOException {
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
    public @Tainted INode retrieveINode(@Tainted MigrationTool.UnversionedStore this, @Tainted Path path) throws IOException {
      return INode.deserialize(get(pathToKey(path)));
    }

    private @Tainted InputStream get(@Tainted MigrationTool.UnversionedStore this, @Tainted String key) throws IOException {
      try {
        @Tainted
        S3Object object = s3Service.getObject(bucket, key);
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
    
    private @Tainted String pathToKey(@Tainted MigrationTool.UnversionedStore this, @Tainted Path path) {
      if (!path.isAbsolute()) {
        throw new @Tainted IllegalArgumentException("Path must be absolute: " + path);
      }
      return urlEncode(path.toUri().getPath());
    }
    
    private @Tainted Path keyToPath(@Tainted MigrationTool.UnversionedStore this, @Tainted String key) {
      return new @Tainted Path(urlDecode(key));
    }

    private @Tainted String urlEncode(@Tainted MigrationTool.UnversionedStore this, @Tainted String s) {
      try {
        return URLEncoder.encode(s, "UTF-8");
      } catch (@Tainted UnsupportedEncodingException e) {
        // Should never happen since every implementation of the Java Platform
        // is required to support UTF-8.
        // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
        throw new @Tainted IllegalStateException(e);
      }
    }
    
    private @Tainted String urlDecode(@Tainted MigrationTool.UnversionedStore this, @Tainted String s) {
      try {
        return URLDecoder.decode(s, "UTF-8");
      } catch (@Tainted UnsupportedEncodingException e) {
        // Should never happen since every implementation of the Java Platform
        // is required to support UTF-8.
        // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
        throw new @Tainted IllegalStateException(e);
      }
    }
    
  }
  
}
