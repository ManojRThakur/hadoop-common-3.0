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

package org.apache.hadoop.security;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A class that provides the facilities of reading and writing 
 * secret keys and Tokens.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Credentials implements @Tainted Writable {
  private static final @Tainted Log LOG = LogFactory.getLog(Credentials.class);

  private  @Tainted Map<@Tainted Text, @Tainted byte @Tainted []> secretKeysMap = new @Tainted HashMap<@Tainted Text, @Tainted byte @Tainted []>();
  private  @Tainted Map<@Tainted Text, @Tainted Token<@Tainted ? extends @Tainted TokenIdentifier>> tokenMap = 
    new @Tainted HashMap<@Tainted Text, @Tainted Token<@Tainted ? extends @Tainted TokenIdentifier>>(); 

  /**
   * Create an empty credentials instance
   */
  public @Tainted Credentials() {
  }
  
  /**
   * Create a copy of the given credentials
   * @param credentials to copy
   */
  public @Tainted Credentials(@Tainted Credentials credentials) {
    this.addAll(credentials);
  }
  
  /**
   * Returns the key bytes for the alias
   * @param alias the alias for the key
   * @return key for this alias
   */
  public @Tainted byte @Tainted [] getSecretKey(@Tainted Credentials this, @Tainted Text alias) {
    return secretKeysMap.get(alias);
  }
  
  /**
   * Returns the Token object for the alias
   * @param alias the alias for the Token
   * @return token for this alias
   */
  public @Tainted Token<@Tainted ? extends @Tainted TokenIdentifier> getToken(@Tainted Credentials this, @Tainted Text alias) {
    return tokenMap.get(alias);
  }
  
  /**
   * Add a token in the storage (in memory)
   * @param alias the alias for the key
   * @param t the token object
   */
  public void addToken(@Tainted Credentials this, @Tainted Text alias, @Tainted Token<@Tainted ? extends @Tainted TokenIdentifier> t) {
    if (t != null) {
      tokenMap.put(alias, t);
    } else {
      LOG.warn("Null token ignored for " + alias);
    }
  }
  
  /**
   * Return all the tokens in the in-memory map
   */
  public @Tainted Collection<@Tainted Token<@Tainted ? extends @Tainted TokenIdentifier>> getAllTokens(@Tainted Credentials this) {
    return tokenMap.values();
  }
  
  /**
   * @return number of Tokens in the in-memory map
   */
  public @Tainted int numberOfTokens(@Tainted Credentials this) {
    return tokenMap.size();
  }
  
  /**
   * @return number of keys in the in-memory map
   */
  public @Tainted int numberOfSecretKeys(@Tainted Credentials this) {
    return secretKeysMap.size();
  }
  
  /**
   * Set the key for an alias
   * @param alias the alias for the key
   * @param key the key bytes
   */
  public void addSecretKey(@Tainted Credentials this, @Tainted Text alias, @Tainted byte @Tainted [] key) {
    secretKeysMap.put(alias, key);
  }
 
  /**
   * Convenience method for reading a token storage file, and loading the Tokens
   * therein in the passed UGI
   * @param filename
   * @param conf
   * @throws IOException
   */
  public static @Tainted Credentials readTokenStorageFile(@Tainted Path filename, @Tainted Configuration conf)
  throws IOException {
    @Tainted
    FSDataInputStream in = null;
    @Tainted
    Credentials credentials = new @Tainted Credentials();
    try {
      in = filename.getFileSystem(conf).open(filename);
      credentials.readTokenStorageStream(in);
      in.close();
      return credentials;
    } catch(@Tainted IOException ioe) {
      throw new @Tainted IOException("Exception reading " + filename, ioe);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
  }

  /**
   * Convenience method for reading a token storage file, and loading the Tokens
   * therein in the passed UGI
   * @param filename
   * @param conf
   * @throws IOException
   */
  public static @Tainted Credentials readTokenStorageFile(@Tainted File filename, @Tainted Configuration conf)
      throws IOException {
    @Tainted
    DataInputStream in = null;
    @Tainted
    Credentials credentials = new @Tainted Credentials();
    try {
      in = new @Tainted DataInputStream(new @Tainted BufferedInputStream(
          new @Tainted FileInputStream(filename)));
      credentials.readTokenStorageStream(in);
      return credentials;
    } catch(@Tainted IOException ioe) {
      throw new @Tainted IOException("Exception reading " + filename, ioe);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
  }
  
  /**
   * Convenience method for reading a token storage file directly from a 
   * datainputstream
   */
  public void readTokenStorageStream(@Tainted Credentials this, @Tainted DataInputStream in) throws IOException {
    @Tainted
    byte @Tainted [] magic = new @Tainted byte @Tainted [TOKEN_STORAGE_MAGIC.length];
    in.readFully(magic);
    if (!Arrays.equals(magic, TOKEN_STORAGE_MAGIC)) {
      throw new @Tainted IOException("Bad header found in token storage.");
    }
    @Tainted
    byte version = in.readByte();
    if (version != TOKEN_STORAGE_VERSION) {
      throw new @Tainted IOException("Unknown version " + version + 
                            " in token storage.");
    }
    readFields(in);
  }
  
  private static final @Tainted byte @Tainted [] TOKEN_STORAGE_MAGIC = "HDTS".getBytes();
  private static final @Tainted byte TOKEN_STORAGE_VERSION = 0;
  
  public void writeTokenStorageToStream(@Tainted Credentials this, @Tainted DataOutputStream os)
    throws IOException {
    os.write(TOKEN_STORAGE_MAGIC);
    os.write(TOKEN_STORAGE_VERSION);
    write(os);
  }

  public void writeTokenStorageFile(@Tainted Credentials this, @Tainted Path filename, 
                                    @Tainted
                                    Configuration conf) throws IOException {
    @Tainted
    FSDataOutputStream os = filename.getFileSystem(conf).create(filename);
    writeTokenStorageToStream(os);
    os.close();
  }

  /**
   * Stores all the keys to DataOutput
   * @param out
   * @throws IOException
   */
  @Override
  public void write(@Tainted Credentials this, @Tainted DataOutput out) throws IOException {
    // write out tokens first
    WritableUtils.writeVInt(out, tokenMap.size());
    for(Map.@Tainted Entry<@Tainted Text, 
        @Tainted
        Token<@Tainted ? extends @Tainted TokenIdentifier>> e: tokenMap.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
    
    // now write out secret keys
    WritableUtils.writeVInt(out, secretKeysMap.size());
    for(Map.@Tainted Entry<@Tainted Text, @Tainted byte @Tainted []> e : secretKeysMap.entrySet()) {
      e.getKey().write(out);
      WritableUtils.writeVInt(out, e.getValue().length);
      out.write(e.getValue());
    }
  }
  
  /**
   * Loads all the keys
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(@Tainted Credentials this, @Tainted DataInput in) throws IOException {
    secretKeysMap.clear();
    tokenMap.clear();
    
    @Tainted
    int size = WritableUtils.readVInt(in);
    for(@Tainted int i=0; i<size; i++) {
      @Tainted
      Text alias = new @Tainted Text();
      alias.readFields(in);
      @Tainted
      Token<@Tainted ? extends @Tainted TokenIdentifier> t = new @Tainted Token<@Tainted TokenIdentifier>();
      t.readFields(in);
      tokenMap.put(alias, t);
    }
    
    size = WritableUtils.readVInt(in);
    for(@Tainted int i=0; i<size; i++) {
      @Tainted
      Text alias = new @Tainted Text();
      alias.readFields(in);
      @Tainted
      int len = WritableUtils.readVInt(in);
      @Tainted
      byte @Tainted [] value = new @Tainted byte @Tainted [len];
      in.readFully(value);
      secretKeysMap.put(alias, value);
    }
  }
 
  /**
   * Copy all of the credentials from one credential object into another.
   * Existing secrets and tokens are overwritten.
   * @param other the credentials to copy
   */
  public void addAll(@Tainted Credentials this, @Tainted Credentials other) {
    addAll(other, true);
  }

  /**
   * Copy all of the credentials from one credential object into another.
   * Existing secrets and tokens are not overwritten.
   * @param other the credentials to copy
   */
  public void mergeAll(@Tainted Credentials this, @Tainted Credentials other) {
    addAll(other, false);
  }

  private void addAll(@Tainted Credentials this, @Tainted Credentials other, @Tainted boolean overwrite) {
    for(Map.@Tainted Entry<@Tainted Text, @Tainted byte @Tainted []> secret: other.secretKeysMap.entrySet()) {
      @Tainted
      Text key = secret.getKey();
      if (!secretKeysMap.containsKey(key) || overwrite) {
        secretKeysMap.put(key, secret.getValue());
      }
    }
    for(Map.@Tainted Entry<@Tainted Text, @Tainted Token<@Tainted ? extends @Tainted TokenIdentifier>> token: other.tokenMap.entrySet()){
      @Tainted
      Text key = token.getKey();
      if (!tokenMap.containsKey(key) || overwrite) {
        tokenMap.put(key, token.getValue());
      }
    }
  }
}
