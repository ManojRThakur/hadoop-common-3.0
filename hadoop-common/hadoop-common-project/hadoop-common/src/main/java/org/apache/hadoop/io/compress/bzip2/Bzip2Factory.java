/*
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

package org.apache.hadoop.io.compress.bzip2;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor;
import org.apache.hadoop.io.compress.bzip2.BZip2DummyCompressor;
import org.apache.hadoop.io.compress.bzip2.BZip2DummyDecompressor;

/**
 * A collection of factories to create the right 
 * bzip2 compressor/decompressor instances.
 * 
 */
public class Bzip2Factory {
  private static final @Tainted Log LOG = LogFactory.getLog(Bzip2Factory.class);

  private static @Tainted String bzip2LibraryName = "";
  private static @Tainted boolean nativeBzip2Loaded;
  
  /**
   * Check if native-bzip2 code is loaded & initialized correctly and 
   * can be loaded for this job.
   * 
   * @param conf configuration
   * @return <code>true</code> if native-bzip2 is loaded & initialized 
   *         and can be loaded for this job, else <code>false</code>
   */
  public static @Tainted boolean isNativeBzip2Loaded(@Tainted Configuration conf) {
    @Tainted
    String libname = conf.get("io.compression.codec.bzip2.library", 
                              "system-native");
    if (!bzip2LibraryName.equals(libname)) {
      nativeBzip2Loaded = false;
      bzip2LibraryName = libname;
      if (libname.equals("java-builtin")) {
        LOG.info("Using pure-Java version of bzip2 library");
      } else if (conf.getBoolean(
                CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY, 
                CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_DEFAULT) &&
          NativeCodeLoader.isNativeCodeLoaded()) {
        try {
          // Initialize the native library.
          Bzip2Compressor.initSymbols(libname);
          Bzip2Decompressor.initSymbols(libname);
          nativeBzip2Loaded = true;
          LOG.info("Successfully loaded & initialized native-bzip2 library " +
                   libname);
        } catch (@Tainted Throwable t) {
          LOG.warn("Failed to load/initialize native-bzip2 library " + 
                   libname + ", will use pure-Java version");
        }
      }
    }
    return nativeBzip2Loaded;
  }

  public static @Tainted String getLibraryName(@Tainted Configuration conf) {
    if (isNativeBzip2Loaded(conf)) {
      return Bzip2Compressor.getLibraryName();
    } else {
      return bzip2LibraryName;
    }
  }

  /**
   * Return the appropriate type of the bzip2 compressor. 
   * 
   * @param conf configuration
   * @return the appropriate type of the bzip2 compressor.
   */
  public static @Tainted Class<@Tainted ? extends @Tainted Compressor> 
  getBzip2CompressorType(@Tainted Configuration conf) {
    return isNativeBzip2Loaded(conf) ? 
      Bzip2Compressor.class : BZip2DummyCompressor.class;
  }
  
  /**
   * Return the appropriate implementation of the bzip2 compressor. 
   * 
   * @param conf configuration
   * @return the appropriate implementation of the bzip2 compressor.
   */
  public static @Tainted Compressor getBzip2Compressor(@Tainted Configuration conf) {
    return isNativeBzip2Loaded(conf)? 
      new @Tainted Bzip2Compressor(conf) : new @Tainted BZip2DummyCompressor();
  }

  /**
   * Return the appropriate type of the bzip2 decompressor. 
   * 
   * @param conf configuration
   * @return the appropriate type of the bzip2 decompressor.
   */
  public static @Tainted Class<@Tainted ? extends @Tainted Decompressor> 
  getBzip2DecompressorType(@Tainted Configuration conf) {
    return  isNativeBzip2Loaded(conf) ? 
      Bzip2Decompressor.class : BZip2DummyDecompressor.class;
  }
  
  /**
   * Return the appropriate implementation of the bzip2 decompressor. 
   * 
   * @param conf configuration
   * @return the appropriate implementation of the bzip2 decompressor.
   */
  public static @Tainted Decompressor getBzip2Decompressor(@Tainted Configuration conf) {
    return isNativeBzip2Loaded(conf) ? 
      new @Tainted Bzip2Decompressor() : new @Tainted BZip2DummyDecompressor();
  }

  public static void setBlockSize(@Tainted Configuration conf, @Tainted int blockSize) {
    conf.setInt("bzip2.compress.blocksize", blockSize);
  }

  public static @Tainted int getBlockSize(@Tainted Configuration conf) {
    return conf.getInt("bzip2.compress.blocksize", 
                       Bzip2Compressor.DEFAULT_BLOCK_SIZE);
  }

  public static void setWorkFactor(@Tainted Configuration conf, @Tainted int workFactor) {
    conf.setInt("bzip2.compress.workfactor", workFactor);
  }

  public static @Tainted int getWorkFactor(@Tainted Configuration conf) {
    return conf.getInt("bzip2.compress.workfactor", 
                       Bzip2Compressor.DEFAULT_WORK_FACTOR);
  }

}
