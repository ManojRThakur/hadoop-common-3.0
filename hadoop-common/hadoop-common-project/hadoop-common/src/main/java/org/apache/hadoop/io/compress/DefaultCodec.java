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

package org.apache.hadoop.io.compress;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DefaultCodec implements @Tainted Configurable, @Tainted CompressionCodec {
  private static final @Tainted Log LOG = LogFactory.getLog(DefaultCodec.class);
  
  @Tainted
  Configuration conf;

  @Override
  public void setConf(@Tainted DefaultCodec this, @Tainted Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public @Tainted Configuration getConf(@Tainted DefaultCodec this) {
    return conf;
  }
  
  @Override
  public @Tainted CompressionOutputStream createOutputStream(@Tainted DefaultCodec this, @Tainted OutputStream out) 
  throws IOException {
    // This may leak memory if called in a loop. The createCompressor() call
    // may cause allocation of an untracked direct-backed buffer if native
    // libs are being used (even if you close the stream).  A Compressor
    // object should be reused between successive calls.
    LOG.warn("DefaultCodec.createOutputStream() may leak memory. "
        + "Create a compressor first.");
    return new @Tainted CompressorStream(out, createCompressor(), 
                                conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public @Tainted CompressionOutputStream createOutputStream(@Tainted DefaultCodec this, @Tainted OutputStream out, 
                                                    @Tainted
                                                    Compressor compressor) 
  throws IOException {
    return new @Tainted CompressorStream(out, compressor, 
                                conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public @Tainted Class<@Tainted ? extends @Tainted Compressor> getCompressorType(@Tainted DefaultCodec this) {
    return ZlibFactory.getZlibCompressorType(conf);
  }

  @Override
  public @Tainted Compressor createCompressor(@Tainted DefaultCodec this) {
    return ZlibFactory.getZlibCompressor(conf);
  }

  @Override
  public @Tainted CompressionInputStream createInputStream(@Tainted DefaultCodec this, @Tainted InputStream in) 
  throws IOException {
    return new @Tainted DecompressorStream(in, createDecompressor(),
                                  conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public @Tainted CompressionInputStream createInputStream(@Tainted DefaultCodec this, @Tainted InputStream in, 
                                                  @Tainted
                                                  Decompressor decompressor) 
  throws IOException {
    return new @Tainted DecompressorStream(in, decompressor, 
                                  conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public @Tainted Class<@Tainted ? extends @Tainted Decompressor> getDecompressorType(@Tainted DefaultCodec this) {
    return ZlibFactory.getZlibDecompressorType(conf);
  }

  @Override
  public @Tainted Decompressor createDecompressor(@Tainted DefaultCodec this) {
    return ZlibFactory.getZlibDecompressor(conf);
  }
  
  @Override
  public @Tainted String getDefaultExtension(@Tainted DefaultCodec this) {
    return ".deflate";
  }

}
