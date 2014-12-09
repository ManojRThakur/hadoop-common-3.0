/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Compression related stuff.
 */
final class Compression {
  static final @Tainted Log LOG = LogFactory.getLog(Compression.class);

  /**
   * Prevent the instantiation of class.
   */
  private @Tainted Compression() {
    // nothing
  }

  static class FinishOnFlushCompressionStream extends @Tainted FilterOutputStream {
    public @Tainted FinishOnFlushCompressionStream(@Tainted CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(Compression.@Tainted FinishOnFlushCompressionStream this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush(Compression.@Tainted FinishOnFlushCompressionStream this) throws IOException {
      @Tainted
      CompressionOutputStream cout = (@Tainted CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /**
   * Compression algorithms.
   */
  static enum Algorithm {

@Tainted  LZO(TFile.COMPRESSION_LZO) {
      private transient boolean checked = false;
      private static final String defaultClazz =
          "org.apache.hadoop.io.compress.LzoCodec";
      private transient CompressionCodec codec = null;

      @Override
      public synchronized boolean isSupported() {
        if (!checked) {
          checked = true;
          String extClazz =
              (conf.get(CONF_LZO_CLASS) == null ? System
                  .getProperty(CONF_LZO_CLASS) : null);
          String clazz = (extClazz != null) ? extClazz : defaultClazz;
          try {
            LOG.info("Trying to load Lzo codec class: " + clazz);
            codec =
                (CompressionCodec) ReflectionUtils.newInstance(Class
                    .forName(clazz), conf);
          } catch (ClassNotFoundException e) {
            // that is okay
          }
        }
        return codec != null;
      }

      @Override
      CompressionCodec getCodec() throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }
        InputStream bis1 = null;
        if (downStreamBufferSize > 0) {
          bis1 = new BufferedInputStream(downStream, downStreamBufferSize);
        } else {
          bis1 = downStream;
        }
        conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
        CompressionInputStream cis =
            codec.createInputStream(bis1, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }
    },


@Tainted  GZ(TFile.COMPRESSION_GZ) {
      private transient DefaultCodec codec;

      @Override
      CompressionCodec getCodec() {
        if (codec == null) {
          codec = new DefaultCodec();
          codec.setConf(conf);
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        // Set the internal buffer size to read from down stream.
        if (downStreamBufferSize > 0) {
          codec.getConf().setInt("io.file.buffer.size", downStreamBufferSize);
        }
        CompressionInputStream cis =
            codec.createInputStream(downStream, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        codec.getConf().setInt("io.file.buffer.size", 32 * 1024);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },


@Tainted  NONE(TFile.COMPRESSION_NONE) {
      @Override
      CompressionCodec getCodec() {
        return null;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedInputStream(downStream, downStreamBufferSize);
        }
        return downStream;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedOutputStream(downStream, downStreamBufferSize);
        }

        return downStream;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    };

    // We require that all compression related settings are configured
    // statically in the Configuration object.
    protected static final @Tainted Configuration conf = new @Tainted Configuration();
    private final @Tainted String compressName;
    // data input buffer size to absorb small reads from application.
    private static final @Tainted int DATA_IBUF_SIZE = 1 * 1024;
    // data output buffer size to absorb small writes from application.
    private static final @Tainted int DATA_OBUF_SIZE = 4 * 1024;
    public static final @Tainted String CONF_LZO_CLASS =
        "io.compression.codec.lzo.class";

    @Tainted
    Algorithm(@Tainted String name) {
      this.compressName = name;
    }

    abstract @Tainted CompressionCodec getCodec(Compression.@Tainted Algorithm this) throws IOException;

    public abstract @Tainted InputStream createDecompressionStream(
        Compression.@Tainted Algorithm this, @Tainted
        InputStream downStream, @Tainted Decompressor decompressor,
        @Tainted
        int downStreamBufferSize) throws IOException;

    public abstract @Tainted OutputStream createCompressionStream(
        Compression.@Tainted Algorithm this, @Tainted
        OutputStream downStream, @Tainted Compressor compressor, @Tainted int downStreamBufferSize)
        throws IOException;

    public abstract @Tainted boolean isSupported(Compression.@Tainted Algorithm this);

    public @Tainted Compressor getCompressor(Compression.@Tainted Algorithm this) throws IOException {
      @Tainted
      CompressionCodec codec = getCodec();
      if (codec != null) {
        @Tainted
        Compressor compressor = CodecPool.getCompressor(codec);
        if (compressor != null) {
          if (compressor.finished()) {
            // Somebody returns the compressor to CodecPool but is still using
            // it.
            LOG.warn("Compressor obtained from CodecPool already finished()");
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Got a compressor: " + compressor.hashCode());
            }
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * compressor is referenced after returned back to the codec pool.
           */
          compressor.reset();
        }
        return compressor;
      }
      return null;
    }

    public void returnCompressor(Compression.@Tainted Algorithm this, @Tainted Compressor compressor) {
      if (compressor != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Return a compressor: " + compressor.hashCode());
        }
        CodecPool.returnCompressor(compressor);
      }
    }

    public @Tainted Decompressor getDecompressor(Compression.@Tainted Algorithm this) throws IOException {
      @Tainted
      CompressionCodec codec = getCodec();
      if (codec != null) {
        @Tainted
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          if (decompressor.finished()) {
            // Somebody returns the decompressor to CodecPool but is still using
            // it.
            LOG.warn("Deompressor obtained from CodecPool already finished()");
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Got a decompressor: " + decompressor.hashCode());
            }
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * decompressor is referenced after returned back to the codec pool.
           */
          decompressor.reset();
        }
        return decompressor;
      }

      return null;
    }

    public void returnDecompressor(Compression.@Tainted Algorithm this, @Tainted Decompressor decompressor) {
      if (decompressor != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Returned a decompressor: " + decompressor.hashCode());
        }
        CodecPool.returnDecompressor(decompressor);
      }
    }

    public @Tainted String getName(Compression.@Tainted Algorithm this) {
      return compressName;
    }
  }

  static @Tainted Algorithm getCompressionAlgorithmByName(@Tainted String compressName) {
    @Tainted
    Algorithm @Tainted [] algos = Algorithm.class.getEnumConstants();

    for (@Tainted Algorithm a : algos) {
      if (a.getName().equals(compressName)) {
        return a;
      }
    }

    throw new @Tainted IllegalArgumentException(
        "Unsupported compression algorithm name: " + compressName);
  }

  static @Tainted String @Tainted [] getSupportedAlgorithms() {
    @Tainted
    Algorithm @Tainted [] algos = Algorithm.class.getEnumConstants();

    @Tainted
    ArrayList<@Tainted String> ret = new @Tainted ArrayList<@Tainted String>();
    for (@Tainted Algorithm a : algos) {
      if (a.isSupported()) {
        ret.add(a.getName());
      }
    }
    return ret.toArray(new @Tainted String @Tainted [ret.size()]);
  }
}
