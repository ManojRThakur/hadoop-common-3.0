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
import java.io.*;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.zlib.*;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

/**
 * This class creates gzip compressors/decompressors. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GzipCodec extends @Tainted DefaultCodec {
  /**
   * A bridge that wraps around a DeflaterOutputStream to make it 
   * a CompressionOutputStream.
   */
  @InterfaceStability.Evolving
  protected static class GzipOutputStream extends @Tainted CompressorStream {

    private static class ResetableGZIPOutputStream extends @Tainted GZIPOutputStream {
      private static final @Tainted int TRAILER_SIZE = 8;
      public static final @Tainted String JVMVersion= System.getProperty("java.version");
      private static final @Tainted boolean HAS_BROKEN_FINISH =
          (IBM_JAVA && JVMVersion.contains("1.6.0"));

      public @Tainted ResetableGZIPOutputStream(@Tainted OutputStream out) throws IOException {
        super(out);
      }

      public void resetState(GzipCodec.GzipOutputStream.@Tainted ResetableGZIPOutputStream this) throws IOException {
        def.reset();
      }

      /**
       * Override this method for HADOOP-8419.
       * Override because IBM implementation calls def.end() which
       * causes problem when reseting the stream for reuse.
       *
       */
      @Override
      public void finish(GzipCodec.GzipOutputStream.@Tainted ResetableGZIPOutputStream this) throws IOException {
        if (HAS_BROKEN_FINISH) {
          if (!def.finished()) {
            def.finish();
            while (!def.finished()) {
              @Tainted
              int i = def.deflate(this.buf, 0, this.buf.length);
              if ((def.finished()) && (i <= this.buf.length - TRAILER_SIZE)) {
                writeTrailer(this.buf, i);
                i += TRAILER_SIZE;
                out.write(this.buf, 0, i);

                return;
              }
              if (i > 0) {
                out.write(this.buf, 0, i);
              }
            }

            @Tainted
            byte @Tainted [] arrayOfByte = new @Tainted byte @Tainted [TRAILER_SIZE];
            writeTrailer(arrayOfByte, 0);
            out.write(arrayOfByte);
          }
        } else {
          super.finish();
        }
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeTrailer(GzipCodec.GzipOutputStream.@Tainted ResetableGZIPOutputStream this, @Tainted byte @Tainted [] paramArrayOfByte, @Tainted int paramInt)
        throws IOException {
        writeInt((@Tainted int)this.crc.getValue(), paramArrayOfByte, paramInt);
        writeInt(this.def.getTotalIn(), paramArrayOfByte, paramInt + 4);
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeInt(GzipCodec.GzipOutputStream.@Tainted ResetableGZIPOutputStream this, @Tainted int paramInt1, @Tainted byte @Tainted [] paramArrayOfByte, @Tainted int paramInt2)
        throws IOException {
        writeShort(paramInt1 & 0xFFFF, paramArrayOfByte, paramInt2);
        writeShort(paramInt1 >> 16 & 0xFFFF, paramArrayOfByte, paramInt2 + 2);
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeShort(GzipCodec.GzipOutputStream.@Tainted ResetableGZIPOutputStream this, @Tainted int paramInt1, @Tainted byte @Tainted [] paramArrayOfByte, @Tainted int paramInt2)
        throws IOException {
        paramArrayOfByte[paramInt2] = (@Tainted byte)(paramInt1 & 0xFF);
        paramArrayOfByte[(paramInt2 + 1)] = (@Tainted byte)(paramInt1 >> 8 & 0xFF);
      }
    }

    public @Tainted GzipOutputStream(@Tainted OutputStream out) throws IOException {
      super(new @Tainted ResetableGZIPOutputStream(out));
    }
    
    /**
     * Allow children types to put a different type in here.
     * @param out the Deflater stream to use
     */
    protected @Tainted GzipOutputStream(@Tainted CompressorStream out) {
      super(out);
    }
    
    @Override
    public void close(GzipCodec.@Tainted GzipOutputStream this) throws IOException {
      out.close();
    }
    
    @Override
    public void flush(GzipCodec.@Tainted GzipOutputStream this) throws IOException {
      out.flush();
    }
    
    @Override
    public void write(GzipCodec.@Tainted GzipOutputStream this, @Tainted int b) throws IOException {
      out.write(b);
    }
    
    @Override
    public void write(GzipCodec.@Tainted GzipOutputStream this, @Tainted byte @Tainted [] data, @Tainted int offset, @Tainted int length) 
      throws IOException {
      out.write(data, offset, length);
    }
    
    @Override
    public void finish(GzipCodec.@Tainted GzipOutputStream this) throws IOException {
      ((@Tainted ResetableGZIPOutputStream) out).finish();
    }

    @Override
    public void resetState(GzipCodec.@Tainted GzipOutputStream this) throws IOException {
      ((@Tainted ResetableGZIPOutputStream) out).resetState();
    }
  }

  @Override
  public @Tainted CompressionOutputStream createOutputStream(@Tainted GzipCodec this, @Tainted OutputStream out) 
    throws IOException {
    return (ZlibFactory.isNativeZlibLoaded(conf)) ?
               new @Tainted CompressorStream(out, createCompressor(),
                                    conf.getInt("io.file.buffer.size", 4*1024)) :
               new @Tainted GzipOutputStream(out);
  }
  
  @Override
  public @Tainted CompressionOutputStream createOutputStream(@Tainted GzipCodec this, @Tainted OutputStream out, 
                                                    @Tainted
                                                    Compressor compressor) 
  throws IOException {
    return (compressor != null) ?
               new @Tainted CompressorStream(out, compressor,
                                    conf.getInt("io.file.buffer.size", 
                                                4*1024)) :
               createOutputStream(out);
  }

  @Override
  public @Tainted Compressor createCompressor(@Tainted GzipCodec this) {
    return (ZlibFactory.isNativeZlibLoaded(conf))
      ? new @Tainted GzipZlibCompressor(conf)
      : null;
  }

  @Override
  public @Tainted Class<@Tainted ? extends @Tainted Compressor> getCompressorType(@Tainted GzipCodec this) {
    return ZlibFactory.isNativeZlibLoaded(conf)
      ? GzipZlibCompressor.class
      : null;
  }

  @Override
  public @Tainted CompressionInputStream createInputStream(@Tainted GzipCodec this, @Tainted InputStream in)
  throws IOException {
    return createInputStream(in, null);
  }

  @Override
  public @Tainted CompressionInputStream createInputStream(@Tainted GzipCodec this, @Tainted InputStream in,
                                                  @Tainted
                                                  Decompressor decompressor)
  throws IOException {
    if (decompressor == null) {
      decompressor = createDecompressor();  // always succeeds (or throws)
    }
    return new @Tainted DecompressorStream(in, decompressor,
                                  conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public @Tainted Decompressor createDecompressor(@Tainted GzipCodec this) {
    return (ZlibFactory.isNativeZlibLoaded(conf))
      ? new @Tainted GzipZlibDecompressor()
      : new @Tainted BuiltInGzipDecompressor();
  }

  @Override
  public @Tainted Class<@Tainted ? extends @Tainted Decompressor> getDecompressorType(@Tainted GzipCodec this) {
    return ZlibFactory.isNativeZlibLoaded(conf)
      ? GzipZlibDecompressor.class
      : BuiltInGzipDecompressor.class;
  }

  @Override
  public @Tainted String getDefaultExtension(@Tainted GzipCodec this) {
    return ".gz";
  }

  static final class GzipZlibCompressor extends @Tainted ZlibCompressor {
    public @Tainted GzipZlibCompressor() {
      super(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
          ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
          ZlibCompressor.CompressionHeader.GZIP_FORMAT, 64*1024);
    }
    
    public @Tainted GzipZlibCompressor(@Tainted Configuration conf) {
      super(ZlibFactory.getCompressionLevel(conf),
           ZlibFactory.getCompressionStrategy(conf),
           ZlibCompressor.CompressionHeader.GZIP_FORMAT,
           64 * 1024);
    }
  }

  static final class GzipZlibDecompressor extends @Tainted ZlibDecompressor {
    public @Tainted GzipZlibDecompressor() {
      super(ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB, 64*1024);
    }
  }

}
