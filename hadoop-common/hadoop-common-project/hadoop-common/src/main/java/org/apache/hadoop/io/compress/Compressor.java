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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Specification of a stream-based 'compressor' which can be  
 * plugged into a {@link CompressionOutputStream} to compress data.
 * This is modelled after {@link java.util.zip.Deflater}
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Compressor {
  /**
   * Sets input data for compression. 
   * This should be called whenever #needsInput() returns 
   * <code>true</code> indicating that more input data is required.
   * 
   * @param b Input data
   * @param off Start offset
   * @param len Length
   */
  public void setInput(@Tainted Compressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len);
  
  /**
   * Returns true if the input data buffer is empty and 
   * #setInput() should be called to provide more input. 
   * 
   * @return <code>true</code> if the input data buffer is empty and 
   * #setInput() should be called in order to provide more input.
   */
  public @Tainted boolean needsInput(@Tainted Compressor this);
  
  /**
   * Sets preset dictionary for compression. A preset dictionary 
   * is used when the history buffer can be predetermined. 
   *
   * @param b Dictionary data bytes
   * @param off Start offset
   * @param len Length
   */
  public void setDictionary(@Tainted Compressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len);

  /**
   * Return number of uncompressed bytes input so far.
   */
  public @Tainted long getBytesRead(@Tainted Compressor this);

  /**
   * Return number of compressed bytes output so far.
   */
  public @Tainted long getBytesWritten(@Tainted Compressor this);

  /**
   * When called, indicates that compression should end
   * with the current contents of the input buffer.
   */
  public void finish(@Tainted Compressor this);
  
  /**
   * Returns true if the end of the compressed 
   * data output stream has been reached.
   * @return <code>true</code> if the end of the compressed
   * data output stream has been reached.
   */
  public @Tainted boolean finished(@Tainted Compressor this);
  
  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   * 
   * @param b Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  public @Tainted int compress(@Tainted Compressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException;
  
  /**
   * Resets compressor so that a new set of input data can be processed.
   */
  public void reset(@Tainted Compressor this);
  
  /**
   * Closes the compressor and discards any unprocessed input.
   */
  public void end(@Tainted Compressor this);

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration
   * 
   * @param conf Configuration from which new setting are fetched
   */
  public void reinit(@Tainted Compressor this, @Tainted Configuration conf);
}
