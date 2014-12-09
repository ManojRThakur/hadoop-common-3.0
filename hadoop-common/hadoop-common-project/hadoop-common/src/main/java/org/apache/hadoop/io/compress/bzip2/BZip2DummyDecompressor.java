/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.io.compress.bzip2;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

import org.apache.hadoop.io.compress.Decompressor;

/**
 * This is a dummy decompressor for BZip2.
 */
public class BZip2DummyDecompressor implements @Tainted Decompressor {

  @Override
  public @Tainted int decompress(@Tainted BZip2DummyDecompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public void end(@Tainted BZip2DummyDecompressor this) {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public @Tainted boolean finished(@Tainted BZip2DummyDecompressor this) {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public @Tainted boolean needsDictionary(@Tainted BZip2DummyDecompressor this) {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public @Tainted boolean needsInput(@Tainted BZip2DummyDecompressor this) {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public @Tainted int getRemaining(@Tainted BZip2DummyDecompressor this) {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public void reset(@Tainted BZip2DummyDecompressor this) {
    // do nothing
  }

  @Override
  public void setDictionary(@Tainted BZip2DummyDecompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    throw new @Tainted UnsupportedOperationException();
  }

  @Override
  public void setInput(@Tainted BZip2DummyDecompressor this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) {
    throw new @Tainted UnsupportedOperationException();
  }

}
