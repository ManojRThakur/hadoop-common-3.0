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
package org.apache.hadoop.fs.local;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * The RawLocalFs implementation of AbstractFileSystem.
 *  This impl delegates to the old FileSystem
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class RawLocalFs extends @Tainted DelegateToFileSystem {

  @Tainted
  RawLocalFs(final @Tainted Configuration conf) throws IOException, URISyntaxException {
    this(FsConstants.LOCAL_FS_URI, conf);
  }
  
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   * 
   * @param theUri which must be that of localFs
   * @param conf
   * @throws IOException
   * @throws URISyntaxException 
   */
  @Tainted
  RawLocalFs(final @Tainted URI theUri, final @Tainted Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, new @Tainted RawLocalFileSystem(), conf, 
        FsConstants.LOCAL_FS_URI.getScheme(), false);
  }
  
  @Override
  public @Tainted int getUriDefaultPort(@Tainted RawLocalFs this) {
    return -1; // No default port for file:///
  }
  
  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted RawLocalFs this) throws IOException {
    return LocalConfigKeys.getServerDefaults();
  }

  @Override
  public @Tainted boolean isValidName(@Tainted RawLocalFs this, @Tainted String src) {
    // Different local file systems have different validation rules. Skip
    // validation here and just let the OS handle it. This is consistent with
    // RawLocalFileSystem.
    return true;
  }
}
