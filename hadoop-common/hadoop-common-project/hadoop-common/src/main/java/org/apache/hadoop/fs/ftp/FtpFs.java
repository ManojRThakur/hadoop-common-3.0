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
package org.apache.hadoop.fs.ftp;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;

/**
 * The FtpFs implementation of AbstractFileSystem.
 * This impl delegates to the old FileSystem
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class FtpFs extends @Tainted DelegateToFileSystem {
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
  FtpFs(final @Tainted URI theUri, final @Tainted Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, new @Tainted FTPFileSystem(), conf, FsConstants.FTP_SCHEME, true);
  }
  
  @Override
  public @Tainted int getUriDefaultPort(@Tainted FtpFs this) {
    return FTP.DEFAULT_PORT;
  }
  
  @Override
  public @Tainted FsServerDefaults getServerDefaults(@Tainted FtpFs this) throws IOException {
    return FtpConfigKeys.getServerDefaults();
  }
}
