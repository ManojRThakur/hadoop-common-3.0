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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class returns build information about Hadoop components.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class VersionInfo {
  private static final @Tainted Log LOG = LogFactory.getLog(VersionInfo.class);

  private @Tainted Properties info;

  protected @Tainted VersionInfo(@Tainted String component) {
    info = new @Tainted Properties();
    @Tainted
    String versionInfoFile = component + "-version-info.properties";
    try {
      @Tainted
      InputStream is = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(versionInfoFile);
      if (is == null) {
        throw new @Tainted IOException("Resource not found");
      }
      info.load(is);
    } catch (@Tainted IOException ex) {
      LogFactory.getLog(getClass()).warn("Could not read '" + 
        versionInfoFile + "', " + ex.toString(), ex);
    }
  }

  protected @Tainted String _getVersion(@Tainted VersionInfo this) {
    return info.getProperty("version", "Unknown");
  }

  protected @Tainted String _getRevision(@Tainted VersionInfo this) {
    return info.getProperty("revision", "Unknown");
  }

  protected @Tainted String _getBranch(@Tainted VersionInfo this) {
    return info.getProperty("branch", "Unknown");
  }

  protected @Tainted String _getDate(@Tainted VersionInfo this) {
    return info.getProperty("date", "Unknown");
  }

  protected @Tainted String _getUser(@Tainted VersionInfo this) {
    return info.getProperty("user", "Unknown");
  }

  protected @Tainted String _getUrl(@Tainted VersionInfo this) {
    return info.getProperty("url", "Unknown");
  }

  protected @Tainted String _getSrcChecksum(@Tainted VersionInfo this) {
    return info.getProperty("srcChecksum", "Unknown");
  }

  protected @Tainted String _getBuildVersion(@Tainted VersionInfo this){
    return getVersion() +
      " from " + _getRevision() +
      " by " + _getUser() +
      " source checksum " + _getSrcChecksum();
  }

  protected @Tainted String _getProtocVersion(@Tainted VersionInfo this) {
    return info.getProperty("protocVersion", "Unknown");
  }

  private static @Tainted VersionInfo COMMON_VERSION_INFO = new @Tainted VersionInfo("common");
  /**
   * Get the Hadoop version.
   * @return the Hadoop version string, eg. "0.6.3-dev"
   */
  public static @Tainted String getVersion() {
    return COMMON_VERSION_INFO._getVersion();
  }
  
  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static @Tainted String getRevision() {
    return COMMON_VERSION_INFO._getRevision();
  }

  /**
   * Get the branch on which this originated.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  public static @Tainted String getBranch() {
    return COMMON_VERSION_INFO._getBranch();
  }

  /**
   * The date that Hadoop was compiled.
   * @return the compilation date in unix date format
   */
  public static @Tainted String getDate() {
    return COMMON_VERSION_INFO._getDate();
  }
  
  /**
   * The user that compiled Hadoop.
   * @return the username of the user
   */
  public static @Tainted String getUser() {
    return COMMON_VERSION_INFO._getUser();
  }
  
  /**
   * Get the subversion URL for the root Hadoop directory.
   */
  public static @Tainted String getUrl() {
    return COMMON_VERSION_INFO._getUrl();
  }

  /**
   * Get the checksum of the source files from which Hadoop was
   * built.
   **/
  public static @Tainted String getSrcChecksum() {
    return COMMON_VERSION_INFO._getSrcChecksum();
  }

  /**
   * Returns the buildVersion which includes version, 
   * revision, user and date. 
   */
  public static @Tainted String getBuildVersion(){
    return COMMON_VERSION_INFO._getBuildVersion();
  }

  /**
   * Returns the protoc version used for the build.
   */
  public static @Tainted String getProtocVersion(){
    return COMMON_VERSION_INFO._getProtocVersion();
  }

  public static void main(@Tainted String @Tainted [] args) {
    LOG.debug("version: "+ getVersion());
    System.out.println("Hadoop " + getVersion());
    System.out.println("Subversion " + getUrl() + " -r " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("Compiled with protoc " + getProtocVersion());
    System.out.println("From source with checksum " + getSrcChecksum());
    System.out.println("This command was run using " + 
        ClassUtil.findContainingJar(VersionInfo.class));
  }
}
