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
package org.apache.hadoop.fs;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Represents the network location of a block, information about the hosts
 * that contain block replicas, and other block metadata (E.g. the file
 * offset associated with the block, length, whether it is corrupt, etc).
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BlockLocation {
  private @Tainted String @Tainted [] hosts; // Datanode hostnames
  private @Tainted String @Tainted [] names; // Datanode IP:xferPort for accessing the block
  private @Tainted String @Tainted [] topologyPaths; // Full path name in network topology
  private @Tainted long offset;  // Offset of the block in the file
  private @Tainted long length;
  private @Tainted boolean corrupt;

  /**
   * Default Constructor
   */
  public @Tainted BlockLocation() {
    this(new @Tainted String @Tainted [0], new @Tainted String @Tainted [0],  0L, 0L);
  }

  /**
   * Constructor with host, name, offset and length
   */
  public @Tainted BlockLocation(@Tainted String @Tainted [] names, @Tainted String @Tainted [] hosts, @Tainted long offset, 
                       @Tainted
                       long length) {
    this(names, hosts, offset, length, false);
  }

  /**
   * Constructor with host, name, offset, length and corrupt flag
   */
  public @Tainted BlockLocation(@Tainted String @Tainted [] names, @Tainted String @Tainted [] hosts, @Tainted long offset, 
                       @Tainted
                       long length, @Tainted boolean corrupt) {
    if (names == null) {
      this.names = new @Tainted String @Tainted [0];
    } else {
      this.names = names;
    }
    if (hosts == null) {
      this.hosts = new @Tainted String @Tainted [0];
    } else {
      this.hosts = hosts;
    }
    this.offset = offset;
    this.length = length;
    this.topologyPaths = new @Tainted String @Tainted [0];
    this.corrupt = corrupt;
  }

  /**
   * Constructor with host, name, network topology, offset and length
   */
  public @Tainted BlockLocation(@Tainted String @Tainted [] names, @Tainted String @Tainted [] hosts, @Tainted String @Tainted [] topologyPaths,
                       @Tainted
                       long offset, @Tainted long length) {
    this(names, hosts, topologyPaths, offset, length, false);
  }

  /**
   * Constructor with host, name, network topology, offset, length 
   * and corrupt flag
   */
  public @Tainted BlockLocation(@Tainted String @Tainted [] names, @Tainted String @Tainted [] hosts, @Tainted String @Tainted [] topologyPaths,
                       @Tainted
                       long offset, @Tainted long length, @Tainted boolean corrupt) {
    this(names, hosts, offset, length, corrupt);
    if (topologyPaths == null) {
      this.topologyPaths = new @Tainted String @Tainted [0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  /**
   * Get the list of hosts (hostname) hosting this block
   */
  public @Tainted String @Tainted [] getHosts(@Tainted BlockLocation this) throws IOException {
    if (hosts == null || hosts.length == 0) {
      return new @Tainted String @Tainted [0];
    } else {
      return hosts;
    }
  }

  /**
   * Get the list of names (IP:xferPort) hosting this block
   */
  public @Tainted String @Tainted [] getNames(@Tainted BlockLocation this) throws IOException {
    if (names == null || names.length == 0) {
      return new @Tainted String @Tainted [0];
    } else {
      return names;
    }
  }

  /**
   * Get the list of network topology paths for each of the hosts.
   * The last component of the path is the "name" (IP:xferPort).
   */
  public @Tainted String @Tainted [] getTopologyPaths(@Tainted BlockLocation this) throws IOException {
    if (topologyPaths == null || topologyPaths.length == 0) {
      return new @Tainted String @Tainted [0];
    } else {
      return topologyPaths;
    }
  }
  
  /**
   * Get the start offset of file associated with this block
   */
  public @Tainted long getOffset(@Tainted BlockLocation this) {
    return offset;
  }
  
  /**
   * Get the length of the block
   */
  public @Tainted long getLength(@Tainted BlockLocation this) {
    return length;
  }

  /**
   * Get the corrupt flag.
   */
  public @Tainted boolean isCorrupt(@Tainted BlockLocation this) {
    return corrupt;
  }

  /**
   * Set the start offset of file associated with this block
   */
  public void setOffset(@Tainted BlockLocation this, @Tainted long offset) {
    this.offset = offset;
  }

  /**
   * Set the length of block
   */
  public void setLength(@Tainted BlockLocation this, @Tainted long length) {
    this.length = length;
  }

  /**
   * Set the corrupt flag.
   */
  public void setCorrupt(@Tainted BlockLocation this, @Tainted boolean corrupt) {
    this.corrupt = corrupt;
  }

  /**
   * Set the hosts hosting this block
   */
  public void setHosts(@Tainted BlockLocation this, @Tainted String @Tainted [] hosts) throws IOException {
    if (hosts == null) {
      this.hosts = new @Tainted String @Tainted [0];
    } else {
      this.hosts = hosts;
    }
  }

  /**
   * Set the names (host:port) hosting this block
   */
  public void setNames(@Tainted BlockLocation this, @Tainted String @Tainted [] names) throws IOException {
    if (names == null) {
      this.names = new @Tainted String @Tainted [0];
    } else {
      this.names = names;
    }
  }

  /**
   * Set the network topology paths of the hosts
   */
  public void setTopologyPaths(@Tainted BlockLocation this, @Tainted String @Tainted [] topologyPaths) throws IOException {
    if (topologyPaths == null) {
      this.topologyPaths = new @Tainted String @Tainted [0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  @Override
  public @Tainted String toString(@Tainted BlockLocation this) {
    @Tainted
    StringBuilder result = new @Tainted StringBuilder();
    result.append(offset);
    result.append(',');
    result.append(length);
    if (corrupt) {
      result.append("(corrupt)");
    }
    for(@Tainted String h: hosts) {
      result.append(',');
      result.append(h);
    }
    return result.toString();
  }
}