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
package org.apache.hadoop.security;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An interface for the implementation of a user-to-groups mapping service
 * used by {@link Groups}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface GroupMappingServiceProvider {
  
  /**
   * Get all various group memberships of a given user.
   * Returns EMPTY list in case of non-existing user
   * @param user User's name
   * @return group memberships of user
   * @throws IOException
   */
  public @Tainted List<@Untainted String> getGroups(@Tainted GroupMappingServiceProvider this, @Untainted String user) throws IOException;
  /**
   * Refresh the cache of groups and user mapping
   * @throws IOException
   */
  public void cacheGroupsRefresh(@Tainted GroupMappingServiceProvider this) throws IOException;
  /**
   * Caches the group user information
   * @param groups list of groups to add to cache
   * @throws IOException
   */
  public void cacheGroupsAdd(@Tainted GroupMappingServiceProvider this, @Tainted List<@Untainted String> groups) throws IOException;
}
