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
import java.security.Principal;

import javax.security.auth.login.LoginContext;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

/**
 * Save the full and short name of the user as a principal. This allows us to
 * have a single type that we always look for when picking up user names.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
class User implements @Tainted Principal {
  private final @Untainted String fullName;
  private final @Tainted String shortName;
  private volatile @Tainted AuthenticationMethod authMethod = null;
  private volatile @Tainted LoginContext login = null;
  private volatile @Tainted long lastLogin = 0;

  public @Tainted User(@Untainted String name) {
    this(name, null, null);
  }
  
  public @Tainted User(@Untainted String name, @Tainted AuthenticationMethod authMethod, @Tainted LoginContext login) {
    try {
      shortName = new @Tainted HadoopKerberosName(name).getShortName();
    } catch (@Tainted IOException ioe) {
      throw new @Tainted IllegalArgumentException("Illegal principal name " + name, ioe);
    }
    fullName = name;

    this.authMethod = authMethod;
    this.login = login;
  }

  /**
   * Get the full name of the user.
   */
  @Override
  public @Untainted String getName(@Tainted User this) {
    return fullName;
  }
  
  /**
   * Get the user name up to the first '/' or '@'
   * @return the leading part of the user name
   */
  public @Tainted String getShortName(@Tainted User this) {
    return shortName;
  }
  
  @Override
  public @Tainted boolean equals(@Tainted User this, @Tainted Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return ((fullName.equals(((@Tainted User) o).fullName)) && (authMethod == ((@Tainted User) o).authMethod));
    }
  }
  
  @Override
  public @Tainted int hashCode(@Tainted User this) {
    return fullName.hashCode();
  }
  
  @Override
  public @Tainted String toString(@Tainted User this) {
    return fullName;
  }

  public void setAuthenticationMethod(@Tainted User this, @Tainted AuthenticationMethod authMethod) {
    this.authMethod = authMethod;
  }

  public @Tainted AuthenticationMethod getAuthenticationMethod(@Tainted User this) {
    return authMethod;
  }
  
  /**
   * Returns login object
   * @return login
   */
  public @Tainted LoginContext getLogin(@Tainted User this) {
    return login;
  }
  
  /**
   * Set the login object
   * @param login
   */
  public void setLogin(@Tainted User this, @Tainted LoginContext login) {
    this.login = login;
  }
  
  /**
   * Set the last login time.
   * @param time the number of milliseconds since the beginning of time
   */
  public void setLastLogin(@Tainted User this, @Tainted long time) {
    lastLogin = time;
  }
  
  /**
   * Get the time of the last login.
   * @return the number of milliseconds since the beginning of time.
   */
  public @Tainted long getLastLogin(@Tainted User this) {
    return lastLogin;
  }
}
