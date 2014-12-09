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
package org.apache.hadoop.security.authorize;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import org.checkerframework.checker.tainting.qual.PolyTainted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.conf.Configuration;

/**
 * Class representing a configured access control list.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class AccessControlList implements @Tainted Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
    (AccessControlList.class,
      new @Tainted WritableFactory() {
        @Override
        public @Tainted Writable newInstance() { return new @Tainted AccessControlList(); }
      });
  }

  // Indicates an ACL string that represents access to all users
  public static final @Untainted String WILDCARD_ACL_VALUE = "*";
  private static final @Tainted int INITIAL_CAPACITY = 256;

  // Set of users who are granted access.
  private @Tainted Set<@Untainted String> users;
  // Set of groups which are granted access
  private @Tainted Set<@Untainted String> groups;
  // Whether all users are granted access.
  private @Tainted boolean allAllowed;

  private @Tainted Groups groupsMapping = Groups.getUserToGroupsMappingService(new @Tainted Configuration());

  /**
   * This constructor exists primarily for AccessControlList to be Writable.
   */
  public @Tainted AccessControlList() {
  }

  /**
   * Construct a new ACL from a String representation of the same.
   * 
   * The String is a a comma separated list of users and groups.
   * The user list comes first and is separated by a space followed 
   * by the group list. For e.g. "user1,user2 group1,group2"
   * 
   * @param aclString String representation of the ACL
   */
  public @Tainted AccessControlList(@Untainted String aclString) {
    buildACL(aclString);
  }

  /**
   * Build ACL from the given string, format of the string is
   * user1,...,userN group1,...,groupN
   *
   * @param aclString build ACL from this string
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  private void buildACL(@Tainted AccessControlList this, @Untainted String aclString) {
    users = new @Tainted TreeSet<@Untainted String>();
    groups = new @Tainted TreeSet<@Untainted String>();
    if (isWildCardACLValue(aclString)) {
      allAllowed = true;
    } else {
      allAllowed = false;
      @Untainted String @Tainted [] userGroupStrings = (@Untainted String @Tainted []) aclString.split(" ", 2);

      if (userGroupStrings.length >= 1) {
        @Tainted
        List<@Untainted String> usersList = new @Tainted LinkedList<@Untainted String>(
          Arrays.asList(  (@Untainted String @Tainted []) userGroupStrings[0].split(",")) );
        cleanupList(usersList);
        addToSet(users, usersList);
      }
      
      if (userGroupStrings.length == 2) {
        @Tainted List<@Untainted String> groupsList = new @Tainted LinkedList<@Untainted String>(
          Arrays.asList( (@Untainted String @Tainted []) userGroupStrings[1].split(",")));
        cleanupList(groupsList);
        addToSet(groups, groupsList);
        groupsMapping.cacheGroupsAdd(groupsList);
      }
    }
  }
  
  /**
   * Checks whether ACL string contains wildcard
   *
   * @param aclString check this ACL string for wildcard
   * @return true if ACL string contains wildcard false otherwise
   */
  private @Tainted boolean isWildCardACLValue(@Tainted AccessControlList this, @Tainted String aclString) {
    if (aclString.contains(WILDCARD_ACL_VALUE) && 
        aclString.trim().equals(WILDCARD_ACL_VALUE)) {
      return true;
    }
    return false;
  }

  public @Tainted boolean isAllAllowed(@Tainted AccessControlList this) {
    return allAllowed;
  }
  
  /**
   * Add user to the names of users allowed for this service.
   * 
   * @param user
   *          The user name
   */
  public void addUser(@Tainted AccessControlList this, @Untainted String user) {
    if (isWildCardACLValue(user)) {
      throw new @Tainted IllegalArgumentException("User " + user + " can not be added");
    }
    if (!isAllAllowed()) {
      users.add(user);
    }
  }

  /**
   * Add group to the names of groups allowed for this service.
   * 
   * @param group
   *          The group name
   */
  public void addGroup(@Tainted AccessControlList this, @Untainted String group) {
    if (isWildCardACLValue(group)) {
      throw new @Tainted IllegalArgumentException("Group " + group + " can not be added");
    }
    if (!isAllAllowed()) {
      @Tainted
      List<@Untainted String> groupsList = new @Tainted LinkedList<@Untainted String>();
      groupsList.add(group);
      groupsMapping.cacheGroupsAdd(groupsList);
      groups.add(group);
    }
  }

  /**
   * Remove user from the names of users allowed for this service.
   * 
   * @param user
   *          The user name
   */
  public void removeUser(@Tainted AccessControlList this, @Tainted String user) {
    if (isWildCardACLValue(user)) {
      throw new @Tainted IllegalArgumentException("User " + user + " can not be removed");
    }
    if (!isAllAllowed()) {
      users.remove(user);
    }
  }

  /**
   * Remove group from the names of groups allowed for this service.
   * 
   * @param group
   *          The group name
   */
  public void removeGroup(@Tainted AccessControlList this, @Tainted String group) {
    if (isWildCardACLValue(group)) {
      throw new @Tainted IllegalArgumentException("Group " + group
          + " can not be removed");
    }
    if (!isAllAllowed()) {
      groups.remove(group);
    }
  }

  /**
   * Get the names of users allowed for this service.
   * @return the set of user names. the set must not be modified.
   */
  @Tainted
  Set<@Untainted String> getUsers(@Tainted AccessControlList this) {
    return users;
  }
  
  /**
   * Get the names of user groups allowed for this service.
   * @return the set of group names. the set must not be modified.
   */
  @Tainted
  Set<@Untainted String> getGroups(@Tainted AccessControlList this) {
    return groups;
  }

  public @Tainted boolean isUserAllowed(@Tainted AccessControlList this, @Tainted UserGroupInformation ugi) {
    if (allAllowed || users.contains(ugi.getShortUserName())) {
      return true;
    } else {
      for(@Tainted String group: ugi.getGroupNames()) {
        if (groups.contains(group)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Cleanup list, remove empty strings, trim leading/trailing spaces
   *
   * @param list clean this list
   */
  @SuppressWarnings("ostrusted:assignment.type.incompatible")
  private static final void cleanupList(@Tainted List<@PolyTainted String> list) {
    @Tainted
    ListIterator<@Tainted String> i = list.listIterator();
    while(i.hasNext()) {
      @Tainted
      String s = i.next();
      if(s.length() == 0) {
        i.remove();
      } else {
        s = s.trim();
        i.set(s);
      }
    }
  }

  /**
   * Add list to a set
   *
   * @param set add list to this set
   * @param list add items of this list to the set
   */
  @SuppressWarnings("ostrusted:arguments.type.incompatible")
  private static final void addToSet(@Tainted Set<@PolyTainted String> set, @Tainted List<@PolyTainted String> list) {
    for(@Tainted String s : list) {
      set.add(s);
    }
  }

  /**
   * Returns descriptive way of users and groups that are part of this ACL.
   * Use {@link #getAclString()} to get the exact String that can be given to
   * the constructor of AccessControlList to create a new instance.
   */
  @Override
  public @Tainted String toString(@Tainted AccessControlList this) {
    @Tainted
    String str = null;

    if (allAllowed) {
      str = "All users are allowed";
    }
    else if (users.isEmpty() && groups.isEmpty()) {
      str = "No users are allowed";
    }
    else {
      @Tainted
      String usersStr = null;
      @Tainted
      String groupsStr = null;
      if (!users.isEmpty()) {
        usersStr = users.toString();
      }
      if (!groups.isEmpty()) {
        groupsStr = groups.toString();
      }

      if (!users.isEmpty() && !groups.isEmpty()) {
        str = "Users " + usersStr + " and members of the groups "
            + groupsStr + " are allowed";
      }
      else if (!users.isEmpty()) {
        str = "Users " + usersStr + " are allowed";
      }
      else {// users is empty array and groups is nonempty
        str = "Members of the groups "
            + groupsStr + " are allowed";
      }
    }

    return str;
  }

  /**
   * Returns the access control list as a String that can be used for building a
   * new instance by sending it to the constructor of {@link AccessControlList}.
   */
  public @Tainted String getAclString(@Tainted AccessControlList this) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder(INITIAL_CAPACITY);
    if (allAllowed) {
      sb.append('*');
    }
    else {
      sb.append(getUsersString());
      sb.append(" ");
      sb.append(getGroupsString());
    }
    return sb.toString();
  }

  /**
   * Serializes the AccessControlList object
   */
  @Override
  public void write(@Tainted AccessControlList this, @Tainted DataOutput out) throws IOException {
    @Tainted
    String aclString = getAclString();
    Text.writeString(out, aclString);
  }

  /**
   * Deserializes the AccessControlList object
   */
  @Override
  //ostrusted, todo: check to see if this is ever called from a Writable reference, if so then we might not have a OsTrusted input
  //ostrusted, and we would need to do sanitation
  @SuppressWarnings( {"ostrusted:cast.unsafe", "override.param.invalid"} )
  public void readFields(@Tainted AccessControlList this, @Untainted DataInput in) throws IOException {
    @Untainted String aclString = (@Untainted String) Text.readString(in);
    buildACL(aclString);
  }

  /**
   * Returns comma-separated concatenated single String of the set 'users'
   *
   * @return comma separated list of users
   */
  private @Untainted String getUsersString(@Tainted AccessControlList this) {
    return getString(users);
  }

  /**
   * Returns comma-separated concatenated single String of the set 'groups'
   *
   * @return comma separated list of groups
   */
  private @Untainted String getGroupsString(@Tainted AccessControlList this) {
    return getString(groups);
  }

  /**
   * Returns comma-separated concatenated single String of all strings of
   * the given set
   *
   * @param strings set of strings to concatenate
   */
  @SuppressWarnings("ostrusted:return.type.incompatible")
  private @PolyTainted String getString(@Tainted AccessControlList this, @Tainted Set<@PolyTainted String> strings) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder(INITIAL_CAPACITY);
    @Tainted
    boolean first = true;
    for(@Tainted String str: strings) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      sb.append(str);
    }
    return sb.toString();
  }
}
