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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.lang.reflect.Array;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Contains utility methods for dealing with Java Generics. 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GenericsUtil {

  /**
   * Returns the Class object (of type <code>Class&lt;T&gt;</code>) of the  
   * argument of type <code>T</code>. 
   * @param <T> The type of the argument
   * @param t the object to get it class
   * @return <code>Class&lt;T&gt;</code>
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted Class<@Tainted T> getClass(@Tainted T t) {
    @SuppressWarnings("unchecked")
    @Tainted
    Class<@Tainted T> clazz = (@Tainted Class<@Tainted T>)t.getClass();
    return clazz;
  }

  /**
   * Converts the given <code>List&lt;T&gt;</code> to a an array of 
   * <code>T[]</code>.
   * @param c the Class object of the items in the list
   * @param list the list to convert
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T @Tainted [] toArray(@Tainted Class<@Tainted T> c, @Tainted List<@Tainted T> list)
  {
    @SuppressWarnings("unchecked")
    @Tainted
    T @Tainted [] ta= (@Tainted T @Tainted [])Array.newInstance(c, list.size());

    for (@Tainted int i= 0; i<list.size(); i++)
      ta[i]= list.get(i);
    return ta;
  }


  /**
   * Converts the given <code>List&lt;T&gt;</code> to a an array of 
   * <code>T[]</code>. 
   * @param list the list to convert
   * @throws ArrayIndexOutOfBoundsException if the list is empty. 
   * Use {@link #toArray(Class, List)} if the list may be empty.
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T @Tainted [] toArray(@Tainted List<@Tainted T> list) {
    return toArray(getClass(list.get(0)), list);
  }

}
