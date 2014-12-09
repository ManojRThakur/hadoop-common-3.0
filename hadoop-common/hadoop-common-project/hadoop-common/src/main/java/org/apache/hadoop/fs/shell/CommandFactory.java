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

package org.apache.hadoop.fs.shell;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** class to search for and register commands */

@InterfaceAudience.Private
@InterfaceStability.Unstable

public class CommandFactory extends @Tainted Configured implements @Tainted Configurable {
  private @Tainted Map<@Tainted String, @Tainted Class<@Tainted ? extends @Tainted Command>> classMap =
    new @Tainted HashMap<@Tainted String, @Tainted Class<@Tainted ? extends @Tainted Command>>();

  private @Tainted Map<@Tainted String, @Tainted Command> objectMap =
    new @Tainted HashMap<@Tainted String, @Tainted Command>();

  /** Factory constructor for commands */
  public @Tainted CommandFactory() {
    this(null);
  }
  
  /**
   * Factory constructor for commands
   * @param conf the hadoop configuration
   */
  public @Tainted CommandFactory(@Tainted Configuration conf) {
    super(conf);
  }

  /**
   * Invokes "static void registerCommands(CommandFactory)" on the given class.
   * This method abstracts the contract between the factory and the command
   * class.  Do not assume that directly invoking registerCommands on the
   * given class will have the same effect.
   * @param registrarClass class to allow an opportunity to register
   */
  public void registerCommands(@Tainted CommandFactory this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> registrarClass) {
    try {
      registrarClass.getMethod(
          "registerCommands", CommandFactory.class
      ).invoke(null, this);
    } catch (@Tainted Exception e) {
      throw new @Tainted RuntimeException(StringUtils.stringifyException(e));
    }
  }

  /**
   * Register the given class as handling the given list of command
   * names.
   * @param cmdClass the class implementing the command names
   * @param names one or more command names that will invoke this class
   */
  public void addClass(@Tainted CommandFactory this, @Tainted Class<@Tainted ? extends @Tainted Command> cmdClass, @Tainted String @Tainted ... names) {
    for (@Tainted String name : names) classMap.put(name, cmdClass);
  }
  
  /**
   * Register the given object as handling the given list of command
   * names.  Avoid calling this method and use
   * {@link #addClass(Class, String...)} whenever possible to avoid
   * startup overhead from excessive command object instantiations.  This
   * method is intended only for handling nested non-static classes that
   * are re-usable.  Namely -help/-usage.
   * @param cmdObject the object implementing the command names
   * @param names one or more command names that will invoke this class
   */
  public void addObject(@Tainted CommandFactory this, @Tainted Command cmdObject, @Tainted String @Tainted ... names) {
    for (@Tainted String name : names) {
      objectMap.put(name, cmdObject);
      classMap.put(name, null); // just so it shows up in the list of commands
    }
  }

  /**
   * Returns an instance of the class implementing the given command.  The
   * class must have been registered via
   * {@link #addClass(Class, String...)}
   * @param cmd name of the command
   * @return instance of the requested command
   */
  public @Tainted Command getInstance(@Tainted CommandFactory this, @Tainted String cmd) {
    return getInstance(cmd, getConf());
  }

  /**
   * Get an instance of the requested command
   * @param cmdName name of the command to lookup
   * @param conf the hadoop configuration
   * @return the {@link Command} or null if the command is unknown
   */
  public @Tainted Command getInstance(@Tainted CommandFactory this, @Tainted String cmdName, @Tainted Configuration conf) {
    if (conf == null) throw new @Tainted NullPointerException("configuration is null");
    
    @Tainted
    Command instance = objectMap.get(cmdName);
    if (instance == null) {
      @Tainted
      Class<@Tainted ? extends @Tainted Command> cmdClass = classMap.get(cmdName);
      if (cmdClass != null) {
        instance = ReflectionUtils.newInstance(cmdClass, conf);
        instance.setName(cmdName);
      }
    }
    return instance;
  }
  
  /**
   * Gets all of the registered commands
   * @return a sorted list of command names
   */
  public @Tainted String @Tainted [] getNames(@Tainted CommandFactory this) {
    @Tainted
    String @Tainted [] names = classMap.keySet().toArray(new @Tainted String @Tainted [0]);
    Arrays.sort(names);
    return names;
  }
}
