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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A driver that is used to run programs added to it
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class ProgramDriver {
    
  /**
   * A description of a program based on its class and a 
   * human-readable description.
   * @date april 2006
   */
  @Tainted
  Map<@Tainted String, @Tainted ProgramDescription> programs;
     
  public @Tainted ProgramDriver(){
    programs = new @Tainted TreeMap<@Tainted String, @Tainted ProgramDescription>();
  }
     
  static private class ProgramDescription {
	
    static final @Tainted Class<?> @Tainted [] paramTypes = new Class<?> @Tainted [] {String[].class};
	
    /**
     * Create a description of an example program.
     * @param mainClass the class with the main for the example program
     * @param description a string to display to the user in help messages
     * @throws SecurityException if we can't use reflection
     * @throws NoSuchMethodException if the class doesn't have a main method
     */
    public @Tainted ProgramDescription(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> mainClass, 
                              @Tainted
                              String description)
      throws SecurityException, NoSuchMethodException {
      this.main = mainClass.getMethod("main", paramTypes);
      this.description = description;
    }
	
    /**
     * Invoke the example application with the given arguments
     * @param args the arguments for the application
     * @throws Throwable The exception thrown by the invoked method
     */
    public void invoke(ProgramDriver.@Tainted ProgramDescription this, @Tainted String @Tainted [] args)
      throws Throwable {
      try {
        main.invoke(null, new @Tainted Object @Tainted []{args});
      } catch (@Tainted InvocationTargetException except) {
        throw except.getCause();
      }
    }
	
    public @Tainted String getDescription(ProgramDriver.@Tainted ProgramDescription this) {
      return description;
    }
	
    private @Tainted Method main;
    private @Tainted String description;
  }
    
  private static void printUsage(@Tainted Map<@Tainted String, @Tainted ProgramDescription> programs) {
    System.out.println("Valid program names are:");
    for(Map.@Tainted Entry<@Tainted String, @Tainted ProgramDescription> item : programs.entrySet()) {
      System.out.println("  " + item.getKey() + ": " +
                         item.getValue().getDescription());         
    } 
  }
    
  /**
   * This is the method that adds the classed to the repository
   * @param name The name of the string you want the class instance to be called with
   * @param mainClass The class that you want to add to the repository
   * @param description The description of the class
   * @throws NoSuchMethodException 
   * @throws SecurityException 
   */
  public void addClass (@Tainted ProgramDriver this, @Tainted String name, @Tainted Class mainClass, @Tainted String description) throws Throwable {
    programs.put(name , new @Tainted ProgramDescription(mainClass, description));
  }
    
  /**
   * This is a driver for the example programs.
   * It looks at the first command line argument and tries to find an
   * example program with that name.
   * If it is found, it calls the main method in that class with the rest 
   * of the command line arguments.
   * @param args The argument from the user. args[0] is the command to run.
   * @return -1 on error, 0 on success
   * @throws NoSuchMethodException 
   * @throws SecurityException 
   * @throws IllegalAccessException 
   * @throws IllegalArgumentException 
   * @throws Throwable Anything thrown by the example program's main
   */
  public @Tainted int run(@Tainted ProgramDriver this, @Tainted String @Tainted [] args)
    throws Throwable 
  {
    // Make sure they gave us a program name.
    if (args.length == 0) {
      System.out.println("An example program must be given as the" + 
                         " first argument.");
      printUsage(programs);
      return -1;
    }
	
    // And that it is good.
    @Tainted
    ProgramDescription pgm = programs.get(args[0]);
    if (pgm == null) {
      System.out.println("Unknown program '" + args[0] + "' chosen.");
      printUsage(programs);
      return -1;
    }
	
    // Remove the leading argument and call main
    @Tainted
    String @Tainted [] new_args = new @Tainted String @Tainted [args.length - 1];
    for(@Tainted int i=1; i < args.length; ++i) {
      new_args[i-1] = args[i];
    }
    pgm.invoke(new_args);
    return 0;
  }

  /**
   * API compatible with Hadoop 1.x
   */
  public void driver(@Tainted ProgramDriver this, @Tainted String @Tainted [] argv) throws Throwable {
    if (run(argv) == -1) {
      System.exit(-1);
    }
  }

}
