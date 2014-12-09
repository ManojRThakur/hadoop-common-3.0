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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parse the args of a command and check the format of args.
 */
public class CommandFormat {
  final @Tainted int minPar;
  final @Tainted int maxPar;
  final @Tainted Map<@Tainted String, @Tainted Boolean> options = new @Tainted HashMap<@Tainted String, @Tainted Boolean>();
  @Tainted
  boolean ignoreUnknownOpts = false;
  
  /**
   * @deprecated use replacement since name is an unused parameter
   * @param name of command, but never used
   * @param min see replacement
   * @param max see replacement
   * @param possibleOpt see replacement
   * @see #CommandFormat(int, int, String...)
   */
  @Deprecated
  public @Tainted CommandFormat(@Tainted String n, @Tainted int min, @Tainted int max, @Tainted String @Tainted ... possibleOpt) {
    this(min, max, possibleOpt);
  }
  
  /**
   * Simple parsing of command line arguments
   * @param min minimum arguments required
   * @param max maximum arguments permitted
   * @param possibleOpt list of the allowed switches
   */
  public @Tainted CommandFormat(@Tainted int min, @Tainted int max, @Tainted String @Tainted ... possibleOpt) {
    minPar = min;
    maxPar = max;
    for (@Tainted String opt : possibleOpt) {
      if (opt == null) {
        ignoreUnknownOpts = true;
      } else {
        options.put(opt, Boolean.FALSE);
      }
    }
  }

  /** Parse parameters starting from the given position
   * Consider using the variant that directly takes a List
   * 
   * @param args an array of input arguments
   * @param pos the position at which starts to parse
   * @return a list of parameters
   */
  public @Tainted List<@Tainted String> parse(@Tainted CommandFormat this, @Tainted String @Tainted [] args, @Tainted int pos) {
    @Tainted
    List<@Tainted String> parameters = new @Tainted ArrayList<@Tainted String>(Arrays.asList(args));
    parameters.subList(0, pos).clear();
    parse(parameters);
    return parameters;
  }

  /** Parse parameters from the given list of args.  The list is
   *  destructively modified to remove the options.
   * 
   * @param args as a list of input arguments
   */
  public void parse(@Tainted CommandFormat this, @Tainted List<@Tainted String> args) {
    @Tainted
    int pos = 0;
    while (pos < args.size()) {
      @Tainted
      String arg = args.get(pos);
      // stop if not an opt, or the stdin arg "-" is found
      if (!arg.startsWith("-") || arg.equals("-")) { 
        break;
      } else if (arg.equals("--")) { // force end of option processing
        args.remove(pos);
        break;
      }
      
      @Tainted
      String opt = arg.substring(1);
      if (options.containsKey(opt)) {
        args.remove(pos);
        options.put(opt, Boolean.TRUE);
      } else if (ignoreUnknownOpts) {
        pos++;
      } else {
        throw new @Tainted UnknownOptionException(arg);
      }
    }
    @Tainted
    int psize = args.size();
    if (psize < minPar) {
      throw new @Tainted NotEnoughArgumentsException(minPar, psize);
    }
    if (psize > maxPar) {
      throw new @Tainted TooManyArgumentsException(maxPar, psize);
    }
  }
  
  /** Return if the option is set or not
   * 
   * @param option String representation of an option
   * @return true is the option is set; false otherwise
   */
  public @Tainted boolean getOpt(@Tainted CommandFormat this, @Tainted String option) {
    return options.containsKey(option) ? options.get(option) : false;
  }
  
  /** Returns all the options that are set
   * 
   * @return Set<String> of the enabled options
   */
  public @Tainted Set<@Tainted String> getOpts(@Tainted CommandFormat this) {
    @Tainted
    Set<@Tainted String> optSet = new @Tainted HashSet<@Tainted String>();
    for (Map.@Tainted Entry<@Tainted String, @Tainted Boolean> entry : options.entrySet()) {
      if (entry.getValue()) {
        optSet.add(entry.getKey());
      }
    }
    return optSet;
  }
  
  /** Used when the arguments exceed their bounds 
   */
  public static abstract class IllegalNumberOfArgumentsException
  extends @Tainted IllegalArgumentException {
    private static final @Tainted long serialVersionUID = 0L;
    protected @Tainted int expected;
    protected @Tainted int actual;

    protected @Tainted IllegalNumberOfArgumentsException(@Tainted int want, @Tainted int got) {
      expected = want;
      actual = got;
    }

    @Override
    public @Tainted String getMessage(CommandFormat.@Tainted IllegalNumberOfArgumentsException this) {
      return "expected " + expected + " but got " + actual;
    }
  }

  /** Used when too many arguments are supplied to a command
   */
  public static class TooManyArgumentsException
  extends @Tainted IllegalNumberOfArgumentsException {
    private static final @Tainted long serialVersionUID = 0L;

    public @Tainted TooManyArgumentsException(@Tainted int expected, @Tainted int actual) {
      super(expected, actual);
    }

    @Override
    public @Tainted String getMessage(CommandFormat.@Tainted TooManyArgumentsException this) {
      return "Too many arguments: " + super.getMessage();
    }
  }
  
  /** Used when too few arguments are supplied to a command
   */
  public static class NotEnoughArgumentsException
  extends @Tainted IllegalNumberOfArgumentsException {
    private static final @Tainted long serialVersionUID = 0L;

    public @Tainted NotEnoughArgumentsException(@Tainted int expected, @Tainted int actual) {
      super(expected, actual);
    }

    @Override
    public @Tainted String getMessage(CommandFormat.@Tainted NotEnoughArgumentsException this) {
      return "Not enough arguments: " + super.getMessage();
    }
  }
  
  /** Used when an unsupported option is supplied to a command
   */
  public static class UnknownOptionException extends @Tainted IllegalArgumentException {
    private static final @Tainted long serialVersionUID = 0L;
    protected @Tainted String option = null;
    
    public @Tainted UnknownOptionException(@Tainted String unknownOption) {
      super("Illegal option " + unknownOption);
      option = unknownOption;
    }
    
    public @Tainted String getOption(CommandFormat.@Tainted UnknownOptionException this) {
      return option;
    }
  }
}
