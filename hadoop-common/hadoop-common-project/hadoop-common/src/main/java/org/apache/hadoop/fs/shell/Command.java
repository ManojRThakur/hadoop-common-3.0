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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.util.StringUtils;

/**
 * An abstract class for the execution of a file system command
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

abstract public class Command extends @Tainted Configured {
  /** field name indicating the default name of the command */
  public static final @Tainted String COMMAND_NAME_FIELD = "NAME";
  /** field name indicating the command's usage switches and arguments format */
  public static final @Tainted String COMMAND_USAGE_FIELD = "USAGE";
  /** field name indicating the command's long description */
  public static final @Tainted String COMMAND_DESCRIPTION_FIELD = "DESCRIPTION";
    
  protected @Tainted String @Tainted [] args;
  protected @Tainted String name;
  protected @Tainted int exitCode = 0;
  protected @Tainted int numErrors = 0;
  protected @Tainted boolean recursive = false;
  private @Tainted int depth = 0;
  protected @Tainted ArrayList<@Tainted Exception> exceptions = new @Tainted ArrayList<@Tainted Exception>();

  private static final @Tainted Log LOG = LogFactory.getLog(Command.class);

  /** allows stdout to be captured if necessary */
  public @Tainted PrintStream out = System.out;
  /** allows stderr to be captured if necessary */
  public @Tainted PrintStream err = System.err;

  /** Constructor */
  protected @Tainted Command() {
    out = System.out;
    err = System.err;
  }
  
  /** Constructor */
  protected @Tainted Command(@Tainted Configuration conf) {
    super(conf);
  }
  
  /** @return the command's name excluding the leading character - */
  abstract public @Tainted String getCommandName(@Tainted Command this);
  
  protected void setRecursive(@Tainted Command this, @Tainted boolean flag) {
    recursive = flag;
  }
  
  protected @Tainted boolean isRecursive(@Tainted Command this) {
    return recursive;
  }

  protected @Tainted int getDepth(@Tainted Command this) {
    return depth;
  }
  
  /** 
   * Execute the command on the input path
   * 
   * @param path the input path
   * @throws IOException if any error occurs
   */
  abstract protected void run(@Tainted Command this, @Tainted Path path) throws IOException;
  
  /** 
   * For each source path, execute the command
   * 
   * @return 0 if it runs successfully; -1 if it fails
   */
  public @Tainted int runAll(@Tainted Command this) {
    @Tainted
    int exitCode = 0;
    for (@Tainted String src : args) {
      try {
        @Tainted
        PathData @Tainted [] srcs = PathData.expandAsGlob(src, getConf());
        for (@Tainted PathData s : srcs) {
          run(s.path);
        }
      } catch (@Tainted IOException e) {
        exitCode = -1;
        displayError(e);
      }
    }
    return exitCode;
  }

  /**
   * Invokes the command handler.  The default behavior is to process options,
   * expand arguments, and then process each argument.
   * <pre>
   * run
   * |-> {@link #processOptions(LinkedList)}
   * \-> {@link #processRawArguments(LinkedList)}
   *      |-> {@link #expandArguments(LinkedList)}
   *      |   \-> {@link #expandArgument(String)}*
   *      \-> {@link #processArguments(LinkedList)}
   *          |-> {@link #processArgument(PathData)}*
   *          |   |-> {@link #processPathArgument(PathData)}
   *          |   \-> {@link #processPaths(PathData, PathData...)}
   *          |        \-> {@link #processPath(PathData)}*
   *          \-> {@link #processNonexistentPath(PathData)}
   * </pre>
   * Most commands will chose to implement just
   * {@link #processOptions(LinkedList)} and {@link #processPath(PathData)}
   * 
   * @param argv the list of command line arguments
   * @return the exit code for the command
   * @throws IllegalArgumentException if called with invalid arguments
   */
  public @Tainted int run(@Tainted Command this, @Tainted String @Tainted ...argv) {
    @Tainted
    LinkedList<@Tainted String> args = new @Tainted LinkedList<@Tainted String>(Arrays.asList(argv));
    try {
      if (isDeprecated()) {
        displayWarning(
            "DEPRECATED: Please use '"+ getReplacementCommand() + "' instead.");
      }
      processOptions(args);
      processRawArguments(args);
    } catch (@Tainted CommandInterruptException e) {
      displayError("Interrupted");
      return 130;
    } catch (@Tainted IOException e) {
      displayError(e);
    }
    
    return (numErrors == 0) ? exitCode : exitCodeForError();
  }

  /**
   * The exit code to be returned if any errors occur during execution.
   * This method is needed to account for the inconsistency in the exit
   * codes returned by various commands.
   * @return a non-zero exit code
   */
  protected @Tainted int exitCodeForError(@Tainted Command this) { return 1; }
  
  /**
   * Must be implemented by commands to process the command line flags and
   * check the bounds of the remaining arguments.  If an
   * IllegalArgumentException is thrown, the FsShell object will print the
   * short usage of the command.
   * @param args the command line arguments
   * @throws IOException
   */
  protected void processOptions(@Tainted Command this, @Tainted LinkedList<@Tainted String> args) throws IOException {}

  /**
   * Allows commands that don't use paths to handle the raw arguments.
   * Default behavior is to expand the arguments via
   * {@link #expandArguments(LinkedList)} and pass the resulting list to
   * {@link #processArguments(LinkedList)} 
   * @param args the list of argument strings
   * @throws IOException
   */
  protected void processRawArguments(@Tainted Command this, @Tainted LinkedList<@Tainted String> args)
  throws IOException {
    processArguments(expandArguments(args));
  }

  /**
   *  Expands a list of arguments into {@link PathData} objects.  The default
   *  behavior is to call {@link #expandArgument(String)} on each element
   *  which by default globs the argument.  The loop catches IOExceptions,
   *  increments the error count, and displays the exception.
   * @param args strings to expand into {@link PathData} objects
   * @return list of all {@link PathData} objects the arguments
   * @throws IOException if anything goes wrong...
   */
  protected @Tainted LinkedList<@Tainted PathData> expandArguments(@Tainted Command this, @Tainted LinkedList<@Tainted String> args)
  throws IOException {
    @Tainted
    LinkedList<@Tainted PathData> expandedArgs = new @Tainted LinkedList<@Tainted PathData>();
    for (@Tainted String arg : args) {
      try {
        expandedArgs.addAll(expandArgument(arg));
      } catch (@Tainted IOException e) { // other exceptions are probably nasty
        displayError(e);
      }
    }
    return expandedArgs;
  }

  /**
   * Expand the given argument into a list of {@link PathData} objects.
   * The default behavior is to expand globs.  Commands may override to
   * perform other expansions on an argument.
   * @param arg string pattern to expand
   * @return list of {@link PathData} objects
   * @throws IOException if anything goes wrong...
   */
  protected @Tainted List<@Tainted PathData> expandArgument(@Tainted Command this, @Tainted String arg) throws IOException {
    @Tainted
    PathData @Tainted [] items = PathData.expandAsGlob(arg, getConf());
    if (items.length == 0) {
      // it's a glob that failed to match
      throw new @Tainted PathNotFoundException(arg);
    }
    return Arrays.asList(items);
  }

  /**
   *  Processes the command's list of expanded arguments.
   *  {@link #processArgument(PathData)} will be invoked with each item
   *  in the list.  The loop catches IOExceptions, increments the error
   *  count, and displays the exception.
   *  @param args a list of {@link PathData} to process
   *  @throws IOException if anything goes wrong... 
   */
  protected void processArguments(@Tainted Command this, @Tainted LinkedList<@Tainted PathData> args)
  throws IOException {
    for (@Tainted PathData arg : args) {
      try {
        processArgument(arg);
      } catch (@Tainted IOException e) {
        displayError(e);
      }
    }
  }

  /**
   * Processes a {@link PathData} item, calling
   * {@link #processPathArgument(PathData)} or
   * {@link #processNonexistentPath(PathData)} on each item.
   * @param item {@link PathData} item to process
   * @throws IOException if anything goes wrong...
   */
  protected void processArgument(@Tainted Command this, @Tainted PathData item) throws IOException {
    if (item.exists) {
      processPathArgument(item);
    } else {
      processNonexistentPath(item);
    }
  }

  /**
   *  This is the last chance to modify an argument before going into the
   *  (possibly) recursive {@link #processPaths(PathData, PathData...)}
   *  -> {@link #processPath(PathData)} loop.  Ex.  ls and du use this to
   *  expand out directories.
   *  @param item a {@link PathData} representing a path which exists
   *  @throws IOException if anything goes wrong... 
   */
  protected void processPathArgument(@Tainted Command this, @Tainted PathData item) throws IOException {
    // null indicates that the call is not via recursion, ie. there is
    // no parent directory that was expanded
    depth = 0;
    processPaths(null, item);
  }
  
  /**
   *  Provides a hook for handling paths that don't exist.  By default it
   *  will throw an exception.  Primarily overriden by commands that create
   *  paths such as mkdir or touch.
   *  @param item the {@link PathData} that doesn't exist
   *  @throws FileNotFoundException if arg is a path and it doesn't exist
   *  @throws IOException if anything else goes wrong... 
   */
  protected void processNonexistentPath(@Tainted Command this, @Tainted PathData item) throws IOException {
    throw new @Tainted PathNotFoundException(item.toString());
  }

  /**
   *  Iterates over the given expanded paths and invokes
   *  {@link #processPath(PathData)} on each element.  If "recursive" is true,
   *  will do a post-visit DFS on directories.
   *  @param parent if called via a recurse, will be the parent dir, else null
   *  @param items a list of {@link PathData} objects to process
   *  @throws IOException if anything goes wrong...
   */
  protected void processPaths(@Tainted Command this, @Tainted PathData parent, @Tainted PathData @Tainted ... items)
  throws IOException {
    // TODO: this really should be iterative
    for (@Tainted PathData item : items) {
      try {
        processPath(item);
        if (recursive && item.stat.isDirectory()) {
          recursePath(item);
        }
        postProcessPath(item);
      } catch (@Tainted IOException e) {
        displayError(e);
      }
    }
  }

  /**
   * Hook for commands to implement an operation to be applied on each
   * path for the command.  Note implementation of this method is optional
   * if earlier methods in the chain handle the operation.
   * @param item a {@link PathData} object
   * @throws RuntimeException if invoked but not implemented
   * @throws IOException if anything else goes wrong in the user-implementation
   */  
  protected void processPath(@Tainted Command this, @Tainted PathData item) throws IOException {
    throw new @Tainted RuntimeException("processPath() is not implemented");    
  }

  /**
   * Hook for commands to implement an operation to be applied on each
   * path for the command after being processed successfully
   * @param item a {@link PathData} object
   * @throws IOException if anything goes wrong...
   */
  protected void postProcessPath(@Tainted Command this, @Tainted PathData item) throws IOException {    
  }

  /**
   *  Gets the directory listing for a path and invokes
   *  {@link #processPaths(PathData, PathData...)}
   *  @param item {@link PathData} for directory to recurse into
   *  @throws IOException if anything goes wrong...
   */
  protected void recursePath(@Tainted Command this, @Tainted PathData item) throws IOException {
    try {
      depth++;
      processPaths(item, item.getDirectoryContents());
    } finally {
      depth--;
    }
  }

  /**
   * Display an exception prefaced with the command name.  Also increments
   * the error count for the command which will result in a non-zero exit
   * code.
   * @param e exception to display
   */
  public void displayError(@Tainted Command this, @Tainted Exception e) {
    // build up a list of exceptions that occurred
    exceptions.add(e);
    // use runtime so it rips up through the stack and exits out 
    if (e instanceof @Tainted InterruptedIOException) {
      throw new @Tainted CommandInterruptException();
    }
    
    @Tainted
    String errorMessage = e.getLocalizedMessage();
    if (errorMessage == null) {
      // this is an unexpected condition, so dump the whole exception since
      // it's probably a nasty internal error where the backtrace would be
      // useful
      errorMessage = StringUtils.stringifyException(e);
      LOG.debug(errorMessage);
    } else {
      errorMessage = errorMessage.split("\n", 2)[0];
    }
    displayError(errorMessage);
  }
  
  /**
   * Display an error string prefaced with the command name.  Also increments
   * the error count for the command which will result in a non-zero exit
   * code.
   * @param message error message to display
   */
  public void displayError(@Tainted Command this, @Tainted String message) {
    numErrors++;
    displayWarning(message);
  }
  
  /**
   * Display an warning string prefaced with the command name.
   * @param message warning message to display
   */
  public void displayWarning(@Tainted Command this, @Tainted String message) {
    err.println(getName() + ": " + message);
  }
  
  /**
   * The name of the command.  Will first try to use the assigned name
   * else fallback to the command's preferred name
   * @return name of the command
   */
  public @Tainted String getName(@Tainted Command this) {
    return (name == null)
      ? getCommandField(COMMAND_NAME_FIELD)
      : name.startsWith("-") ? name.substring(1) : name;
  }

  /**
   * Define the name of the command.
   * @param name as invoked
   */
  public void setName(@Tainted Command this, @Tainted String name) {
    this.name = name;
  }
  
  /**
   * The short usage suitable for the synopsis
   * @return "name options"
   */
  public @Tainted String getUsage(@Tainted Command this) {
    @Tainted
    String cmd = "-" + getName();
    @Tainted
    String usage = isDeprecated() ? "" : getCommandField(COMMAND_USAGE_FIELD);
    return usage.isEmpty() ? cmd : cmd + " " + usage; 
  }

  /**
   * The long usage suitable for help output
   * @return text of the usage
   */
  public @Tainted String getDescription(@Tainted Command this) {
    return isDeprecated()
      ? "(DEPRECATED) Same as '" + getReplacementCommand() + "'"
      : getCommandField(COMMAND_DESCRIPTION_FIELD);
  }

  /**
   * Is the command deprecated?
   * @return boolean
   */
  public final @Tainted boolean isDeprecated(@Tainted Command this) {
    return (getReplacementCommand() != null);
  }
  
  /**
   * The replacement for a deprecated command
   * @return null if not deprecated, else alternative command
   */
  public @Tainted String getReplacementCommand(@Tainted Command this) {
    return null;
  }

  /**
   * Get a public static class field
   * @param field the field to retrieve
   * @return String of the field
   */
  private @Tainted String getCommandField(@Tainted Command this, @Tainted String field) {
    @Tainted
    String value;
    try {
      @Tainted
      Field f = this.getClass().getDeclaredField(field);
      f.setAccessible(true);
      value = f.get(this).toString();
    } catch (@Tainted Exception e) {
      throw new @Tainted RuntimeException(
          "failed to get " + this.getClass().getSimpleName()+"."+field, e);
    }
    return value;
  }
  
  @SuppressWarnings("serial")
  static class CommandInterruptException extends @Tainted RuntimeException {}
}
