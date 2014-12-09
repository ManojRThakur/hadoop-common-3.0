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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Provide command line access to a FileSystem. */
@InterfaceAudience.Private
public class FsShell extends @Tainted Configured implements @Tainted Tool {
  
  static final @Tainted Log LOG = LogFactory.getLog(FsShell.class);

  private @Tainted FileSystem fs;
  private @Tainted Trash trash;
  protected @Tainted CommandFactory commandFactory;

  private final @Tainted String usagePrefix =
    "Usage: hadoop fs [generic options]";

  /**
   * Default ctor with no configuration.  Be sure to invoke
   * {@link #setConf(Configuration)} with a valid configuration prior
   * to running commands.
   */
  public @Tainted FsShell() {
    this(null);
  }

  /**
   * Construct a FsShell with the given configuration.  Commands can be
   * executed via {@link #run(String[])}
   * @param conf the hadoop configuration
   */
  public @Tainted FsShell(@Tainted Configuration conf) {
    super(conf);
  }
  
  protected @Tainted FileSystem getFS(@Tainted FsShell this) throws IOException {
    if (fs == null) {
      fs = FileSystem.get(getConf());
    }
    return fs;
  }
  
  protected @Tainted Trash getTrash(@Tainted FsShell this) throws IOException {
    if (this.trash == null) {
      this.trash = new @Tainted Trash(getConf());
    }
    return this.trash;
  }
  
  protected void init(@Tainted FsShell this) throws IOException {
    getConf().setQuietMode(true);
    if (commandFactory == null) {
      commandFactory = new @Tainted CommandFactory(getConf());
      commandFactory.addObject(new @Tainted Help(), "-help");
      commandFactory.addObject(new @Tainted Usage(), "-usage");
      registerCommands(commandFactory);
    }
  }

  protected void registerCommands(@Tainted FsShell this, @Tainted CommandFactory factory) {
    // TODO: DFSAdmin subclasses FsShell so need to protect the command
    // registration.  This class should morph into a base class for
    // commands, and then this method can be abstract
    if (this.getClass().equals(FsShell.class)) {
      factory.registerCommands(FsCommand.class);
    }
  }
  
  /**
   * Returns the Trash object associated with this shell.
   * @return Path to the trash
   * @throws IOException upon error
   */
  public @Tainted Path getCurrentTrashDir(@Tainted FsShell this) throws IOException {
    return getTrash().getCurrentTrashDir();
  }

  // NOTE: Usage/Help are inner classes to allow access to outer methods
  // that access commandFactory
  
  /**
   *  Display help for commands with their short usage and long description
   */
   protected class Usage extends @Tainted FsCommand {
    public static final @Tainted String NAME = "usage";
    public static final @Tainted String USAGE = "[cmd ...]";
    public static final @Tainted String DESCRIPTION =
      "Displays the usage for given command or all commands if none\n" +
      "is specified.";
    
    @Override
    protected void processRawArguments(@Tainted FsShell.Usage this, @Tainted LinkedList<@Tainted String> args) {
      if (args.isEmpty()) {
        printUsage(System.out);
      } else {
        for (@Tainted String arg : args) printUsage(System.out, arg);
      }
    }
  } 

  /**
   * Displays short usage of commands sans the long description
   */
  protected class Help extends @Tainted FsCommand {
    public static final @Tainted String NAME = "help";
    public static final @Tainted String USAGE = "[cmd ...]";
    public static final @Tainted String DESCRIPTION =
      "Displays help for given command or all commands if none\n" +
      "is specified.";
    
    @Override
    protected void processRawArguments(@Tainted FsShell.Help this, @Tainted LinkedList<@Tainted String> args) {
      if (args.isEmpty()) {
        printHelp(System.out);
      } else {
        for (@Tainted String arg : args) printHelp(System.out, arg);
      }
    }
  }

  /*
   * The following are helper methods for getInfo().  They are defined
   * outside of the scope of the Help/Usage class because the run() method
   * needs to invoke them too. 
   */

  // print all usages
  private void printUsage(@Tainted FsShell this, @Tainted PrintStream out) {
    printInfo(out, null, false);
  }
  
  // print one usage
  private void printUsage(@Tainted FsShell this, @Tainted PrintStream out, @Tainted String cmd) {
    printInfo(out, cmd, false);
  }

  // print all helps
  private void printHelp(@Tainted FsShell this, @Tainted PrintStream out) {
    printInfo(out, null, true);
  }

  // print one help
  private void printHelp(@Tainted FsShell this, @Tainted PrintStream out, @Tainted String cmd) {
    printInfo(out, cmd, true);
  }

  private void printInfo(@Tainted FsShell this, @Tainted PrintStream out, @Tainted String cmd, @Tainted boolean showHelp) {
    if (cmd != null) {
      // display help or usage for one command
      @Tainted
      Command instance = commandFactory.getInstance("-" + cmd);
      if (instance == null) {
        throw new @Tainted UnknownCommandException(cmd);
      }
      if (showHelp) {
        printInstanceHelp(out, instance);
      } else {
        printInstanceUsage(out, instance);
      }
    } else {
      // display help or usage for all commands 
      out.println(usagePrefix);
      
      // display list of short usages
      @Tainted
      ArrayList<@Tainted Command> instances = new @Tainted ArrayList<@Tainted Command>();
      for (@Tainted String name : commandFactory.getNames()) {
        @Tainted
        Command instance = commandFactory.getInstance(name);
        if (!instance.isDeprecated()) {
          System.out.println("\t[" + instance.getUsage() + "]");
          instances.add(instance);
        }
      }
      // display long descriptions for each command
      if (showHelp) {
        for (@Tainted Command instance : instances) {
          out.println();
          printInstanceHelp(out, instance);
        }
      }
      out.println();
      ToolRunner.printGenericCommandUsage(out);
    }
  }

  private void printInstanceUsage(@Tainted FsShell this, @Tainted PrintStream out, @Tainted Command instance) {
    out.println(usagePrefix + " " + instance.getUsage());
  }

  // TODO: will eventually auto-wrap the text, but this matches the expected
  // output for the hdfs tests...
  private void printInstanceHelp(@Tainted FsShell this, @Tainted PrintStream out, @Tainted Command instance) {
    @Tainted
    boolean firstLine = true;
    for (@Tainted String line : instance.getDescription().split("\n")) {
      @Tainted
      String prefix;
      if (firstLine) {
        prefix = instance.getUsage() + ":\t";
        firstLine = false;
      } else {
        prefix = "\t\t";
      }
      System.out.println(prefix + line);
    }    
  }

  /**
   * run
   */
  @Override
  public @Tainted int run(@Tainted FsShell this, @Tainted String argv @Tainted []) throws Exception {
    // initialize FsShell
    init();

    @Tainted
    int exitCode = -1;
    if (argv.length < 1) {
      printUsage(System.err);
    } else {
      @Tainted
      String cmd = argv[0];
      @Tainted
      Command instance = null;
      try {
        instance = commandFactory.getInstance(cmd);
        if (instance == null) {
          throw new @Tainted UnknownCommandException();
        }
        exitCode = instance.run(Arrays.copyOfRange(argv, 1, argv.length));
      } catch (@Tainted IllegalArgumentException e) {
        displayError(cmd, e.getLocalizedMessage());
        if (instance != null) {
          printInstanceUsage(System.err, instance);
        }
      } catch (@Tainted Exception e) {
        // instance.run catches IOE, so something is REALLY wrong if here
        LOG.debug("Error", e);
        displayError(cmd, "Fatal internal error");
        e.printStackTrace(System.err);
      }
    }
    return exitCode;
  }
  
  private void displayError(@Tainted FsShell this, @Tainted String cmd, @Tainted String message) {
    for (@Tainted String line : message.split("\n")) {
      System.err.println(cmd + ": " + line);
      if (cmd.charAt(0) != '-') {
        @Tainted
        Command instance = null;
        instance = commandFactory.getInstance("-" + cmd);
        if (instance != null) {
          System.err.println("Did you mean -" + cmd + "?  This command " +
              "begins with a dash.");
        }
      }
    }
  }
  
  /**
   *  Performs any necessary cleanup
   * @throws IOException upon error
   */
  public void close(@Tainted FsShell this) throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * main() has some simple utility methods
   * @param argv the command and its arguments
   * @throws Exception upon error
   */
  // @SuppressWarnings("ostrusted") ostrusted Input args from command line are assumed trusted.
  public static void main(@Untainted String argv @Tainted []) throws Exception {
    @Tainted
    FsShell shell = newShellInstance();
    @Tainted
    Configuration conf = new @Tainted Configuration();
    conf.setQuietMode(false);
    shell.setConf(conf);
    @Tainted
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }

  // TODO: this should be abstract in a base class
  protected static @Tainted FsShell newShellInstance() {
    return new @Tainted FsShell();
  }
  
  /**
   * The default ctor signals that the command being executed does not exist,
   * while other ctor signals that a specific command does not exist.  The
   * latter is used by commands that process other commands, ex. -usage/-help
   */
  @SuppressWarnings("serial")
  static class UnknownCommandException extends @Tainted IllegalArgumentException {
    private final @Tainted String cmd;    
    @Tainted
    UnknownCommandException() { this(null); }
    @Tainted
    UnknownCommandException(@Tainted String cmd) { this.cmd = cmd; }
    
    @Override
    public @Tainted String getMessage(FsShell.@Tainted UnknownCommandException this) {
      return ((cmd != null) ? "`"+cmd+"': " : "") + "Unknown command";
    }
  }
}
