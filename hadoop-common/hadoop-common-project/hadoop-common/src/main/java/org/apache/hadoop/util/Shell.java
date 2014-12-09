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
import org.checkerframework.checker.tainting.qual.Untainted;
import org.checkerframework.checker.tainting.qual.PolyTainted;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * A base class for running a Unix command.
 * 
 * <code>Shell</code> can be used to run unix commands like <code>du</code> or
 * <code>df</code>. It also offers facilities to gate commands by 
 * time-intervals.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
abstract public class Shell {
  
  public static final @Tainted Log LOG = LogFactory.getLog(Shell.class);
  
  private static @Tainted boolean IS_JAVA7_OR_ABOVE =
      System.getProperty("java.version").substring(0, 3).compareTo("1.7") >= 0;

  public static @Tainted boolean isJava7OrAbove() {
    return IS_JAVA7_OR_ABOVE;
  }

  /** a Unix command to get the current user's name */
  public final static @Tainted String USER_NAME_COMMAND = "whoami";

  /** Windows CreateProcess synchronization object */
  public static final @Tainted Object WindowsProcessLaunchLock = new @Tainted Object();

  // OSType detection

  public enum OSType {

@Tainted  OS_TYPE_LINUX,

@Tainted  OS_TYPE_WIN,

@Tainted  OS_TYPE_SOLARIS,

@Tainted  OS_TYPE_MAC,

@Tainted  OS_TYPE_FREEBSD,

@Tainted  OS_TYPE_OTHER
  }

  public static final @Tainted OSType osType = getOSType();

  static private @Tainted OSType getOSType() {
    @Tainted
    String osName = System.getProperty("os.name");
    if (osName.startsWith("Windows")) {
      return OSType.OS_TYPE_WIN;
    } else if (osName.contains("SunOS") || osName.contains("Solaris")) {
      return OSType.OS_TYPE_SOLARIS;
    } else if (osName.contains("Mac")) {
      return OSType.OS_TYPE_MAC;
    } else if (osName.contains("FreeBSD")) {
      return OSType.OS_TYPE_FREEBSD;
    } else if (osName.startsWith("Linux")) {
      return OSType.OS_TYPE_LINUX;
    } else {
      // Some other form of Unix
      return OSType.OS_TYPE_OTHER;
    }
  }

  // Helper static vars for each platform
  public static final @Tainted boolean WINDOWS = (osType == OSType.OS_TYPE_WIN);
  public static final @Tainted boolean SOLARIS = (osType == OSType.OS_TYPE_SOLARIS);
  public static final @Tainted boolean MAC     = (osType == OSType.OS_TYPE_MAC);
  public static final @Tainted boolean FREEBSD = (osType == OSType.OS_TYPE_FREEBSD);
  public static final @Tainted boolean LINUX   = (osType == OSType.OS_TYPE_LINUX);
  public static final @Tainted boolean OTHER   = (osType == OSType.OS_TYPE_OTHER);

  /** a Unix command to get the current user's groups list */
  public static @Untainted String @Tainted [] getGroupsCommand() {
    return (WINDOWS)? new @Untainted String @Tainted []{"cmd", "/c", "groups"}
                    : new @Untainted String @Tainted []{"bash", "-c", "groups"};
  }

  /** a Unix command to get a given user's groups list */
  public static @Untainted String @Tainted [] getGroupsForUserCommand(final @Untainted String user) {
    //'groups username' command return is non-consistent across different unixes
    return (WINDOWS)? new @Untainted String @Tainted [] { WINUTILS, "groups", "-F", "\"" + user + "\""}
                    : new @Untainted String @Tainted [] {"bash", "-c", "id -Gn " + user};
  }

  /** a Unix command to get a given netgroup's user list */
  public static @Untainted String @Tainted [] getUsersForNetgroupCommand(final @Untainted String netgroup) {
    //'groups username' command return is non-consistent across different unixes
    return (WINDOWS)? new @Untainted String @Tainted [] {"cmd", "/c", "getent netgroup " + netgroup}
                    : new @Untainted String @Tainted [] {"bash", "-c", "getent netgroup " + netgroup};
  }

  /** Return a command to get permission information. */
  public static @Untainted String @Tainted [] getGetPermissionCommand() {
    return (WINDOWS) ? new @Untainted String @Tainted [] { WINUTILS, "ls", "-F" }
                     : new @Untainted String @Tainted [] { "/bin/ls", "-ld" };
  }

  /** Return a command to set permission */
  public static @Untainted String @Tainted [] getSetPermissionCommand(@Untainted String perm, @Tainted boolean recursive) {
    if (recursive) {
      return (WINDOWS) ? new @Untainted String @Tainted [] { WINUTILS, "chmod", "-R", perm }
                         : new @Untainted String @Tainted [] { "chmod", "-R", perm };
    } else {
      return (WINDOWS) ? new @Untainted String @Tainted [] { WINUTILS, "chmod", perm }
                       : new @Untainted String @Tainted [] { "chmod", perm };
    }
  }

  /**
   * Return a command to set permission for specific file.
   * 
   * @param perm String permission to set
   * @param recursive boolean true to apply to all sub-directories recursively
   * @param file String file to set
   * @return String[] containing command and arguments
   */
  public static @Untainted String @Tainted [] getSetPermissionCommand(@Untainted String perm, @Tainted boolean recursive,
                                                 @Untainted String file) {
    @Untainted String @Tainted [] baseCmd = getSetPermissionCommand(perm, recursive);
    @Untainted String @Tainted [] cmdWithFile = Arrays.copyOf(baseCmd, baseCmd.length + 1);
    cmdWithFile[cmdWithFile.length - 1] = file;
    return cmdWithFile;
  }

  /** Return a command to set owner */
  public static @Untainted String @Tainted [] getSetOwnerCommand(@Untainted String owner) {
    return (WINDOWS) ? new @Untainted String @Tainted [] { WINUTILS, "chown", "\"" + owner + "\"" }
                     : new @Untainted String @Tainted [] { "chown", owner };
  }
  
  /** Return a command to create symbolic links */
  public static @Untainted String @Tainted [] getSymlinkCommand(@Untainted String target, @Untainted String link) {
    return WINDOWS ? new @Untainted String @Tainted [] { WINUTILS, "symlink", link, target }
                   : new @Untainted String @Tainted [] { "ln", "-s", target, link };
  }

  /** Return a command to read the target of the a symbolic link*/
  public static @Untainted String @Tainted [] getReadlinkCommand(@Untainted String link) {
    return WINDOWS ? new @Untainted String @Tainted [] { WINUTILS, "readlink", link }
        : new @Untainted String @Tainted [] { "readlink", link };
  }

  /** Return a command for determining if process with specified pid is alive. */
  public static @Tainted String @Tainted [] getCheckProcessIsAliveCommand(@Tainted String pid) {
    return Shell.WINDOWS ?
      new @Tainted String @Tainted [] { Shell.WINUTILS, "task", "isAlive", pid } :
      new @Tainted String @Tainted [] { "kill", "-0", isSetsidAvailable ? "-" + pid : pid };
  }

  /** Return a command to send a signal to a given pid */
  public static @Tainted String @Tainted [] getSignalKillCommand(@Tainted int code, @Tainted String pid) {
    return Shell.WINDOWS ? new @Tainted String @Tainted [] { Shell.WINUTILS, "task", "kill", pid } :
      new @Tainted String @Tainted [] { "kill", "-" + code, isSetsidAvailable ? "-" + pid : pid };
  }

  /** Return a regular expression string that match environment variables */
  public static @Tainted String getEnvironmentVariableRegex() {
    return (WINDOWS) ? "%([A-Za-z_][A-Za-z0-9_]*?)%" :
      "\\$([A-Za-z_][A-Za-z0-9_]*)";
  }
  
  /**
   * Returns a File referencing a script with the given basename, inside the
   * given parent directory.  The file extension is inferred by platform: ".cmd"
   * on Windows, or ".sh" otherwise.
   * 
   * @param parent File parent directory
   * @param basename String script file basename
   * @return File referencing the script in the directory
   */
  public static @Tainted File appendScriptExtension(@Tainted File parent, @Tainted String basename) {
    return new @Tainted File(parent, appendScriptExtension(basename));
  }

  /**
   * Returns a script file name with the given basename.  The file extension is
   * inferred by platform: ".cmd" on Windows, or ".sh" otherwise.
   * 
   * @param basename String script file basename
   * @return String script file name
   */
  public static @Tainted String appendScriptExtension(@Tainted String basename) {
    return basename + (WINDOWS ? ".cmd" : ".sh");
  }

  /**
   * Returns a command to run the given script.  The script interpreter is
   * inferred by platform: cmd on Windows or bash otherwise.
   * 
   * @param script File script to run
   * @return String[] command to run the script
   */
  public static @Tainted String @Tainted [] getRunScriptCommand(@Tainted File script) {
    @Tainted
    String absolutePath = script.getAbsolutePath();
    return WINDOWS ? new @Tainted String @Tainted [] { "cmd", "/c", absolutePath } :
      new @Tainted String @Tainted [] { "/bin/bash", absolutePath };
  }

  /** a Unix command to set permission */
  public static final @Tainted String SET_PERMISSION_COMMAND = "chmod";
  /** a Unix command to set owner */
  public static final @Tainted String SET_OWNER_COMMAND = "chown";

  /** a Unix command to set the change user's groups list */
  public static final @Tainted String SET_GROUP_COMMAND = "chgrp";
  /** a Unix command to create a link */
  public static final @Tainted String LINK_COMMAND = "ln";
  /** a Unix command to get a link target */
  public static final @Tainted String READ_LINK_COMMAND = "readlink";

  /**Time after which the executing script would be timedout*/
  protected @Tainted long timeOutInterval = 0L;
  /** If or not script timed out*/
  private @Tainted AtomicBoolean timedOut;


  /** Centralized logic to discover and validate the sanity of the Hadoop 
   *  home directory. Returns either NULL or a directory that exists and 
   *  was specified via either -Dhadoop.home.dir or the HADOOP_HOME ENV 
   *  variable.  This does a lot of work so it should only be called 
   *  privately for initialization once per process.
   **/
  @SuppressWarnings("ostrusted:cast.unsafe")
  //ostrusted, getProperty/getEnv are trusted
  private static @Untainted String checkHadoopHome() {

    // first check the Dflag hadoop.home.dir with JVM scope
    @Untainted
    String home = (@Untainted String) System.getProperty("hadoop.home.dir");

    // fall back to the system/user-global env variable
    if (home == null) {
      home = (@Untainted String)  System.getenv("HADOOP_HOME");
    }

    try {
       // couldn't find either setting for hadoop's home directory
       if (home == null) {
         throw new @Tainted IOException("HADOOP_HOME or hadoop.home.dir are not set.");
       }

       if (home.startsWith("\"") && home.endsWith("\"")) {
         home = (@Untainted String) home.substring(1, home.length()-1);
       }

       // check that the home setting is actually a directory that exists
       @Tainted
       File homedir = new @Tainted File(home);
       if (!homedir.isAbsolute() || !homedir.exists() || !homedir.isDirectory()) {
         throw new @Tainted IOException("Hadoop home directory " + homedir
           + " does not exist, is not a directory, or is not an absolute path.");
       }

       home = (@Untainted String) homedir.getCanonicalPath();

    } catch (@Tainted IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to detect a valid hadoop home directory", ioe);
      }
      home = null;
    }
    
    return home;
  }
  private static @Untainted String HADOOP_HOME_DIR = checkHadoopHome();

  // Public getter, throws an exception if HADOOP_HOME failed validation
  // checks and is being referenced downstream.
  public static final @Tainted String getHadoopHome() throws IOException {
    if (HADOOP_HOME_DIR == null) {
      throw new @Tainted IOException("Misconfigured HADOOP_HOME cannot be referenced.");
    }

    return HADOOP_HOME_DIR;
  }

  /** fully qualify the path to a binary that should be in a known hadoop 
   *  bin location. This is primarily useful for disambiguating call-outs 
   *  to executable sub-components of Hadoop to avoid clashes with other 
   *  executables that may be in the path.  Caveat:  this call doesn't 
   *  just format the path to the bin directory.  It also checks for file 
   *  existence of the composed path. The output of this call should be 
   *  cached by callers.
   * */

  @SuppressWarnings("ostrusted:return.type.incompatible")
  //ostrusted, fulExeName is trusted except for executable, hence the @PolyTainted
  public static final @PolyTainted String getQualifiedBinPath(@PolyTainted String executable)
    throws IOException {
    // construct hadoop bin path to the specified executable
    @Tainted
    String fullExeName = HADOOP_HOME_DIR + File.separator + "bin" 
      + File.separator + executable;

    @Tainted
    File exeFile = new @Tainted File(fullExeName);
    if (!exeFile.exists()) {
      throw new @Tainted IOException("Could not locate executable " + fullExeName
        + " in the Hadoop binaries.");
    }

    return exeFile.getCanonicalPath();
  }

  /** a Windows utility to emulate Unix commands */
  public static final @Untainted String WINUTILS = getWinUtilsPath();

  public static final @Untainted String getWinUtilsPath() {
    @Untainted String winUtilsPath = null;

    try {
      if (WINDOWS) {
        winUtilsPath = getQualifiedBinPath("winutils.exe");
      }
    } catch (@Tainted IOException ioe) {
       LOG.error("Failed to locate the winutils binary in the hadoop binary path",
         ioe);
    }

    return winUtilsPath;
  }

  public static final @Tainted boolean isSetsidAvailable = isSetsidSupported();
  private static @Tainted boolean isSetsidSupported() {
    if (Shell.WINDOWS) {
      return false;
    }
    @Tainted
    ShellCommandExecutor shexec = null;
    @Tainted
    boolean setsidSupported = true;
    try {
      @Untainted String @Tainted [] args = new @Untainted String @Tainted [] {"setsid", "bash", "-c", "echo $$"};
      shexec = new @Tainted ShellCommandExecutor(args);
      shexec.execute();
    } catch (@Tainted IOException ioe) {
      LOG.debug("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // handle the exit code
      if (LOG.isDebugEnabled()) {
        LOG.debug("setsid exited with exit code "
                 + (shexec != null ? shexec.getExitCode() : "(null executor)"));
      }
    }
    return setsidSupported;
  }

  /** Token separator regex used to parse Shell tool outputs */
  public static final @Tainted String TOKEN_SEPARATOR_REGEX
                = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";

  private @Tainted long    interval;   // refresh interval in msec
  private @Tainted long    lastTime;   // last time the command was performed
  final private @Tainted boolean redirectErrorStream; // merge stdout and stderr
  private @Tainted Map<@Tainted String, @Tainted String> environment; // env for the command execution
  private @Tainted File dir;
  private @Tainted Process process; // sub process used to execute the command
  private @Tainted int exitCode;

  /**If or not script finished executing*/
  private volatile @Tainted AtomicBoolean completed;
  
  public @Tainted Shell() {
    this(0L);
  }
  
  public @Tainted Shell(@Tainted long interval) {
    this(interval, false);
  }

  /**
   * @param interval the minimum duration to wait before re-executing the 
   *        command.
   */
  public @Tainted Shell(@Tainted long interval, @Tainted boolean redirectErrorStream) {
    this.interval = interval;
    this.lastTime = (interval<0) ? 0 : -interval;
    this.redirectErrorStream = redirectErrorStream;
  }
  
  /** set the environment for the command 
   * @param env Mapping of environment variables
   */
  protected void setEnvironment(@Tainted Shell this, @Tainted Map<@Tainted String, @Tainted String> env) {
    this.environment = env;
  }

  /** set the working directory 
   * @param dir The directory where the command would be executed
   */
  protected void setWorkingDirectory(@Tainted Shell this, @Tainted File dir) {
    this.dir = dir;
  }

  /** check to see if a command needs to be executed and execute if needed */
  protected void run(@Tainted Shell this) throws IOException {
    if (lastTime + interval > Time.now())
      return;
    exitCode = 0; // reset for next run
    runCommand();
  }

  /** Run a command */
  private void runCommand(@Tainted Shell this) throws IOException { 
    @Tainted
    ProcessBuilder builder = new @Tainted ProcessBuilder(getExecString());
    @Tainted
    Timer timeOutTimer = null;
    @Tainted
    ShellTimeoutTimerTask timeoutTimerTask = null;
    timedOut = new @Tainted AtomicBoolean(false);
    completed = new @Tainted AtomicBoolean(false);
    
    if (environment != null) {
      builder.environment().putAll(this.environment);
    }
    if (dir != null) {
      builder.directory(this.dir);
    }

    builder.redirectErrorStream(redirectErrorStream);
    
    if (Shell.WINDOWS) {
      synchronized (WindowsProcessLaunchLock) {
        // To workaround the race condition issue with child processes
        // inheriting unintended handles during process launch that can
        // lead to hangs on reading output and error streams, we
        // serialize process creation. More info available at:
        // http://support.microsoft.com/kb/315939
        process = builder.start();
      }
    } else {
      process = builder.start();
    }

    if (timeOutInterval > 0) {
      timeOutTimer = new @Tainted Timer("Shell command timeout");
      timeoutTimerTask = new @Tainted ShellTimeoutTimerTask(
          this);
      //One time scheduling.
      timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
    }
    //ostrusted, standard out and standard in can be considered trusted when coming from an executed process
    final @Untainted BufferedReader errReader =
            new @Untainted BufferedReader(new @Tainted InputStreamReader(process
                                                     .getErrorStream()));
    @Untainted
    BufferedReader inReader = 
            new @Untainted BufferedReader(new @Tainted InputStreamReader(process.getInputStream()));
    final @Tainted StringBuffer errMsg = new @Tainted StringBuffer();
    
    // read error and input streams as this would free up the buffers
    // free the error stream buffer
    @Tainted
    Thread errThread = new @Tainted Thread() {
      @Override
      public void run() {
        try {
          @Tainted
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            errMsg.append(line);
            errMsg.append(System.getProperty("line.separator"));
            line = errReader.readLine();
          }
        } catch(@Tainted IOException ioe) {
          LOG.warn("Error reading the error stream", ioe);
        }
      }
    };
    try {
      errThread.start();
    } catch (@Tainted IllegalStateException ise) { }
    try {
      parseExecResult(inReader); // parse the output
      // clear the input stream buffer
      @Tainted
      String line = inReader.readLine();
      while(line != null) { 
        line = inReader.readLine();
      }
      // wait for the process to finish and check the exit code
      exitCode  = process.waitFor();
      try {
        // make sure that the error thread exits
        errThread.join();
      } catch (@Tainted InterruptedException ie) {
        LOG.warn("Interrupted while reading the error stream", ie);
      }
      completed.set(true);
      //the timeout thread handling
      //taken care in finally block
      if (exitCode != 0) {
        throw new @Tainted ExitCodeException(exitCode, errMsg.toString());
      }
    } catch (@Tainted InterruptedException ie) {
      throw new @Tainted IOException(ie.toString());
    } finally {
      if (timeOutTimer != null) {
        timeOutTimer.cancel();
      }
      // close the input stream
      try {
        inReader.close();
      } catch (@Tainted IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      try {
        if (!completed.get()) {
          errThread.interrupt();
          errThread.join();
        }
      } catch (@Tainted InterruptedException ie) {
        LOG.warn("Interrupted while joining errThread");
      }
      try {
        errReader.close();
      } catch (@Tainted IOException ioe) {
        LOG.warn("Error while closing the error stream", ioe);
      }
      process.destroy();
      lastTime = Time.now();
    }
  }

  /** return an array containing the command name & its parameters */ 
  protected abstract @Untainted String @Tainted [] getExecString(@Tainted Shell this);
  
  /** Parse the execution result */
  protected abstract void parseExecResult(@Tainted Shell this, @Untainted BufferedReader lines)
  throws IOException;

  /** 
   * Get the environment variable
   */
  public @Tainted String getEnvironment(@Tainted Shell this, @Tainted String env) {
    return environment.get(env);
  }
  
  /** get the current sub-process executing the given command 
   * @return process executing the command
   */
  public @Tainted Process getProcess(@Tainted Shell this) {
    return process;
  }

  /** get the exit code 
   * @return the exit code of the process
   */
  public @Tainted int getExitCode(@Tainted Shell this) {
    return exitCode;
  }

  /**
   * This is an IOException with exit code added.
   */
  public static class ExitCodeException extends @Tainted IOException {
    @Tainted
    int exitCode;
    
    public @Tainted ExitCodeException(@Tainted int exitCode, @Tainted String message) {
      super(message);
      this.exitCode = exitCode;
    }
    
    public @Tainted int getExitCode(Shell.@Tainted ExitCodeException this) {
      return exitCode;
    }
  }
  
  /**
   * A simple shell command executor.
   * 
   * <code>ShellCommandExecutor</code>should be used in cases where the output 
   * of the command needs no explicit parsing and where the command, working 
   * directory and the environment remains unchanged. The output of the command 
   * is stored as-is and is expected to be small.
   */
  public static class ShellCommandExecutor extends @Tainted Shell {
    
    private @Untainted String @Tainted [] command;
    private @Untainted StringBuffer output;
    
    
    public @Tainted ShellCommandExecutor(@Untainted String @Tainted [] execString) {
      this(execString, null);
    }
    
    public @Tainted ShellCommandExecutor(@Untainted String @Tainted [] execString, @Tainted File dir) {
      this(execString, dir, null);
    }
   
    public @Tainted ShellCommandExecutor(@Untainted String @Tainted [] execString, @Tainted File dir, 
                                 @Tainted
                                 Map<@Tainted String, @Tainted String> env) {
      this(execString, dir, env , 0L);
    }

    /**
     * Create a new instance of the ShellCommandExecutor to execute a command.
     * 
     * @param execString The command to execute with arguments
     * @param dir If not-null, specifies the directory which should be set
     *            as the current working directory for the command.
     *            If null, the current working directory is not modified.
     * @param env If not-null, environment of the command will include the
     *            key-value pairs specified in the map. If null, the current
     *            environment is not modified.
     * @param timeout Specifies the time in milliseconds, after which the
     *                command will be killed and the status marked as timedout.
     *                If 0, the command will not be timed out. 
     */
    public @Tainted ShellCommandExecutor(@Untainted String @Tainted [] execString, @Tainted File dir, 
        @Tainted
        Map<@Tainted String, @Tainted String> env, @Tainted long timeout) {
      command = execString.clone();
      if (dir != null) {
        setWorkingDirectory(dir);
      }
      if (env != null) {
        setEnvironment(env);
      }
      timeOutInterval = timeout;
    }
        

    /** Execute the shell command. */
    public void execute(Shell.@Tainted ShellCommandExecutor this) throws IOException {
      this.run();    
    }

    @Override
    public @Untainted String @Tainted [] getExecString(Shell.@Tainted ShellCommandExecutor this) {
      return command;
    }

    @Override
    protected void parseExecResult(Shell.@Tainted ShellCommandExecutor this, @Untainted BufferedReader lines) throws IOException {
      output = new @Untainted StringBuffer();
      @Untainted char @Tainted [] buf = new @Untainted char @Tainted [512];
      @Tainted
      int nRead;
      while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
        output.append(buf, 0, nRead);
      }
    }
    
    /** Get the output of the shell command.*/
    @SuppressWarnings("ostrusted:cast.unsafe")
    public @Untainted String getOutput(Shell.@Tainted ShellCommandExecutor this) {
      return (@Untainted String) ( (output == null) ? "" : output.toString() );
    }

    /**
     * Returns the commands of this instance.
     * Arguments with spaces in are presented with quotes round; other
     * arguments are presented raw
     *
     * @return a string representation of the object.
     */
    @Override
    public @Tainted String toString(Shell.@Tainted ShellCommandExecutor this) {
      @Tainted
      StringBuilder builder = new @Tainted StringBuilder();
      @Tainted
      String @Tainted [] args = getExecString();
      for (@Tainted String s : args) {
        if (s.indexOf(' ') >= 0) {
          builder.append('"').append(s).append('"');
        } else {
          builder.append(s);
        }
        builder.append(' ');
      }
      return builder.toString();
    }
  }
  
  /**
   * To check if the passed script to shell command executor timed out or
   * not.
   * 
   * @return if the script timed out.
   */
  public @Tainted boolean isTimedOut(@Tainted Shell this) {
    return timedOut.get();
  }
  
  /**
   * Set if the command has timed out.
   * 
   */
  private void setTimedOut(@Tainted Shell this) {
    this.timedOut.set(true);
  }
  
  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static @Untainted String execCommand(@Untainted String @Tainted ... cmd) throws IOException {
    return execCommand(null, cmd, 0L);
  }
  
  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @param timeout time in milliseconds after which script should be marked timeout
   * @return the output of the executed command.o
   */
  
  public static @Untainted String execCommand(@Tainted Map<@Tainted String, @Tainted String> env, @Untainted String @Tainted [] cmd,
      @Tainted
      long timeout) throws IOException {
    @Tainted
    ShellCommandExecutor exec = new @Tainted ShellCommandExecutor(cmd, null, env, 
                                                          timeout);
    exec.execute();
    return exec.getOutput();
  }

  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static @Tainted String execCommand(@Tainted Map<@Tainted String, @Tainted String> env, @Untainted String @Tainted ... cmd) 
  throws IOException {
    return execCommand(env, cmd, 0L);
  }
  
  /**
   * Timer which is used to timeout scripts spawned off by shell.
   */
  private static class ShellTimeoutTimerTask extends @Tainted TimerTask {

    private @Tainted Shell shell;

    public @Tainted ShellTimeoutTimerTask(@Tainted Shell shell) {
      this.shell = shell;
    }

    @Override
    public void run(Shell.@Tainted ShellTimeoutTimerTask this) {
      @Tainted
      Process p = shell.getProcess();
      try {
        p.exitValue();
      } catch (@Tainted Exception e) {
        //Process has not terminated.
        //So check if it has completed 
        //if not just destroy it.
        if (p != null && !shell.completed.get()) {
          shell.setTimedOut();
          p.destroy();
        }
      }
    }
  }
}
