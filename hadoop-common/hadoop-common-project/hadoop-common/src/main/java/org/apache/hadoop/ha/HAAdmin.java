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
package org.apache.hadoop.ha;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * A command-line tool for making calls in the HAServiceProtocol.
 * For example,. this can be used to force a service to standby or active
 * mode, or to trigger a health-check.
 */
@InterfaceAudience.Private

public abstract class HAAdmin extends @Tainted Configured implements @Tainted Tool {
  
  private static final @Tainted String FORCEFENCE  = "forcefence";
  private static final @Tainted String FORCEACTIVE = "forceactive";
  
  /**
   * Undocumented flag which allows an administrator to use manual failover
   * state transitions even when auto-failover is enabled. This is an unsafe
   * operation, which is why it is not documented in the usage below.
   */
  private static final @Tainted String FORCEMANUAL = "forcemanual";
  private static final @Tainted Log LOG = LogFactory.getLog(HAAdmin.class);

  private @Tainted int rpcTimeoutForChecks = -1;
  
  private static @Tainted Map<@Tainted String, @Tainted UsageInfo> USAGE =
    ImmutableMap.<@Tainted String, @Tainted UsageInfo>builder()
    .put("-transitionToActive",
        new @Tainted UsageInfo("<serviceId>", "Transitions the service into Active state"))
    .put("-transitionToStandby",
        new @Tainted UsageInfo("<serviceId>", "Transitions the service into Standby state"))
    .put("-failover",
        new @Tainted UsageInfo("[--"+FORCEFENCE+"] [--"+FORCEACTIVE+"] <serviceId> <serviceId>",
            "Failover from the first service to the second.\n" +
            "Unconditionally fence services if the "+FORCEFENCE+" option is used.\n" +
            "Try to failover to the target service even if it is not ready if the " + 
            FORCEACTIVE + " option is used."))
    .put("-getServiceState",
        new @Tainted UsageInfo("<serviceId>", "Returns the state of the service"))
    .put("-checkHealth",
        new @Tainted UsageInfo("<serviceId>",
            "Requests that the service perform a health check.\n" + 
            "The HAAdmin tool will exit with a non-zero exit code\n" +
            "if the check fails."))
    .put("-help",
        new @Tainted UsageInfo("<command>", "Displays help on the specified command"))
    .build();

  /** Output stream for errors, for use in tests */
  protected @Tainted PrintStream errOut = System.err;
  protected @Tainted PrintStream out = System.out;
  private @Tainted RequestSource requestSource = RequestSource.REQUEST_BY_USER;

  protected abstract @Tainted HAServiceTarget resolveTarget(@Tainted HAAdmin this, @Tainted String string);

  protected @Tainted String getUsageString(@Tainted HAAdmin this) {
    return "Usage: HAAdmin";
  }

  protected void printUsage(@Tainted HAAdmin this, @Tainted PrintStream errOut) {
    errOut.println(getUsageString());
    for (Map.@Tainted Entry<@Tainted String, @Tainted UsageInfo> e : USAGE.entrySet()) {
      @Tainted
      String cmd = e.getKey();
      @Tainted
      UsageInfo usage = e.getValue();
      
      errOut.println("    [" + cmd + " " + usage.args + "]"); 
    }
    errOut.println();
    ToolRunner.printGenericCommandUsage(errOut);    
  }
  
  private static void printUsage(@Tainted PrintStream errOut, @Tainted String cmd) {
    @Tainted
    UsageInfo usage = USAGE.get(cmd);
    if (usage == null) {
      throw new @Tainted RuntimeException("No usage for cmd " + cmd);
    }
    errOut.println("Usage: HAAdmin [" + cmd + " " + usage.args + "]");
  }

  private @Tainted int transitionToActive(@Tainted HAAdmin this, final @Tainted CommandLine cmd)
      throws IOException, ServiceFailedException {
    @Tainted
    String @Tainted [] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("transitionToActive: incorrect number of arguments");
      printUsage(errOut, "-transitionToActive");
      return -1;
    }
    @Tainted
    HAServiceTarget target = resolveTarget(argv[0]);
    if (!checkManualStateManagementOK(target)) {
      return -1;
    }
    @Tainted
    HAServiceProtocol proto = target.getProxy(
        getConf(), 0);
    HAServiceProtocolHelper.transitionToActive(proto, createReqInfo());
    return 0;
  }

  private @Tainted int transitionToStandby(@Tainted HAAdmin this, final @Tainted CommandLine cmd)
      throws IOException, ServiceFailedException {
    @Tainted
    String @Tainted [] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("transitionToStandby: incorrect number of arguments");
      printUsage(errOut, "-transitionToStandby");
      return -1;
    }
    
    @Tainted
    HAServiceTarget target = resolveTarget(argv[0]);
    if (!checkManualStateManagementOK(target)) {
      return -1;
    }
    @Tainted
    HAServiceProtocol proto = target.getProxy(
        getConf(), 0);
    HAServiceProtocolHelper.transitionToStandby(proto, createReqInfo());
    return 0;
  }
  /**
   * Ensure that we are allowed to manually manage the HA state of the target
   * service. If automatic failover is configured, then the automatic
   * failover controllers should be doing state management, and it is generally
   * an error to use the HAAdmin command line to do so.
   * 
   * @param target the target to check
   * @return true if manual state management is allowed
   */
  private @Tainted boolean checkManualStateManagementOK(@Tainted HAAdmin this, @Tainted HAServiceTarget target) {
    if (target.isAutoFailoverEnabled()) {
      if (requestSource != RequestSource.REQUEST_BY_USER_FORCED) {
        errOut.println(
            "Automatic failover is enabled for " + target + "\n" +
            "Refusing to manually manage HA state, since it may cause\n" +
            "a split-brain scenario or other incorrect state.\n" +
            "If you are very sure you know what you are doing, please \n" +
            "specify the " + FORCEMANUAL + " flag.");
        return false;
      } else {
        LOG.warn("Proceeding with manual HA state management even though\n" +
            "automatic failover is enabled for " + target);
        return true;
      }
    }
    return true;
  }

  private @Tainted StateChangeRequestInfo createReqInfo(@Tainted HAAdmin this) {
    return new @Tainted StateChangeRequestInfo(requestSource);
  }

  private @Tainted int failover(@Tainted HAAdmin this, @Tainted CommandLine cmd)
      throws IOException, ServiceFailedException {
    @Tainted
    boolean forceFence = cmd.hasOption(FORCEFENCE);
    @Tainted
    boolean forceActive = cmd.hasOption(FORCEACTIVE);

    @Tainted
    int numOpts = cmd.getOptions() == null ? 0 : cmd.getOptions().length;
    final @Tainted String @Tainted [] args = cmd.getArgs();

    if (numOpts > 3 || args.length != 2) {
      errOut.println("failover: incorrect arguments");
      printUsage(errOut, "-failover");
      return -1;
    }

    @Tainted
    HAServiceTarget fromNode = resolveTarget(args[0]);
    @Tainted
    HAServiceTarget toNode = resolveTarget(args[1]);
    
    // Check that auto-failover is consistently configured for both nodes.
    Preconditions.checkState(
        fromNode.isAutoFailoverEnabled() ==
          toNode.isAutoFailoverEnabled(),
          "Inconsistent auto-failover configs between %s and %s!",
          fromNode, toNode);
    
    if (fromNode.isAutoFailoverEnabled()) {
      if (forceFence || forceActive) {
        // -forceActive doesn't make sense with auto-HA, since, if the node
        // is not healthy, then its ZKFC will immediately quit the election
        // again the next time a health check runs.
        //
        // -forceFence doesn't seem to have any real use cases with auto-HA
        // so it isn't implemented.
        errOut.println(FORCEFENCE + " and " + FORCEACTIVE + " flags not " +
            "supported with auto-failover enabled.");
        return -1;
      }
      return gracefulFailoverThroughZKFCs(toNode);
    }
    
    @Tainted
    FailoverController fc = new @Tainted FailoverController(getConf(),
        requestSource);
    
    try {
      fc.failover(fromNode, toNode, forceFence, forceActive); 
      out.println("Failover from "+args[0]+" to "+args[1]+" successful");
    } catch (@Tainted FailoverFailedException ffe) {
      errOut.println("Failover failed: " + ffe.getLocalizedMessage());
      return -1;
    }
    return 0;
  }
  

  /**
   * Initiate a graceful failover by talking to the target node's ZKFC.
   * This sends an RPC to the ZKFC, which coordinates the failover.
   * 
   * @param toNode the node to fail to
   * @return status code (0 for success)
   * @throws IOException if failover does not succeed
   */
  private @Tainted int gracefulFailoverThroughZKFCs(@Tainted HAAdmin this, @Tainted HAServiceTarget toNode)
      throws IOException {

    @Tainted
    int timeout = FailoverController.getRpcTimeoutToNewActive(getConf());
    @Tainted
    ZKFCProtocol proxy = toNode.getZKFCProxy(getConf(), timeout);
    try {
      proxy.gracefulFailover();
      out.println("Failover to " + toNode + " successful");
    } catch (@Tainted ServiceFailedException sfe) {
      errOut.println("Failover failed: " + sfe.getLocalizedMessage());
      return -1;
    }

    return 0;
  }

  private @Tainted int checkHealth(@Tainted HAAdmin this, final @Tainted CommandLine cmd)
      throws IOException, ServiceFailedException {
    @Tainted
    String @Tainted [] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("checkHealth: incorrect number of arguments");
      printUsage(errOut, "-checkHealth");
      return -1;
    }
    @Tainted
    HAServiceProtocol proto = resolveTarget(argv[0]).getProxy(
        getConf(), rpcTimeoutForChecks);
    try {
      HAServiceProtocolHelper.monitorHealth(proto, createReqInfo());
    } catch (@Tainted HealthCheckFailedException e) {
      errOut.println("Health check failed: " + e.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  private @Tainted int getServiceState(@Tainted HAAdmin this, final @Tainted CommandLine cmd)
      throws IOException, ServiceFailedException {
    @Tainted
    String @Tainted [] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("getServiceState: incorrect number of arguments");
      printUsage(errOut, "-getServiceState");
      return -1;
    }

    @Tainted
    HAServiceProtocol proto = resolveTarget(argv[0]).getProxy(
        getConf(), rpcTimeoutForChecks);
    out.println(proto.getServiceStatus().getState());
    return 0;
  }

  /**
   * Return the serviceId as is, we are assuming it was
   * given as a service address of form <host:ipcport>.
   */
  protected @Tainted String getServiceAddr(@Tainted HAAdmin this, @Tainted String serviceId) {
    return serviceId;
  }

  @Override
  public void setConf(@Tainted HAAdmin this, @Tainted Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      rpcTimeoutForChecks = conf.getInt(
          CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
          CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);
    }
  }

  @Override
  public @Tainted int run(@Tainted HAAdmin this, @Tainted String @Tainted [] argv) throws Exception {
    try {
      return runCmd(argv);
    } catch (@Tainted IllegalArgumentException iae) {
      errOut.println("Illegal argument: " + iae.getLocalizedMessage());
      return -1;
    } catch (@Tainted IOException ioe) {
      errOut.println("Operation failed: " + ioe.getLocalizedMessage());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Operation failed", ioe);
      }
      return -1;
    }
  }
  
  protected @Tainted int runCmd(@Tainted HAAdmin this, @Tainted String @Tainted [] argv) throws Exception {
    if (argv.length < 1) {
      printUsage(errOut);
      return -1;
    }

    @Tainted
    String cmd = argv[0];

    if (!cmd.startsWith("-")) {
      errOut.println("Bad command '" + cmd + "': expected command starting with '-'");
      printUsage(errOut);
      return -1;
    }
    
    if (!USAGE.containsKey(cmd)) {
      errOut.println(cmd.substring(1) + ": Unknown command");
      printUsage(errOut);
      return -1;
    }
    
    @Tainted
    Options opts = new @Tainted Options();

    // Add command-specific options
    if ("-failover".equals(cmd)) {
      addFailoverCliOpts(opts);
    }
    // Mutative commands take FORCEMANUAL option
    if ("-transitionToActive".equals(cmd) ||
        "-transitionToStandby".equals(cmd) ||
        "-failover".equals(cmd)) {
      opts.addOption(FORCEMANUAL, false,
          "force manual control even if auto-failover is enabled");
    }
         
    @Tainted
    CommandLine cmdLine = parseOpts(cmd, opts, argv);
    if (cmdLine == null) {
      // error already printed
      return -1;
    }
    
    if (cmdLine.hasOption(FORCEMANUAL)) {
      if (!confirmForceManual()) {
        LOG.fatal("Aborted");
        return -1;
      }
      // Instruct the NNs to honor this request even if they're
      // configured for manual failover.
      requestSource = RequestSource.REQUEST_BY_USER_FORCED;
    }

    if ("-transitionToActive".equals(cmd)) {
      return transitionToActive(cmdLine);
    } else if ("-transitionToStandby".equals(cmd)) {
      return transitionToStandby(cmdLine);
    } else if ("-failover".equals(cmd)) {
      return failover(cmdLine);
    } else if ("-getServiceState".equals(cmd)) {
      return getServiceState(cmdLine);
    } else if ("-checkHealth".equals(cmd)) {
      return checkHealth(cmdLine);
    } else if ("-help".equals(cmd)) {
      return help(argv);
    } else {
      // we already checked command validity above, so getting here
      // would be a coding error
      throw new @Tainted AssertionError("Should not get here, command: " + cmd);
    } 
  }
  
  private @Tainted boolean confirmForceManual(@Tainted HAAdmin this) throws IOException {
     return ToolRunner.confirmPrompt(
        "You have specified the " + FORCEMANUAL + " flag. This flag is " +
        "dangerous, as it can induce a split-brain scenario that WILL " +
        "CORRUPT your HDFS namespace, possibly irrecoverably.\n" +
        "\n" +
        "It is recommended not to use this flag, but instead to shut down the " +
        "cluster and disable automatic failover if you prefer to manually " +
        "manage your HA state.\n" +
        "\n" +
        "You may abort safely by answering 'n' or hitting ^C now.\n" +
        "\n" +
        "Are you sure you want to continue?");
  }

  /**
   * Add CLI options which are specific to the failover command and no
   * others.
   */
  private void addFailoverCliOpts(@Tainted HAAdmin this, @Tainted Options failoverOpts) {
    failoverOpts.addOption(FORCEFENCE, false, "force fencing");
    failoverOpts.addOption(FORCEACTIVE, false, "force failover");
    // Don't add FORCEMANUAL, since that's added separately for all commands
    // that change state.
  }
  
  private @Tainted CommandLine parseOpts(@Tainted HAAdmin this, @Tainted String cmdName, @Tainted Options opts, @Tainted String @Tainted [] argv) {
    try {
      // Strip off the first arg, since that's just the command name
      argv = Arrays.copyOfRange(argv, 1, argv.length); 
      return new @Tainted GnuParser().parse(opts, argv);
    } catch (@Tainted ParseException pe) {
      errOut.println(cmdName.substring(1) +
          ": incorrect arguments");
      printUsage(errOut, cmdName);
      return null;
    }
  }
  
  private @Tainted int help(@Tainted HAAdmin this, @Tainted String @Tainted [] argv) {
    if (argv.length == 1) { // only -help
      printUsage(out);
      return 0;
    } else if (argv.length != 2) {
      printUsage(errOut, "-help");
      return -1;
    }
    @Tainted
    String cmd = argv[1];
    if (!cmd.startsWith("-")) {
      cmd = "-" + cmd;
    }
    @Tainted
    UsageInfo usageInfo = USAGE.get(cmd);
    if (usageInfo == null) {
      errOut.println(cmd + ": Unknown command");
      printUsage(errOut);
      return -1;
    }
    
    out.println(cmd + " [" + usageInfo.args + "]: " + usageInfo.help);
    return 0;
  }
  
  private static class UsageInfo {
    private final @Tainted String args;
    private final @Tainted String help;
    
    public @Tainted UsageInfo(@Tainted String args, @Tainted String help) {
      this.args = args;
      this.help = help;
    }
  }
}
