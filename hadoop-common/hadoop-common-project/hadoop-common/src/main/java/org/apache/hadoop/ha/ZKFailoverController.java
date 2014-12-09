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
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.hadoop.ha.HealthMonitor.State;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.data.ACL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@InterfaceAudience.LimitedPrivate("HDFS")
public abstract class ZKFailoverController {

  static final @Tainted Log LOG = LogFactory.getLog(ZKFailoverController.class);
  
  public static final @Tainted String ZK_QUORUM_KEY = "ha.zookeeper.quorum";
  private static final @Tainted String ZK_SESSION_TIMEOUT_KEY = "ha.zookeeper.session-timeout.ms";
  private static final @Tainted int ZK_SESSION_TIMEOUT_DEFAULT = 5*1000;
  private static final @Tainted String ZK_PARENT_ZNODE_KEY = "ha.zookeeper.parent-znode";
  public static final @Tainted String ZK_ACL_KEY = "ha.zookeeper.acl";
  private static final @Tainted String ZK_ACL_DEFAULT = "world:anyone:rwcda";
  public static final @Tainted String ZK_AUTH_KEY = "ha.zookeeper.auth";
  static final @Tainted String ZK_PARENT_ZNODE_DEFAULT = "/hadoop-ha";

  /**
   * All of the conf keys used by the ZKFC. This is used in order to allow
   * them to be overridden on a per-nameservice or per-namenode basis.
   */
  protected static final @Tainted String @Tainted [] ZKFC_CONF_KEYS = new @Tainted String @Tainted [] {
    ZK_QUORUM_KEY,
    ZK_SESSION_TIMEOUT_KEY,
    ZK_PARENT_ZNODE_KEY,
    ZK_ACL_KEY,
    ZK_AUTH_KEY
  };
  
  protected static final @Tainted String USAGE = 
      "Usage: java zkfc [ -formatZK [-force] [-nonInteractive] ]";

  /** Unable to format the parent znode in ZK */
  static final @Tainted int ERR_CODE_FORMAT_DENIED = 2;
  /** The parent znode doesn't exist in ZK */
  static final @Tainted int ERR_CODE_NO_PARENT_ZNODE = 3;
  /** Fencing is not properly configured */
  static final @Tainted int ERR_CODE_NO_FENCER = 4;
  /** Automatic failover is not enabled */
  static final @Tainted int ERR_CODE_AUTO_FAILOVER_NOT_ENABLED = 5;
  /** Cannot connect to ZooKeeper */
  static final @Tainted int ERR_CODE_NO_ZK = 6;
  
  protected @Tainted Configuration conf;
  private @Tainted String zkQuorum;
  protected final @Tainted HAServiceTarget localTarget;

  private @Tainted HealthMonitor healthMonitor;
  private @Tainted ActiveStandbyElector elector;
  protected @Tainted ZKFCRpcServer rpcServer;

  private @Tainted State lastHealthState = State.INITIALIZING;

  /** Set if a fatal error occurs */
  private @Tainted String fatalError = null;

  /**
   * A future nanotime before which the ZKFC will not join the election.
   * This is used during graceful failover.
   */
  private @Tainted long delayJoiningUntilNanotime = 0;

  /** Executor on which {@link #scheduleRecheck(long)} schedules events */
  private @Tainted ScheduledExecutorService delayExecutor =
    Executors.newScheduledThreadPool(1,
        new @Tainted ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("ZKFC Delay timer #%d")
            .build());

  private @Tainted ActiveAttemptRecord lastActiveAttemptRecord;
  private @Tainted Object activeAttemptRecordLock = new @Tainted Object();

  protected @Tainted ZKFailoverController(@Tainted Configuration conf, @Tainted HAServiceTarget localTarget) {
    this.localTarget = localTarget;
    this.conf = conf;
  }
  

  protected abstract @Tainted byte @Tainted [] targetToData(@Tainted ZKFailoverController this, @Tainted HAServiceTarget target);
  protected abstract @Tainted HAServiceTarget dataToTarget(@Tainted ZKFailoverController this, @Tainted byte @Tainted [] data);
  protected abstract void loginAsFCUser(@Tainted ZKFailoverController this) throws IOException;
  protected abstract void checkRpcAdminAccess(@Tainted ZKFailoverController this)
      throws AccessControlException, IOException;
  protected abstract @Tainted InetSocketAddress getRpcAddressToBindTo(@Tainted ZKFailoverController this);
  protected abstract @Tainted PolicyProvider getPolicyProvider(@Tainted ZKFailoverController this);

  /**
   * Return the name of a znode inside the configured parent znode in which
   * the ZKFC will do all of its work. This is so that multiple federated
   * nameservices can run on the same ZK quorum without having to manually
   * configure them to separate subdirectories.
   */
  protected abstract @Tainted String getScopeInsideParentNode(@Tainted ZKFailoverController this);

  public @Tainted HAServiceTarget getLocalTarget(@Tainted ZKFailoverController this) {
    return localTarget;
  }
  
  public @Tainted int run(@Tainted ZKFailoverController this, final @Tainted String @Tainted [] args) throws Exception {
    if (!localTarget.isAutoFailoverEnabled()) {
      LOG.fatal("Automatic failover is not enabled for " + localTarget + "." +
          " Please ensure that automatic failover is enabled in the " +
          "configuration before running the ZK failover controller.");
      return ERR_CODE_AUTO_FAILOVER_NOT_ENABLED;
    }
    loginAsFCUser();
    try {
      return SecurityUtil.doAsLoginUserOrFatal(new @Tainted PrivilegedAction<@Tainted Integer>() {
        @Override
        public @Tainted Integer run() {
          try {
            return doRun(args);
          } catch (@Tainted Exception t) {
            throw new @Tainted RuntimeException(t);
          } finally {
            if (elector != null) {
              elector.terminateConnection();
            }
          }
        }
      });
    } catch (@Tainted RuntimeException rte) {
      throw (@Tainted Exception)rte.getCause();
    }
  }
  

  private @Tainted int doRun(@Tainted ZKFailoverController this, @Tainted String @Tainted [] args)
      throws HadoopIllegalArgumentException, IOException, InterruptedException {
    try {
      initZK();
    } catch (@Tainted KeeperException ke) {
      LOG.fatal("Unable to start failover controller. Unable to connect "
          + "to ZooKeeper quorum at " + zkQuorum + ". Please check the "
          + "configured value for " + ZK_QUORUM_KEY + " and ensure that "
          + "ZooKeeper is running.");
      return ERR_CODE_NO_ZK;
    }
    if (args.length > 0) {
      if ("-formatZK".equals(args[0])) {
        @Tainted
        boolean force = false;
        @Tainted
        boolean interactive = true;
        for (@Tainted int i = 1; i < args.length; i++) {
          if ("-force".equals(args[i])) {
            force = true;
          } else if ("-nonInteractive".equals(args[i])) {
            interactive = false;
          } else {
            badArg(args[i]);
          }
        }
        return formatZK(force, interactive);
      } else {
        badArg(args[0]);
      }
    }

    if (!elector.parentZNodeExists()) {
      LOG.fatal("Unable to start failover controller. "
          + "Parent znode does not exist.\n"
          + "Run with -formatZK flag to initialize ZooKeeper.");
      return ERR_CODE_NO_PARENT_ZNODE;
    }

    try {
      localTarget.checkFencingConfigured();
    } catch (@Tainted BadFencingConfigurationException e) {
      LOG.fatal("Fencing is not configured for " + localTarget + ".\n" +
          "You must configure a fencing method before using automatic " +
          "failover.", e);
      return ERR_CODE_NO_FENCER;
    }

    initRPC();
    initHM();
    startRPC();
    try {
      mainLoop();
    } finally {
      rpcServer.stopAndJoin();
      
      elector.quitElection(true);
      healthMonitor.shutdown();
      healthMonitor.join();
    }
    return 0;
  }

  private void badArg(@Tainted ZKFailoverController this, @Tainted String arg) {
    printUsage();
    throw new @Tainted HadoopIllegalArgumentException(
        "Bad argument: " + arg);
  }

  private void printUsage(@Tainted ZKFailoverController this) {
    System.err.println(USAGE + "\n");
  }

  private @Tainted int formatZK(@Tainted ZKFailoverController this, @Tainted boolean force, @Tainted boolean interactive)
      throws IOException, InterruptedException {
    if (elector.parentZNodeExists()) {
      if (!force && (!interactive || !confirmFormat())) {
        return ERR_CODE_FORMAT_DENIED;
      }
      
      try {
        elector.clearParentZNode();
      } catch (@Tainted IOException e) {
        LOG.error("Unable to clear zk parent znode", e);
        return 1;
      }
    }
    
    elector.ensureParentZNode();
    return 0;
  }

  private @Tainted boolean confirmFormat(@Tainted ZKFailoverController this) {
    @Tainted
    String parentZnode = getParentZnode();
    System.err.println(
        "===============================================\n" +
        "The configured parent znode " + parentZnode + " already exists.\n" +
        "Are you sure you want to clear all failover information from\n" +
        "ZooKeeper?\n" +
        "WARNING: Before proceeding, ensure that all HDFS services and\n" +
        "failover controllers are stopped!\n" +
        "===============================================");
    try {
      return ToolRunner.confirmPrompt("Proceed formatting " + parentZnode + "?");
    } catch (@Tainted IOException e) {
      LOG.debug("Failed to confirm", e);
      return false;
    }
  }

  // ------------------------------------------
  // Begin actual guts of failover controller
  // ------------------------------------------
  
  private void initHM(@Tainted ZKFailoverController this) {
    healthMonitor = new @Tainted HealthMonitor(conf, localTarget);
    healthMonitor.addCallback(new @Tainted HealthCallbacks());
    healthMonitor.start();
  }
  
  protected void initRPC(@Tainted ZKFailoverController this) throws IOException {
    @Tainted
    InetSocketAddress bindAddr = getRpcAddressToBindTo();
    rpcServer = new @Tainted ZKFCRpcServer(conf, bindAddr, this, getPolicyProvider());
  }

  protected void startRPC(@Tainted ZKFailoverController this) throws IOException {
    rpcServer.start();
  }


  private void initZK(@Tainted ZKFailoverController this) throws HadoopIllegalArgumentException, IOException,
      KeeperException {
    zkQuorum = conf.get(ZK_QUORUM_KEY);
    @Tainted
    int zkTimeout = conf.getInt(ZK_SESSION_TIMEOUT_KEY,
        ZK_SESSION_TIMEOUT_DEFAULT);
    // Parse ACLs from configuration.
    @Tainted
    String zkAclConf = conf.get(ZK_ACL_KEY, ZK_ACL_DEFAULT);
    zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
    @Tainted
    List<@Tainted ACL> zkAcls = ZKUtil.parseACLs(zkAclConf);
    if (zkAcls.isEmpty()) {
      zkAcls = Ids.CREATOR_ALL_ACL;
    }
    
    // Parse authentication from configuration.
    @Tainted
    String zkAuthConf = conf.get(ZK_AUTH_KEY);
    zkAuthConf = ZKUtil.resolveConfIndirection(zkAuthConf);
    @Tainted
    List<@Tainted ZKAuthInfo> zkAuths;
    if (zkAuthConf != null) {
      zkAuths = ZKUtil.parseAuth(zkAuthConf);
    } else {
      zkAuths = Collections.emptyList();
    }

    // Sanity check configuration.
    Preconditions.checkArgument(zkQuorum != null,
        "Missing required configuration '%s' for ZooKeeper quorum",
        ZK_QUORUM_KEY);
    Preconditions.checkArgument(zkTimeout > 0,
        "Invalid ZK session timeout %s", zkTimeout);
    

    elector = new @Tainted ActiveStandbyElector(zkQuorum,
        zkTimeout, getParentZnode(), zkAcls, zkAuths,
        new @Tainted ElectorCallbacks());
  }
  
  private @Tainted String getParentZnode(@Tainted ZKFailoverController this) {
    @Tainted
    String znode = conf.get(ZK_PARENT_ZNODE_KEY,
        ZK_PARENT_ZNODE_DEFAULT);
    if (!znode.endsWith("/")) {
      znode += "/";
    }
    return znode + getScopeInsideParentNode();
  }

  private synchronized void mainLoop(@Tainted ZKFailoverController this) throws InterruptedException {
    while (fatalError == null) {
      wait();
    }
    assert fatalError != null; // only get here on fatal
    throw new @Tainted RuntimeException(
        "ZK Failover Controller failed: " + fatalError);
  }
  
  private synchronized void fatalError(@Tainted ZKFailoverController this, @Tainted String err) {
    LOG.fatal("Fatal error occurred:" + err);
    fatalError = err;
    notifyAll();
  }
  
  private synchronized void becomeActive(@Tainted ZKFailoverController this) throws ServiceFailedException {
    LOG.info("Trying to make " + localTarget + " active...");
    try {
      HAServiceProtocolHelper.transitionToActive(localTarget.getProxy(
          conf, FailoverController.getRpcTimeoutToNewActive(conf)),
          createReqInfo());
      @Tainted
      String msg = "Successfully transitioned " + localTarget +
          " to active state";
      LOG.info(msg);
      recordActiveAttempt(new @Tainted ActiveAttemptRecord(true, msg));

    } catch (@Tainted Throwable t) {
      @Tainted
      String msg = "Couldn't make " + localTarget + " active";
      LOG.fatal(msg, t);
      
      recordActiveAttempt(new @Tainted ActiveAttemptRecord(false, msg + "\n" +
          StringUtils.stringifyException(t)));

      if (t instanceof @Tainted ServiceFailedException) {
        throw (@Tainted ServiceFailedException)t;
      } else {
        throw new @Tainted ServiceFailedException("Couldn't transition to active",
            t);
      }
/*
* TODO:
* we need to make sure that if we get fenced and then quickly restarted,
* none of these calls will retry across the restart boundary
* perhaps the solution is that, whenever the nn starts, it gets a unique
* ID, and when we start becoming active, we record it, and then any future
* calls use the same ID
*/
      
    }
  }

  /**
   * Store the results of the last attempt to become active.
   * This is used so that, during manually initiated failover,
   * we can report back the results of the attempt to become active
   * to the initiator of the failover.
   */
  private void recordActiveAttempt(
      @Tainted ZKFailoverController this, @Tainted
      ActiveAttemptRecord record) {
    synchronized (activeAttemptRecordLock) {
      lastActiveAttemptRecord = record;
      activeAttemptRecordLock.notifyAll();
    }
  }

  /**
   * Wait until one of the following events:
   * <ul>
   * <li>Another thread publishes the results of an attempt to become active
   * using {@link #recordActiveAttempt(ActiveAttemptRecord)}</li>
   * <li>The node enters bad health status</li>
   * <li>The specified timeout elapses</li>
   * </ul>
   * 
   * @param timeoutMillis number of millis to wait
   * @return the published record, or null if the timeout elapses or the
   * service becomes unhealthy 
   * @throws InterruptedException if the thread is interrupted.
   */
  private @Tainted ActiveAttemptRecord waitForActiveAttempt(@Tainted ZKFailoverController this, @Tainted int timeoutMillis)
      throws InterruptedException {
    @Tainted
    long st = System.nanoTime();
    @Tainted
    long waitUntil = st + TimeUnit.NANOSECONDS.convert(
        timeoutMillis, TimeUnit.MILLISECONDS);
    
    do {
      // periodically check health state, because entering an
      // unhealthy state could prevent us from ever attempting to
      // become active. We can detect this and respond to the user
      // immediately.
      synchronized (this) {
        if (lastHealthState != State.SERVICE_HEALTHY) {
          // early out if service became unhealthy
          return null;
        }
      }

      synchronized (activeAttemptRecordLock) {
        if ((lastActiveAttemptRecord != null &&
            lastActiveAttemptRecord.nanoTime >= st)) {
          return lastActiveAttemptRecord;
        }
        // Only wait 1sec so that we periodically recheck the health state
        // above.
        activeAttemptRecordLock.wait(1000);
      }
    } while (System.nanoTime() < waitUntil);
    
    // Timeout elapsed.
    LOG.warn(timeoutMillis + "ms timeout elapsed waiting for an attempt " +
        "to become active");
    return null;
  }

  private @Tainted StateChangeRequestInfo createReqInfo(@Tainted ZKFailoverController this) {
    return new @Tainted StateChangeRequestInfo(RequestSource.REQUEST_BY_ZKFC);
  }

  private synchronized void becomeStandby(@Tainted ZKFailoverController this) {
    LOG.info("ZK Election indicated that " + localTarget +
        " should become standby");
    try {
      @Tainted
      int timeout = FailoverController.getGracefulFenceTimeout(conf);
      localTarget.getProxy(conf, timeout).transitionToStandby(createReqInfo());
      LOG.info("Successfully transitioned " + localTarget +
          " to standby state");
    } catch (@Tainted Exception e) {
      LOG.error("Couldn't transition " + localTarget + " to standby state",
          e);
      // TODO handle this. It's a likely case since we probably got fenced
      // at the same time.
    }
  }
  

  private synchronized void fenceOldActive(@Tainted ZKFailoverController this, @Tainted byte @Tainted [] data) {
    @Tainted
    HAServiceTarget target = dataToTarget(data);
    
    try {
      doFence(target);
    } catch (@Tainted Throwable t) {
      recordActiveAttempt(new @Tainted ActiveAttemptRecord(false, "Unable to fence old active: " + StringUtils.stringifyException(t)));
      Throwables.propagate(t);
    }
  }
  
  private void doFence(@Tainted ZKFailoverController this, @Tainted HAServiceTarget target) {
    LOG.info("Should fence: " + target);
    @Tainted
    boolean gracefulWorked = new @Tainted FailoverController(conf,
        RequestSource.REQUEST_BY_ZKFC).tryGracefulFence(target);
    if (gracefulWorked) {
      // It's possible that it's in standby but just about to go into active,
      // no? Is there some race here?
      LOG.info("Successfully transitioned " + target + " to standby " +
          "state without fencing");
      return;
    }
    
    try {
      target.checkFencingConfigured();
    } catch (@Tainted BadFencingConfigurationException e) {
      LOG.error("Couldn't fence old active " + target, e);
      recordActiveAttempt(new @Tainted ActiveAttemptRecord(false, "Unable to fence old active"));
      throw new @Tainted RuntimeException(e);
    }
    
    if (!target.getFencer().fence(target)) {
      throw new @Tainted RuntimeException("Unable to fence " + target);
    }
  }


  /**
   * Request from graceful failover to cede active role. Causes
   * this ZKFC to transition its local node to standby, then quit
   * the election for the specified period of time, after which it
   * will rejoin iff it is healthy.
   */
  void cedeActive(@Tainted ZKFailoverController this, final @Tainted int millisToCede)
      throws AccessControlException, ServiceFailedException, IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(new @Tainted PrivilegedExceptionAction<@Tainted Void>() {
        @Override
        public @Tainted Void run() throws Exception {
          doCedeActive(millisToCede);
          return null;
        }
      });
    } catch (@Tainted InterruptedException e) {
      throw new @Tainted IOException(e);
    }
  }
  
  private void doCedeActive(@Tainted ZKFailoverController this, @Tainted int millisToCede) 
      throws AccessControlException, ServiceFailedException, IOException {
    @Tainted
    int timeout = FailoverController.getGracefulFenceTimeout(conf);

    // Lock elector to maintain lock ordering of elector -> ZKFC
    synchronized (elector) {
      synchronized (this) {
        if (millisToCede <= 0) {
          delayJoiningUntilNanotime = 0;
          recheckElectability();
          return;
        }
  
        LOG.info("Requested by " + UserGroupInformation.getCurrentUser() +
            " at " + Server.getRemoteAddress() + " to cede active role.");
        @Tainted
        boolean needFence = false;
        try {
          localTarget.getProxy(conf, timeout).transitionToStandby(createReqInfo());
          LOG.info("Successfully ensured local node is in standby mode");
        } catch (@Tainted IOException ioe) {
          LOG.warn("Unable to transition local node to standby: " +
              ioe.getLocalizedMessage());
          LOG.warn("Quitting election but indicating that fencing is " +
              "necessary");
          needFence = true;
        }
        delayJoiningUntilNanotime = System.nanoTime() +
            TimeUnit.MILLISECONDS.toNanos(millisToCede);
        elector.quitElection(needFence);
      }
    }
    recheckElectability();
  }
  
  /**
   * Coordinate a graceful failover to this node.
   * @throws ServiceFailedException if the node fails to become active
   * @throws IOException some other error occurs
   */
  void gracefulFailoverToYou(@Tainted ZKFailoverController this) throws ServiceFailedException, IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(new @Tainted PrivilegedExceptionAction<@Tainted Void>() {
        @Override
        public @Tainted Void run() throws Exception {
          doGracefulFailover();
          return null;
        }
        
      });
    } catch (@Tainted InterruptedException e) {
      throw new @Tainted IOException(e);
    }
  }

  /**
   * Coordinate a graceful failover. This proceeds in several phases:
   * 1) Pre-flight checks: ensure that the local node is healthy, and
   * thus a candidate for failover.
   * 2) Determine the current active node. If it is the local node, no
   * need to failover - return success.
   * 3) Ask that node to yield from the election for a number of seconds.
   * 4) Allow the normal election path to run in other threads. Wait until
   * we either become unhealthy or we see an election attempt recorded by
   * the normal code path.
   * 5) Allow the old active to rejoin the election, so a future
   * failback is possible.
   */
  private void doGracefulFailover(@Tainted ZKFailoverController this)
      throws ServiceFailedException, IOException, InterruptedException {
    @Tainted
    int timeout = FailoverController.getGracefulFenceTimeout(conf) * 2;
    
    // Phase 1: pre-flight checks
    checkEligibleForFailover();
    
    // Phase 2: determine old/current active node. Check that we're not
    // ourselves active, etc.
    @Tainted
    HAServiceTarget oldActive = getCurrentActive();
    if (oldActive == null) {
      // No node is currently active. So, if we aren't already
      // active ourselves by means of a normal election, then there's
      // probably something preventing us from becoming active.
      throw new @Tainted ServiceFailedException(
          "No other node is currently active.");
    }
    
    if (oldActive.getAddress().equals(localTarget.getAddress())) {
      LOG.info("Local node " + localTarget + " is already active. " +
          "No need to failover. Returning success.");
      return;
    }
    
    // Phase 3: ask the old active to yield from the election.
    LOG.info("Asking " + oldActive + " to cede its active state for " +
        timeout + "ms");
    @Tainted
    ZKFCProtocol oldZkfc = oldActive.getZKFCProxy(conf, timeout);
    oldZkfc.cedeActive(timeout);

    // Phase 4: wait for the normal election to make the local node
    // active.
    @Tainted
    ActiveAttemptRecord attempt = waitForActiveAttempt(timeout + 60000);
    
    if (attempt == null) {
      // We didn't even make an attempt to become active.
      synchronized(this) {
        if (lastHealthState != State.SERVICE_HEALTHY) {
          throw new @Tainted ServiceFailedException("Unable to become active. " +
            "Service became unhealthy while trying to failover.");          
        }
      }
      
      throw new @Tainted ServiceFailedException("Unable to become active. " +
          "Local node did not get an opportunity to do so from ZooKeeper, " +
          "or the local node took too long to transition to active.");
    }

    // Phase 5. At this point, we made some attempt to become active. So we
    // can tell the old active to rejoin if it wants. This allows a quick
    // fail-back if we immediately crash.
    oldZkfc.cedeActive(-1);
    
    if (attempt.succeeded) {
      LOG.info("Successfully became active. " + attempt.status);
    } else {
      // Propagate failure
      @Tainted
      String msg = "Failed to become active. " + attempt.status;
      throw new @Tainted ServiceFailedException(msg);
    }
  }

  /**
   * Ensure that the local node is in a healthy state, and thus
   * eligible for graceful failover.
   * @throws ServiceFailedException if the node is unhealthy
   */
  private synchronized void checkEligibleForFailover(@Tainted ZKFailoverController this)
      throws ServiceFailedException {
    // Check health
    if (this.getLastHealthState() != State.SERVICE_HEALTHY) {
      throw new @Tainted ServiceFailedException(
          localTarget + " is not currently healthy. " +
          "Cannot be failover target");
    }
  }

  /**
   * @return an {@link HAServiceTarget} for the current active node
   * in the cluster, or null if no node is active.
   * @throws IOException if a ZK-related issue occurs
   * @throws InterruptedException if thread is interrupted 
   */
  private @Tainted HAServiceTarget getCurrentActive(@Tainted ZKFailoverController this)
      throws IOException, InterruptedException {
    synchronized (elector) {
      synchronized (this) {
        @Tainted
        byte @Tainted [] activeData;
        try {
          activeData = elector.getActiveData();
        } catch (@Tainted ActiveNotFoundException e) {
          return null;
        } catch (@Tainted KeeperException ke) {
          throw new @Tainted IOException(
              "Unexpected ZooKeeper issue fetching active node info", ke);
        }
        
        @Tainted
        HAServiceTarget oldActive = dataToTarget(activeData);
        return oldActive;
      }
    }
  }

  /**
   * Check the current state of the service, and join the election
   * if it should be in the election.
   */
  private void recheckElectability(@Tainted ZKFailoverController this) {
    // Maintain lock ordering of elector -> ZKFC
    synchronized (elector) {
      synchronized (this) {
        @Tainted
        boolean healthy = lastHealthState == State.SERVICE_HEALTHY;
    
        @Tainted
        long remainingDelay = delayJoiningUntilNanotime - System.nanoTime(); 
        if (remainingDelay > 0) {
          if (healthy) {
            LOG.info("Would have joined master election, but this node is " +
                "prohibited from doing so for " +
                TimeUnit.NANOSECONDS.toMillis(remainingDelay) + " more ms");
          }
          scheduleRecheck(remainingDelay);
          return;
        }
    
        switch (lastHealthState) {
        case SERVICE_HEALTHY:
          elector.joinElection(targetToData(localTarget));
          break;
          
        case INITIALIZING:
          LOG.info("Ensuring that " + localTarget + " does not " +
              "participate in active master election");
          elector.quitElection(false);
          break;
    
        case SERVICE_UNHEALTHY:
        case SERVICE_NOT_RESPONDING:
          LOG.info("Quitting master election for " + localTarget +
              " and marking that fencing is necessary");
          elector.quitElection(true);
          break;
          
        case HEALTH_MONITOR_FAILED:
          fatalError("Health monitor failed!");
          break;
          
        default:
          throw new @Tainted IllegalArgumentException("Unhandled state:" + lastHealthState);
        }
      }
    }
  }
  
  /**
   * Schedule a call to {@link #recheckElectability()} in the future.
   */
  private void scheduleRecheck(@Tainted ZKFailoverController this, @Tainted long whenNanos) {
    delayExecutor.schedule(
        new @Tainted Runnable() {
          @Override
          public void run() {
            try {
              recheckElectability();
            } catch (@Tainted Throwable t) {
              fatalError("Failed to recheck electability: " +
                  StringUtils.stringifyException(t));
            }
          }
        },
        whenNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * @return the last health state passed to the FC
   * by the HealthMonitor.
   */
  @VisibleForTesting
  synchronized @Tainted State getLastHealthState(@Tainted ZKFailoverController this) {
    return lastHealthState;
  }

  private synchronized void setLastHealthState(@Tainted ZKFailoverController this, HealthMonitor.@Tainted State newState) {
    LOG.info("Local service " + localTarget +
        " entered state: " + newState);
    lastHealthState = newState;
  }
  
  @VisibleForTesting
  @Tainted
  ActiveStandbyElector getElectorForTests(@Tainted ZKFailoverController this) {
    return elector;
  }
  
  @VisibleForTesting
  @Tainted
  ZKFCRpcServer getRpcServerForTests(@Tainted ZKFailoverController this) {
    return rpcServer;
  }

  /**
   * Callbacks from elector
   */
  class ElectorCallbacks implements @Tainted ActiveStandbyElectorCallback {
    @Override
    public void becomeActive(@Tainted ZKFailoverController.ElectorCallbacks this) throws ServiceFailedException {
      ZKFailoverController.this.becomeActive();
    }

    @Override
    public void becomeStandby(@Tainted ZKFailoverController.ElectorCallbacks this) {
      ZKFailoverController.this.becomeStandby();
    }

    @Override
    public void enterNeutralMode(@Tainted ZKFailoverController.ElectorCallbacks this) {
    }

    @Override
    public void notifyFatalError(@Tainted ZKFailoverController.ElectorCallbacks this, @Tainted String errorMessage) {
      fatalError(errorMessage);
    }

    @Override
    public void fenceOldActive(@Tainted ZKFailoverController.ElectorCallbacks this, @Tainted byte @Tainted [] data) {
      ZKFailoverController.this.fenceOldActive(data);
    }
    
    @Override
    public @Tainted String toString(@Tainted ZKFailoverController.ElectorCallbacks this) {
      synchronized (ZKFailoverController.this) {
        return "Elector callbacks for " + localTarget;
      }
    }
  }
  
  /**
   * Callbacks from HealthMonitor
   */
  class HealthCallbacks implements HealthMonitor.@Tainted Callback {
    @Override
    public void enteredState(@Tainted ZKFailoverController.HealthCallbacks this, HealthMonitor.@Tainted State newState) {
      setLastHealthState(newState);
      recheckElectability();
    }
  }
  
  private static class ActiveAttemptRecord {
    private final @Tainted boolean succeeded;
    private final @Tainted String status;
    private final @Tainted long nanoTime;
    
    public @Tainted ActiveAttemptRecord(@Tainted boolean succeeded, @Tainted String status) {
      this.succeeded = succeeded;
      this.status = status;
      this.nanoTime = System.nanoTime();
    }
  }

}
