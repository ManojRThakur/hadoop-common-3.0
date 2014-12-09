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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ipc.RPC;

import com.google.common.base.Preconditions;

/**
 * The FailOverController is responsible for electing an active service
 * on startup or when the current active is changing (eg due to failure),
 * monitoring the health of a service, and performing a fail-over when a
 * new active service is either manually selected by a user or elected.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FailoverController {

  private static final @Tainted Log LOG = LogFactory.getLog(FailoverController.class);

  private final @Tainted int gracefulFenceTimeout;
  private final @Tainted int rpcTimeoutToNewActive;
  
  private final @Tainted Configuration conf;
  /*
   * Need a copy of conf for graceful fence to set 
   * configurable retries for IPC client.
   * Refer HDFS-3561
   */
  private final @Tainted Configuration gracefulFenceConf;

  private final @Tainted RequestSource requestSource;
  
  public @Tainted FailoverController(@Tainted Configuration conf,
      @Tainted
      RequestSource source) {
    this.conf = conf;
    this.gracefulFenceConf = new @Tainted Configuration(conf);
    this.requestSource = source;
    
    this.gracefulFenceTimeout = getGracefulFenceTimeout(conf);
    this.rpcTimeoutToNewActive = getRpcTimeoutToNewActive(conf);
    
    //Configure less retries for graceful fence 
    @Tainted
    int gracefulFenceConnectRetries = conf.getInt(
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES,
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);
    gracefulFenceConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        gracefulFenceConnectRetries);
    gracefulFenceConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        gracefulFenceConnectRetries);
  }

  static @Tainted int getGracefulFenceTimeout(@Tainted Configuration conf) {
    return conf.getInt(
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT);
  }
  
  static @Tainted int getRpcTimeoutToNewActive(@Tainted Configuration conf) {
    return conf.getInt(
        CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);
  }
  
  /**
   * Perform pre-failover checks on the given service we plan to
   * failover to, eg to prevent failing over to a service (eg due
   * to it being inaccessible, already active, not healthy, etc).
   *
   * An option to ignore toSvc if it claims it is not ready to
   * become active is provided in case performing a failover will
   * allow it to become active, eg because it triggers a log roll
   * so the standby can learn about new blocks and leave safemode.
   *
   * @param from currently active service
   * @param target service to make active
   * @param forceActive ignore toSvc if it reports that it is not ready
   * @throws FailoverFailedException if we should avoid failover
   */
  private void preFailoverChecks(@Tainted FailoverController this, @Tainted HAServiceTarget from,
                                 @Tainted
                                 HAServiceTarget target,
                                 @Tainted
                                 boolean forceActive)
      throws FailoverFailedException {
    @Tainted
    HAServiceStatus toSvcStatus;
    @Tainted
    HAServiceProtocol toSvc;

    if (from.getAddress().equals(target.getAddress())) {
      throw new @Tainted FailoverFailedException(
          "Can't failover a service to itself");
    }

    try {
      toSvc = target.getProxy(conf, rpcTimeoutToNewActive);
      toSvcStatus = toSvc.getServiceStatus();
    } catch (@Tainted IOException e) {
      @Tainted
      String msg = "Unable to get service state for " + target;
      LOG.error(msg + ": " + e.getLocalizedMessage());
      throw new @Tainted FailoverFailedException(msg, e);
    }

    if (!toSvcStatus.getState().equals(HAServiceState.STANDBY)) {
      throw new @Tainted FailoverFailedException(
          "Can't failover to an active service");
    }
    
    if (!toSvcStatus.isReadyToBecomeActive()) {
      @Tainted
      String notReadyReason = toSvcStatus.getNotReadyReason();
      if (!forceActive) {
        throw new @Tainted FailoverFailedException(
            target + " is not ready to become active: " +
            notReadyReason);
      } else {
        LOG.warn("Service is not ready to become active, but forcing: " +
            notReadyReason);
      }
    }

    try {
      HAServiceProtocolHelper.monitorHealth(toSvc, createReqInfo());
    } catch (@Tainted HealthCheckFailedException hce) {
      throw new @Tainted FailoverFailedException(
          "Can't failover to an unhealthy service", hce);
    } catch (@Tainted IOException e) {
      throw new @Tainted FailoverFailedException(
          "Got an IO exception", e);
    }
  }
  
  private @Tainted StateChangeRequestInfo createReqInfo(@Tainted FailoverController this) {
    return new @Tainted StateChangeRequestInfo(requestSource);
  }

  /**
   * Try to get the HA state of the node at the given address. This
   * function is guaranteed to be "quick" -- ie it has a short timeout
   * and no retries. Its only purpose is to avoid fencing a node that
   * has already restarted.
   */
  @Tainted
  boolean tryGracefulFence(@Tainted FailoverController this, @Tainted HAServiceTarget svc) {
    @Tainted
    HAServiceProtocol proxy = null;
    try {
      proxy = svc.getProxy(gracefulFenceConf, gracefulFenceTimeout);
      proxy.transitionToStandby(createReqInfo());
      return true;
    } catch (@Tainted ServiceFailedException sfe) {
      LOG.warn("Unable to gracefully make " + svc + " standby (" +
          sfe.getMessage() + ")");
    } catch (@Tainted IOException ioe) {
      LOG.warn("Unable to gracefully make " + svc +
          " standby (unable to connect)", ioe);
    } finally {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    return false;
  }
  
  /**
   * Failover from service 1 to service 2. If the failover fails
   * then try to failback.
   *
   * @param fromSvc currently active service
   * @param toSvc service to make active
   * @param forceFence to fence fromSvc even if not strictly necessary
   * @param forceActive try to make toSvc active even if it is not ready
   * @throws FailoverFailedException if the failover fails
   */
  public void failover(@Tainted FailoverController this, @Tainted HAServiceTarget fromSvc,
                       @Tainted
                       HAServiceTarget toSvc,
                       @Tainted
                       boolean forceFence,
                       @Tainted
                       boolean forceActive)
      throws FailoverFailedException {
    Preconditions.checkArgument(fromSvc.getFencer() != null,
        "failover requires a fencer");
    preFailoverChecks(fromSvc, toSvc, forceActive);

    // Try to make fromSvc standby
    @Tainted
    boolean tryFence = true;
    
    if (tryGracefulFence(fromSvc)) {
      tryFence = forceFence;
    }

    // Fence fromSvc if it's required or forced by the user
    if (tryFence) {
      if (!fromSvc.getFencer().fence(fromSvc)) {
        throw new @Tainted FailoverFailedException("Unable to fence " +
            fromSvc + ". Fencing failed.");
      }
    }

    // Try to make toSvc active
    @Tainted
    boolean failed = false;
    @Tainted
    Throwable cause = null;
    try {
      HAServiceProtocolHelper.transitionToActive(
          toSvc.getProxy(conf, rpcTimeoutToNewActive),
          createReqInfo());
    } catch (@Tainted ServiceFailedException sfe) {
      LOG.error("Unable to make " + toSvc + " active (" +
          sfe.getMessage() + "). Failing back.");
      failed = true;
      cause = sfe;
    } catch (@Tainted IOException ioe) {
      LOG.error("Unable to make " + toSvc +
          " active (unable to connect). Failing back.", ioe);
      failed = true;
      cause = ioe;
    }

    // We failed to make toSvc active
    if (failed) {
      @Tainted
      String msg = "Unable to failover to " + toSvc;
      // Only try to failback if we didn't fence fromSvc
      if (!tryFence) {
        try {
          // Unconditionally fence toSvc in case it is still trying to
          // become active, eg we timed out waiting for its response.
          // Unconditionally force fromSvc to become active since it
          // was previously active when we initiated failover.
          failover(toSvc, fromSvc, true, true);
        } catch (@Tainted FailoverFailedException ffe) {
          msg += ". Failback to " + fromSvc +
            " failed (" + ffe.getMessage() + ")";
          LOG.fatal(msg);
        }
      }
      throw new @Tainted FailoverFailedException(msg, cause);
    }
  }
}
