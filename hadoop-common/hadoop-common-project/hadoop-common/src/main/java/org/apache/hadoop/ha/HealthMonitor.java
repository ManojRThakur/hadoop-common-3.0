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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeys.*;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Daemon;

import com.google.common.base.Preconditions;

/**
 * This class is a daemon which runs in a loop, periodically heartbeating
 * with an HA service. It is responsible for keeping track of that service's
 * health and exposing callbacks to the failover controller when the health
 * status changes.
 * 
 * Classes which need callbacks should implement the {@link Callback}
 * interface.
 */
@InterfaceAudience.Private
public class HealthMonitor {
  private static final @Tainted Log LOG = LogFactory.getLog(
      HealthMonitor.class);

  private @Tainted Daemon daemon;
  private @Tainted long connectRetryInterval;
  private @Tainted long checkIntervalMillis;
  private @Tainted long sleepAfterDisconnectMillis;

  private @Tainted int rpcTimeout;

  private volatile @Tainted boolean shouldRun = true;

  /** The connected proxy */
  private @Tainted HAServiceProtocol proxy;

  /** The HA service to monitor */
  private final @Tainted HAServiceTarget targetToMonitor;

  private final @Tainted Configuration conf;
  
  private @Tainted State state = State.INITIALIZING;

  /**
   * Listeners for state changes
   */
  private @Tainted List<@Tainted Callback> callbacks = Collections.synchronizedList(
      new @Tainted LinkedList<@Tainted Callback>());

  private @Tainted HAServiceStatus lastServiceState = new @Tainted HAServiceStatus(
      HAServiceState.INITIALIZING);
  
  @InterfaceAudience.Private
  public enum State {
    /**
     * The health monitor is still starting up.
     */

@Tainted  INITIALIZING,

    /**
     * The service is not responding to health check RPCs.
     */

@Tainted  SERVICE_NOT_RESPONDING,

    /**
     * The service is connected and healthy.
     */

@Tainted  SERVICE_HEALTHY,
    
    /**
     * The service is running but unhealthy.
     */

@Tainted  SERVICE_UNHEALTHY,
    
    /**
     * The health monitor itself failed unrecoverably and can
     * no longer provide accurate information.
     */

@Tainted  HEALTH_MONITOR_FAILED;
  }


  @Tainted
  HealthMonitor(@Tainted Configuration conf, @Tainted HAServiceTarget target) {
    this.targetToMonitor = target;
    this.conf = conf;
    
    this.sleepAfterDisconnectMillis = conf.getLong(
        HA_HM_SLEEP_AFTER_DISCONNECT_KEY,
        HA_HM_SLEEP_AFTER_DISCONNECT_DEFAULT);
    this.checkIntervalMillis = conf.getLong(
        HA_HM_CHECK_INTERVAL_KEY,
        HA_HM_CHECK_INTERVAL_DEFAULT);
    this.connectRetryInterval = conf.getLong(
        HA_HM_CONNECT_RETRY_INTERVAL_KEY,
        HA_HM_CONNECT_RETRY_INTERVAL_DEFAULT);
    this.rpcTimeout = conf.getInt(
        HA_HM_RPC_TIMEOUT_KEY,
        HA_HM_RPC_TIMEOUT_DEFAULT);
    
    this.daemon = new @Tainted MonitorDaemon();
  }
  
  public void addCallback(@Tainted HealthMonitor this, @Tainted Callback cb) {
    this.callbacks.add(cb);
  }
  
  public void removeCallback(@Tainted HealthMonitor this, @Tainted Callback cb) {
    callbacks.remove(cb);
  }
  
  public void shutdown(@Tainted HealthMonitor this) {
    LOG.info("Stopping HealthMonitor thread");
    shouldRun = false;
    daemon.interrupt();
  }

  /**
   * @return the current proxy object to the underlying service.
   * Note that this may return null in the case that the service
   * is not responding. Also note that, even if the last indicated
   * state is healthy, the service may have gone down in the meantime.
   */
  public synchronized @Tainted HAServiceProtocol getProxy(@Tainted HealthMonitor this) {
    return proxy;
  }
  
  private void loopUntilConnected(@Tainted HealthMonitor this) throws InterruptedException {
    tryConnect();
    while (proxy == null) {
      Thread.sleep(connectRetryInterval);
      tryConnect();
    }
    assert proxy != null;
  }

  private void tryConnect(@Tainted HealthMonitor this) {
    Preconditions.checkState(proxy == null);
    
    try {
      synchronized (this) {
        proxy = createProxy();
      }
    } catch (@Tainted IOException e) {
      LOG.warn("Could not connect to local service at " + targetToMonitor +
          ": " + e.getMessage());
      proxy = null;
      enterState(State.SERVICE_NOT_RESPONDING);
    }
  }
  
  /**
   * Connect to the service to be monitored. Stubbed out for easier testing.
   */
  protected @Tainted HAServiceProtocol createProxy(@Tainted HealthMonitor this) throws IOException {
    return targetToMonitor.getProxy(conf, rpcTimeout);
  }

  private void doHealthChecks(@Tainted HealthMonitor this) throws InterruptedException {
    while (shouldRun) {
      @Tainted
      HAServiceStatus status = null;
      @Tainted
      boolean healthy = false;
      try {
        status = proxy.getServiceStatus();
        proxy.monitorHealth();
        healthy = true;
      } catch (@Tainted HealthCheckFailedException e) {
        LOG.warn("Service health check failed for " + targetToMonitor
            + ": " + e.getMessage());
        enterState(State.SERVICE_UNHEALTHY);
      } catch (@Tainted Throwable t) {
        LOG.warn("Transport-level exception trying to monitor health of " +
            targetToMonitor + ": " + t.getLocalizedMessage());
        RPC.stopProxy(proxy);
        proxy = null;
        enterState(State.SERVICE_NOT_RESPONDING);
        Thread.sleep(sleepAfterDisconnectMillis);
        return;
      }
      
      if (status != null) {
        setLastServiceStatus(status);
      }
      if (healthy) {
        enterState(State.SERVICE_HEALTHY);
      }

      Thread.sleep(checkIntervalMillis);
    }
  }
  
  private synchronized void setLastServiceStatus(@Tainted HealthMonitor this, @Tainted HAServiceStatus status) {
    this.lastServiceState = status;
  }

  private synchronized void enterState(@Tainted HealthMonitor this, @Tainted State newState) {
    if (newState != state) {
      LOG.info("Entering state " + newState);
      state = newState;
      synchronized (callbacks) {
        for (@Tainted Callback cb : callbacks) {
          cb.enteredState(newState);
        }
      }
    }
  }

  synchronized @Tainted State getHealthState(@Tainted HealthMonitor this) {
    return state;
  }
  
  synchronized @Tainted HAServiceStatus getLastServiceStatus(@Tainted HealthMonitor this) {
    return lastServiceState;
  }
  
  @Tainted
  boolean isAlive(@Tainted HealthMonitor this) {
    return daemon.isAlive();
  }

  void join(@Tainted HealthMonitor this) throws InterruptedException {
    daemon.join();
  }

  void start(@Tainted HealthMonitor this) {
    daemon.start();
  }

  private class MonitorDaemon extends @Tainted Daemon {
    private @Tainted MonitorDaemon() {
      super();
      setName("Health Monitor for " + targetToMonitor);
      setUncaughtExceptionHandler(new  @Tainted  UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(@Tainted Thread t, @Tainted Throwable e) {
          LOG.fatal("Health monitor failed", e);
          enterState(HealthMonitor.State.HEALTH_MONITOR_FAILED);
        }
      });
    }
    
    @Override
    public void run(@Tainted HealthMonitor.MonitorDaemon this) {
      while (shouldRun) {
        try { 
          loopUntilConnected();
          doHealthChecks();
        } catch (@Tainted InterruptedException ie) {
          Preconditions.checkState(!shouldRun,
              "Interrupted but still supposed to run");
        }
      }
    }
  }
  
  /**
   * Callback interface for state change events.
   * 
   * This interface is called from a single thread which also performs
   * the health monitoring. If the callback processing takes a long time,
   * no further health checks will be made during this period, nor will
   * other registered callbacks be called.
   * 
   * If the callback itself throws an unchecked exception, no other
   * callbacks following it will be called, and the health monitor
   * will terminate, entering HEALTH_MONITOR_FAILED state.
   */
  static interface Callback {
    void enteredState(HealthMonitor.@Tainted Callback this, @Tainted State newState);
  }
}
