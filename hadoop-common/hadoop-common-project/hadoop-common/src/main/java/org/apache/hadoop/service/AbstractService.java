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

package org.apache.hadoop.service;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is the base implementation class for services.
 */
@Public
@Evolving
public abstract class AbstractService implements @Tainted Service {

  private static final @Tainted Log LOG = LogFactory.getLog(AbstractService.class);

  /**
   * Service name.
   */
  private final @Tainted String name;

  /** service state */
  private final @Tainted ServiceStateModel stateModel;

  /**
   * Service start time. Will be zero until the service is started.
   */
  private @Tainted long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private volatile @Tainted Configuration config;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  private final ServiceOperations.@Tainted ServiceListeners listeners
    = new ServiceOperations.@Tainted ServiceListeners();
  /**
   * Static listeners to all events across all services
   */
  private static ServiceOperations.@Tainted ServiceListeners globalListeners
    = new ServiceOperations.@Tainted ServiceListeners();

  /**
   * The cause of any failure -will be null.
   * if a service did not stop due to a failure.
   */
  private @Tainted Exception failureCause;

  /**
   * the state in which the service was when it failed.
   * Only valid when the service is stopped due to a failure
   */
  private @Tainted STATE failureState = null;

  /**
   * object used to co-ordinate {@link #waitForServiceToStop(long)}
   * across threads.
   */
  private final @Tainted AtomicBoolean terminationNotification =
    new @Tainted AtomicBoolean(false);

  /**
   * History of lifecycle transitions
   */
  private final @Tainted List<@Tainted LifecycleEvent> lifecycleHistory
    = new @Tainted ArrayList<@Tainted LifecycleEvent>(5);

  /**
   * Map of blocking dependencies
   */
  private final @Tainted Map<@Tainted String, @Tainted String> blockerMap = new @Tainted HashMap<@Tainted String, @Tainted String>();

  private final @Tainted Object stateChangeLock = new @Tainted Object();
 
  /**
   * Construct the service.
   * @param name service name
   */
  public @Tainted AbstractService(@Tainted String name) {
    this.name = name;
    stateModel = new @Tainted ServiceStateModel(name);
  }

  @Override
  public final @Tainted STATE getServiceState(@Tainted AbstractService this) {
    return stateModel.getState();
  }

  @Override
  public final synchronized @Tainted Throwable getFailureCause(@Tainted AbstractService this) {
    return failureCause;
  }

  @Override
  public synchronized @Tainted STATE getFailureState(@Tainted AbstractService this) {
    return failureState;
  }

  /**
   * Set the configuration for this service.
   * This method is called during {@link #init(Configuration)}
   * and should only be needed if for some reason a service implementation
   * needs to override that initial setting -for example replacing
   * it with a new subclass of {@link Configuration}
   * @param conf new configuration.
   */
  protected void setConfig(@Tainted AbstractService this, @Tainted Configuration conf) {
    this.config = conf;
  }

  /**
   * {@inheritDoc}
   * This invokes {@link #serviceInit}
   * @param conf the configuration of the service. This must not be null
   * @throws ServiceStateException if the configuration was null,
   * the state change not permitted, or something else went wrong
   */
  @Override
  public void init(@Tainted AbstractService this, @Tainted Configuration conf) {
    if (conf == null) {
      throw new @Tainted ServiceStateException("Cannot initialize service "
                                      + getName() + ": null configuration");
    }
    if (isInState(STATE.INITED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.INITED) != STATE.INITED) {
        setConfig(conf);
        try {
          serviceInit(config);
          if (isInState(STATE.INITED)) {
            //if the service ended up here during init,
            //notify the listeners
            notifyListeners();
          }
        } catch (@Tainted Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * @throws ServiceStateException if the current service state does not permit
   * this action
   */
  @Override
  public void start(@Tainted AbstractService this) {
    if (isInState(STATE.STARTED)) {
      return;
    }
    //enter the started state
    synchronized (stateChangeLock) {
      if (stateModel.enterState(STATE.STARTED) != STATE.STARTED) {
        try {
          startTime = System.currentTimeMillis();
          serviceStart();
          if (isInState(STATE.STARTED)) {
            //if the service started (and isn't now in a later state), notify
            if (LOG.isDebugEnabled()) {
              LOG.debug("Service " + getName() + " is started");
            }
            notifyListeners();
          }
        } catch (@Tainted Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(@Tainted AbstractService this) {
    if (isInState(STATE.STOPPED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.STOPPED) != STATE.STOPPED) {
        try {
          serviceStop();
        } catch (@Tainted Exception e) {
          //stop-time exceptions are logged if they are the first one,
          noteFailure(e);
          throw ServiceStateException.convert(e);
        } finally {
          //report that the service has terminated
          terminationNotification.set(true);
          synchronized (terminationNotification) {
            terminationNotification.notifyAll();
          }
          //notify anything listening for events
          notifyListeners();
        }
      } else {
        //already stopped: note it
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring re-entrant call to stop()");
        }
      }
    }
  }

  /**
   * Relay to {@link #stop()}
   * @throws IOException
   */
  @Override
  public final void close(@Tainted AbstractService this) throws IOException {
    stop();
  }

  /**
   * Failure handling: record the exception
   * that triggered it -if there was not one already.
   * Services are free to call this themselves.
   * @param exception the exception
   */
  protected final void noteFailure(@Tainted AbstractService this, @Tainted Exception exception) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("noteFailure " + exception, null);
    }
    if (exception == null) {
      //make sure failure logic doesn't itself cause problems
      return;
    }
    //record the failure details, and log it
    synchronized (this) {
      if (failureCause == null) {
        failureCause = exception;
        failureState = getServiceState();
        LOG.info("Service " + getName()
                 + " failed in state " + failureState
                 + "; cause: " + exception,
                 exception);
      }
    }
  }

  @Override
  public final @Tainted boolean waitForServiceToStop(@Tainted AbstractService this, @Tainted long timeout) {
    @Tainted
    boolean completed = terminationNotification.get();
    while (!completed) {
      try {
        synchronized(terminationNotification) {
          terminationNotification.wait(timeout);
        }
        // here there has been a timeout, the object has terminated,
        // or there has been a spurious wakeup (which we ignore)
        completed = true;
      } catch (@Tainted InterruptedException e) {
        // interrupted; have another look at the flag
        completed = terminationNotification.get();
      }
    }
    return terminationNotification.get();
  }

  /* ===================================================================== */
  /* Override Points */
  /* ===================================================================== */

  /**
   * All initialization code needed by a service.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #init(Configuration)} prevents re-entrancy.
   *
   * The base implementation checks to see if the subclass has created
   * a new configuration instance, and if so, updates the base class value
   * @param conf configuration
   * @throws Exception on a failure -these will be caught,
   * possibly wrapped, and wil; trigger a service stop
   */
  protected void serviceInit(@Tainted AbstractService this, @Tainted Configuration conf) throws Exception {
    if (conf != config) {
      LOG.debug("Config has been overridden during init");
      setConfig(conf);
    }
  }

  /**
   * Actions called during the INITED to STARTED transition.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #start()} prevents re-entrancy.
   *
   * @throws Exception if needed -these will be caught,
   * wrapped, and trigger a service stop
   */
  protected void serviceStart(@Tainted AbstractService this) throws Exception {

  }

  /**
   * Actions called during the transition to the STOPPED state.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #stop()} prevents re-entrancy.
   *
   * Implementations MUST write this to be robust against failures, including
   * checks for null references -and for the first failure to not stop other
   * attempts to shut down parts of the service.
   *
   * @throws Exception if needed -these will be caught and logged.
   */
  protected void serviceStop(@Tainted AbstractService this) throws Exception {

  }

  @Override
  public void registerServiceListener(@Tainted AbstractService this, @Tainted ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public void unregisterServiceListener(@Tainted AbstractService this, @Tainted ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  /**
   * Register a global listener, which receives notifications
   * from the state change events of all services in the JVM
   * @param l listener
   */
  public static void registerGlobalListener(@Tainted ServiceStateChangeListener l) {
    globalListeners.add(l);
  }

  /**
   * unregister a global listener.
   * @param l listener to unregister
   * @return true if the listener was found (and then deleted)
   */
  public static @Tainted boolean unregisterGlobalListener(@Tainted ServiceStateChangeListener l) {
    return globalListeners.remove(l);
  }

  /**
   * Package-scoped method for testing -resets the global listener list
   */
  @VisibleForTesting
  static void resetGlobalListeners() {
    globalListeners.reset();
  }

  @Override
  public @Tainted String getName(@Tainted AbstractService this) {
    return name;
  }

  @Override
  public synchronized @Tainted Configuration getConfig(@Tainted AbstractService this) {
    return config;
  }

  @Override
  public @Tainted long getStartTime(@Tainted AbstractService this) {
    return startTime;
  }

  /**
   * Notify local and global listeners of state changes.
   * Exceptions raised by listeners are NOT passed up.
   */
  private void notifyListeners(@Tainted AbstractService this) {
    try {
      listeners.notifyListeners(this);
      globalListeners.notifyListeners(this);
    } catch (@Tainted Throwable e) {
      LOG.warn("Exception while notifying listeners of " + this + ": " + e,
               e);
    }
  }

  /**
   * Add a state change event to the lifecycle history
   */
  private void recordLifecycleEvent(@Tainted AbstractService this) {
    @Tainted
    LifecycleEvent event = new @Tainted LifecycleEvent();
    event.time = System.currentTimeMillis();
    event.state = getServiceState();
    lifecycleHistory.add(event);
  }

  @Override
  public synchronized @Tainted List<@Tainted LifecycleEvent> getLifecycleHistory(@Tainted AbstractService this) {
    return new @Tainted ArrayList<@Tainted LifecycleEvent>(lifecycleHistory);
  }

  /**
   * Enter a state; record this via {@link #recordLifecycleEvent}
   * and log at the info level.
   * @param newState the proposed new state
   * @return the original state
   * it wasn't already in that state, and the state model permits state re-entrancy.
   */
  private @Tainted STATE enterState(@Tainted AbstractService this, @Tainted STATE newState) {
    assert stateModel != null : "null state in " + name + " " + this.getClass();
    @Tainted
    STATE oldState = stateModel.enterState(newState);
    if (oldState != newState) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Service: " + getName() + " entered state " + getServiceState());
      }
      recordLifecycleEvent();
    }
    return oldState;
  }

  @Override
  public final @Tainted boolean isInState(@Tainted AbstractService this, Service.@Tainted STATE expected) {
    return stateModel.isInState(expected);
  }

  @Override
  public @Tainted String toString(@Tainted AbstractService this) {
    return "Service " + name + " in state " + stateModel;
  }

  /**
   * Put a blocker to the blocker map -replacing any
   * with the same name.
   * @param name blocker name
   * @param details any specifics on the block. This must be non-null.
   */
  protected void putBlocker(@Tainted AbstractService this, @Tainted String name, @Tainted String details) {
    synchronized (blockerMap) {
      blockerMap.put(name, details);
    }
  }

  /**
   * Remove a blocker from the blocker map -
   * this is a no-op if the blocker is not present
   * @param name the name of the blocker
   */
  public void removeBlocker(@Tainted AbstractService this, @Tainted String name) {
    synchronized (blockerMap) {
      blockerMap.remove(name);
    }
  }

  @Override
  public @Tainted Map<@Tainted String, @Tainted String> getBlockers(@Tainted AbstractService this) {
    synchronized (blockerMap) {
      @Tainted
      Map<@Tainted String, @Tainted String> map = new @Tainted HashMap<@Tainted String, @Tainted String>(blockerMap);
      return map;
    }
  }
}
