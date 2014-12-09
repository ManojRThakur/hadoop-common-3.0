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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Service LifeCycle.
 */
@Public
@Evolving
public interface Service extends @Tainted Closeable {

  /**
   * Service states
   */
  public enum STATE {
    /** Constructed but not initialized */

@Tainted  NOTINITED(0, "NOTINITED"),

    /** Initialized but not started or stopped */

@Tainted  INITED(1, "INITED"),

    /** started and not stopped */

@Tainted  STARTED(2, "STARTED"),

    /** stopped. No further state transitions are permitted */

@Tainted  STOPPED(3, "STOPPED");

    /**
     * An integer value for use in array lookup and JMX interfaces.
     * Although {@link Enum#ordinal()} could do this, explicitly
     * identify the numbers gives more stability guarantees over time.
     */
    private final @Tainted int value;

    /**
     * A name of the state that can be used in messages
     */
    private final @Tainted String statename;

    private @Tainted STATE(@Tainted int value, @Tainted String name) {
      this.value = value;
      this.statename = name;
    }

    /**
     * Get the integer value of a state
     * @return the numeric value of the state
     */
    public @Tainted int getValue(Service.@Tainted STATE this) {
      return value;
    }

    /**
     * Get the name of a state
     * @return the state's name
     */
    @Override
    public @Tainted String toString(Service.@Tainted STATE this) {
      return statename;
    }
  }

  /**
   * Initialize the service.
   *
   * The transition MUST be from {@link STATE#NOTINITED} to {@link STATE#INITED}
   * unless the operation failed and an exception was raised, in which case
   * {@link #stop()} MUST be invoked and the service enter the state
   * {@link STATE#STOPPED}.
   * @param config the configuration of the service
   * @throws RuntimeException on any failure during the operation

   */
  void init(@Tainted Service this, @Tainted Configuration config);


  /**
   * Start the service.
   *
   * The transition MUST be from {@link STATE#INITED} to {@link STATE#STARTED}
   * unless the operation failed and an exception was raised, in which case
   * {@link #stop()} MUST be invoked and the service enter the state
   * {@link STATE#STOPPED}.
   * @throws RuntimeException on any failure during the operation
   */

  void start(@Tainted Service this);

  /**
   * Stop the service. This MUST be a no-op if the service is already
   * in the {@link STATE#STOPPED} state. It SHOULD be a best-effort attempt
   * to stop all parts of the service.
   *
   * The implementation must be designed to complete regardless of the service
   * state, including the initialized/uninitialized state of all its internal
   * fields.
   * @throws RuntimeException on any failure during the stop operation
   */
  void stop(@Tainted Service this);

  /**
   * A version of stop() that is designed to be usable in Java7 closure
   * clauses.
   * Implementation classes MUST relay this directly to {@link #stop()}
   * @throws IOException never
   * @throws RuntimeException on any failure during the stop operation
   */
  void close(@Tainted Service this) throws IOException;

  /**
   * Register a listener to the service state change events.
   * If the supplied listener is already listening to this service,
   * this method is a no-op.
   * @param listener a new listener
   */
  void registerServiceListener(@Tainted Service this, @Tainted ServiceStateChangeListener listener);

  /**
   * Unregister a previously registered listener of the service state
   * change events. No-op if the listener is already unregistered.
   * @param listener the listener to unregister.
   */
  void unregisterServiceListener(@Tainted Service this, @Tainted ServiceStateChangeListener listener);

  /**
   * Get the name of this service.
   * @return the service name
   */
  @Tainted
  String getName(@Tainted Service this);

  /**
   * Get the configuration of this service.
   * This is normally not a clone and may be manipulated, though there are no
   * guarantees as to what the consequences of such actions may be
   * @return the current configuration, unless a specific implentation chooses
   * otherwise.
   */
  @Tainted
  Configuration getConfig(@Tainted Service this);

  /**
   * Get the current service state
   * @return the state of the service
   */
  @Tainted
  STATE getServiceState(@Tainted Service this);

  /**
   * Get the service start time
   * @return the start time of the service. This will be zero if the service
   * has not yet been started.
   */
  @Tainted
  long getStartTime(@Tainted Service this);

  /**
   * Query to see if the service is in a specific state.
   * In a multi-threaded system, the state may not hold for very long.
   * @param state the expected state
   * @return true if, at the time of invocation, the service was in that state.
   */
  @Tainted
  boolean isInState(@Tainted Service this, @Tainted STATE state);

  /**
   * Get the first exception raised during the service failure. If null,
   * no exception was logged
   * @return the failure logged during a transition to the stopped state
   */
  @Tainted
  Throwable getFailureCause(@Tainted Service this);

  /**
   * Get the state in which the failure in {@link #getFailureCause()} occurred.
   * @return the state or null if there was no failure
   */
  @Tainted
  STATE getFailureState(@Tainted Service this);

  /**
   * Block waiting for the service to stop; uses the termination notification
   * object to do so.
   *
   * This method will only return after all the service stop actions
   * have been executed (to success or failure), or the timeout elapsed
   * This method can be called before the service is inited or started; this is
   * to eliminate any race condition with the service stopping before
   * this event occurs.
   * @param timeout timeout in milliseconds. A value of zero means "forever"
   * @return true iff the service stopped in the time period
   */
  @Tainted
  boolean waitForServiceToStop(@Tainted Service this, @Tainted long timeout);

  /**
   * Get a snapshot of the lifecycle history; it is a static list
   * @return a possibly empty but never null list of lifecycle events.
   */
  public @Tainted List<@Tainted LifecycleEvent> getLifecycleHistory(@Tainted Service this);

  /**
   * Get the blockers on a service -remote dependencies
   * that are stopping the service from being <i>live</i>.
   * @return a (snapshotted) map of blocker name-&gt;description values
   */
  public @Tainted Map<@Tainted String, @Tainted String> getBlockers(@Tainted Service this);
}
