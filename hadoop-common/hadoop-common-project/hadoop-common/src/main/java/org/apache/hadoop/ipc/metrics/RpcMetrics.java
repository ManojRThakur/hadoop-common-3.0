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
package org.apache.hadoop.ipc.metrics;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Aggregate RPC metrics", context="rpc")
public class RpcMetrics {

  static final @Tainted Log LOG = LogFactory.getLog(RpcMetrics.class);
  final @Tainted Server server;
  final @Tainted MetricsRegistry registry;
  final @Tainted String name;
  
  @Tainted
  RpcMetrics(@Tainted Server server) {
    @Tainted
    String port = String.valueOf(server.getListenerAddress().getPort());
    name = "RpcActivityForPort"+ port;
    this.server = server;
    registry = new @Tainted MetricsRegistry("rpc").tag("port", "RPC port", port);
    LOG.debug("Initialized "+ registry);
  }

  public @Tainted String name(@Tainted RpcMetrics this) { return name; }

  public static @Tainted RpcMetrics create(@Tainted Server server) {
    @Tainted
    RpcMetrics m = new @Tainted RpcMetrics(server);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  @Metric("Number of received bytes") @Tainted MutableCounterLong receivedBytes;
  @Metric("Number of sent bytes") @Tainted MutableCounterLong sentBytes;
  @Metric("Queue time") @Tainted MutableRate rpcQueueTime;
  @Metric("Processsing time") @Tainted MutableRate rpcProcessingTime;
  @Metric("Number of authentication failures")
  @Tainted
  MutableCounterInt rpcAuthenticationFailures;
  @Metric("Number of authentication successes")
  @Tainted
  MutableCounterInt rpcAuthenticationSuccesses;
  @Metric("Number of authorization failures")
  @Tainted
  MutableCounterInt rpcAuthorizationFailures;
  @Metric("Number of authorization sucesses")
  @Tainted
  MutableCounterInt rpcAuthorizationSuccesses;

  @Metric("Number of open connections") public @Tainted int numOpenConnections(@Tainted RpcMetrics this) {
    return server.getNumOpenConnections();
  }

  @Metric("Length of the call queue") public @Tainted int callQueueLength(@Tainted RpcMetrics this) {
    return server.getCallQueueLen();
  }

  // Public instrumentation methods that could be extracted to an
  // abstract class if we decide to do custom instrumentation classes a la
  // JobTrackerInstrumenation. The methods with //@Override comment are
  // candidates for abstract methods in a abstract instrumentation class.

  /**
   * One authentication failure event
   */
  //@Override
  public void incrAuthenticationFailures(@Tainted RpcMetrics this) {
    rpcAuthenticationFailures.incr();
  }

  /**
   * One authentication success event
   */
  //@Override
  public void incrAuthenticationSuccesses(@Tainted RpcMetrics this) {
    rpcAuthenticationSuccesses.incr();
  }

  /**
   * One authorization success event
   */
  //@Override
  public void incrAuthorizationSuccesses(@Tainted RpcMetrics this) {
    rpcAuthorizationSuccesses.incr();
  }

  /**
   * One authorization failure event
   */
  //@Override
  public void incrAuthorizationFailures(@Tainted RpcMetrics this) {
    rpcAuthorizationFailures.incr();
  }

  /**
   * Shutdown the instrumentation for the process
   */
  //@Override
  public void shutdown(@Tainted RpcMetrics this) {}

  /**
   * Increment sent bytes by count
   * @param count to increment
   */
  //@Override
  public void incrSentBytes(@Tainted RpcMetrics this, @Tainted int count) {
    sentBytes.incr(count);
  }

  /**
   * Increment received bytes by count
   * @param count to increment
   */
  //@Override
  public void incrReceivedBytes(@Tainted RpcMetrics this, @Tainted int count) {
    receivedBytes.incr(count);
  }

  /**
   * Add an RPC queue time sample
   * @param qTime the queue time
   */
  //@Override
  public void addRpcQueueTime(@Tainted RpcMetrics this, @Tainted int qTime) {
    rpcQueueTime.add(qTime);
  }

  /**
   * Add an RPC processing time sample
   * @param processingTime the processing time
   */
  //@Override
  public void addRpcProcessingTime(@Tainted RpcMetrics this, @Tainted int processingTime) {
    rpcProcessingTime.add(processingTime);
  }
}
