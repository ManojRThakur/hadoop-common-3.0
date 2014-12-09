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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

/**
 * Composition of services.
 */
@Public
@Evolving
public class CompositeService extends @Tainted AbstractService {

  private static final @Tainted Log LOG = LogFactory.getLog(CompositeService.class);

  /**
   * Policy on shutdown: attempt to close everything (purest) or
   * only try to close started services (which assumes
   * that the service implementations may not handle the stop() operation
   * except when started.
   * Irrespective of this policy, if a child service fails during
   * its init() or start() operations, it will have stop() called on it.
   */
  protected static final @Tainted boolean STOP_ONLY_STARTED_SERVICES = false;

  private final @Tainted List<@Tainted Service> serviceList = new @Tainted ArrayList<@Tainted Service>();

  public @Tainted CompositeService(@Tainted String name) {
    super(name);
  }

  /**
   * Get an unmodifiable list of services
   * @return a list of child services at the time of invocation -
   * added services will not be picked up.
   */
  public @Tainted List<@Tainted Service> getServices(@Tainted CompositeService this) {
    synchronized (serviceList) {
      return Collections.unmodifiableList(serviceList);
    }
  }

  /**
   * Add the passed {@link Service} to the list of services managed by this
   * {@link CompositeService}
   * @param service the {@link Service} to be added
   */
  protected void addService(@Tainted CompositeService this, @Tainted Service service) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding service " + service.getName());
    }
    synchronized (serviceList) {
      serviceList.add(service);
    }
  }

  /**
   * If the passed object is an instance of {@link Service},
   * add it to the list of services managed by this {@link CompositeService}
   * @param object
   * @return true if a service is added, false otherwise.
   */
  protected @Tainted boolean addIfService(@Tainted CompositeService this, @Tainted Object object) {
    if (object instanceof @Tainted Service) {
      addService((@Tainted Service) object);
      return true;
    } else {
      return false;
    }
  }

  protected synchronized @Tainted boolean removeService(@Tainted CompositeService this, @Tainted Service service) {
    synchronized (serviceList) {
      return serviceList.add(service);
    }
  }

  protected void serviceInit(@Tainted CompositeService this, @Tainted Configuration conf) throws Exception {
    @Tainted
    List<@Tainted Service> services = getServices();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": initing services, size=" + services.size());
    }
    for (@Tainted Service service : services) {
      service.init(conf);
    }
    super.serviceInit(conf);
  }

  protected void serviceStart(@Tainted CompositeService this) throws Exception {
    @Tainted
    List<@Tainted Service> services = getServices();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": starting services, size=" + services.size());
    }
    for (@Tainted Service service : services) {
      // start the service. If this fails that service
      // will be stopped and an exception raised
      service.start();
    }
    super.serviceStart();
  }

  protected void serviceStop(@Tainted CompositeService this) throws Exception {
    //stop all services that were started
    @Tainted
    int numOfServicesToStop = serviceList.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": stopping services, size=" + numOfServicesToStop);
    }
    stop(numOfServicesToStop, STOP_ONLY_STARTED_SERVICES);
    super.serviceStop();
  }

  /**
   * Stop the services in reverse order
   *
   * @param numOfServicesStarted index from where the stop should work
   * @param stopOnlyStartedServices flag to say "only start services that are
   * started, not those that are NOTINITED or INITED.
   * @throws RuntimeException the first exception raised during the
   * stop process -<i>after all services are stopped</i>
   */
  private synchronized void stop(@Tainted CompositeService this, @Tainted int numOfServicesStarted,
                                 @Tainted
                                 boolean stopOnlyStartedServices) {
    // stop in reverse order of start
    @Tainted
    Exception firstException = null;
    @Tainted
    List<@Tainted Service> services = getServices();
    for (@Tainted int i = numOfServicesStarted - 1; i >= 0; i--) {
      @Tainted
      Service service = services.get(i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopping service #" + i + ": " + service);
      }
      @Tainted
      STATE state = service.getServiceState();
      //depending on the stop police
      if (state == STATE.STARTED 
         || (!stopOnlyStartedServices && state == STATE.INITED)) {
        @Tainted
        Exception ex = ServiceOperations.stopQuietly(LOG, service);
        if (ex != null && firstException == null) {
          firstException = ex;
        }
      }
    }
    //after stopping all services, rethrow the first exception raised
    if (firstException != null) {
      throw ServiceStateException.convert(firstException);
    }
  }

  /**
   * JVM Shutdown hook for CompositeService which will stop the give
   * CompositeService gracefully in case of JVM shutdown.
   */
  public static class CompositeServiceShutdownHook implements @Tainted Runnable {

    private @Tainted CompositeService compositeService;

    public @Tainted CompositeServiceShutdownHook(@Tainted CompositeService compositeService) {
      this.compositeService = compositeService;
    }

    @Override
    public void run(CompositeService.@Tainted CompositeServiceShutdownHook this) {
      ServiceOperations.stopQuietly(compositeService);
    }
  }

}
