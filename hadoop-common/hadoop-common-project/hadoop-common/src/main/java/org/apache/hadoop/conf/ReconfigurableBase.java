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

package org.apache.hadoop.conf;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import org.apache.commons.logging.*;

import java.util.Collection;

/**
 * Utility base class for implementing the Reconfigurable interface.
 *
 * Subclasses should override reconfigurePropertyImpl to change individual
 * properties and getReconfigurableProperties to get all properties that
 * can be changed at run time.
 */
public abstract class ReconfigurableBase 
  extends @Tainted Configured implements @Tainted Reconfigurable {
  
  private static final @Tainted Log LOG =
    LogFactory.getLog(ReconfigurableBase.class);

  /**
   * Construct a ReconfigurableBase.
   */
  public @Tainted ReconfigurableBase() {
    super(new @Tainted Configuration());
  }

  /**
   * Construct a ReconfigurableBase with the {@link Configuration}
   * conf.
   */
  public @Tainted ReconfigurableBase(@Tainted Configuration conf) {
    super((conf == null) ? new @Tainted Configuration() : conf);
  }

  /**
   * {@inheritDoc}
   *
   * This method makes the change to this objects {@link Configuration}
   * and calls reconfigurePropertyImpl to update internal data structures.
   * This method cannot be overridden, subclasses should instead override
   * reconfigureProperty.
   */
  @Override
  public final @Tainted String reconfigureProperty(@Tainted ReconfigurableBase this, @Tainted String property, @Untainted String newVal)
    throws ReconfigurationException {
    if (isPropertyReconfigurable(property)) {
      LOG.info("changing property " + property + " to " + newVal);
      @Tainted
      String oldVal;
      synchronized(getConf()) {
        oldVal = getConf().get(property);
        reconfigurePropertyImpl(property, newVal);
        if (newVal != null) {
          getConf().set(property, newVal);
        } else {
          getConf().unset(property);
        }
      }
      return oldVal;
    } else {
      throw new @Tainted ReconfigurationException(property, newVal,
                                             getConf().get(property));
    }
  }

  /**
   * {@inheritDoc}
   *
   * Subclasses must override this.
   */
  @Override 
  public abstract @Tainted Collection<@Tainted String> getReconfigurableProperties(@Tainted ReconfigurableBase this);


  /**
   * {@inheritDoc}
   *
   * Subclasses may wish to override this with a more efficient implementation.
   */
  @Override
  public @Tainted boolean isPropertyReconfigurable(@Tainted ReconfigurableBase this, @Tainted String property) {
    return getReconfigurableProperties().contains(property);
  }

  /**
   * Change a configuration property.
   *
   * Subclasses must override this. This method applies the change to
   * all internal data structures derived from the configuration property
   * that is being changed. If this object owns other Reconfigurable objects
   * reconfigureProperty should be called recursively to make sure that
   * to make sure that the configuration of these objects is updated.
   */
  protected abstract void reconfigurePropertyImpl(@Tainted ReconfigurableBase this, @Tainted String property, @Tainted String newVal) 
    throws ReconfigurationException;

}
