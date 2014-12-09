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
package org.apache.hadoop.net;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A cached implementation of DNSToSwitchMapping that takes an
 * raw DNSToSwitchMapping and stores the resolved network location in 
 * a cache. The following calls to a resolved network location
 * will get its location from the cache. 
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachedDNSToSwitchMapping extends @Tainted AbstractDNSToSwitchMapping {
  private @Tainted Map<@Tainted String, @Tainted String> cache = new @Tainted ConcurrentHashMap<@Tainted String, @Tainted String>();

  /**
   * The uncached mapping
   */
  protected final @Tainted DNSToSwitchMapping rawMapping;

  /**
   * cache a raw DNS mapping
   * @param rawMapping the raw mapping to cache
   */
  public @Tainted CachedDNSToSwitchMapping(@Tainted DNSToSwitchMapping rawMapping) {
    this.rawMapping = rawMapping;
  }

  /**
   * @param names a list of hostnames to probe for being cached
   * @return the hosts from 'names' that have not been cached previously
   */
  private @Tainted List<@Tainted String> getUncachedHosts(@Tainted CachedDNSToSwitchMapping this, @Tainted List<@Tainted String> names) {
    // find out all names without cached resolved location
    @Tainted
    List<@Tainted String> unCachedHosts = new @Tainted ArrayList<@Tainted String>(names.size());
    for (@Tainted String name : names) {
      if (cache.get(name) == null) {
        unCachedHosts.add(name);
      } 
    }
    return unCachedHosts;
  }

  /**
   * Caches the resolved host:rack mappings. The two list
   * parameters must be of equal size.
   *
   * @param uncachedHosts a list of hosts that were uncached
   * @param resolvedHosts a list of resolved host entries where the element
   * at index(i) is the resolved value for the entry in uncachedHosts[i]
   */
  private void cacheResolvedHosts(@Tainted CachedDNSToSwitchMapping this, @Tainted List<@Tainted String> uncachedHosts, 
      @Tainted
      List<@Tainted String> resolvedHosts) {
    // Cache the result
    if (resolvedHosts != null) {
      for (@Tainted int i=0; i<uncachedHosts.size(); i++) {
        cache.put(uncachedHosts.get(i), resolvedHosts.get(i));
      }
    }
  }

  /**
   * @param names a list of hostnames to look up (can be be empty)
   * @return the cached resolution of the list of hostnames/addresses.
   *  or null if any of the names are not currently in the cache
   */
  private @Tainted List<@Tainted String> getCachedHosts(@Tainted CachedDNSToSwitchMapping this, @Tainted List<@Tainted String> names) {
    @Tainted
    List<@Tainted String> result = new @Tainted ArrayList<@Tainted String>(names.size());
    // Construct the result
    for (@Tainted String name : names) {
      @Tainted
      String networkLocation = cache.get(name);
      if (networkLocation != null) {
        result.add(networkLocation);
      } else {
        return null;
      }
    }
    return result;
  }

  @Override
  public @Tainted List<@Tainted String> resolve(@Tainted CachedDNSToSwitchMapping this, @Tainted List<@Tainted String> names) {
    // normalize all input names to be in the form of IP addresses
    names = NetUtils.normalizeHostNames(names);

    @Tainted
    List <@Tainted String> result = new @Tainted ArrayList<@Tainted String>(names.size());
    if (names.isEmpty()) {
      return result;
    }

    @Tainted
    List<@Tainted String> uncachedHosts = getUncachedHosts(names);

    // Resolve the uncached hosts
    @Tainted
    List<@Tainted String> resolvedHosts = rawMapping.resolve(uncachedHosts);
    //cache them
    cacheResolvedHosts(uncachedHosts, resolvedHosts);
    //now look up the entire list in the cache
    return getCachedHosts(names);

  }

  /**
   * Get the (host x switch) map.
   * @return a copy of the cached map of hosts to rack
   */
  @Override
  public @Tainted Map<@Tainted String, @Tainted String> getSwitchMap(@Tainted CachedDNSToSwitchMapping this) {
    @Tainted
    Map<@Tainted String, @Tainted String > switchMap = new @Tainted HashMap<@Tainted String, @Tainted String>(cache);
    return switchMap;
  }


  @Override
  public @Tainted String toString(@Tainted CachedDNSToSwitchMapping this) {
    return "cached switch mapping relaying to " + rawMapping;
  }

  /**
   * Delegate the switch topology query to the raw mapping, via
   * {@link AbstractDNSToSwitchMapping#isMappingSingleSwitch(DNSToSwitchMapping)}
   * @return true iff the raw mapper is considered single-switch.
   */
  @Override
  public @Tainted boolean isSingleSwitch(@Tainted CachedDNSToSwitchMapping this) {
    return isMappingSingleSwitch(rawMapping);
  }
  
  @Override
  public void reloadCachedMappings(@Tainted CachedDNSToSwitchMapping this) {
    cache.clear();
  }

  @Override
  public void reloadCachedMappings(@Tainted CachedDNSToSwitchMapping this, @Tainted List<@Tainted String> names) {
    for (@Tainted String name : names) {
      cache.remove(name);
    }
  }
}
