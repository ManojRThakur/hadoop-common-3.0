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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * <p>
 * Simple {@link DNSToSwitchMapping} implementation that reads a 2 column text
 * file. The columns are separated by whitespace. The first column is a DNS or
 * IP address and the second column specifies the rack where the address maps.
 * </p>
 * <p>
 * This class uses the configuration parameter {@code
 * net.topology.table.file.name} to locate the mapping file.
 * </p>
 * <p>
 * Calls to {@link #resolve(List)} will look up the address as defined in the
 * mapping file. If no entry corresponding to the address is found, the value
 * {@code /default-rack} is returned.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TableMapping extends @Tainted CachedDNSToSwitchMapping {

  private static final @Tainted Log LOG = LogFactory.getLog(TableMapping.class);
  
  public @Tainted TableMapping() {
    super(new @Tainted RawTableMapping());
  }
  
  private @Tainted RawTableMapping getRawMapping(@Tainted TableMapping this) {
    return (@Tainted RawTableMapping) rawMapping;
  }

  @Override
  public @Tainted Configuration getConf(@Tainted TableMapping this) {
    return getRawMapping().getConf();
  }

  @Override
  public void setConf(@Tainted TableMapping this, @Tainted Configuration conf) {
    super.setConf(conf);
    getRawMapping().setConf(conf);
  }
  
  @Override
  public void reloadCachedMappings(@Tainted TableMapping this) {
    super.reloadCachedMappings();
    getRawMapping().reloadCachedMappings();
  }
  
  private static final class RawTableMapping extends @Tainted Configured
      implements @Tainted DNSToSwitchMapping {
    
    private @Tainted Map<@Tainted String, @Tainted String> map;
  
    private @Tainted Map<@Tainted String, @Tainted String> load(TableMapping.@Tainted RawTableMapping this) {
      @Tainted
      Map<@Tainted String, @Tainted String> loadMap = new @Tainted HashMap<@Tainted String, @Tainted String>();
  
      @Tainted
      String filename = getConf().get(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, null);
      if (StringUtils.isBlank(filename)) {
        LOG.warn(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY + " not configured. ");
        return null;
      }
  
      @Tainted
      BufferedReader reader = null;
      try {
        reader = new @Tainted BufferedReader(new @Tainted FileReader(filename));
        @Tainted
        String line = reader.readLine();
        while (line != null) {
          line = line.trim();
          if (line.length() != 0 && line.charAt(0) != '#') {
            @Tainted
            String @Tainted [] columns = line.split("\\s+");
            if (columns.length == 2) {
              loadMap.put(columns[0], columns[1]);
            } else {
              LOG.warn("Line does not have two columns. Ignoring. " + line);
            }
          }
          line = reader.readLine();
        }
      } catch (@Tainted Exception e) {
        LOG.warn(filename + " cannot be read.", e);
        return null;
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (@Tainted IOException e) {
            LOG.warn(filename + " cannot be read.", e);
            return null;
          }
        }
      }
      return loadMap;
    }
  
    @Override
    public synchronized @Tainted List<@Tainted String> resolve(TableMapping.@Tainted RawTableMapping this, @Tainted List<@Tainted String> names) {
      if (map == null) {
        map = load();
        if (map == null) {
          LOG.warn("Failed to read topology table. " +
            NetworkTopology.DEFAULT_RACK + " will be used for all nodes.");
          map = new @Tainted HashMap<@Tainted String, @Tainted String>();
        }
      }
      @Tainted
      List<@Tainted String> results = new @Tainted ArrayList<@Tainted String>(names.size());
      for (@Tainted String name : names) {
        @Tainted
        String result = map.get(name);
        if (result != null) {
          results.add(result);
        } else {
          results.add(NetworkTopology.DEFAULT_RACK);
        }
      }
      return results;
    }

    @Override
    public void reloadCachedMappings(TableMapping.@Tainted RawTableMapping this) {
      @Tainted
      Map<@Tainted String, @Tainted String> newMap = load();
      if (newMap == null) {
        LOG.error("Failed to reload the topology table.  The cached " +
            "mappings will not be cleared.");
      } else {
        synchronized(this) {
          map = newMap;
        }
      }
    }

    @Override
    public void reloadCachedMappings(TableMapping.@Tainted RawTableMapping this, @Tainted List<@Tainted String> names) {
      // TableMapping has to reload all mappings at once, so no chance to 
      // reload mappings on specific nodes
      reloadCachedMappings();
    }
  }
}
