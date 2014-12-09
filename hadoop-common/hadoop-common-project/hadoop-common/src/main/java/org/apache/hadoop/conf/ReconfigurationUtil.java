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
import java.util.Map;
import java.util.Collection;
import java.util.HashMap;

public class ReconfigurationUtil {

  public static class PropertyChange {
    public @Tainted String prop;
    public @Tainted String oldVal;
    public @Tainted String newVal;

    public @Tainted PropertyChange(@Tainted String prop, @Tainted String newVal, @Tainted String oldVal) {
      this.prop = prop;
      this.newVal = newVal;
      this.oldVal = oldVal;
    }
  }

  public static @Tainted Collection<@Tainted PropertyChange> 
    getChangedProperties(@Tainted Configuration newConf, @Tainted Configuration oldConf) {
    @Tainted
    Map<@Tainted String, @Tainted PropertyChange> changes = new @Tainted HashMap<@Tainted String, @Tainted PropertyChange>();

    // iterate over old configuration
    for (Map.@Tainted Entry<@Tainted String, @Untainted String> oldEntry: oldConf) {
      @Tainted
      String prop = oldEntry.getKey();
      @Tainted
      String oldVal = oldEntry.getValue();
      @Tainted
      String newVal = newConf.getRaw(prop);
      
      if (newVal == null || !newVal.equals(oldVal)) {
        changes.put(prop, new @Tainted PropertyChange(prop, newVal, oldVal));
      }
    }
    
    // now iterate over new configuration
    // (to look for properties not present in old conf)
    for (Map.@Tainted Entry<@Tainted String, @Untainted String> newEntry: newConf) {
      @Tainted
      String prop = newEntry.getKey();
      @Tainted
      String newVal = newEntry.getValue();
      if (oldConf.get(prop) == null) {
        changes.put(prop, new @Tainted PropertyChange(prop, newVal, null));
      }
    } 

    return changes.values();
  }
}