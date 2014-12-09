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
package org.apache.hadoop.metrics.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.InstanceAlreadyExistsException;

import org.apache.hadoop.classification.InterfaceAudience;


/**
 * This util class provides a method to register an MBean using
 * our standard naming convention as described in the doc
 *  for {link {@link #registerMBean(String, String, Object)}
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class MBeanUtil {
	
  /**
   * Register the MBean using our standard MBeanName format
   * "hadoop:service=<serviceName>,name=<nameName>"
   * Where the <serviceName> and <nameName> are the supplied parameters
   *    
   * @param serviceName
   * @param nameName
   * @param theMbean - the MBean to register
   * @return the named used to register the MBean
   */	
  static public @Tainted ObjectName registerMBean(final @Tainted String serviceName, 
		  							final @Tainted String nameName,
		  							final @Tainted Object theMbean) {
    final @Tainted MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    @Tainted
    ObjectName name = getMBeanName(serviceName, nameName);
    try {
      mbs.registerMBean(theMbean, name);
      return name;
    } catch (@Tainted InstanceAlreadyExistsException ie) {
      // Ignore if instance already exists 
    } catch (@Tainted Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  static public void unregisterMBean(@Tainted ObjectName mbeanName) {
    final @Tainted MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (mbeanName == null) 
        return;
    try {
      mbs.unregisterMBean(mbeanName);
    } catch (@Tainted InstanceNotFoundException e ) {
      // ignore
    } catch (@Tainted Exception e) {
      e.printStackTrace();
    } 
  }
  
  static private @Tainted ObjectName getMBeanName(final @Tainted String serviceName,
		  								 final @Tainted String nameName) {
    @Tainted
    ObjectName name = null;
    try {
      name = new @Tainted ObjectName("hadoop:" +
                  "service=" + serviceName + ",name=" + nameName);
    } catch (@Tainted MalformedObjectNameException e) {
      e.printStackTrace();
    }
    return name;
  }
}
