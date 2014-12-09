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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics.MetricsUtil;



/**
 * This abstract base class facilitates creating dynamic mbeans automatically from
 * metrics. 
 * The metrics constructors registers metrics in a registry. 
 * Different categories of metrics should be in differnt classes with their own
 * registry (as in NameNodeMetrics and DataNodeMetrics).
 * Then the MBean can be created passing the registry to the constructor.
 * The MBean should be then registered using a mbean name (example):
 *  MetricsHolder myMetrics = new MetricsHolder(); // has metrics and registry
 *  MetricsTestMBean theMBean = new MetricsTestMBean(myMetrics.mregistry);
 *  ObjectName mbeanName = MBeanUtil.registerMBean("ServiceFoo",
 *                "TestStatistics", theMBean);
 * 
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public abstract class MetricsDynamicMBeanBase implements @Tainted DynamicMBean {
  private final static @Tainted String AVG_TIME = "AvgTime";
  private final static @Tainted String MIN_TIME = "MinTime";
  private final static @Tainted String MAX_TIME = "MaxTime";
  private final static @Tainted String NUM_OPS = "NumOps";
  private final static @Tainted String RESET_ALL_MIN_MAX_OP = "resetAllMinMax";
  private @Tainted MetricsRegistry metricsRegistry;
  private @Tainted MBeanInfo mbeanInfo;
  private @Tainted Map<@Tainted String, @Tainted MetricsBase> metricsRateAttributeMod;
  private @Tainted int numEntriesInRegistry = 0;
  private @Tainted String mbeanDescription;
  
  protected @Tainted MetricsDynamicMBeanBase(final @Tainted MetricsRegistry mr, final @Tainted String aMBeanDescription) {
    metricsRegistry = mr;
    mbeanDescription = aMBeanDescription;
    metricsRateAttributeMod = new @Tainted ConcurrentHashMap<@Tainted String, @Tainted MetricsBase>();
    createMBeanInfo();
  }
  
  private void updateMbeanInfoIfMetricsListChanged(@Tainted MetricsDynamicMBeanBase this)  {
    if (numEntriesInRegistry != metricsRegistry.size())
      createMBeanInfo();
  }
  
  private void createMBeanInfo(@Tainted MetricsDynamicMBeanBase this) {
    @Tainted
    boolean needsMinMaxResetOperation = false;
    @Tainted
    List<@Tainted MBeanAttributeInfo> attributesInfo = new @Tainted ArrayList<@Tainted MBeanAttributeInfo>();
    @Tainted
    MBeanOperationInfo @Tainted [] operationsInfo = null;
    numEntriesInRegistry = metricsRegistry.size();
    
    for (@Tainted MetricsBase o : metricsRegistry.getMetricsList()) {

      if (MetricsTimeVaryingRate.class.isInstance(o)) {
        // For each of the metrics there are 3 different attributes
        attributesInfo.add(new @Tainted MBeanAttributeInfo(o.getName() + NUM_OPS, "java.lang.Integer",
            o.getDescription(), true, false, false));
        attributesInfo.add(new @Tainted MBeanAttributeInfo(o.getName() + AVG_TIME, "java.lang.Long",
            o.getDescription(), true, false, false));
        attributesInfo.add(new @Tainted MBeanAttributeInfo(o.getName() + MIN_TIME, "java.lang.Long",
            o.getDescription(), true, false, false));
        attributesInfo.add(new @Tainted MBeanAttributeInfo(o.getName() + MAX_TIME, "java.lang.Long",
            o.getDescription(), true, false, false));
        needsMinMaxResetOperation = true;  // the min and max can be reset.
        
        // Note the special attributes (AVG_TIME, MIN_TIME, ..) are derived from metrics 
        // Rather than check for the suffix we store them in a map.
        metricsRateAttributeMod.put(o.getName() + NUM_OPS, o);
        metricsRateAttributeMod.put(o.getName() + AVG_TIME, o);
        metricsRateAttributeMod.put(o.getName() + MIN_TIME, o);
        metricsRateAttributeMod.put(o.getName() + MAX_TIME, o);
        
      }  else if ( MetricsIntValue.class.isInstance(o) || MetricsTimeVaryingInt.class.isInstance(o) ) {
        attributesInfo.add(new @Tainted MBeanAttributeInfo(o.getName(), "java.lang.Integer",
            o.getDescription(), true, false, false)); 
      } else if ( MetricsLongValue.class.isInstance(o) || MetricsTimeVaryingLong.class.isInstance(o) ) {
        attributesInfo.add(new @Tainted MBeanAttributeInfo(o.getName(), "java.lang.Long",
            o.getDescription(), true, false, false));     
      } else {
        MetricsUtil.LOG.error("unknown metrics type: " + o.getClass().getName());
      }

      if (needsMinMaxResetOperation) {
        operationsInfo = new @Tainted MBeanOperationInfo @Tainted [] {
            new @Tainted MBeanOperationInfo(RESET_ALL_MIN_MAX_OP, "Reset (zero) All Min Max",
                    null, "void", MBeanOperationInfo.ACTION) };
      }
    }
    @Tainted
    MBeanAttributeInfo @Tainted [] attrArray = new @Tainted MBeanAttributeInfo @Tainted [attributesInfo.size()];
    mbeanInfo =  new @Tainted MBeanInfo(this.getClass().getName(), mbeanDescription, 
        attributesInfo.toArray(attrArray), null, operationsInfo, null);
  }
  
  @Override
  public @Tainted Object getAttribute(@Tainted MetricsDynamicMBeanBase this, @Tainted String attributeName) throws AttributeNotFoundException,
      MBeanException, ReflectionException {
    if (attributeName == null || attributeName.isEmpty()) 
      throw new @Tainted IllegalArgumentException();
    
    updateMbeanInfoIfMetricsListChanged();
    
    @Tainted
    Object o = metricsRateAttributeMod.get(attributeName);
    if (o == null) {
      o = metricsRegistry.get(attributeName);
    }
    if (o == null)
      throw new @Tainted AttributeNotFoundException();
    
    if (o instanceof @Tainted MetricsIntValue)
      return ((@Tainted MetricsIntValue) o).get();
    else if (o instanceof @Tainted MetricsLongValue)
      return ((@Tainted MetricsLongValue) o).get();
    else if (o instanceof @Tainted MetricsTimeVaryingInt)
      return ((@Tainted MetricsTimeVaryingInt) o).getPreviousIntervalValue();
    else if (o instanceof @Tainted MetricsTimeVaryingLong)
      return ((@Tainted MetricsTimeVaryingLong) o).getPreviousIntervalValue();
    else if (o instanceof @Tainted MetricsTimeVaryingRate) {
      @Tainted
      MetricsTimeVaryingRate or = (@Tainted MetricsTimeVaryingRate) o;
      if (attributeName.endsWith(NUM_OPS))
        return or.getPreviousIntervalNumOps();
      else if (attributeName.endsWith(AVG_TIME))
        return or.getPreviousIntervalAverageTime();
      else if (attributeName.endsWith(MIN_TIME))
        return or.getMinTime();
      else if (attributeName.endsWith(MAX_TIME))
        return or.getMaxTime();
      else {
        MetricsUtil.LOG.error("Unexpected attrubute suffix");
        throw new @Tainted AttributeNotFoundException();
      }
    } else {
        MetricsUtil.LOG.error("unknown metrics type: " + o.getClass().getName());
        throw new @Tainted AttributeNotFoundException();
    }
  }

  @Override
  public @Tainted AttributeList getAttributes(@Tainted MetricsDynamicMBeanBase this, @Tainted String @Tainted [] attributeNames) {
    if (attributeNames == null || attributeNames.length == 0) 
      throw new @Tainted IllegalArgumentException();
    
    updateMbeanInfoIfMetricsListChanged();
    
    @Tainted
    AttributeList result = new @Tainted AttributeList(attributeNames.length);
    for (@Tainted String iAttributeName : attributeNames) {
      try {
        @Tainted
        Object value = getAttribute(iAttributeName);
        result.add(new @Tainted Attribute(iAttributeName, value));
      } catch (@Tainted Exception e) {
        continue;
      } 
    }
    return result;
  }

  @Override
  public @Tainted MBeanInfo getMBeanInfo(@Tainted MetricsDynamicMBeanBase this) {
    return mbeanInfo;
  }

  @Override
  public @Tainted Object invoke(@Tainted MetricsDynamicMBeanBase this, @Tainted String actionName, @Tainted Object @Tainted [] parms, @Tainted String @Tainted [] signature)
      throws MBeanException, ReflectionException {
    
    if (actionName == null || actionName.isEmpty()) 
      throw new @Tainted IllegalArgumentException();
    
    
    // Right now we support only one fixed operation (if it applies)
    if (!(actionName.equals(RESET_ALL_MIN_MAX_OP)) || 
        mbeanInfo.getOperations().length != 1) {
      throw new @Tainted ReflectionException(new @Tainted NoSuchMethodException(actionName));
    }
    for (@Tainted MetricsBase m : metricsRegistry.getMetricsList())  {
      if ( MetricsTimeVaryingRate.class.isInstance(m) ) {
        MetricsTimeVaryingRate.class.cast(m).resetMinMax();
      }
    }
    return null;
  }

  @Override
  public void setAttribute(@Tainted MetricsDynamicMBeanBase this, @Tainted Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
      MBeanException, ReflectionException {
    throw new @Tainted ReflectionException(new @Tainted NoSuchMethodException("set" + attribute));
  }

  @Override
  public @Tainted AttributeList setAttributes(@Tainted MetricsDynamicMBeanBase this, @Tainted AttributeList attributes) {
    return null;
  }
}
