/*
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

package org.apache.hadoop.metrics2.lib;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import static org.apache.hadoop.metrics2.util.Contracts.*;

/**
 * Metric generated from a method, mostly used by annotation
 */
class MethodMetric extends @Tainted MutableMetric {
  private static final @Tainted Log LOG = LogFactory.getLog(MethodMetric.class);

  private final @Tainted Object obj;
  private final @Tainted Method method;
  private final @Tainted MetricsInfo info;
  private final @Tainted MutableMetric impl;

  @Tainted
  MethodMetric(@Tainted Object obj, @Tainted Method method, @Tainted MetricsInfo info, Metric.@Tainted Type type) {
    this.obj = checkNotNull(obj, "object");
    this.method = checkArg(method, method.getParameterTypes().length == 0,
                           "Metric method should have no arguments");
    this.method.setAccessible(true);
    this.info = checkNotNull(info, "info");
    impl = newImpl(checkNotNull(type, "metric type"));
  }

  private @Tainted MutableMetric newImpl(@Tainted MethodMetric this, Metric.@Tainted Type metricType) {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> resType = method.getReturnType();
    switch (metricType) {
      case COUNTER:
        return newCounter(resType);
      case GAUGE:
        return newGauge(resType);
      case DEFAULT:
        return resType == String.class ? newTag(resType) : newGauge(resType);
      case TAG:
        return newTag(resType);
      default:
        checkArg(metricType, false, "unsupported metric type");
        return null;
    }
  }

  @Tainted
  MutableMetric newCounter(@Tainted MethodMetric this, final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> type) {
    if (isInt(type) || isLong(type)) {
      return new @Tainted MutableMetric() {
        @Override public void snapshot(@Tainted MetricsRecordBuilder rb, @Tainted boolean all) {
          try {
            @Tainted
            Object ret = method.invoke(obj, (@Tainted Object @Tainted [])null);
            if (isInt(type)) rb.addCounter(info, ((@Tainted Integer) ret).intValue());
            else rb.addCounter(info, ((@Tainted Long) ret).longValue());
          } catch (@Tainted Exception ex) {
            LOG.error("Error invoking method "+ method.getName(), ex);
          }
        }
      };
    }
    throw new @Tainted MetricsException("Unsupported counter type: "+ type.getName());
  }

  static @Tainted boolean isInt(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> type) {
    @Tainted
    boolean ret = type == Integer.TYPE || type == Integer.class;
    return ret;
  }

  static @Tainted boolean isLong(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> type) {
    return type == Long.TYPE || type == Long.class;
  }

  static @Tainted boolean isFloat(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> type) {
    return type == Float.TYPE || type == Float.class;
  }

  static @Tainted boolean isDouble(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> type) {
    return type == Double.TYPE || type == Double.class;
  }

  @Tainted
  MutableMetric newGauge(@Tainted MethodMetric this, final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> t) {
    if (isInt(t) || isLong(t) || isFloat(t) || isDouble(t)) {
      return new @Tainted MutableMetric() {
        @Override public void snapshot(@Tainted MetricsRecordBuilder rb, @Tainted boolean all) {
          try {
            @Tainted
            Object ret = method.invoke(obj, (@Tainted Object @Tainted []) null);
            if (isInt(t)) rb.addGauge(info, ((@Tainted Integer) ret).intValue());
            else if (isLong(t)) rb.addGauge(info, ((@Tainted Long) ret).longValue());
            else if (isFloat(t)) rb.addGauge(info, ((@Tainted Float) ret).floatValue());
            else rb.addGauge(info, ((@Tainted Double) ret).doubleValue());
          } catch (@Tainted Exception ex) {
            LOG.error("Error invoking method "+ method.getName(), ex);
          }
        }
      };
    }
    throw new @Tainted MetricsException("Unsupported gauge type: "+ t.getName());
  }

  @Tainted
  MutableMetric newTag(@Tainted MethodMetric this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> resType) {
    if (resType == String.class) {
      return new @Tainted MutableMetric() {
        @Override public void snapshot(@Tainted MetricsRecordBuilder rb, @Tainted boolean all) {
          try {
            @Tainted
            Object ret = method.invoke(obj, (@Tainted Object @Tainted []) null);
            rb.tag(info, (@Tainted String) ret);
          } catch (@Tainted Exception ex) {
            LOG.error("Error invoking method "+ method.getName(), ex);
          }
        }
      };
    }
    throw new @Tainted MetricsException("Unsupported tag type: "+ resType.getName());
  }

  @Override public void snapshot(@Tainted MethodMetric this, @Tainted MetricsRecordBuilder builder, @Tainted boolean all) {
    impl.snapshot(builder, all);
  }

  static @Tainted MetricsInfo metricInfo(@Tainted Method method) {
    return Interns.info(nameFrom(method), "Metric for "+ method.getName());
  }

  static @Tainted String nameFrom(@Tainted Method method) {
    @Tainted
    String methodName = method.getName();
    if (methodName.startsWith("get")) {
      return StringUtils.capitalize(methodName.substring(3));
    }
    return StringUtils.capitalize(methodName);
  }
}
