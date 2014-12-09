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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Helper class to build {@link MetricsSource} object from annotations.
 * <p>
 * For a given source object:
 * <ul>
 * <li>Sets the {@link Field}s annotated with {@link Metric} to
 * {@link MutableMetric} and adds it to the {@link MetricsRegistry}.</li>
 * <li>
 * For {@link Method}s annotated with {@link Metric} creates
 * {@link MutableMetric} and adds it to the {@link MetricsRegistry}.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class MetricsSourceBuilder {
  private static final @Tainted Log LOG = LogFactory.getLog(MetricsSourceBuilder.class);

  private final @Tainted Object source;
  private final @Tainted MutableMetricsFactory factory;
  private final @Tainted MetricsRegistry registry;
  private @Tainted MetricsInfo info;
  private @Tainted boolean hasAtMetric = false;
  private @Tainted boolean hasRegistry = false;

  @Tainted
  MetricsSourceBuilder(@Tainted Object source, @Tainted MutableMetricsFactory factory) {
    this.source = checkNotNull(source, "source");
    this.factory = checkNotNull(factory, "mutable metrics factory");
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> cls = source.getClass();
    registry = initRegistry(source);

    for (@Tainted Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(cls)) {
      add(source, field);
    }
    for (@Tainted Method method : ReflectionUtils.getDeclaredMethodsIncludingInherited(cls)) {
      add(source, method);
    }
  }

  public @Tainted MetricsSource build(@Tainted MetricsSourceBuilder this) {
    if (source instanceof @Tainted MetricsSource) {
      if (hasAtMetric && !hasRegistry) {
        throw new @Tainted MetricsException("Hybrid metrics: registry required.");
      }
      return (@Tainted MetricsSource) source;
    }
    else if (!hasAtMetric) {
      throw new @Tainted MetricsException("No valid @Metric annotation found.");
    }
    return new @Tainted MetricsSource() {
      @Override
      public void getMetrics(@Tainted MetricsCollector builder, @Tainted boolean all) {
        registry.snapshot(builder.addRecord(registry.info()), all);
      }
    };
  }

  public @Tainted MetricsInfo info(@Tainted MetricsSourceBuilder this) {
    return info;
  }

  private @Tainted MetricsRegistry initRegistry(@Tainted MetricsSourceBuilder this, @Tainted Object source) {
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> cls = source.getClass();
    @Tainted
    MetricsRegistry r = null;
    // Get the registry if it already exists.
    for (@Tainted Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(cls)) {
      if (field.getType() != MetricsRegistry.class) continue;
      try {
        field.setAccessible(true);
        r = (@Tainted MetricsRegistry) field.get(source);
        hasRegistry = r != null;
        break;
      } catch (@Tainted Exception e) {
        LOG.warn("Error accessing field "+ field, e);
        continue;
      }
    }
    // Create a new registry according to annotation
    for (@Tainted Annotation annotation : cls.getAnnotations()) {
      if (annotation instanceof @Tainted Metrics) {
        @Tainted
        Metrics ma = (@Tainted Metrics) annotation;
        info = factory.getInfo(cls, ma);
        if (r == null) {
          r = new @Tainted MetricsRegistry(info);
        }
        r.setContext(ma.context());
      }
    }
    if (r == null) return new @Tainted MetricsRegistry(cls.getSimpleName());
    return r;
  }

  /**
   * Change the declared field {@code field} in {@code source} Object to
   * {@link MutableMetric}
   */
  private void add(@Tainted MetricsSourceBuilder this, @Tainted Object source, @Tainted Field field) {
    for (@Tainted Annotation annotation : field.getAnnotations()) {
      if (!(annotation instanceof @Tainted Metric)) {
        continue;
      }
      try {
        // skip fields already set
        field.setAccessible(true);
        if (field.get(source) != null) continue;
      } catch (@Tainted Exception e) {
        LOG.warn("Error accessing field "+ field +" annotated with"+
                 annotation, e);
        continue;
      }
      @Tainted
      MutableMetric mutable = factory.newForField(field, (@Tainted Metric) annotation,
                                                  registry);
      if (mutable != null) {
        try {
          field.set(source, mutable); // Set the source field to MutableMetric
          hasAtMetric = true;
        } catch (@Tainted Exception e) {
          throw new @Tainted MetricsException("Error setting field "+ field +
                                     " annotated with "+ annotation, e);
        }
      }
    }
  }

  /** Add {@link MutableMetric} for a method annotated with {@link Metric} */
  private void add(@Tainted MetricsSourceBuilder this, @Tainted Object source, @Tainted Method method) {
    for (@Tainted Annotation annotation : method.getAnnotations()) {
      if (!(annotation instanceof @Tainted Metric)) {
        continue;
      }
      factory.newForMethod(source, method, (@Tainted Metric) annotation, registry);
      hasAtMetric = true;
    }
  }
}
