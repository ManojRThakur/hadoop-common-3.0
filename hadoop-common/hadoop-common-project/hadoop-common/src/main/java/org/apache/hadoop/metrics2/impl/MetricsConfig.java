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

package org.apache.hadoop.metrics2.impl;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import static java.security.AccessController.*;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsPlugin;
import org.apache.hadoop.metrics2.filter.GlobFilter;

/**
 * Metrics configuration for MetricsSystemImpl
 */
class MetricsConfig extends @Tainted SubsetConfiguration {
  static final @Tainted Log LOG = LogFactory.getLog(MetricsConfig.class);

  static final @Tainted String DEFAULT_FILE_NAME = "hadoop-metrics2.properties";
  static final @Tainted String PREFIX_DEFAULT = "*.";

  static final @Tainted String PERIOD_KEY = "period";
  static final @Tainted int PERIOD_DEFAULT = 10; // seconds

  static final @Tainted String QUEUE_CAPACITY_KEY = "queue.capacity";
  static final @Tainted int QUEUE_CAPACITY_DEFAULT = 1;

  static final @Tainted String RETRY_DELAY_KEY = "retry.delay";
  static final @Tainted int RETRY_DELAY_DEFAULT = 10;  // seconds
  static final @Tainted String RETRY_BACKOFF_KEY = "retry.backoff";
  static final @Tainted int RETRY_BACKOFF_DEFAULT = 2; // back off factor
  static final @Tainted String RETRY_COUNT_KEY = "retry.count";
  static final @Tainted int RETRY_COUNT_DEFAULT = 1;

  static final @Tainted String JMX_CACHE_TTL_KEY = "jmx.cache.ttl";
  static final @Tainted String START_MBEANS_KEY = "source.start_mbeans";
  static final @Tainted String PLUGIN_URLS_KEY = "plugin.urls";

  static final @Tainted String CONTEXT_KEY = "context";
  static final @Tainted String NAME_KEY = "name";
  static final @Tainted String DESC_KEY = "description";
  static final @Tainted String SOURCE_KEY = "source";
  static final @Tainted String SINK_KEY = "sink";
  static final @Tainted String METRIC_FILTER_KEY = "metric.filter";
  static final @Tainted String RECORD_FILTER_KEY = "record.filter";
  static final @Tainted String SOURCE_FILTER_KEY = "source.filter";

  static final @Tainted Pattern INSTANCE_REGEX = Pattern.compile("([^.*]+)\\..+");
  static final @Tainted Splitter SPLITTER = Splitter.on(',').trimResults();
  private @Tainted ClassLoader pluginLoader;

  @Tainted
  MetricsConfig(@Tainted Configuration c, @Tainted String prefix) {
    super(c, prefix.toLowerCase(Locale.US), ".");
  }

  static @Tainted MetricsConfig create(@Tainted String prefix) {
    return loadFirst(prefix, "hadoop-metrics2-"+ prefix.toLowerCase(Locale.US)
                     +".properties", DEFAULT_FILE_NAME);
  }

  static @Tainted MetricsConfig create(@Tainted String prefix, @Tainted String @Tainted ... fileNames) {
    return loadFirst(prefix, fileNames);
  }

  /**
   * Load configuration from a list of files until the first successful load
   * @param conf  the configuration object
   * @param files the list of filenames to try
   * @return  the configuration object
   */
  static @Tainted MetricsConfig loadFirst(@Tainted String prefix, @Tainted String @Tainted ... fileNames) {
    for (@Tainted String fname : fileNames) {
      try {
        @Tainted
        Configuration cf = new @Tainted PropertiesConfiguration(fname)
            .interpolatedConfiguration();
        LOG.info("loaded properties from "+ fname);
        LOG.debug(toString(cf));
        @Tainted
        MetricsConfig mc = new @Tainted MetricsConfig(cf, prefix);
        LOG.debug(mc);
        return mc;
      } catch (@Tainted ConfigurationException e) {
        if (e.getMessage().startsWith("Cannot locate configuration")) {
          continue;
        }
        throw new @Tainted MetricsConfigException(e);
      }
    }
    LOG.warn("Cannot locate configuration: tried "+
             Joiner.on(",").join(fileNames));
    // default to an empty configuration
    return new @Tainted MetricsConfig(new @Tainted PropertiesConfiguration(), prefix);
  }

  @Override
  public @Tainted MetricsConfig subset(@Tainted MetricsConfig this, @Tainted String prefix) {
    return new @Tainted MetricsConfig(this, prefix);
  }

  /**
   * Return sub configs for instance specified in the config.
   * Assuming format specified as follows:<pre>
   * [type].[instance].[option] = [value]</pre>
   * Note, '*' is a special default instance, which is excluded in the result.
   * @param type  of the instance
   * @return  a map with [instance] as key and config object as value
   */
  @Tainted
  Map<@Tainted String, @Tainted MetricsConfig> getInstanceConfigs(@Tainted MetricsConfig this, @Tainted String type) {
    @Tainted
    Map<@Tainted String, @Tainted MetricsConfig> map = Maps.newHashMap();
    @Tainted
    MetricsConfig sub = subset(type);

    for (@Tainted String key : sub.keys()) {
      @Tainted
      Matcher matcher = INSTANCE_REGEX.matcher(key);
      if (matcher.matches()) {
        @Tainted
        String instance = matcher.group(1);
        if (!map.containsKey(instance)) {
          map.put(instance, sub.subset(instance));
        }
      }
    }
    return map;
  }

  @Tainted
  Iterable<@Tainted String> keys(@Tainted MetricsConfig this) {
    return new @Tainted Iterable<@Tainted String>() {
      @SuppressWarnings("unchecked")
      @Override
      public @Tainted Iterator<@Tainted String> iterator() {
        return (@Tainted Iterator<@Tainted String>) getKeys();
      }
    };
  }

  /**
   * Will poke parents for defaults
   * @param key to lookup
   * @return  the value or null
   */
  @Override
  public @Tainted Object getProperty(@Tainted MetricsConfig this, @Tainted String key) {
    @Tainted
    Object value = super.getProperty(key);
    if (value == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("poking parent '"+ getParent().getClass().getSimpleName() +
                  "' for key: "+ key);
      }
      return getParent().getProperty(key.startsWith(PREFIX_DEFAULT) ? key
                                     : PREFIX_DEFAULT + key);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("returning '"+ value +"' for key: "+ key);
    }
    return value;
  }

  <@Tainted T extends @Tainted MetricsPlugin> @Tainted T getPlugin(@Tainted MetricsConfig this, @Tainted String name) {
    @Tainted
    String clsName = getClassName(name);
    if (clsName == null) return null;
    try {
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> cls = Class.forName(clsName, true, getPluginLoader());
      @SuppressWarnings("unchecked")
      @Tainted
      T plugin = (@Tainted T) cls.newInstance();
      plugin.init(name.isEmpty() ? this : subset(name));
      return plugin;
    } catch (@Tainted Exception e) {
      throw new @Tainted MetricsConfigException("Error creating plugin: "+ clsName, e);
    }
  }

  @Tainted
  String getClassName(@Tainted MetricsConfig this, @Tainted String prefix) {
    @Tainted
    String classKey = prefix.isEmpty() ? "class" : prefix +".class";
    @Tainted
    String clsName = getString(classKey);
    LOG.debug(clsName);
    if (clsName == null || clsName.isEmpty()) {
      return null;
    }
    return clsName;
  }

  @Tainted
  ClassLoader getPluginLoader(@Tainted MetricsConfig this) {
    if (pluginLoader != null) return pluginLoader;
    final @Tainted ClassLoader defaultLoader = getClass().getClassLoader();
    @Tainted
    Object purls = super.getProperty(PLUGIN_URLS_KEY);
    if (purls == null) return defaultLoader;
    @Tainted
    Iterable<@Tainted String> jars = SPLITTER.split((@Tainted String) purls);
    @Tainted
    int len = Iterables.size(jars);
    if ( len > 0) {
      final @Tainted URL @Tainted [] urls = new @Tainted URL @Tainted [len];
      try {
        @Tainted
        int i = 0;
        for (@Tainted String jar : jars) {
          LOG.debug(jar);
          urls[i++] = new @Tainted URL(jar);
        }
      } catch (@Tainted Exception e) {
        throw new @Tainted MetricsConfigException(e);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("using plugin jars: "+ Iterables.toString(jars));
      }
      pluginLoader = doPrivileged(new @Tainted PrivilegedAction<@Tainted ClassLoader>() {
        @Override public @Tainted ClassLoader run() {
          return new @Tainted URLClassLoader(urls, defaultLoader);
        }
      });
      return pluginLoader;
    }
    if (parent instanceof @Tainted MetricsConfig) {
      return ((@Tainted MetricsConfig) parent).getPluginLoader();
    }
    return defaultLoader;
  }

  @Override public void clear(@Tainted MetricsConfig this) {
    super.clear();
    // pluginLoader.close(); // jdk7 is saner
  }

  @Tainted
  MetricsFilter getFilter(@Tainted MetricsConfig this, @Tainted String prefix) {
    // don't create filter instances without out options
    @Tainted
    MetricsConfig conf = subset(prefix);
    if (conf.isEmpty()) return null;
    @Tainted
    MetricsFilter filter = getPlugin(prefix);
    if (filter != null) return filter;
    // glob filter is assumed if pattern is specified but class is not.
    filter = new @Tainted GlobFilter();
    filter.init(conf);
    return filter;
  }

  @Override
  public @Tainted String toString(@Tainted MetricsConfig this) {
    return toString(this);
  }

  static @Tainted String toString(@Tainted Configuration c) {
    @Tainted
    ByteArrayOutputStream buffer = new @Tainted ByteArrayOutputStream();
    @Tainted
    PrintStream ps = new @Tainted PrintStream(buffer);
    @Tainted
    PropertiesConfiguration tmp = new @Tainted PropertiesConfiguration();
    tmp.copy(c);
    try {
      tmp.save(ps);
    } catch (@Tainted Exception e) {
      throw new @Tainted MetricsConfigException(e);
    }
    return buffer.toString();
  }
}
