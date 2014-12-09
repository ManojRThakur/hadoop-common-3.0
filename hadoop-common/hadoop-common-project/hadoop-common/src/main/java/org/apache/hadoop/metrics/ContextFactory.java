/*
 * ContextFactory.java
 *
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

package org.apache.hadoop.metrics;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.spi.NullContext;

/**
 * Factory class for creating MetricsContext objects.  To obtain an instance
 * of this class, use the static <code>getFactory()</code> method.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ContextFactory {
    
  private static final @Tainted String PROPERTIES_FILE = 
    "/hadoop-metrics.properties";
  private static final @Tainted String CONTEXT_CLASS_SUFFIX =
    ".class";
  private static final @Tainted String DEFAULT_CONTEXT_CLASSNAME =
    "org.apache.hadoop.metrics.spi.NullContext";
    
  private static @Tainted ContextFactory theFactory = null;
    
  private @Tainted Map<@Tainted String, @Tainted Object> attributeMap = new @Tainted HashMap<@Tainted String, @Tainted Object>();
  private @Tainted Map<@Tainted String, @Tainted MetricsContext> contextMap = 
    new @Tainted HashMap<@Tainted String, @Tainted MetricsContext>();
    
  // Used only when contexts, or the ContextFactory itself, cannot be
  // created.
  private static @Tainted Map<@Tainted String, @Tainted MetricsContext> nullContextMap = 
    new @Tainted HashMap<@Tainted String, @Tainted MetricsContext>();
    
  /** Creates a new instance of ContextFactory */
  protected @Tainted ContextFactory() {
  }
    
  /**
   * Returns the value of the named attribute, or null if there is no 
   * attribute of that name.
   *
   * @param attributeName the attribute name
   * @return the attribute value
   */
  public @Tainted Object getAttribute(@Tainted ContextFactory this, @Tainted String attributeName) {
    return attributeMap.get(attributeName);
  }
    
  /**
   * Returns the names of all the factory's attributes.
   * 
   * @return the attribute names
   */
  public @Tainted String @Tainted [] getAttributeNames(@Tainted ContextFactory this) {
    @Tainted
    String @Tainted [] result = new @Tainted String @Tainted [attributeMap.size()];
    @Tainted
    int i = 0;
    // for (String attributeName : attributeMap.keySet()) {
    @Tainted
    Iterator it = attributeMap.keySet().iterator();
    while (it.hasNext()) {
      result[i++] = (@Tainted String) it.next();
    }
    return result;
  }
    
  /**
   * Sets the named factory attribute to the specified value, creating it
   * if it did not already exist.  If the value is null, this is the same as
   * calling removeAttribute.
   *
   * @param attributeName the attribute name
   * @param value the new attribute value
   */
  public void setAttribute(@Tainted ContextFactory this, @Tainted String attributeName, @Tainted Object value) {
    attributeMap.put(attributeName, value);
  }

  /**
   * Removes the named attribute if it exists.
   *
   * @param attributeName the attribute name
   */
  public void removeAttribute(@Tainted ContextFactory this, @Tainted String attributeName) {
    attributeMap.remove(attributeName);
  }
    
  /**
   * Returns the named MetricsContext instance, constructing it if necessary 
   * using the factory's current configuration attributes. <p/>
   * 
   * When constructing the instance, if the factory property 
   * <i>contextName</i>.class</code> exists, 
   * its value is taken to be the name of the class to instantiate.  Otherwise,
   * the default is to create an instance of 
   * <code>org.apache.hadoop.metrics.spi.NullContext</code>, which is a 
   * dummy "no-op" context which will cause all metric data to be discarded.
   * 
   * @param contextName the name of the context
   * @return the named MetricsContext
   */
  public synchronized @Tainted MetricsContext getContext(@Tainted ContextFactory this, @Tainted String refName, @Tainted String contextName)
      throws IOException, ClassNotFoundException,
             InstantiationException, IllegalAccessException {
    @Tainted
    MetricsContext metricsContext = contextMap.get(refName);
    if (metricsContext == null) {
      @Tainted
      String classNameAttribute = refName + CONTEXT_CLASS_SUFFIX;
      @Tainted
      String className = (@Tainted String) getAttribute(classNameAttribute);
      if (className == null) {
        className = DEFAULT_CONTEXT_CLASSNAME;
      }
      @Tainted
      Class contextClass = Class.forName(className);
      metricsContext = (@Tainted MetricsContext) contextClass.newInstance();
      metricsContext.init(contextName, this);
      contextMap.put(contextName, metricsContext);
    }
    return metricsContext;
  }

  public synchronized @Tainted MetricsContext getContext(@Tainted ContextFactory this, @Tainted String contextName)
    throws IOException, ClassNotFoundException, InstantiationException,
           IllegalAccessException {
    return getContext(contextName, contextName);
  }
  
  /** 
   * Returns all MetricsContexts built by this factory.
   */
  public synchronized @Tainted Collection<@Tainted MetricsContext> getAllContexts(@Tainted ContextFactory this) {
    // Make a copy to avoid race conditions with creating new contexts.
    return new @Tainted ArrayList<@Tainted MetricsContext>(contextMap.values());
  }
    
  /**
   * Returns a "null" context - one which does nothing.
   */
  public static synchronized @Tainted MetricsContext getNullContext(@Tainted String contextName) {
    @Tainted
    MetricsContext nullContext = nullContextMap.get(contextName);
    if (nullContext == null) {
      nullContext = new @Tainted NullContext();
      nullContextMap.put(contextName, nullContext);
    }
    return nullContext;
  }
    
  /**
   * Returns the singleton ContextFactory instance, constructing it if 
   * necessary. <p/>
   * 
   * When the instance is constructed, this method checks if the file 
   * <code>hadoop-metrics.properties</code> exists on the class path.  If it 
   * exists, it must be in the format defined by java.util.Properties, and all 
   * the properties in the file are set as attributes on the newly created
   * ContextFactory instance.
   *
   * @return the singleton ContextFactory instance
   */
  public static synchronized @Tainted ContextFactory getFactory() throws IOException {
    if (theFactory == null) {
      theFactory = new @Tainted ContextFactory();
      theFactory.setAttributes();
    }
    return theFactory;
  }
    
  private void setAttributes(@Tainted ContextFactory this) throws IOException {
    @Tainted
    InputStream is = getClass().getResourceAsStream(PROPERTIES_FILE);
    if (is != null) {
      try {
        @Tainted
        Properties properties = new @Tainted Properties();
        properties.load(is);
        //for (Object propertyNameObj : properties.keySet()) {
        @Tainted
        Iterator it = properties.keySet().iterator();
        while (it.hasNext()) {
          @Tainted
          String propertyName = (@Tainted String) it.next();
          @Tainted
          String propertyValue = properties.getProperty(propertyName);
          setAttribute(propertyName, propertyValue);
        }
      } finally {
        is.close();
      }
    }
  }
    
}
