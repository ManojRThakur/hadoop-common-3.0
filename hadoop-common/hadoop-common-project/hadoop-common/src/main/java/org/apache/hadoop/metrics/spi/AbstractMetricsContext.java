/*
 * AbstractMetricsContext.java
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

package org.apache.hadoop.metrics.spi;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Updater;

/**
 * The main class of the Service Provider Interface.  This class should be
 * extended in order to integrate the Metrics API with a specific metrics
 * client library. <p/>
 *
 * This class implements the internal table of metric data, and the timer
 * on which data is to be sent to the metrics system.  Subclasses must
 * override the abstract <code>emitRecord</code> method in order to transmit
 * the data. <p/>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractMetricsContext implements @Tainted MetricsContext {
    
  private @Tainted int period = MetricsContext.DEFAULT_PERIOD;
  private @Tainted Timer timer = null;
    
  private @Tainted Set<@Tainted Updater> updaters = new @Tainted HashSet<@Tainted Updater>(1);
  private volatile @Tainted boolean isMonitoring = false;
    
  private @Tainted ContextFactory factory = null;
  private @Tainted String contextName = null;
    
  @InterfaceAudience.Private
  public static class TagMap extends @Tainted TreeMap<@Tainted String, @Tainted Object> {
    private static final @Tainted long serialVersionUID = 3546309335061952993L;
    @Tainted
    TagMap() {
      super();
    }
    @Tainted
    TagMap(@Tainted TagMap orig) {
      super(orig);
    }
    /**
     * Returns true if this tagmap contains every tag in other.
     */
    public @Tainted boolean containsAll(AbstractMetricsContext.@Tainted TagMap this, @Tainted TagMap other) {
      for (Map.@Tainted Entry<@Tainted String, @Tainted Object> entry : other.entrySet()) {
        @Tainted
        Object value = get(entry.getKey());
        if (value == null || !value.equals(entry.getValue())) {
          // either key does not exist here, or the value is different
          return false;
        }
      }
      return true;
    }
  }
  
  @InterfaceAudience.Private
  public static class MetricMap extends @Tainted TreeMap<@Tainted String, @Tainted Number> {
    private static final @Tainted long serialVersionUID = -7495051861141631609L;
    @Tainted
    MetricMap() {
      super();
    }
    @Tainted
    MetricMap(@Tainted MetricMap orig) {
      super(orig);
    }
  }
            
  static class RecordMap extends @Tainted HashMap<@Tainted TagMap, @Tainted MetricMap> {
    private static final @Tainted long serialVersionUID = 259835619700264611L;
  }
    
  private @Tainted Map<@Tainted String, @Tainted RecordMap> bufferedData = new @Tainted HashMap<@Tainted String, @Tainted RecordMap>();
    

  /**
   * Creates a new instance of AbstractMetricsContext
   */
  protected @Tainted AbstractMetricsContext() {
  }
    
  /**
   * Initializes the context.
   */
  @Override
  public void init(@Tainted AbstractMetricsContext this, @Tainted String contextName, @Tainted ContextFactory factory) 
  {
    this.contextName = contextName;
    this.factory = factory;
  }
    
  /**
   * Convenience method for subclasses to access factory attributes.
   */
  protected @Tainted String getAttribute(@Tainted AbstractMetricsContext this, @Tainted String attributeName) {
    @Tainted
    String factoryAttribute = contextName + "." + attributeName;
    return (@Tainted String) factory.getAttribute(factoryAttribute);  
  }
    
  /**
   * Returns an attribute-value map derived from the factory attributes
   * by finding all factory attributes that begin with 
   * <i>contextName</i>.<i>tableName</i>.  The returned map consists of
   * those attributes with the contextName and tableName stripped off.
   */
  protected @Tainted Map<@Tainted String, @Tainted String> getAttributeTable(@Tainted AbstractMetricsContext this, @Tainted String tableName) {
    @Tainted
    String prefix = contextName + "." + tableName + ".";
    @Tainted
    Map<@Tainted String, @Tainted String> result = new @Tainted HashMap<@Tainted String, @Tainted String>();
    for (@Tainted String attributeName : factory.getAttributeNames()) {
      if (attributeName.startsWith(prefix)) {
        @Tainted
        String name = attributeName.substring(prefix.length());
        @Tainted
        String value = (@Tainted String) factory.getAttribute(attributeName);
        result.put(name, value);
      }
    }
    return result;
  }
    
  /**
   * Returns the context name.
   */
  @Override
  public @Tainted String getContextName(@Tainted AbstractMetricsContext this) {
    return contextName;
  }
    
  /**
   * Returns the factory by which this context was created.
   */
  public @Tainted ContextFactory getContextFactory(@Tainted AbstractMetricsContext this) {
    return factory;
  }
    
  /**
   * Starts or restarts monitoring, the emitting of metrics records.
   */
  @Override
  public synchronized void startMonitoring(@Tainted AbstractMetricsContext this)
    throws IOException {
    if (!isMonitoring) {
      startTimer();
      isMonitoring = true;
    }
  }
    
  /**
   * Stops monitoring.  This does not free buffered data. 
   * @see #close()
   */
  @Override
  public synchronized void stopMonitoring(@Tainted AbstractMetricsContext this) {
    if (isMonitoring) {
      stopTimer();
      isMonitoring = false;
    }
  }
    
  /**
   * Returns true if monitoring is currently in progress.
   */
  @Override
  public @Tainted boolean isMonitoring(@Tainted AbstractMetricsContext this) {
    return isMonitoring;
  }
    
  /**
   * Stops monitoring and frees buffered data, returning this
   * object to its initial state.  
   */
  @Override
  public synchronized void close(@Tainted AbstractMetricsContext this) {
    stopMonitoring();
    clearUpdaters();
  } 
    
  /**
   * Creates a new AbstractMetricsRecord instance with the given <code>recordName</code>.
   * Throws an exception if the metrics implementation is configured with a fixed
   * set of record names and <code>recordName</code> is not in that set.
   * 
   * @param recordName the name of the record
   * @throws MetricsException if recordName conflicts with configuration data
   */
  @Override
  public final synchronized @Tainted MetricsRecord createRecord(@Tainted AbstractMetricsContext this, @Tainted String recordName) {
    if (bufferedData.get(recordName) == null) {
      bufferedData.put(recordName, new @Tainted RecordMap());
    }
    return newRecord(recordName);
  }
    
  /**
   * Subclasses should override this if they subclass MetricsRecordImpl.
   * @param recordName the name of the record
   * @return newly created instance of MetricsRecordImpl or subclass
   */
  protected @Tainted MetricsRecord newRecord(@Tainted AbstractMetricsContext this, @Tainted String recordName) {
    return new @Tainted MetricsRecordImpl(recordName, this);
  }
    
  /**
   * Registers a callback to be called at time intervals determined by
   * the configuration.
   *
   * @param updater object to be run periodically; it should update
   * some metrics records 
   */
  @Override
  public synchronized void registerUpdater(@Tainted AbstractMetricsContext this, final @Tainted Updater updater) {
    if (!updaters.contains(updater)) {
      updaters.add(updater);
    }
  }
    
  /**
   * Removes a callback, if it exists.
   *
   * @param updater object to be removed from the callback list
   */
  @Override
  public synchronized void unregisterUpdater(@Tainted AbstractMetricsContext this, @Tainted Updater updater) {
    updaters.remove(updater);
  }
    
  private synchronized void clearUpdaters(@Tainted AbstractMetricsContext this) {
    updaters.clear();
  }
    
  /**
   * Starts timer if it is not already started
   */
  private synchronized void startTimer(@Tainted AbstractMetricsContext this) {
    if (timer == null) {
      timer = new @Tainted Timer("Timer thread for monitoring " + getContextName(), 
                        true);
      @Tainted
      TimerTask task = new @Tainted TimerTask() {
          @Override
          public void run() {
            try {
              timerEvent();
            } catch (@Tainted IOException ioe) {
              ioe.printStackTrace();
            }
          }
        };
      @Tainted
      long millis = period * 1000;
      timer.scheduleAtFixedRate(task, millis, millis);
    }
  }
    
  /**
   * Stops timer if it is running
   */
  private synchronized void stopTimer(@Tainted AbstractMetricsContext this) {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }
    
  /**
   * Timer callback.
   */
  private void timerEvent(@Tainted AbstractMetricsContext this) throws IOException {
    if (isMonitoring) {
      @Tainted
      Collection<@Tainted Updater> myUpdaters;
      synchronized (this) {
        myUpdaters = new @Tainted ArrayList<@Tainted Updater>(updaters);
      }     
      // Run all the registered updates without holding a lock
      // on this context
      for (@Tainted Updater updater : myUpdaters) {
        try {
          updater.doUpdates(this);
        } catch (@Tainted Throwable throwable) {
          throwable.printStackTrace();
        }
      }
      emitRecords();
    }
  }
    
  /**
   *  Emits the records.
   */
  private synchronized void emitRecords(@Tainted AbstractMetricsContext this) throws IOException {
    for (@Tainted String recordName : bufferedData.keySet()) {
      @Tainted
      RecordMap recordMap = bufferedData.get(recordName);
      synchronized (recordMap) {
        @Tainted
        Set<@Tainted Entry<@Tainted TagMap, @Tainted MetricMap>> entrySet = recordMap.entrySet ();
        for (@Tainted Entry<@Tainted TagMap, @Tainted MetricMap> entry : entrySet) {
          @Tainted
          OutputRecord outRec = new @Tainted OutputRecord(entry.getKey(), entry.getValue());
          emitRecord(contextName, recordName, outRec);
        }
      }
    }
    flush();
  }
  
  /**
   * Retrieves all the records managed by this MetricsContext.
   * Useful for monitoring systems that are polling-based.
   * @return A non-null collection of all monitoring records.
   */
  @Override
  public synchronized @Tainted Map<@Tainted String, @Tainted Collection<@Tainted OutputRecord>> getAllRecords(@Tainted AbstractMetricsContext this) {
    @Tainted
    Map<@Tainted String, @Tainted Collection<@Tainted OutputRecord>> out = new @Tainted TreeMap<@Tainted String, @Tainted Collection<@Tainted OutputRecord>>();
    for (@Tainted String recordName : bufferedData.keySet()) {
      @Tainted
      RecordMap recordMap = bufferedData.get(recordName);
      synchronized (recordMap) {
        @Tainted
        List<@Tainted OutputRecord> records = new @Tainted ArrayList<@Tainted OutputRecord>();
        @Tainted
        Set<@Tainted Entry<@Tainted TagMap, @Tainted MetricMap>> entrySet = recordMap.entrySet();
        for (@Tainted Entry<@Tainted TagMap, @Tainted MetricMap> entry : entrySet) {
          @Tainted
          OutputRecord outRec = new @Tainted OutputRecord(entry.getKey(), entry.getValue());
          records.add(outRec);
        }
        out.put(recordName, records);
      }
    }
    return out;
  }

  /**
   * Sends a record to the metrics system.
   */
  protected abstract void emitRecord(@Tainted AbstractMetricsContext this, @Tainted String contextName, @Tainted String recordName, 
                                     @Tainted
                                     OutputRecord outRec) throws IOException;
    
  /**
   * Called each period after all records have been emitted, this method does nothing.
   * Subclasses may override it in order to perform some kind of flush.
   */
  protected void flush(@Tainted AbstractMetricsContext this) throws IOException {
  }
    
  /**
   * Called by MetricsRecordImpl.update().  Creates or updates a row in
   * the internal table of metric data.
   */
  protected void update(@Tainted AbstractMetricsContext this, @Tainted MetricsRecordImpl record) {
    @Tainted
    String recordName = record.getRecordName();
    @Tainted
    TagMap tagTable = record.getTagTable();
    @Tainted
    Map<@Tainted String, @Tainted MetricValue> metricUpdates = record.getMetricTable();
        
    @Tainted
    RecordMap recordMap = getRecordMap(recordName);
    synchronized (recordMap) {
      @Tainted
      MetricMap metricMap = recordMap.get(tagTable);
      if (metricMap == null) {
        metricMap = new @Tainted MetricMap();
        @Tainted
        TagMap tagMap = new @Tainted TagMap(tagTable); // clone tags
        recordMap.put(tagMap, metricMap);
      }

      @Tainted
      Set<@Tainted Entry<@Tainted String, @Tainted MetricValue>> entrySet = metricUpdates.entrySet();
      for (@Tainted Entry<@Tainted String, @Tainted MetricValue> entry : entrySet) {
        @Tainted
        String metricName = entry.getKey ();
        @Tainted
        MetricValue updateValue = entry.getValue ();
        @Tainted
        Number updateNumber = updateValue.getNumber();
        @Tainted
        Number currentNumber = metricMap.get(metricName);
        if (currentNumber == null || updateValue.isAbsolute()) {
          metricMap.put(metricName, updateNumber);
        }
        else {
          @Tainted
          Number newNumber = sum(updateNumber, currentNumber);
          metricMap.put(metricName, newNumber);
        }
      }
    }
  }
    
  private synchronized @Tainted RecordMap getRecordMap(@Tainted AbstractMetricsContext this, @Tainted String recordName) {
    return bufferedData.get(recordName);
  }
    
  /**
   * Adds two numbers, coercing the second to the type of the first.
   *
   */
  private @Tainted Number sum(@Tainted AbstractMetricsContext this, @Tainted Number a, @Tainted Number b) {
    if (a instanceof @Tainted Integer) {
      return Integer.valueOf(a.intValue() + b.intValue());
    }
    else if (a instanceof @Tainted Float) {
      return new @Tainted Float(a.floatValue() + b.floatValue());
    }
    else if (a instanceof @Tainted Short) {
      return Short.valueOf((@Tainted short)(a.shortValue() + b.shortValue()));
    }
    else if (a instanceof @Tainted Byte) {
      return Byte.valueOf((@Tainted byte)(a.byteValue() + b.byteValue()));
    }
    else if (a instanceof @Tainted Long) {
      return Long.valueOf((a.longValue() + b.longValue()));
    }
    else {
      // should never happen
      throw new @Tainted MetricsException("Invalid number type");
    }
            
  }
    
  /**
   * Called by MetricsRecordImpl.remove().  Removes all matching rows in
   * the internal table of metric data.  A row matches if it has the same
   * tag names and values as record, but it may also have additional
   * tags.
   */    
  protected void remove(@Tainted AbstractMetricsContext this, @Tainted MetricsRecordImpl record) {
    @Tainted
    String recordName = record.getRecordName();
    @Tainted
    TagMap tagTable = record.getTagTable();
        
    @Tainted
    RecordMap recordMap = getRecordMap(recordName);
    synchronized (recordMap) {
      @Tainted
      Iterator<@Tainted TagMap> it = recordMap.keySet().iterator();
      while (it.hasNext()) {
        @Tainted
        TagMap rowTags = it.next();
        if (rowTags.containsAll(tagTable)) {
          it.remove();
        }
      }
    }
  }
    
  /**
   * Returns the timer period.
   */
  @Override
  public @Tainted int getPeriod(@Tainted AbstractMetricsContext this) {
    return period;
  }
    
  /**
   * Sets the timer period
   */
  protected void setPeriod(@Tainted AbstractMetricsContext this, @Tainted int period) {
    this.period = period;
  }
  
  /**
   * If a period is set in the attribute passed in, override
   * the default with it.
   */
  protected void parseAndSetPeriod(@Tainted AbstractMetricsContext this, @Tainted String attributeName) {
    @Tainted
    String periodStr = getAttribute(attributeName);
    if (periodStr != null) {
      @Tainted
      int period = 0;
      try {
        period = Integer.parseInt(periodStr);
      } catch (@Tainted NumberFormatException nfe) {
      }
      if (period <= 0) {
        throw new @Tainted MetricsException("Invalid period: " + periodStr);
      }
      setPeriod(period);
    }
  }
}
