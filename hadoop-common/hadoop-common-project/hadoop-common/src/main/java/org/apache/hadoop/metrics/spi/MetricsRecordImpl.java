/*
 * MetricsRecordImpl.java
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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap;

/**
 * An implementation of MetricsRecord.  Keeps a back-pointer to the context
 * from which it was created, and delegates back to it on <code>update</code>
 * and <code>remove()</code>.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsRecordImpl implements @Tainted MetricsRecord {
    
  private @Tainted TagMap tagTable = new @Tainted TagMap();
  private @Tainted Map<@Tainted String, @Tainted MetricValue> metricTable = new @Tainted LinkedHashMap<@Tainted String, @Tainted MetricValue>();
    
  private @Tainted String recordName;
  private @Tainted AbstractMetricsContext context;
    
    
  /** Creates a new instance of FileRecord */
  protected @Tainted MetricsRecordImpl(@Tainted String recordName, @Tainted AbstractMetricsContext context)
  {
    this.recordName = recordName;
    this.context = context;
  }
    
  /**
   * Returns the record name. 
   *
   * @return the record name
   */
  @Override
  public @Tainted String getRecordName(@Tainted MetricsRecordImpl this) {
    return recordName;
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  @Override
  public void setTag(@Tainted MetricsRecordImpl this, @Tainted String tagName, @Tainted String tagValue) {
    if (tagValue == null) {
      tagValue = "";
    }
    tagTable.put(tagName, tagValue);
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  @Override
  public void setTag(@Tainted MetricsRecordImpl this, @Tainted String tagName, @Tainted int tagValue) {
    tagTable.put(tagName, Integer.valueOf(tagValue));
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  @Override
  public void setTag(@Tainted MetricsRecordImpl this, @Tainted String tagName, @Tainted long tagValue) {
    tagTable.put(tagName, Long.valueOf(tagValue));
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  @Override
  public void setTag(@Tainted MetricsRecordImpl this, @Tainted String tagName, @Tainted short tagValue) {
    tagTable.put(tagName, Short.valueOf(tagValue));
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  @Override
  public void setTag(@Tainted MetricsRecordImpl this, @Tainted String tagName, @Tainted byte tagValue) {
    tagTable.put(tagName, Byte.valueOf(tagValue));
  }
    
  /**
   * Removes any tag of the specified name.
   */
  @Override
  public void removeTag(@Tainted MetricsRecordImpl this, @Tainted String tagName) {
    tagTable.remove(tagName);
  }
  
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void setMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted int metricValue) {
    setAbsolute(metricName, Integer.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void setMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted long metricValue) {
    setAbsolute(metricName, Long.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void setMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted short metricValue) {
    setAbsolute(metricName, Short.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void setMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted byte metricValue) {
    setAbsolute(metricName, Byte.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void setMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted float metricValue) {
    setAbsolute(metricName, new @Tainted Float(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void incrMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted int metricValue) {
    setIncrement(metricName, Integer.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void incrMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted long metricValue) {
    setIncrement(metricName, Long.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void incrMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted short metricValue) {
    setIncrement(metricName, Short.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void incrMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted byte metricValue) {
    setIncrement(metricName, Byte.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  @Override
  public void incrMetric(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted float metricValue) {
    setIncrement(metricName, new @Tainted Float(metricValue));
  }
    
  private void setAbsolute(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted Number metricValue) {
    metricTable.put(metricName, new @Tainted MetricValue(metricValue, MetricValue.ABSOLUTE));
  }
    
  private void setIncrement(@Tainted MetricsRecordImpl this, @Tainted String metricName, @Tainted Number metricValue) {
    metricTable.put(metricName, new @Tainted MetricValue(metricValue, MetricValue.INCREMENT));
  }
    
  /**
   * Updates the table of buffered data which is to be sent periodically.
   * If the tag values match an existing row, that row is updated; 
   * otherwise, a new row is added.
   */
  @Override
  public void update(@Tainted MetricsRecordImpl this) {
    context.update(this);
  }
    
  /**
   * Removes the row, if it exists, in the buffered data table having tags 
   * that equal the tags that have been set on this record. 
   */
  @Override
  public void remove(@Tainted MetricsRecordImpl this) {
    context.remove(this);
  }

  @Tainted
  TagMap getTagTable(@Tainted MetricsRecordImpl this) {
    return tagTable;
  }

  @Tainted
  Map<@Tainted String, @Tainted MetricValue> getMetricTable(@Tainted MetricsRecordImpl this) {
    return metricTable;
  }
}
