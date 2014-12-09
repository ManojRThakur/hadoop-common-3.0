/*
 * FileContext.java
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

package org.apache.hadoop.metrics.file;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;

/**
 * Metrics context for writing metrics to a file.<p/>
 *
 * This class is configured by setting ContextFactory attributes which in turn
 * are usually configured through a properties file.  All the attributes are
 * prefixed by the contextName. For example, the properties file might contain:
 * <pre>
 * myContextName.fileName=/tmp/metrics.log
 * myContextName.period=5
 * </pre>
 * @see org.apache.hadoop.metrics2.sink.FileSink for metrics 2.0.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@Deprecated
public class FileContext extends @Tainted AbstractMetricsContext {
    
  /* Configuration attribute names */
  @InterfaceAudience.Private
  protected static final @Tainted String FILE_NAME_PROPERTY = "fileName";
  @InterfaceAudience.Private
  protected static final @Tainted String PERIOD_PROPERTY = "period";
    
  private @Tainted File file = null;              // file for metrics to be written to
  private @Tainted PrintWriter writer = null;
    
  /** Creates a new instance of FileContext */
  @InterfaceAudience.Private
  public @Tainted FileContext() {}
    
  @Override
  @InterfaceAudience.Private
  public void init(@Tainted FileContext this, @Tainted String contextName, @Tainted ContextFactory factory) {
    super.init(contextName, factory);
        
    @Tainted
    String fileName = getAttribute(FILE_NAME_PROPERTY);
    if (fileName != null) {
      file = new @Tainted File(fileName);
    }
        
    parseAndSetPeriod(PERIOD_PROPERTY);
  }

  /**
   * Returns the configured file name, or null.
   */
  @InterfaceAudience.Private
  public @Tainted String getFileName(@Tainted FileContext this) {
    if (file == null) {
      return null;
    } else {
      return file.getName();
    }
  }
    
  /**
   * Starts or restarts monitoring, by opening in append-mode, the
   * file specified by the <code>fileName</code> attribute,
   * if specified. Otherwise the data will be written to standard
   * output.
   */
  @Override
  @InterfaceAudience.Private
  public void startMonitoring(@Tainted FileContext this)
    throws IOException 
  {
    if (file == null) {
      writer = new @Tainted PrintWriter(new @Tainted BufferedOutputStream(System.out));
    } else {
      writer = new @Tainted PrintWriter(new @Tainted FileWriter(file, true));
    }
    super.startMonitoring();
  }
    
  /**
   * Stops monitoring, closing the file.
   * @see #close()
   */
  @Override
  @InterfaceAudience.Private
  public void stopMonitoring(@Tainted FileContext this) {
    super.stopMonitoring();
        
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }
    
  /**
   * Emits a metrics record to a file.
   */
  @Override
  @InterfaceAudience.Private
  public void emitRecord(@Tainted FileContext this, @Tainted String contextName, @Tainted String recordName, @Tainted OutputRecord outRec) {
    writer.print(contextName);
    writer.print(".");
    writer.print(recordName);
    @Tainted
    String separator = ": ";
    for (@Tainted String tagName : outRec.getTagNames()) {
      writer.print(separator);
      separator = ", ";
      writer.print(tagName);
      writer.print("=");
      writer.print(outRec.getTag(tagName));
    }
    for (@Tainted String metricName : outRec.getMetricNames()) {
      writer.print(separator);
      separator = ", ";
      writer.print(metricName);
      writer.print("=");
      writer.print(outRec.getMetric(metricName));
    }
    writer.println();
  }
    
  /**
   * Flushes the output writer, forcing updates to disk.
   */
  @Override
  @InterfaceAudience.Private
  public void flush(@Tainted FileContext this) {
    writer.flush();
  }
}
