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

package org.apache.hadoop.metrics.spi;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;

/**
 * A null context which has a thread calling 
 * periodically when monitoring is started. This keeps the data sampled 
 * correctly.
 * In all other respects, this is like the NULL context: No data is emitted.
 * This is suitable for Monitoring systems like JMX which reads the metrics
 *  when someone reads the data from JMX.
 * 
 * The default impl of start and stop monitoring:
 *  is the AbstractMetricsContext is good enough.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class NullContextWithUpdateThread extends @Tainted AbstractMetricsContext {
  
  private static final @Tainted String PERIOD_PROPERTY = "period";
    
  /** Creates a new instance of NullContextWithUpdateThread */
  @InterfaceAudience.Private
  public @Tainted NullContextWithUpdateThread() {
  }
  
  @Override
  @InterfaceAudience.Private
  public void init(@Tainted NullContextWithUpdateThread this, @Tainted String contextName, @Tainted ContextFactory factory) {
    super.init(contextName, factory);
    parseAndSetPeriod(PERIOD_PROPERTY);
  }
   
    
  /**
   * Do-nothing version of emitRecord
   */
  @Override
  @InterfaceAudience.Private
  protected void emitRecord(@Tainted NullContextWithUpdateThread this, @Tainted String contextName, @Tainted String recordName,
                            @Tainted
                            OutputRecord outRec) 
  {}
    
  /**
   * Do-nothing version of update
   */
  @Override
  @InterfaceAudience.Private
  protected void update(@Tainted NullContextWithUpdateThread this, @Tainted MetricsRecordImpl record) {
  }
    
  /**
   * Do-nothing version of remove
   */
  @Override
  @InterfaceAudience.Private
  protected void remove(@Tainted NullContextWithUpdateThread this, @Tainted MetricsRecordImpl record) {
  }
}
