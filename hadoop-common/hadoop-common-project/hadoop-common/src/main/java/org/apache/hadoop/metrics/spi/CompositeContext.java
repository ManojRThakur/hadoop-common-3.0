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
package org.apache.hadoop.metrics.spi;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompositeContext extends @Tainted AbstractMetricsContext {

  private static final @Tainted Log LOG = LogFactory.getLog(CompositeContext.class);
  private static final @Tainted String ARITY_LABEL = "arity";
  private static final @Tainted String SUB_FMT = "%s.sub%d";
  private final @Tainted ArrayList<@Tainted MetricsContext> subctxt =
    new @Tainted ArrayList<@Tainted MetricsContext>();

  @InterfaceAudience.Private
  public @Tainted CompositeContext() {
  }

  @Override
  @InterfaceAudience.Private
  public void init(@Tainted CompositeContext this, @Tainted String contextName, @Tainted ContextFactory factory) {
    super.init(contextName, factory);
    @Tainted
    int nKids;
    try {
      @Tainted
      String sKids = getAttribute(ARITY_LABEL);
      nKids = Integer.valueOf(sKids);
    } catch (@Tainted Exception e) {
      LOG.error("Unable to initialize composite metric " + contextName +
                ": could not init arity", e);
      return;
    }
    for (@Tainted int i = 0; i < nKids; ++i) {
      @Tainted
      MetricsContext ctxt = MetricsUtil.getContext(
          String.format(SUB_FMT, contextName, i), contextName);
      if (null != ctxt) {
        subctxt.add(ctxt);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  public @Tainted MetricsRecord newRecord(@Tainted CompositeContext this, @Tainted String recordName) {
    return (@Tainted MetricsRecord) Proxy.newProxyInstance(
        MetricsRecord.class.getClassLoader(),
        new @Tainted Class @Tainted [] { MetricsRecord.class },
        new @Tainted MetricsRecordDelegator(recordName, subctxt));
  }

  @InterfaceAudience.Private
  @Override
  protected void emitRecord(@Tainted CompositeContext this, @Tainted String contextName, @Tainted String recordName,
      @Tainted
      OutputRecord outRec) throws IOException {
    for (@Tainted MetricsContext ctxt : subctxt) {
      try {
        ((@Tainted AbstractMetricsContext)ctxt).emitRecord(
          contextName, recordName, outRec);
        if (contextName == null || recordName == null || outRec == null) {
          throw new @Tainted IOException(contextName + ":" + recordName + ":" + outRec);
        }
      } catch (@Tainted IOException e) {
        LOG.warn("emitRecord failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  protected void flush(@Tainted CompositeContext this) throws IOException {
    for (@Tainted MetricsContext ctxt : subctxt) {
      try {
        ((@Tainted AbstractMetricsContext)ctxt).flush();
      } catch (@Tainted IOException e) {
        LOG.warn("flush failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  public void startMonitoring(@Tainted CompositeContext this) throws IOException {
    for (@Tainted MetricsContext ctxt : subctxt) {
      try {
        ctxt.startMonitoring();
      } catch (@Tainted IOException e) {
        LOG.warn("startMonitoring failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  public void stopMonitoring(@Tainted CompositeContext this) {
    for (@Tainted MetricsContext ctxt : subctxt) {
      ctxt.stopMonitoring();
    }
  }

  /**
   * Return true if all subcontexts are monitoring.
   */
  @InterfaceAudience.Private
  @Override
  public @Tainted boolean isMonitoring(@Tainted CompositeContext this) {
    @Tainted
    boolean ret = true;
    for (@Tainted MetricsContext ctxt : subctxt) {
      ret &= ctxt.isMonitoring();
    }
    return ret;
  }

  @InterfaceAudience.Private
  @Override
  public void close(@Tainted CompositeContext this) {
    for (@Tainted MetricsContext ctxt : subctxt) {
      ctxt.close();
    }
  }

  @InterfaceAudience.Private
  @Override
  public void registerUpdater(@Tainted CompositeContext this, @Tainted Updater updater) {
    for (@Tainted MetricsContext ctxt : subctxt) {
      ctxt.registerUpdater(updater);
    }
  }

  @InterfaceAudience.Private
  @Override
  public void unregisterUpdater(@Tainted CompositeContext this, @Tainted Updater updater) {
    for (@Tainted MetricsContext ctxt : subctxt) {
      ctxt.unregisterUpdater(updater);
    }
  }

  private static class MetricsRecordDelegator implements @Tainted InvocationHandler {
    private static final @Tainted Method m_getRecordName = initMethod();
    private static @Tainted Method initMethod() {
      try {
        return MetricsRecord.class.getMethod("getRecordName", new @Tainted Class @Tainted [0]);
      } catch (@Tainted Exception e) {
        throw new @Tainted RuntimeException("Internal error", e);
      }
    }

    private final @Tainted String recordName;
    private final @Tainted ArrayList<@Tainted MetricsRecord> subrecs;

    @Tainted
    MetricsRecordDelegator(@Tainted String recordName, @Tainted ArrayList<@Tainted MetricsContext> ctxts) {
      this.recordName = recordName;
      this.subrecs = new @Tainted ArrayList<@Tainted MetricsRecord>(ctxts.size());
      for (@Tainted MetricsContext ctxt : ctxts) {
        subrecs.add(ctxt.createRecord(recordName));
      }
    }

    @Override
    public @Tainted Object invoke(CompositeContext.@Tainted MetricsRecordDelegator this, @Tainted Object p, @Tainted Method m, @Tainted Object @Tainted [] args) throws Throwable {
      if (m_getRecordName.equals(m)) {
        return recordName;
      }
      assert Void.TYPE.equals(m.getReturnType());
      for (@Tainted MetricsRecord rec : subrecs) {
        m.invoke(rec, args);
      }
      return null;
    }
  }

}
