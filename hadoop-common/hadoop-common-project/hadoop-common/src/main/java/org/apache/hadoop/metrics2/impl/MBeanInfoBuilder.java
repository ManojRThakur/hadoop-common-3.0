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
import java.util.List;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

import com.google.common.collect.Lists;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsVisitor;

/**
 * Helper class to build MBeanInfo from metrics records
 */
class MBeanInfoBuilder implements @Tainted MetricsVisitor {

  private final @Tainted String name;
  private final @Tainted String description;
  private @Tainted List<@Tainted MBeanAttributeInfo> attrs;
  private @Tainted Iterable<@Tainted MetricsRecordImpl> recs;
  private @Tainted int curRecNo;

  @Tainted
  MBeanInfoBuilder(@Tainted String name, @Tainted String desc) {
    this.name = name;
    description = desc;
    attrs = Lists.newArrayList();
  }

  @Tainted
  MBeanInfoBuilder reset(@Tainted MBeanInfoBuilder this, @Tainted Iterable<@Tainted MetricsRecordImpl> recs) {
    this.recs = recs;
    attrs.clear();
    return this;
  }

  @Tainted
  MBeanAttributeInfo newAttrInfo(@Tainted MBeanInfoBuilder this, @Tainted String name, @Tainted String desc, @Tainted String type) {
    return new @Tainted MBeanAttributeInfo(getAttrName(name), type, desc,
                                  true, false, false); // read-only, non-is
  }

  @Tainted
  MBeanAttributeInfo newAttrInfo(@Tainted MBeanInfoBuilder this, @Tainted MetricsInfo info, @Tainted String type) {
    return newAttrInfo(info.name(), info.description(), type);
  }

  @Override
  public void gauge(@Tainted MBeanInfoBuilder this, @Tainted MetricsInfo info, @Tainted int value) {
    attrs.add(newAttrInfo(info, "java.lang.Integer"));
  }

  @Override
  public void gauge(@Tainted MBeanInfoBuilder this, @Tainted MetricsInfo info, @Tainted long value) {
    attrs.add(newAttrInfo(info, "java.lang.Long"));
  }

  @Override
  public void gauge(@Tainted MBeanInfoBuilder this, @Tainted MetricsInfo info, @Tainted float value) {
    attrs.add(newAttrInfo(info, "java.lang.Float"));
  }

  @Override
  public void gauge(@Tainted MBeanInfoBuilder this, @Tainted MetricsInfo info, @Tainted double value) {
    attrs.add(newAttrInfo(info, "java.lang.Double"));
  }

  @Override
  public void counter(@Tainted MBeanInfoBuilder this, @Tainted MetricsInfo info, @Tainted int value) {
    attrs.add(newAttrInfo(info, "java.lang.Integer"));
  }

  @Override
  public void counter(@Tainted MBeanInfoBuilder this, @Tainted MetricsInfo info, @Tainted long value) {
    attrs.add(newAttrInfo(info, "java.lang.Long"));
  }

  @Tainted
  String getAttrName(@Tainted MBeanInfoBuilder this, @Tainted String name) {
    return curRecNo > 0 ? name +"."+ curRecNo : name;
  }

  @Tainted
  MBeanInfo get(@Tainted MBeanInfoBuilder this) {
    curRecNo = 0;
    for (@Tainted MetricsRecordImpl rec : recs) {
      for (@Tainted MetricsTag t : rec.tags()) {
        attrs.add(newAttrInfo("tag."+ t.name(), t.description(),
                  "java.lang.String"));
      }
      for (@Tainted AbstractMetric m : rec.metrics()) {
        m.visit(this);
      }
      ++curRecNo;
    }
    MetricsSystemImpl.LOG.debug(attrs);
    @Tainted
    MBeanAttributeInfo @Tainted [] attrsArray = new @Tainted MBeanAttributeInfo @Tainted [attrs.size()];
    return new @Tainted MBeanInfo(name, description, attrs.toArray(attrsArray),
                         null, null, null); // no ops/ctors/notifications
  }
}
