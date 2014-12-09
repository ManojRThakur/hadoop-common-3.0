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

package org.apache.hadoop.metrics2.sink;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * A metrics sink that writes to a file
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FileSink implements @Tainted MetricsSink {
  private static final @Tainted String FILENAME_KEY = "filename";
  private @Tainted PrintWriter writer;

  @Override
  public void init(@Tainted FileSink this, @Tainted SubsetConfiguration conf) {
    @Tainted
    String filename = conf.getString(FILENAME_KEY);
    try {
      writer = filename == null
          ? new @Tainted PrintWriter(System.out)
          : new @Tainted PrintWriter(new @Tainted FileWriter(new @Tainted File(filename), true));
    } catch (@Tainted Exception e) {
      throw new @Tainted MetricsException("Error creating "+ filename, e);
    }
  }

  @Override
  public void putMetrics(@Tainted FileSink this, @Tainted MetricsRecord record) {
    writer.print(record.timestamp());
    writer.print(" ");
    writer.print(record.context());
    writer.print(".");
    writer.print(record.name());
    @Tainted
    String separator = ": ";
    for (@Tainted MetricsTag tag : record.tags()) {
      writer.print(separator);
      separator = ", ";
      writer.print(tag.name());
      writer.print("=");
      writer.print(tag.value());
    }
    for (@Tainted AbstractMetric metric : record.metrics()) {
      writer.print(separator);
      separator = ", ";
      writer.print(metric.name());
      writer.print("=");
      writer.print(metric.value());
    }
    writer.println();
  }

  @Override
  public void flush(@Tainted FileSink this) {
    writer.flush();
  }
}
