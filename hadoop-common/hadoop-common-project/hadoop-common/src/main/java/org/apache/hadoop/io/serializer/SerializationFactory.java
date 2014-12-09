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

package org.apache.hadoop.io.serializer;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <p>
 * A factory for {@link Serialization}s.
 * </p>
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SerializationFactory extends @Tainted Configured {

  static final @Tainted Log LOG =
    LogFactory.getLog(SerializationFactory.class.getName());

  private @Tainted List<@Tainted Serialization<@Tainted ? extends java.lang.@Tainted Object>> serializations = new @Tainted ArrayList<@Tainted Serialization<@Tainted ? extends java.lang.@Tainted Object>>();

  /**
   * <p>
   * Serializations are found by reading the <code>io.serializations</code>
   * property from <code>conf</code>, which is a comma-delimited list of
   * classnames.
   * </p>
   */
  public @Tainted SerializationFactory(@Tainted Configuration conf) {
    super(conf);
    if (conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY).equals("")) {
      LOG.warn("Serialization for various data types may not be available. Please configure "
          + CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
          + " properly to have serialization support (it is currently not set).");
    } else {
      for (@Tainted String serializerName : conf.getStrings(
          CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, new @Tainted String @Tainted [] {
              WritableSerialization.class.getName(),
              AvroSpecificSerialization.class.getName(),
              AvroReflectSerialization.class.getName() })) {
        add(conf, serializerName);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void add(@Tainted SerializationFactory this, @Tainted Configuration conf, @Tainted String serializationName) {
    try {
      @Tainted
      Class<@Tainted ? extends @Tainted Serialization> serializionClass =
        (@Tainted Class<@Tainted ? extends @Tainted Serialization>) conf.getClassByName(serializationName);
      serializations.add((@Tainted Serialization)
      ReflectionUtils.newInstance(serializionClass, getConf()));
    } catch (@Tainted ClassNotFoundException e) {
      LOG.warn("Serialization class not found: ", e);
    }
  }

  public <@Tainted T extends java.lang.@Tainted Object> @Tainted Serializer<@Tainted T> getSerializer(@Tainted SerializationFactory this, @Tainted Class<@Tainted T> c) {
    @Tainted
    Serialization<@Tainted T> serializer = getSerialization(c);
    if (serializer != null) {
      return serializer.getSerializer(c);
    }
    return null;
  }

  public <@Tainted T extends java.lang.@Tainted Object> @Tainted Deserializer<@Tainted T> getDeserializer(@Tainted SerializationFactory this, @Tainted Class<@Tainted T> c) {
    @Tainted
    Serialization<@Tainted T> serializer = getSerialization(c);
    if (serializer != null) {
      return serializer.getDeserializer(c);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public <@Tainted T extends java.lang.@Tainted Object> @Tainted Serialization<@Tainted T> getSerialization(@Tainted SerializationFactory this, @Tainted Class<@Tainted T> c) {
    for (@Tainted Serialization serialization : serializations) {
      if (serialization.accept(c)) {
        return (@Tainted Serialization<@Tainted T>) serialization;
      }
    }
    return null;
  }

}
