/*
 * GangliaContext.java
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

package org.apache.hadoop.metrics.ganglia;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.spi.Util;

/**
 * Context for sending metrics to Ganglia.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GangliaContext extends @Tainted AbstractMetricsContext {
    
  private static final @Tainted String PERIOD_PROPERTY = "period";
  private static final @Tainted String SERVERS_PROPERTY = "servers";
  private static final @Tainted String UNITS_PROPERTY = "units";
  private static final @Tainted String SLOPE_PROPERTY = "slope";
  private static final @Tainted String TMAX_PROPERTY = "tmax";
  private static final @Tainted String DMAX_PROPERTY = "dmax";
    
  private static final @Tainted String DEFAULT_UNITS = "";
  private static final @Tainted String DEFAULT_SLOPE = "both";
  private static final @Tainted int DEFAULT_TMAX = 60;
  private static final @Tainted int DEFAULT_DMAX = 0;
  private static final @Tainted int DEFAULT_PORT = 8649;
  private static final @Tainted int BUFFER_SIZE = 1500;       // as per libgmond.c

  private final @Tainted Log LOG = LogFactory.getLog(this.getClass());    

  private static final @Tainted Map<@Tainted Class, @Tainted String> typeTable = new @Tainted HashMap<@Tainted Class, @Tainted String>(5);
    
  static {
    typeTable.put(String.class, "string");
    typeTable.put(Byte.class, "int8");
    typeTable.put(Short.class, "int16");
    typeTable.put(Integer.class, "int32");
    typeTable.put(Long.class, "float");
    typeTable.put(Float.class, "float");
  }
    
  protected @Tainted byte @Tainted [] buffer = new @Tainted byte @Tainted [BUFFER_SIZE];
  protected @Tainted int offset;
    
  protected @Tainted List<@Tainted ? extends @Tainted SocketAddress> metricsServers;
  private @Tainted Map<@Tainted String, @Tainted String> unitsTable;
  private @Tainted Map<@Tainted String, @Tainted String> slopeTable;
  private @Tainted Map<@Tainted String, @Tainted String> tmaxTable;
  private @Tainted Map<@Tainted String, @Tainted String> dmaxTable;
    
  protected @Tainted DatagramSocket datagramSocket;
    
  /** Creates a new instance of GangliaContext */
  @InterfaceAudience.Private
  public @Tainted GangliaContext() {
  }
    
  @Override
  @InterfaceAudience.Private
  public void init(@Tainted GangliaContext this, @Tainted String contextName, @Tainted ContextFactory factory) {
    super.init(contextName, factory);
    parseAndSetPeriod(PERIOD_PROPERTY);
        
    metricsServers = 
      Util.parse(getAttribute(SERVERS_PROPERTY), DEFAULT_PORT); 
        
    unitsTable = getAttributeTable(UNITS_PROPERTY);
    slopeTable = getAttributeTable(SLOPE_PROPERTY);
    tmaxTable  = getAttributeTable(TMAX_PROPERTY);
    dmaxTable  = getAttributeTable(DMAX_PROPERTY);
        
    try {
      datagramSocket = new @Tainted DatagramSocket();
    } catch (@Tainted SocketException se) {
      se.printStackTrace();
    }
  }

    /**
   * method to close the datagram socket
   */
  @Override
  public void close(@Tainted GangliaContext this) {
    super.close();
    if (datagramSocket != null) {
      datagramSocket.close();
    }
  }
  
  @Override
  @InterfaceAudience.Private
  public void emitRecord(@Tainted GangliaContext this, @Tainted String contextName, @Tainted String recordName,
    @Tainted
    OutputRecord outRec) 
  throws IOException {
    // Setup so that the records have the proper leader names so they are
    // unambiguous at the ganglia level, and this prevents a lot of rework
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder();
    sb.append(contextName);
    sb.append('.');

    if (contextName.equals("jvm") && outRec.getTag("processName") != null) {
      sb.append(outRec.getTag("processName"));
      sb.append('.');
    }

    sb.append(recordName);
    sb.append('.');
    @Tainted
    int sbBaseLen = sb.length();

    // emit each metric in turn
    for (@Tainted String metricName : outRec.getMetricNames()) {
      @Tainted
      Object metric = outRec.getMetric(metricName);
      @Tainted
      String type = typeTable.get(metric.getClass());
      if (type != null) {
        sb.append(metricName);
        emitMetric(sb.toString(), type, metric.toString());
        sb.setLength(sbBaseLen);
      } else {
        LOG.warn("Unknown metrics type: " + metric.getClass());
      }
    }
  }
    
  protected void emitMetric(@Tainted GangliaContext this, @Tainted String name, @Tainted String type,  @Tainted String value) 
  throws IOException {
    @Tainted
    String units = getUnits(name);
    @Tainted
    int slope = getSlope(name);
    @Tainted
    int tmax = getTmax(name);
    @Tainted
    int dmax = getDmax(name);
        
    offset = 0;
    xdr_int(0);             // metric_user_defined
    xdr_string(type);
    xdr_string(name);
    xdr_string(value);
    xdr_string(units);
    xdr_int(slope);
    xdr_int(tmax);
    xdr_int(dmax);
        
    for (@Tainted SocketAddress socketAddress : metricsServers) {
      @Tainted
      DatagramPacket packet = 
        new @Tainted DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }
  }
    
  protected @Tainted String getUnits(@Tainted GangliaContext this, @Tainted String metricName) {
    @Tainted
    String result = unitsTable.get(metricName);
    if (result == null) {
      result = DEFAULT_UNITS;
    }
    return result;
  }
    
  protected @Tainted int getSlope(@Tainted GangliaContext this, @Tainted String metricName) {
    @Tainted
    String slopeString = slopeTable.get(metricName);
    if (slopeString == null) {
      slopeString = DEFAULT_SLOPE; 
    }
    return ("zero".equals(slopeString) ? 0 : 3); // see gmetric.c
  }
    
  protected @Tainted int getTmax(@Tainted GangliaContext this, @Tainted String metricName) {
    if (tmaxTable == null) {
      return DEFAULT_TMAX;
    }
    @Tainted
    String tmaxString = tmaxTable.get(metricName);
    if (tmaxString == null) {
      return DEFAULT_TMAX;
    }
    else {
      return Integer.parseInt(tmaxString);
    }
  }
    
  protected @Tainted int getDmax(@Tainted GangliaContext this, @Tainted String metricName) {
    @Tainted
    String dmaxString = dmaxTable.get(metricName);
    if (dmaxString == null) {
      return DEFAULT_DMAX;
    }
    else {
      return Integer.parseInt(dmaxString);
    }
  }
    
  /**
   * Puts a string into the buffer by first writing the size of the string
   * as an int, followed by the bytes of the string, padded if necessary to
   * a multiple of 4.
   */
  protected void xdr_string(@Tainted GangliaContext this, @Tainted String s) {
    @Tainted
    byte @Tainted [] bytes = s.getBytes();
    @Tainted
    int len = bytes.length;
    xdr_int(len);
    System.arraycopy(bytes, 0, buffer, offset, len);
    offset += len;
    pad();
  }

  /**
   * Pads the buffer with zero bytes up to the nearest multiple of 4.
   */
  private void pad(@Tainted GangliaContext this) {
    @Tainted
    int newOffset = ((offset + 3) / 4) * 4;
    while (offset < newOffset) {
      buffer[offset++] = 0;
    }
  }
        
  /**
   * Puts an integer into the buffer as 4 bytes, big-endian.
   */
  protected void xdr_int(@Tainted GangliaContext this, @Tainted int i) {
    buffer[offset++] = (@Tainted byte)((i >> 24) & 0xff);
    buffer[offset++] = (@Tainted byte)((i >> 16) & 0xff);
    buffer[offset++] = (@Tainted byte)((i >> 8) & 0xff);
    buffer[offset++] = (@Tainted byte)(i & 0xff);
  }
}
