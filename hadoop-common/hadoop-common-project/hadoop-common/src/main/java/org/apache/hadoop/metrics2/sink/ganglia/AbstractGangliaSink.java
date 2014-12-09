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

package org.apache.hadoop.metrics2.sink.ganglia;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.util.Servers;
import org.apache.hadoop.net.DNS;

/**
 * This the base class for Ganglia sink classes using metrics2. Lot of the code
 * has been derived from org.apache.hadoop.metrics.ganglia.GangliaContext.
 * As per the documentation, sink implementations doesn't have to worry about
 * thread safety. Hence the code wasn't written for thread safety and should
 * be modified in case the above assumption changes in the future.
 */
public abstract class AbstractGangliaSink implements @Tainted MetricsSink {

  public final @Tainted Log LOG = LogFactory.getLog(this.getClass());

  /*
   * Output of "gmetric --help" showing allowable values
   * -t, --type=STRING
   *     Either string|int8|uint8|int16|uint16|int32|uint32|float|double
   * -u, --units=STRING Unit of measure for the value e.g. Kilobytes, Celcius
   *     (default='')
   * -s, --slope=STRING Either zero|positive|negative|both
   *     (default='both')
   * -x, --tmax=INT The maximum time in seconds between gmetric calls
   *     (default='60')
   */
  public static final @Tainted String DEFAULT_UNITS = "";
  public static final @Tainted int DEFAULT_TMAX = 60;
  public static final @Tainted int DEFAULT_DMAX = 0;
  public static final @Tainted GangliaSlope DEFAULT_SLOPE = GangliaSlope.both;
  public static final @Tainted int DEFAULT_PORT = 8649;
  public static final @Tainted String SERVERS_PROPERTY = "servers";
  public static final @Tainted int BUFFER_SIZE = 1500; // as per libgmond.c
  public static final @Tainted String SUPPORT_SPARSE_METRICS_PROPERTY = "supportsparse";
  public static final @Tainted boolean SUPPORT_SPARSE_METRICS_DEFAULT = false;
  public static final @Tainted String EQUAL = "=";

  private @Tainted String hostName = "UNKNOWN.example.com";
  private @Tainted DatagramSocket datagramSocket;
  private @Tainted List<@Tainted ? extends @Tainted SocketAddress> metricsServers;
  private @Tainted byte @Tainted [] buffer = new @Tainted byte @Tainted [BUFFER_SIZE];
  private @Tainted int offset;
  private @Tainted boolean supportSparseMetrics = SUPPORT_SPARSE_METRICS_DEFAULT;

  /**
   * Used for visiting Metrics
   */
  protected final @Tainted GangliaMetricVisitor gangliaMetricVisitor =
    new @Tainted GangliaMetricVisitor();

  private @Tainted SubsetConfiguration conf;
  private @Tainted Map<@Tainted String, @Tainted GangliaConf> gangliaConfMap;
  private @Tainted GangliaConf DEFAULT_GANGLIA_CONF = new @Tainted GangliaConf();

  /**
   * ganglia slope values which equal the ordinal
   */
  public enum GangliaSlope {

@Tainted  zero,       // 0

@Tainted  positive,   // 1

@Tainted  negative,   // 2

@Tainted  both        // 3
  };

  /**
   * define enum for various type of conf
   */
  public enum GangliaConfType {

@Tainted  slope,  @Tainted  units,  @Tainted  dmax,  @Tainted  tmax
  };

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.metrics2.MetricsPlugin#init(org.apache.commons.configuration
   * .SubsetConfiguration)
   */
  @Override
  public void init(@Tainted AbstractGangliaSink this, @Tainted SubsetConfiguration conf) {
    LOG.debug("Initializing the GangliaSink for Ganglia metrics.");

    this.conf = conf;

    // Take the hostname from the DNS class.
    if (conf.getString("slave.host.name") != null) {
      hostName = conf.getString("slave.host.name");
    } else {
      try {
        hostName = DNS.getDefaultHost(
            conf.getString("dfs.datanode.dns.interface", "default"),
            conf.getString("dfs.datanode.dns.nameserver", "default"));
      } catch (@Tainted UnknownHostException uhe) {
        LOG.error(uhe);
        hostName = "UNKNOWN.example.com";
      }
    }

    // load the gannglia servers from properties
    metricsServers = Servers.parse(conf.getString(SERVERS_PROPERTY),
        DEFAULT_PORT);

    // extract the Ganglia conf per metrics
    gangliaConfMap = new @Tainted HashMap<@Tainted String, @Tainted GangliaConf>();
    loadGangliaConf(GangliaConfType.units);
    loadGangliaConf(GangliaConfType.tmax);
    loadGangliaConf(GangliaConfType.dmax);
    loadGangliaConf(GangliaConfType.slope);

    try {
      datagramSocket = new @Tainted DatagramSocket();
    } catch (@Tainted SocketException se) {
      LOG.error(se);
    }

    // see if sparseMetrics is supported. Default is false
    supportSparseMetrics = conf.getBoolean(SUPPORT_SPARSE_METRICS_PROPERTY,
        SUPPORT_SPARSE_METRICS_DEFAULT);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.metrics2.MetricsSink#flush()
   */
  @Override
  public void flush(@Tainted AbstractGangliaSink this) {
    // nothing to do as we are not buffering data
  }

  // Load the configurations for a conf type
  private void loadGangliaConf(@Tainted AbstractGangliaSink this, @Tainted GangliaConfType gtype) {
    @Tainted
    String propertyarr @Tainted [] = conf.getStringArray(gtype.name());
    if (propertyarr != null && propertyarr.length > 0) {
      for (@Tainted String metricNValue : propertyarr) {
        @Tainted
        String metricNValueArr @Tainted [] = metricNValue.split(EQUAL);
        if (metricNValueArr.length != 2 || metricNValueArr[0].length() == 0) {
          LOG.error("Invalid propertylist for " + gtype.name());
        }

        @Tainted
        String metricName = metricNValueArr[0].trim();
        @Tainted
        String metricValue = metricNValueArr[1].trim();
        @Tainted
        GangliaConf gconf = gangliaConfMap.get(metricName);
        if (gconf == null) {
          gconf = new @Tainted GangliaConf();
          gangliaConfMap.put(metricName, gconf);
        }

        switch (gtype) {
        case units:
          gconf.setUnits(metricValue);
          break;
        case dmax:
          gconf.setDmax(Integer.parseInt(metricValue));
          break;
        case tmax:
          gconf.setTmax(Integer.parseInt(metricValue));
          break;
        case slope:
          gconf.setSlope(GangliaSlope.valueOf(metricValue));
          break;
        }
      }
    }
  }

  /**
   * Lookup GangliaConf from cache. If not found, return default values
   *
   * @param metricName
   * @return looked up GangliaConf
   */
  protected @Tainted GangliaConf getGangliaConfForMetric(@Tainted AbstractGangliaSink this, @Tainted String metricName) {
    @Tainted
    GangliaConf gconf = gangliaConfMap.get(metricName);

    return gconf != null ? gconf : DEFAULT_GANGLIA_CONF;
  }

  /**
   * @return the hostName
   */
  protected @Tainted String getHostName(@Tainted AbstractGangliaSink this) {
    return hostName;
  }

  /**
   * Puts a string into the buffer by first writing the size of the string as an
   * int, followed by the bytes of the string, padded if necessary to a multiple
   * of 4.
   * @param s the string to be written to buffer at offset location
   */
  protected void xdr_string(@Tainted AbstractGangliaSink this, @Tainted String s) {
    @Tainted
    byte @Tainted [] bytes = s.getBytes();
    @Tainted
    int len = bytes.length;
    xdr_int(len);
    System.arraycopy(bytes, 0, buffer, offset, len);
    offset += len;
    pad();
  }

  // Pads the buffer with zero bytes up to the nearest multiple of 4.
  private void pad(@Tainted AbstractGangliaSink this) {
    @Tainted
    int newOffset = ((offset + 3) / 4) * 4;
    while (offset < newOffset) {
      buffer[offset++] = 0;
    }
  }

  /**
   * Puts an integer into the buffer as 4 bytes, big-endian.
   */
  protected void xdr_int(@Tainted AbstractGangliaSink this, @Tainted int i) {
    buffer[offset++] = (@Tainted byte) ((i >> 24) & 0xff);
    buffer[offset++] = (@Tainted byte) ((i >> 16) & 0xff);
    buffer[offset++] = (@Tainted byte) ((i >> 8) & 0xff);
    buffer[offset++] = (@Tainted byte) (i & 0xff);
  }

  /**
   * Sends Ganglia Metrics to the configured hosts
   * @throws IOException
   */
  protected void emitToGangliaHosts(@Tainted AbstractGangliaSink this) throws IOException {
    try {
      for (@Tainted SocketAddress socketAddress : metricsServers) {
        @Tainted
        DatagramPacket packet =
          new @Tainted DatagramPacket(buffer, offset, socketAddress);
        datagramSocket.send(packet);
      }
    } finally {
      // reset the buffer for the next metric to be built
      offset = 0;
    }
  }

  /**
   * Reset the buffer for the next metric to be built
   */
  void resetBuffer(@Tainted AbstractGangliaSink this) {
    offset = 0;
  }

  /**
   * @return whether sparse metrics are supported
   */
  protected @Tainted boolean isSupportSparseMetrics(@Tainted AbstractGangliaSink this) {
    return supportSparseMetrics;
  }

  /**
   * Used only by unit test
   * @param datagramSocket the datagramSocket to set.
   */
  void setDatagramSocket(@Tainted AbstractGangliaSink this, @Tainted DatagramSocket datagramSocket) {
    this.datagramSocket = datagramSocket;
  }
}
