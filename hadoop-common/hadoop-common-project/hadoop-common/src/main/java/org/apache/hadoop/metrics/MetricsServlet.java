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
package org.apache.hadoop.metrics;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap;
import org.mortbay.util.ajax.JSON;
import org.mortbay.util.ajax.JSON.Output;

/**
 * A servlet to print out metrics data.  By default, the servlet returns a 
 * textual representation (no promises are made for parseability), and
 * users can use "?format=json" for parseable output.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetricsServlet extends @Tainted HttpServlet {
  
  /**
   * A helper class to hold a TagMap and MetricMap.
   */
  static class TagsMetricsPair implements JSON.@Tainted Convertible {
    final @Tainted TagMap tagMap;
    final @Tainted MetricMap metricMap;
    
    public @Tainted TagsMetricsPair(@Tainted TagMap tagMap, @Tainted MetricMap metricMap) {
      this.tagMap = tagMap;
      this.metricMap = metricMap;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void fromJSON(MetricsServlet.@Tainted TagsMetricsPair this, @Tainted Map map) {
      throw new @Tainted UnsupportedOperationException();
    }

    /** Converts to JSON by providing an array. */
    @Override
    public void toJSON(MetricsServlet.@Tainted TagsMetricsPair this, @Tainted Output out) {
      out.add(new @Tainted Object @Tainted [] { tagMap, metricMap });
    }
  }
  
  /**
   * Collects all metric data, and returns a map:
   *   contextName -> recordName -> [ (tag->tagValue), (metric->metricValue) ].
   * The values are either String or Number.  The final value is implemented
   * as a list of TagsMetricsPair.
   */
   @Tainted
   Map<@Tainted String, @Tainted Map<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>>> makeMap(
       @Tainted MetricsServlet this, @Tainted
       Collection<@Tainted MetricsContext> contexts) throws IOException {
    @Tainted
    Map<@Tainted String, @Tainted Map<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>>> map = 
      new @Tainted TreeMap<@Tainted String, @Tainted Map<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>>>();

    for (@Tainted MetricsContext context : contexts) {
      @Tainted
      Map<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>> records = 
        new @Tainted TreeMap<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>>();
      map.put(context.getContextName(), records);
    
      for (Map.@Tainted Entry<@Tainted String, @Tainted Collection<@Tainted OutputRecord>> r : 
          context.getAllRecords().entrySet()) {
        @Tainted
        List<@Tainted TagsMetricsPair> metricsAndTags = 
          new @Tainted ArrayList<@Tainted TagsMetricsPair>();
        records.put(r.getKey(), metricsAndTags);
        for (@Tainted OutputRecord outputRecord : r.getValue()) {
          @Tainted
          TagMap tagMap = outputRecord.getTagsCopy();
          @Tainted
          MetricMap metricMap = outputRecord.getMetricsCopy();
          metricsAndTags.add(new @Tainted TagsMetricsPair(tagMap, metricMap));
        }
      }
    }
    return map;
  }
  
  @Override
  public void doGet(@Tainted MetricsServlet this, @Tainted HttpServletRequest request, @Tainted HttpServletResponse response)
      throws ServletException, IOException {

    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                   request, response)) {
      return;
    }

    @Tainted
    String format = request.getParameter("format");
    @Tainted
    Collection<@Tainted MetricsContext> allContexts = 
      ContextFactory.getFactory().getAllContexts();
    if ("json".equals(format)) {
      response.setContentType("application/json; charset=utf-8");
      @Tainted
      PrintWriter out = response.getWriter();
      try {
        // Uses Jetty's built-in JSON support to convert the map into JSON.
        out.print(new @Tainted JSON().toJSON(makeMap(allContexts)));
      } finally {
        out.close();
      }
    } else {
      @Tainted
      PrintWriter out = response.getWriter();
      try {
        printMap(out, makeMap(allContexts));
      } finally {
        out.close();
      }
    }
  }
  
  /**
   * Prints metrics data in a multi-line text form.
   */
  void printMap(@Tainted MetricsServlet this, @Tainted PrintWriter out, @Tainted Map<@Tainted String, @Tainted Map<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>>> map) {
    for (Map.@Tainted Entry<@Tainted String, @Tainted Map<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>>> context : map.entrySet()) {
      out.print(context.getKey());
      out.print("\n");
      for (Map.@Tainted Entry<@Tainted String, @Tainted List<@Tainted TagsMetricsPair>> record : context.getValue().entrySet()) {
        indent(out, 1);
        out.print(record.getKey());
        out.print("\n");
        for (@Tainted TagsMetricsPair pair : record.getValue()) {
          indent(out, 2);
          // Prints tag values in the form "{key=value,key=value}:"
          out.print("{");
          @Tainted
          boolean first = true;
          for (Map.@Tainted Entry<@Tainted String, @Tainted Object> tagValue : pair.tagMap.entrySet()) {
            if (first) {
              first = false;
            } else {
              out.print(",");
            }
            out.print(tagValue.getKey());
            out.print("=");
            out.print(tagValue.getValue().toString());
          }
          out.print("}:\n");
          
          // Now print metric values, one per line
          for (Map.@Tainted Entry<@Tainted String, @Tainted Number> metricValue : 
              pair.metricMap.entrySet()) {
            indent(out, 3);
            out.print(metricValue.getKey());
            out.print("=");
            out.print(metricValue.getValue().toString());
            out.print("\n");
          }
        }
      }
    }    
  }
  
  private void indent(@Tainted MetricsServlet this, @Tainted PrintWriter out, @Tainted int indent) {
    for (@Tainted int i = 0; i < indent; ++i) {
      out.append("  ");
    }
  }
}
