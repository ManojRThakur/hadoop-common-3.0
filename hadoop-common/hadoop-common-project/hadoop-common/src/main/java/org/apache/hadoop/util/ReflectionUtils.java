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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * General reflection utils
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ReflectionUtils {
    
  private static final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] EMPTY_ARRAY = new @Tainted Class @Tainted []{};
  volatile private static @Tainted SerializationFactory serialFactory = null;

  /** 
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final @Tainted Map<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted Constructor<@Tainted ? extends java.lang.@Tainted Object>> CONSTRUCTOR_CACHE = 
    new @Tainted ConcurrentHashMap<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>, @Tainted Constructor<@Tainted ? extends java.lang.@Tainted Object>>();

  /**
   * Check and set 'configuration' if necessary.
   * 
   * @param theObject object for which to set configuration
   * @param conf Configuration
   */
  public static void setConf(@Tainted Object theObject, @Tainted Configuration conf) {
    if (conf != null) {
      if (theObject instanceof @Tainted Configurable) {
        ((@Tainted Configurable) theObject).setConf(conf);
      }
      setJobConf(theObject, conf);
    }
  }
  
  /**
   * This code is to support backward compatibility and break the compile  
   * time dependency of core on mapred.
   * This should be made deprecated along with the mapred package HADOOP-1230. 
   * Should be removed when mapred package is removed.
   */
  private static void setJobConf(@Tainted Object theObject, @Tainted Configuration conf) {
    //If JobConf and JobConfigurable are in classpath, AND
    //theObject is of type JobConfigurable AND
    //conf is of type JobConf then
    //invoke configure on theObject
    try {
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> jobConfClass = 
        conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConf");
      if (jobConfClass == null) {
        return;
      }
      
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> jobConfigurableClass = 
        conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConfigurable");
      if (jobConfigurableClass == null) {
        return;
      }
      if (jobConfClass.isAssignableFrom(conf.getClass()) &&
            jobConfigurableClass.isAssignableFrom(theObject.getClass())) {
        @Tainted
        Method configureMethod = 
          jobConfigurableClass.getMethod("configure", jobConfClass);
        configureMethod.invoke(theObject, conf);
      }
    } catch (@Tainted Exception e) {
      throw new @Tainted RuntimeException("Error in configuring object", e);
    }
  }

  /** Create an object for the given class and initialize it from conf
   * 
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T newInstance(@Tainted Class<@Tainted T> theClass, @Tainted Configuration conf) {
    @Tainted
    T result;
    try {
      @Tainted
      Constructor<@Tainted T> meth = (@Tainted Constructor<@Tainted T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance();
    } catch (@Tainted Exception e) {
      throw new @Tainted RuntimeException(e);
    }
    setConf(result, conf);
    return result;
  }

  static private @Tainted ThreadMXBean threadBean = 
    ManagementFactory.getThreadMXBean();
    
  public static void setContentionTracing(@Tainted boolean val) {
    threadBean.setThreadContentionMonitoringEnabled(val);
  }
    
  private static @Tainted String getTaskName(@Tainted long id, @Tainted String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }
    
  /**
   * Print all of the thread's information and stack traces.
   * 
   * @param stream the stream to
   * @param title a string title for the stack trace
   */
  public synchronized static void printThreadInfo(@Tainted PrintWriter stream,
                                     @Tainted
                                     String title) {
    final @Tainted int STACK_DEPTH = 20;
    @Tainted
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    @Tainted
    long @Tainted [] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (@Tainted long tid: threadIds) {
      @Tainted
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " + 
                     getTaskName(info.getThreadId(),
                                 info.getThreadName()) + ":");
      Thread.@Tainted State state = info.getThreadState();
      stream.println("  State: " + state);
      stream.println("  Blocked count: " + info.getBlockedCount());
      stream.println("  Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime());
        stream.println("  Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else  if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName());
        stream.println("  Blocked by " + 
                       getTaskName(info.getLockOwnerId(),
                                   info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (@Tainted StackTraceElement frame: info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
    }
    stream.flush();
  }
    
  private static @Tainted long previousLogTime = 0;
    
  /**
   * Log the current thread stacks at INFO level.
   * @param log the logger that logs the stack trace
   * @param title a descriptive title for the call stacks
   * @param minInterval the minimum time from the last 
   */
  public static void logThreadInfo(@Tainted Log log,
                                   @Tainted
                                   String title,
                                   @Tainted
                                   long minInterval) {
    @Tainted
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ReflectionUtils.class) {
        @Tainted
        long now = Time.now();
        if (now - previousLogTime >= minInterval * 1000) {
          previousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        @Tainted
        ByteArrayOutputStream buffer = new @Tainted ByteArrayOutputStream();
        printThreadInfo(new @Tainted PrintWriter(buffer), title);
        log.info(buffer.toString());
      }
    }
  }

  /**
   * Return the correctly-typed {@link Class} of the given object.
   *  
   * @param o object whose correctly-typed <code>Class</code> is to be obtained
   * @return the correctly typed <code>Class</code> of the given object.
   */
  @SuppressWarnings("unchecked")
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted Class<@Tainted T> getClass(@Tainted T o) {
    return (@Tainted Class<@Tainted T>)o.getClass();
  }
  
  // methods to support testing
  static void clearCache() {
    CONSTRUCTOR_CACHE.clear();
  }
    
  static @Tainted int getCacheSize() {
    return CONSTRUCTOR_CACHE.size();
  }
  /**
   * A pair of input/output buffers that we use to clone writables.
   */
  private static class CopyInCopyOutBuffer {
    @Tainted
    DataOutputBuffer outBuffer = new @Tainted DataOutputBuffer();
    @Tainted
    DataInputBuffer inBuffer = new @Tainted DataInputBuffer();
    /**
     * Move the data from the output buffer to the input buffer.
     */
    void moveData(ReflectionUtils.@Tainted CopyInCopyOutBuffer this) {
      inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
    }
  }
  
  /**
   * Allocate a buffer for each thread that tries to clone objects.
   */
  private static @Tainted ThreadLocal<@Tainted CopyInCopyOutBuffer> cloneBuffers
      = new @Tainted ThreadLocal<@Tainted CopyInCopyOutBuffer>() {
      @Override
      protected synchronized @Tainted CopyInCopyOutBuffer initialValue() {
        return new @Tainted CopyInCopyOutBuffer();
      }
    };

  private static @Tainted SerializationFactory getFactory(@Tainted Configuration conf) {
    if (serialFactory == null) {
      serialFactory = new @Tainted SerializationFactory(conf);
    }
    return serialFactory;
  }
  
  /**
   * Make a copy of the writable object using serialization to a buffer
   * @param dst the object to copy from
   * @param src the object to copy into, which is destroyed
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T copy(@Tainted Configuration conf, 
                                @Tainted
                                T src, @Tainted T dst) throws IOException {
    @Tainted
    CopyInCopyOutBuffer buffer = cloneBuffers.get();
    buffer.outBuffer.reset();
    @Tainted
    SerializationFactory factory = getFactory(conf);
    @Tainted
    Class<@Tainted T> cls = (@Tainted Class<@Tainted T>) src.getClass();
    @Tainted
    Serializer<@Tainted T> serializer = factory.getSerializer(cls);
    serializer.open(buffer.outBuffer);
    serializer.serialize(src);
    buffer.moveData();
    @Tainted
    Deserializer<@Tainted T> deserializer = factory.getDeserializer(cls);
    deserializer.open(buffer.inBuffer);
    dst = deserializer.deserialize(dst);
    return dst;
  }

  @Deprecated
  public static void cloneWritableInto(@Tainted Writable dst, 
                                       @Tainted
                                       Writable src) throws IOException {
    @Tainted
    CopyInCopyOutBuffer buffer = cloneBuffers.get();
    buffer.outBuffer.reset();
    src.write(buffer.outBuffer);
    buffer.moveData();
    dst.readFields(buffer.inBuffer);
  }
  
  /**
   * Gets all the declared fields of a class including fields declared in
   * superclasses.
   */
  public static @Tainted List<@Tainted Field> getDeclaredFieldsIncludingInherited(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> clazz) {
    @Tainted
    List<@Tainted Field> fields = new @Tainted ArrayList<@Tainted Field>();
    while (clazz != null) {
      for (@Tainted Field field : clazz.getDeclaredFields()) {
        fields.add(field);
      }
      clazz = clazz.getSuperclass();
    }
    
    return fields;
  }
  
  /**
   * Gets all the declared methods of a class including methods declared in
   * superclasses.
   */
  public static @Tainted List<@Tainted Method> getDeclaredMethodsIncludingInherited(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> clazz) {
    @Tainted
    List<@Tainted Method> methods = new @Tainted ArrayList<@Tainted Method>();
    while (clazz != null) {
      for (@Tainted Method method : clazz.getDeclaredMethods()) {
        methods.add(method);
      }
      clazz = clazz.getSuperclass();
    }
    
    return methods;
  }
}
