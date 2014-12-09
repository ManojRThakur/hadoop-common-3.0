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
package org.apache.hadoop.ha;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * This class parses the configured list of fencing methods, and
 * is responsible for trying each one in turn while logging informative
 * output.<p>
 * 
 * The fencing methods are configured as a carriage-return separated list.
 * Each line in the list is of the form:<p>
 * <code>com.example.foo.MyMethod(arg string)</code>
 * or
 * <code>com.example.foo.MyMethod</code>
 * The class provided must implement the {@link FenceMethod} interface.
 * The fencing methods that ship with Hadoop may also be referred to
 * by shortened names:<p>
 * <ul>
 * <li><code>shell(/path/to/some/script.sh args...)</code></li>
 * <li><code>sshfence(...)</code> (see {@link SshFenceByTcpPort})
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NodeFencer {
  private static final @Tainted String CLASS_RE = "([a-zA-Z0-9\\.\\$]+)";
  private static final @Tainted Pattern CLASS_WITH_ARGUMENT =
    Pattern.compile(CLASS_RE + "\\((.+?)\\)");
  private static final @Tainted Pattern CLASS_WITHOUT_ARGUMENT =
    Pattern.compile(CLASS_RE);
  private static final @Tainted Pattern HASH_COMMENT_RE =
    Pattern.compile("#.*$");

  private static final @Tainted Log LOG = LogFactory.getLog(NodeFencer.class);

  /**
   * Standard fencing methods included with Hadoop.
   */
  private static final @Tainted Map<@Tainted String, @Tainted Class<@Tainted ? extends @Tainted FenceMethod>> STANDARD_METHODS =
    ImmutableMap.<@Tainted String, @Tainted Class<? extends FenceMethod>>of(
        "shell", ShellCommandFencer.class,
        "sshfence", SshFenceByTcpPort.class);
  
  private final @Tainted List<@Tainted FenceMethodWithArg> methods;
  
  @Tainted
  NodeFencer(@Tainted Configuration conf, @Untainted String spec)
      throws BadFencingConfigurationException {
    this.methods = parseMethods(conf, spec);
  }
  
  public static @Tainted NodeFencer create(@Tainted Configuration conf, @Tainted String confKey)
      throws BadFencingConfigurationException {
    @Untainted
    String confStr = conf.get(confKey);
    if (confStr == null) {
      return null;
    }
    return new @Tainted NodeFencer(conf, confStr);
  }

  public @Tainted boolean fence(@Tainted NodeFencer this, @Tainted HAServiceTarget fromSvc) {
    LOG.info("====== Beginning Service Fencing Process... ======");
    @Tainted
    int i = 0;
    for (@Tainted FenceMethodWithArg method : methods) {
      LOG.info("Trying method " + (++i) + "/" + methods.size() +": " + method);
      
      try {
        if (method.method.tryFence(fromSvc, method.arg)) {
          LOG.info("====== Fencing successful by method " + method + " ======");
          return true;
        }
      } catch (@Tainted BadFencingConfigurationException e) {
        LOG.error("Fencing method " + method + " misconfigured", e);
        continue;
      } catch (@Tainted Throwable t) {
        LOG.error("Fencing method " + method + " failed with an unexpected error.", t);
        continue;
      }
      LOG.warn("Fencing method " + method + " was unsuccessful.");
    }
    
    LOG.error("Unable to fence service by any configured method.");
    return false;
  }

  private static @Tainted List<@Tainted FenceMethodWithArg> parseMethods(@Tainted Configuration conf,
      @Untainted
      String spec)
      throws BadFencingConfigurationException {
    @SuppressWarnings("ostrusted:cast.unsafe") // String split still safe
    @Untainted
    String @Tainted [] lines = (@Untainted String @Tainted [] ) spec.split("\\s*\n\\s*");
    
    @Tainted
    List<@Tainted FenceMethodWithArg> methods = Lists.newArrayList();
    for (@Untainted String line : lines) {
      @SuppressWarnings("ostrusted:cast.unsafe") // String operation is safe
      @Untainted String matchLine = (@Untainted String) HASH_COMMENT_RE.matcher(line).replaceAll("");
      @SuppressWarnings("ostrusted:cast.unsafe") // String operation is safe
      @Untainted String trimLine = (@Untainted String) matchLine.trim();
      if (!line.isEmpty()) {
        methods.add(parseMethod(conf, trimLine));
      }
    }
    
    return methods;
  }

  private static @Tainted FenceMethodWithArg parseMethod(@Tainted Configuration conf, @Untainted String line)
      throws BadFencingConfigurationException {
    @Tainted
    Matcher m;
    if ((m = CLASS_WITH_ARGUMENT.matcher(line)).matches()) {
      @Tainted
      String className = m.group(1);
      @SuppressWarnings("ostrusted:cast.unsafe") // TODO: poly on args
      @Untainted
      String arg = (@Untainted String) m.group(2);
      return createFenceMethod(conf, className, arg);
    } else if ((m = CLASS_WITHOUT_ARGUMENT.matcher(line)).matches()) {
      @Tainted
      String className = m.group(1);
      return createFenceMethod(conf, className, null);
    } else {
      throw new @Tainted BadFencingConfigurationException(
          "Unable to parse line: '" + line + "'");
    }
  }

  private static @Tainted FenceMethodWithArg createFenceMethod(
      @Tainted
      Configuration conf, @Tainted String clazzName, @Untainted String arg)
      throws BadFencingConfigurationException {

    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> clazz;
    try {
      // See if it's a short name for one of the built-in methods
      clazz = STANDARD_METHODS.get(clazzName);
      if (clazz == null) {
        // Try to instantiate the user's custom method
        clazz = Class.forName(clazzName);
      }
    } catch (@Tainted Exception e) {
      throw new @Tainted BadFencingConfigurationException(
          "Could not find configured fencing method " + clazzName,
          e);
    }
    
    // Check that it implements the right interface
    if (!FenceMethod.class.isAssignableFrom(clazz)) {
      throw new @Tainted BadFencingConfigurationException("Class " + clazzName +
          " does not implement FenceMethod");
    }
    
    @Tainted
    FenceMethod method = (@Tainted FenceMethod)ReflectionUtils.newInstance(
        clazz, conf);
    method.checkArgs(arg);
    return new @Tainted FenceMethodWithArg(method, arg);
  }
  
  private static class FenceMethodWithArg {
    private final @Tainted FenceMethod method;
    private final @Untainted String arg;
    
    private @Tainted FenceMethodWithArg(@Tainted FenceMethod method, @Untainted String arg) {
      this.method = method;
      this.arg = arg;
    }
    
    @Override
    public @Tainted String toString(NodeFencer.@Tainted FenceMethodWithArg this) {
      return method.getClass().getCanonicalName() + "(" + arg + ")";
    }
  }
}
