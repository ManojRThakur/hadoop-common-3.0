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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The <code>ShutdownHookManager</code> enables running shutdownHook
 * in a deterministic order, higher priority first.
 * <p/>
 * The JVM runs ShutdownHooks in a non-deterministic order or in parallel.
 * This class registers a single JVM shutdownHook and run all the
 * shutdownHooks registered to it (to this class) in order based on their
 * priority.
 */
public class ShutdownHookManager {

  private static final @Tainted ShutdownHookManager MGR = new @Tainted ShutdownHookManager();

  private static final @Tainted Log LOG = LogFactory.getLog(ShutdownHookManager.class);

  static {
    Runtime.getRuntime().addShutdownHook(
      new @Tainted Thread() {
        @Override
        public void run() {
          MGR.shutdownInProgress.set(true);
          for (@Tainted Runnable hook: MGR.getShutdownHooksInOrder()) {
            try {
              hook.run();
            } catch (@Tainted Throwable ex) {
              LOG.warn("ShutdownHook '" + hook.getClass().getSimpleName() +
                       "' failed, " + ex.toString(), ex);
            }
          }
        }
      }
    );
  }

  /**
   * Return <code>ShutdownHookManager</code> singleton.
   *
   * @return <code>ShutdownHookManager</code> singleton.
   */
  public static @Tainted ShutdownHookManager get() {
    return MGR;
  }

  /**
   * Private structure to store ShutdownHook and its priority.
   */
  private static class HookEntry {
    @Tainted
    Runnable hook;
    @Tainted
    int priority;

    public @Tainted HookEntry(@Tainted Runnable hook, @Tainted int priority) {
      this.hook = hook;
      this.priority = priority;
    }

    @Override
    public @Tainted int hashCode(ShutdownHookManager.@Tainted HookEntry this) {
      return hook.hashCode();
    }

    @Override
    public @Tainted boolean equals(ShutdownHookManager.@Tainted HookEntry this, @Tainted Object obj) {
      @Tainted
      boolean eq = false;
      if (obj != null) {
        if (obj instanceof @Tainted HookEntry) {
          eq = (hook == ((@Tainted HookEntry)obj).hook);
        }
      }
      return eq;
    }

  }

  private @Tainted Set<@Tainted HookEntry> hooks =
    Collections.synchronizedSet(new @Tainted HashSet<@Tainted HookEntry>());

  private @Tainted AtomicBoolean shutdownInProgress = new @Tainted AtomicBoolean(false);

  //private to constructor to ensure singularity
  private @Tainted ShutdownHookManager() {
  }

  /**
   * Returns the list of shutdownHooks in order of execution,
   * Highest priority first.
   *
   * @return the list of shutdownHooks in order of execution.
   */
  @Tainted
  List<@Tainted Runnable> getShutdownHooksInOrder(@Tainted ShutdownHookManager this) {
    @Tainted
    List<@Tainted HookEntry> list;
    synchronized (MGR.hooks) {
      list = new @Tainted ArrayList<@Tainted HookEntry>(MGR.hooks);
    }
    Collections.sort(list, new @Tainted Comparator<@Tainted HookEntry>() {

      //reversing comparison so highest priority hooks are first
      @Override
      public @Tainted int compare(@Tainted HookEntry o1, @Tainted HookEntry o2) {
        return o2.priority - o1.priority;
      }
    });
    @Tainted
    List<@Tainted Runnable> ordered = new @Tainted ArrayList<@Tainted Runnable>();
    for (@Tainted HookEntry entry: list) {
      ordered.add(entry.hook);
    }
    return ordered;
  }

  /**
   * Adds a shutdownHook with a priority, the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook.
   */
  public void addShutdownHook(@Tainted ShutdownHookManager this, @Tainted Runnable shutdownHook, @Tainted int priority) {
    if (shutdownHook == null) {
      throw new @Tainted IllegalArgumentException("shutdownHook cannot be NULL");
    }
    if (shutdownInProgress.get()) {
      throw new @Tainted IllegalStateException("Shutdown in progress, cannot add a shutdownHook");
    }
    hooks.add(new @Tainted HookEntry(shutdownHook, priority));
  }

  /**
   * Removes a shutdownHook.
   *
   * @param shutdownHook shutdownHook to remove.
   * @return TRUE if the shutdownHook was registered and removed,
   * FALSE otherwise.
   */
  public @Tainted boolean removeShutdownHook(@Tainted ShutdownHookManager this, @Tainted Runnable shutdownHook) {
    if (shutdownInProgress.get()) {
      throw new @Tainted IllegalStateException("Shutdown in progress, cannot remove a shutdownHook");
    }
    return hooks.remove(new @Tainted HookEntry(shutdownHook, 0));
  }

  /**
   * Indicates if a shutdownHook is registered or not.
   *
   * @param shutdownHook shutdownHook to check if registered.
   * @return TRUE/FALSE depending if the shutdownHook is is registered.
   */
  public @Tainted boolean hasShutdownHook(@Tainted ShutdownHookManager this, @Tainted Runnable shutdownHook) {
    return hooks.contains(new @Tainted HookEntry(shutdownHook, 0));
  }
  
  /**
   * Indicates if shutdown is in progress or not.
   * 
   * @return TRUE if the shutdown is in progress, otherwise FALSE.
   */
  public @Tainted boolean isShutdownInProgress(@Tainted ShutdownHookManager this) {
    return shutdownInProgress.get();
  }

}
