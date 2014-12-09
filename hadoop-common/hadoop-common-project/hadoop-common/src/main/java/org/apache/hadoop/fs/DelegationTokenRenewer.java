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

package org.apache.hadoop.fs;

import org.checkerframework.checker.tainting.qual.Tainted;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;

/**
 * A daemon thread that waits for the next file system to renew.
 */
@InterfaceAudience.Private
public class DelegationTokenRenewer
    extends @Tainted Thread {
  private static final @Tainted Log LOG = LogFactory
      .getLog(DelegationTokenRenewer.class);

  /** The renewable interface used by the renewer. */
  public interface Renewable {
    /** @return the renew token. */
    public @Tainted Token<@Tainted ? extends java.lang.@Tainted Object> getRenewToken(DelegationTokenRenewer.@Tainted Renewable this);

    /** Set delegation token. */
    public <@Tainted T extends @Tainted TokenIdentifier> void setDelegationToken(DelegationTokenRenewer.@Tainted Renewable this, Token<T> token);
  }

  /**
   * An action that will renew and replace the file system's delegation 
   * tokens automatically.
   */
  public static class RenewAction<@Tainted T extends FileSystem & Renewable>
      implements @Tainted Delayed {
    /** when should the renew happen */
    private @Tainted long renewalTime;
    /** a weak reference to the file system so that it can be garbage collected */
    private final @Tainted WeakReference<@Tainted T> weakFs;
    private @Tainted Token<@Tainted ? extends java.lang.@Tainted Object> token; 
    @Tainted
    boolean isValid = true;

    private @Tainted RenewAction(final @Tainted T fs) {
      this.weakFs = new @Tainted WeakReference<T>(fs);
      this.token = fs.getRenewToken();
      updateRenewalTime(renewCycle);
    }
 
    public @Tainted boolean isValid(DelegationTokenRenewer.@Tainted RenewAction<T> this) {
      return isValid;
    }
    
    /** Get the delay until this event should happen. */
    @Override
    public @Tainted long getDelay(DelegationTokenRenewer.@Tainted RenewAction<T> this, final @Tainted TimeUnit unit) {
      final @Tainted long millisLeft = renewalTime - Time.now();
      return unit.convert(millisLeft, TimeUnit.MILLISECONDS);
    }

    @Override
    public @Tainted int compareTo(DelegationTokenRenewer.@Tainted RenewAction<T> this, final @Tainted Delayed delayed) {
      final @Tainted RenewAction<@Tainted ? extends java.lang.@Tainted Object> that = (@Tainted RenewAction<@Tainted ? extends java.lang.@Tainted Object>)delayed;
      return this.renewalTime < that.renewalTime? -1
          : this.renewalTime == that.renewalTime? 0: 1;
    }

    @Override
    public @Tainted int hashCode(DelegationTokenRenewer.@Tainted RenewAction<T> this) {
      return token.hashCode();
    }

    @Override
    public @Tainted boolean equals(DelegationTokenRenewer.@Tainted RenewAction<T> this, final @Tainted Object that) {
      if (this == that) {
        return true;
      } else if (that == null || !(that instanceof @Tainted RenewAction)) {
        return false;
      }
      return token.equals(((@Tainted RenewAction<@Tainted ? extends java.lang.@Tainted Object>)that).token);
    }

    /**
     * Set a new time for the renewal.
     * It can only be called when the action is not in the queue or any
     * collection because the hashCode may change
     * @param newTime the new time
     */
    private void updateRenewalTime(DelegationTokenRenewer.@Tainted RenewAction<T> this, @Tainted long delay) {
      renewalTime = Time.now() + delay - delay/10;
    }

    /**
     * Renew or replace the delegation token for this file system.
     * It can only be called when the action is not in the queue.
     * @return
     * @throws IOException
     */
    private @Tainted boolean renew(DelegationTokenRenewer.@Tainted RenewAction<T> this) throws IOException, InterruptedException {
      final T fs = weakFs.get();
      final @Tainted boolean b = fs != null;
      if (b) {
        synchronized(fs) {
          try {
            @Tainted
            long expires = token.renew(fs.getConf());
            updateRenewalTime(expires - Time.now());
          } catch (@Tainted IOException ie) {
            try {
              @Tainted
              Token<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] tokens = fs.addDelegationTokens(null, null);
              if (tokens.length == 0) {
                throw new @Tainted IOException("addDelegationTokens returned no tokens");
              }
              token = tokens[0];
              updateRenewalTime(renewCycle);
              fs.setDelegationToken(token);
            } catch (@Tainted IOException ie2) {
              isValid = false;
              throw new @Tainted IOException("Can't renew or get new delegation token ", ie);
            }
          }
        }
      }
      return b;
    }

    private void cancel(DelegationTokenRenewer.@Tainted RenewAction<T> this) throws IOException, InterruptedException {
      final T fs = weakFs.get();
      if (fs != null) {
        token.cancel(fs.getConf());
      }
    }

    @Override
    public @Tainted String toString(DelegationTokenRenewer.@Tainted RenewAction<T> this) {
      @Tainted
      Renewable fs = weakFs.get();
      return fs == null? "evaporated token renew"
          : "The token will be renewed in " + getDelay(TimeUnit.SECONDS)
            + " secs, renewToken=" + token;
    }
  }

  /** assumes renew cycle for a token is 24 hours... */
  private static final @Tainted long RENEW_CYCLE = 24 * 60 * 60 * 1000; 

  @InterfaceAudience.Private
  @VisibleForTesting
  public static @Tainted long renewCycle = RENEW_CYCLE;

  /** Queue to maintain the RenewActions to be processed by the {@link #run()} */
  private volatile @Tainted DelayQueue<@Tainted RenewAction<@Tainted ? extends java.lang.@Tainted Object>> queue = new @Tainted DelayQueue<@Tainted RenewAction<@Tainted ? extends java.lang.@Tainted Object>>();
  
  /** For testing purposes */
  @VisibleForTesting
  protected @Tainted int getRenewQueueLength(@Tainted DelegationTokenRenewer this) {
    return queue.size();
  }

  /**
   * Create the singleton instance. However, the thread can be started lazily in
   * {@link #addRenewAction(FileSystem)}
   */
  private static @Tainted DelegationTokenRenewer INSTANCE = null;

  private @Tainted DelegationTokenRenewer(final @Tainted Class<@Tainted ? extends @Tainted FileSystem> clazz) {
    super(clazz.getSimpleName() + "-" + DelegationTokenRenewer.class.getSimpleName());
    setDaemon(true);
  }

  public static synchronized @Tainted DelegationTokenRenewer getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new @Tainted DelegationTokenRenewer(FileSystem.class);
    }
    return INSTANCE;
  }

  @VisibleForTesting
  static synchronized void reset() {
    if (INSTANCE != null) {
      INSTANCE.queue.clear();
      INSTANCE.interrupt();
      try {
        INSTANCE.join();
      } catch (@Tainted InterruptedException e) {
        LOG.warn("Failed to reset renewer");
      } finally {
        INSTANCE = null;
      }
    }
  }
  
  /** Add a renew action to the queue. */
  @SuppressWarnings("static-access")
  public <@Tainted T extends FileSystem & Renewable> @Tainted RenewAction<@Tainted T> addRenewAction(@Tainted DelegationTokenRenewer this, final @Tainted T fs) {
    synchronized (this) {
      if (!isAlive()) {
        start();
      }
    }
    @Tainted
    RenewAction<@Tainted T> action = new @Tainted RenewAction<@Tainted T>(fs);
    if (action.token != null) {
      queue.add(action);
    } else {
      fs.LOG.error("does not have a token for renewal");
    }
    return action;
  }

  /**
   * Remove the associated renew action from the queue
   * 
   * @throws IOException
   */
  public <@Tainted T extends FileSystem & Renewable> void removeRenewAction(
      @Tainted DelegationTokenRenewer this, final @Tainted T fs) throws IOException {
    @Tainted
    RenewAction<@Tainted T> action = new @Tainted RenewAction<@Tainted T>(fs);
    if (queue.remove(action)) {
      try {
        action.cancel();
      } catch (@Tainted InterruptedException ie) {
        LOG.error("Interrupted while canceling token for " + fs.getUri()
            + "filesystem");
        if (LOG.isDebugEnabled()) {
          LOG.debug(ie.getStackTrace());
        }
      }
    }
  }

  @SuppressWarnings("static-access")
  @Override
  public void run(@Tainted DelegationTokenRenewer this) {
    for(;;) {
      @Tainted
      RenewAction<@Tainted ? extends java.lang.@Tainted Object> action = null;
      try {
        action = queue.take();
        if (action.renew()) {
          queue.add(action);
        }
      } catch (@Tainted InterruptedException ie) {
        return;
      } catch (@Tainted Exception ie) {
        action.weakFs.get().LOG.warn("Failed to renew token, action=" + action,
            ie);
      }
    }
  }
}
