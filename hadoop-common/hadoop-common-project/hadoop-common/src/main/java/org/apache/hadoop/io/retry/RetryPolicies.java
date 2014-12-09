/*
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
package org.apache.hadoop.io.retry;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.ConnectTimeoutException;

/**
 * <p>
 * A collection of useful implementations of {@link RetryPolicy}.
 * </p>
 */
public class RetryPolicies {
  
  public static final @Tainted Log LOG = LogFactory.getLog(RetryPolicies.class);
  
  private static @Tainted ThreadLocal<@Tainted Random> RANDOM = new @Tainted ThreadLocal<@Tainted Random>() {
    @Override
    protected @Tainted Random initialValue() {
      return new @Tainted Random();
    }
  };
  
  /**
   * <p>
   * Try once, and fail by re-throwing the exception.
   * This corresponds to having no retry mechanism in place.
   * </p>
   */
  public static final @Tainted RetryPolicy TRY_ONCE_THEN_FAIL = new @Tainted TryOnceThenFail();
  
  /**
   * <p>
   * Keep trying forever.
   * </p>
   */
  public static final @Tainted RetryPolicy RETRY_FOREVER = new @Tainted RetryForever();
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final @Tainted RetryPolicy retryUpToMaximumCountWithFixedSleep(@Tainted int maxRetries, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
    return new @Tainted RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying for a maximum time, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final @Tainted RetryPolicy retryUpToMaximumTimeWithFixedSleep(@Tainted long maxTime, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
    return new @Tainted RetryUpToMaximumTimeWithFixedSleep(maxTime, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by the number of tries so far.
   * </p>
   */
  public static final @Tainted RetryPolicy retryUpToMaximumCountWithProportionalSleep(@Tainted int maxRetries, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
    return new @Tainted RetryUpToMaximumCountWithProportionalSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by a random
   * number in the range of [0, 2 to the number of retries)
   * </p>
   */
  public static final @Tainted RetryPolicy exponentialBackoffRetry(
      @Tainted
      int maxRetries, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
    return new @Tainted ExponentialBackoffRetry(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final @Tainted RetryPolicy retryByException(@Tainted RetryPolicy defaultPolicy,
                                                   @Tainted
                                                   Map<@Tainted Class<@Tainted ? extends @Tainted Exception>, @Tainted RetryPolicy> exceptionToPolicyMap) {
    return new @Tainted ExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  /**
   * <p>
   * A retry policy for RemoteException
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final @Tainted RetryPolicy retryByRemoteException(
      @Tainted
      RetryPolicy defaultPolicy,
      @Tainted
      Map<@Tainted Class<@Tainted ? extends @Tainted Exception>, @Tainted RetryPolicy> exceptionToPolicyMap) {
    return new @Tainted RemoteExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  public static final @Tainted RetryPolicy failoverOnNetworkException(@Tainted int maxFailovers) {
    return failoverOnNetworkException(TRY_ONCE_THEN_FAIL, maxFailovers);
  }
  
  public static final @Tainted RetryPolicy failoverOnNetworkException(
      @Tainted
      RetryPolicy fallbackPolicy, @Tainted int maxFailovers) {
    return failoverOnNetworkException(fallbackPolicy, maxFailovers, 0, 0);
  }
  
  public static final @Tainted RetryPolicy failoverOnNetworkException(
      @Tainted
      RetryPolicy fallbackPolicy, @Tainted int maxFailovers, @Tainted long delayMillis,
      @Tainted
      long maxDelayBase) {
    return new @Tainted FailoverOnNetworkExceptionRetry(fallbackPolicy, maxFailovers,
        delayMillis, maxDelayBase);
  }
  
  static class TryOnceThenFail implements @Tainted RetryPolicy {
    @Override
    public @Tainted RetryAction shouldRetry(RetryPolicies.@Tainted TryOnceThenFail this, @Tainted Exception e, @Tainted int retries, @Tainted int failovers,
        @Tainted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return RetryAction.FAIL;
    }
  }
  
  static class RetryForever implements @Tainted RetryPolicy {
    @Override
    public @Tainted RetryAction shouldRetry(RetryPolicies.@Tainted RetryForever this, @Tainted Exception e, @Tainted int retries, @Tainted int failovers,
        @Tainted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return RetryAction.RETRY;
    }
  }
  
  /**
   * Retry up to maxRetries.
   * The actual sleep time of the n-th retry is f(n, sleepTime),
   * where f is a function provided by the subclass implementation.
   *
   * The object of the subclasses should be immutable;
   * otherwise, the subclass must override hashCode(), equals(..) and toString().
   */
  static abstract class RetryLimited implements @Tainted RetryPolicy {
    final @Tainted int maxRetries;
    final @Tainted long sleepTime;
    final @Tainted TimeUnit timeUnit;
    
    private @Tainted String myString;

    @Tainted
    RetryLimited(@Tainted int maxRetries, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
      if (maxRetries < 0) {
        throw new @Tainted IllegalArgumentException("maxRetries = " + maxRetries+" < 0");
      }
      if (sleepTime < 0) {
        throw new @Tainted IllegalArgumentException("sleepTime = " + sleepTime + " < 0");
      }

      this.maxRetries = maxRetries;
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
    }

    @Override
    public @Tainted RetryAction shouldRetry(RetryPolicies.@Tainted RetryLimited this, @Tainted Exception e, @Tainted int retries, @Tainted int failovers,
        @Tainted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      if (retries >= maxRetries) {
        return RetryAction.FAIL;
      }
      return new @Tainted RetryAction(RetryAction.RetryDecision.RETRY,
          timeUnit.toMillis(calculateSleepTime(retries)));
    }
    
    protected abstract @Tainted long calculateSleepTime(RetryPolicies.@Tainted RetryLimited this, @Tainted int retries);
    
    @Override
    public @Tainted int hashCode(RetryPolicies.@Tainted RetryLimited this) {
      return toString().hashCode();
    }
    
    @Override
    public @Tainted boolean equals(RetryPolicies.@Tainted RetryLimited this, final @Tainted Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public @Tainted String toString(RetryPolicies.@Tainted RetryLimited this) {
      if (myString == null) {
        myString = getClass().getSimpleName() + "(maxRetries=" + maxRetries
            + ", sleepTime=" + sleepTime + " " + timeUnit + ")";
      }
      return myString;
    }
  }
  
  static class RetryUpToMaximumCountWithFixedSleep extends @Tainted RetryLimited {
    public @Tainted RetryUpToMaximumCountWithFixedSleep(@Tainted int maxRetries, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected @Tainted long calculateSleepTime(RetryPolicies.@Tainted RetryUpToMaximumCountWithFixedSleep this, @Tainted int retries) {
      return sleepTime;
    }
  }
  
  static class RetryUpToMaximumTimeWithFixedSleep extends @Tainted RetryUpToMaximumCountWithFixedSleep {
    public @Tainted RetryUpToMaximumTimeWithFixedSleep(@Tainted long maxTime, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
      super((@Tainted int) (maxTime / sleepTime), sleepTime, timeUnit);
    }
  }
  
  static class RetryUpToMaximumCountWithProportionalSleep extends @Tainted RetryLimited {
    public @Tainted RetryUpToMaximumCountWithProportionalSleep(@Tainted int maxRetries, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected @Tainted long calculateSleepTime(RetryPolicies.@Tainted RetryUpToMaximumCountWithProportionalSleep this, @Tainted int retries) {
      return sleepTime * (retries + 1);
    }
  }
  
  /**
   * Given pairs of number of retries and sleep time (n0, t0), (n1, t1), ...,
   * the first n0 retries sleep t0 milliseconds on average,
   * the following n1 retries sleep t1 milliseconds on average, and so on.
   * 
   * For all the sleep, the actual sleep time is randomly uniform distributed
   * in the close interval [0.5t, 1.5t], where t is the sleep time specified.
   *
   * The objects of this class are immutable.
   */
  public static class MultipleLinearRandomRetry implements @Tainted RetryPolicy {
    /** Pairs of numRetries and sleepSeconds */
    public static class Pair {
      final @Tainted int numRetries;
      final @Tainted int sleepMillis;
      
      public @Tainted Pair(final @Tainted int numRetries, final @Tainted int sleepMillis) {
        if (numRetries < 0) {
          throw new @Tainted IllegalArgumentException("numRetries = " + numRetries+" < 0");
        }
        if (sleepMillis < 0) {
          throw new @Tainted IllegalArgumentException("sleepMillis = " + sleepMillis + " < 0");
        }

        this.numRetries = numRetries;
        this.sleepMillis = sleepMillis;
      }
      
      @Override
      public @Tainted String toString(RetryPolicies.MultipleLinearRandomRetry.@Tainted Pair this) {
        return numRetries + "x" + sleepMillis + "ms";
      }
    }

    private final @Tainted List<@Tainted Pair> pairs;
    private @Tainted String myString;

    public @Tainted MultipleLinearRandomRetry(@Tainted List<@Tainted Pair> pairs) {
      if (pairs == null || pairs.isEmpty()) {
        throw new @Tainted IllegalArgumentException("pairs must be neither null nor empty.");
      }
      this.pairs = Collections.unmodifiableList(pairs);
    }

    @Override
    public @Tainted RetryAction shouldRetry(RetryPolicies.@Tainted MultipleLinearRandomRetry this, @Tainted Exception e, @Tainted int curRetry, @Tainted int failovers,
        @Tainted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      final @Tainted Pair p = searchPair(curRetry);
      if (p == null) {
        //no more retries.
        return RetryAction.FAIL;
      }

      //calculate sleep time and return.
      final @Tainted double ratio = RANDOM.get().nextDouble() + 0.5;//0.5 <= ratio <=1.5
      final @Tainted long sleepTime = Math.round(p.sleepMillis * ratio);
      return new @Tainted RetryAction(RetryAction.RetryDecision.RETRY, sleepTime);
    }

    /**
     * Given the current number of retry, search the corresponding pair.
     * @return the corresponding pair,
     *   or null if the current number of retry > maximum number of retry. 
     */
    private @Tainted Pair searchPair(RetryPolicies.@Tainted MultipleLinearRandomRetry this, @Tainted int curRetry) {
      @Tainted
      int i = 0;
      for(; i < pairs.size() && curRetry > pairs.get(i).numRetries; i++) {
        curRetry -= pairs.get(i).numRetries;
      }
      return i == pairs.size()? null: pairs.get(i);
    }
    
    @Override
    public @Tainted int hashCode(RetryPolicies.@Tainted MultipleLinearRandomRetry this) {
      return toString().hashCode();
    }
    
    @Override
    public @Tainted boolean equals(RetryPolicies.@Tainted MultipleLinearRandomRetry this, final @Tainted Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public @Tainted String toString(RetryPolicies.@Tainted MultipleLinearRandomRetry this) {
      if (myString == null) {
        myString = getClass().getSimpleName() + pairs;
      }
      return myString;
    }

    /**
     * Parse the given string as a MultipleLinearRandomRetry object.
     * The format of the string is "t_1, n_1, t_2, n_2, ...",
     * where t_i and n_i are the i-th pair of sleep time and number of retires.
     * Note that the white spaces in the string are ignored.
     *
     * @return the parsed object, or null if the parsing fails.
     */
    public static @Tainted MultipleLinearRandomRetry parseCommaSeparatedString(@Tainted String s) {
      final @Tainted String @Tainted [] elements = s.split(",");
      if (elements.length == 0) {
        LOG.warn("Illegal value: there is no element in \"" + s + "\".");
        return null;
      }
      if (elements.length % 2 != 0) {
        LOG.warn("Illegal value: the number of elements in \"" + s + "\" is "
            + elements.length + " but an even number of elements is expected.");
        return null;
      }

      final @Tainted List<RetryPolicies.MultipleLinearRandomRetry.@Tainted Pair> pairs
          = new @Tainted ArrayList<RetryPolicies.MultipleLinearRandomRetry.@Tainted Pair>();
   
      for(@Tainted int i = 0; i < elements.length; ) {
        //parse the i-th sleep-time
        final @Tainted int sleep = parsePositiveInt(elements, i++, s);
        if (sleep == -1) {
          return null; //parse fails
        }

        //parse the i-th number-of-retries
        final @Tainted int retries = parsePositiveInt(elements, i++, s);
        if (retries == -1) {
          return null; //parse fails
        }

        pairs.add(new RetryPolicies.MultipleLinearRandomRetry.@Tainted Pair(retries, sleep));
      }
      return new RetryPolicies.@Tainted MultipleLinearRandomRetry(pairs);
    }

    /**
     * Parse the i-th element as an integer.
     * @return -1 if the parsing fails or the parsed value <= 0;
     *   otherwise, return the parsed value.
     */
    private static @Tainted int parsePositiveInt(final @Tainted String @Tainted [] elements,
        final @Tainted int i, final @Tainted String originalString) {
      final @Tainted String s = elements[i].trim();
      final @Tainted int n;
      try {
        n = Integer.parseInt(s);
      } catch(@Tainted NumberFormatException nfe) {
        LOG.warn("Failed to parse \"" + s + "\", which is the index " + i
            + " element in \"" + originalString + "\"", nfe);
        return -1;
      }

      if (n <= 0) {
        LOG.warn("The value " + n + " <= 0: it is parsed from the string \""
            + s + "\" which is the index " + i + " element in \""
            + originalString + "\"");
        return -1;
      }
      return n;
    }
  }

  static class ExceptionDependentRetry implements @Tainted RetryPolicy {

    @Tainted
    RetryPolicy defaultPolicy;
    @Tainted
    Map<@Tainted Class<@Tainted ? extends @Tainted Exception>, @Tainted RetryPolicy> exceptionToPolicyMap;
    
    public @Tainted ExceptionDependentRetry(@Tainted RetryPolicy defaultPolicy,
                                   @Tainted
                                   Map<@Tainted Class<@Tainted ? extends @Tainted Exception>, @Tainted RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionToPolicyMap = exceptionToPolicyMap;
    }

    @Override
    public @Tainted RetryAction shouldRetry(RetryPolicies.@Tainted ExceptionDependentRetry this, @Tainted Exception e, @Tainted int retries, @Tainted int failovers,
        @Tainted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      @Tainted
      RetryPolicy policy = exceptionToPolicyMap.get(e.getClass());
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
    }
    
  }
  
  static class RemoteExceptionDependentRetry implements @Tainted RetryPolicy {

    @Tainted
    RetryPolicy defaultPolicy;
    @Tainted
    Map<@Tainted String, @Tainted RetryPolicy> exceptionNameToPolicyMap;
    
    public @Tainted RemoteExceptionDependentRetry(@Tainted RetryPolicy defaultPolicy,
                                   @Tainted
                                   Map<@Tainted Class<@Tainted ? extends @Tainted Exception>,
                                   @Tainted
                                   RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionNameToPolicyMap = new @Tainted HashMap<@Tainted String, @Tainted RetryPolicy>();
      for (@Tainted Entry<@Tainted Class<@Tainted ? extends @Tainted Exception>, @Tainted RetryPolicy> e :
          exceptionToPolicyMap.entrySet()) {
        exceptionNameToPolicyMap.put(e.getKey().getName(), e.getValue());
      }
    }

    @Override
    public @Tainted RetryAction shouldRetry(RetryPolicies.@Tainted RemoteExceptionDependentRetry this, @Tainted Exception e, @Tainted int retries, @Tainted int failovers,
        @Tainted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      @Tainted
      RetryPolicy policy = null;
      if (e instanceof @Tainted RemoteException) {
        policy = exceptionNameToPolicyMap.get(
            ((@Tainted RemoteException) e).getClassName());
      }
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
    }
  }
  
  static class ExponentialBackoffRetry extends @Tainted RetryLimited {
    
    public @Tainted ExponentialBackoffRetry(
        @Tainted
        int maxRetries, @Tainted long sleepTime, @Tainted TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);

      if (maxRetries < 0) {
        throw new @Tainted IllegalArgumentException("maxRetries = " + maxRetries + " < 0");
      } else if (maxRetries >= Long.SIZE - 1) {
        //calculateSleepTime may overflow. 
        throw new @Tainted IllegalArgumentException("maxRetries = " + maxRetries
            + " >= " + (Long.SIZE - 1));
      }
    }
    
    @Override
    protected @Tainted long calculateSleepTime(RetryPolicies.@Tainted ExponentialBackoffRetry this, @Tainted int retries) {
      return calculateExponentialTime(sleepTime, retries + 1);
    }
  }
  
  /**
   * Fail over and retry in the case of:
   *   Remote StandbyException (server is up, but is not the active server)
   *   Immediate socket exceptions (e.g. no route to host, econnrefused)
   *   Socket exceptions after initial connection when operation is idempotent
   * 
   * The first failover is immediate, while all subsequent failovers wait an
   * exponentially-increasing random amount of time.
   * 
   * Fail immediately in the case of:
   *   Socket exceptions after initial connection when operation is not idempotent
   * 
   * Fall back on underlying retry policy otherwise.
   */
  static class FailoverOnNetworkExceptionRetry implements @Tainted RetryPolicy {
    
    private @Tainted RetryPolicy fallbackPolicy;
    private @Tainted int maxFailovers;
    private @Tainted long delayMillis;
    private @Tainted long maxDelayBase;
    
    public @Tainted FailoverOnNetworkExceptionRetry(@Tainted RetryPolicy fallbackPolicy,
        @Tainted
        int maxFailovers) {
      this(fallbackPolicy, maxFailovers, 0, 0);
    }
    
    public @Tainted FailoverOnNetworkExceptionRetry(@Tainted RetryPolicy fallbackPolicy,
        @Tainted
        int maxFailovers, @Tainted long delayMillis, @Tainted long maxDelayBase) {
      this.fallbackPolicy = fallbackPolicy;
      this.maxFailovers = maxFailovers;
      this.delayMillis = delayMillis;
      this.maxDelayBase = maxDelayBase;
    }

    /**
     * @return 0 if this is our first failover/retry (i.e., retry immediately),
     *         sleep exponentially otherwise
     */
    private @Tainted long getFailoverOrRetrySleepTime(RetryPolicies.@Tainted FailoverOnNetworkExceptionRetry this, @Tainted int times) {
      return times == 0 ? 0 : 
        calculateExponentialTime(delayMillis, times, maxDelayBase);
    }
    
    @Override
    public @Tainted RetryAction shouldRetry(RetryPolicies.@Tainted FailoverOnNetworkExceptionRetry this, @Tainted Exception e, @Tainted int retries,
        @Tainted
        int failovers, @Tainted boolean isIdempotentOrAtMostOnce) throws Exception {
      if (failovers >= maxFailovers) {
        return new @Tainted RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "failovers (" + failovers + ") exceeded maximum allowed ("
            + maxFailovers + ")");
      }
      
      if (e instanceof @Tainted ConnectException ||
          e instanceof @Tainted NoRouteToHostException ||
          e instanceof @Tainted UnknownHostException ||
          e instanceof @Tainted StandbyException ||
          e instanceof @Tainted ConnectTimeoutException ||
          isWrappedStandbyException(e)) {
        return new @Tainted RetryAction(RetryAction.RetryDecision.FAILOVER_AND_RETRY,
            getFailoverOrRetrySleepTime(failovers));
      } else if (e instanceof @Tainted SocketException ||
                 (e instanceof @Tainted IOException && !(e instanceof @Tainted RemoteException))) {
        if (isIdempotentOrAtMostOnce) {
          return RetryAction.FAILOVER_AND_RETRY;
        } else {
          return new @Tainted RetryAction(RetryAction.RetryDecision.FAIL, 0,
              "the invoked method is not idempotent, and unable to determine " +
              "whether it was invoked");
        }
      } else {
        @Tainted
        RetriableException re = getWrappedRetriableException(e);
        if (re != null) {
          return new @Tainted RetryAction(RetryAction.RetryDecision.RETRY,
              getFailoverOrRetrySleepTime(retries));
        } else {
          return fallbackPolicy.shouldRetry(e, retries, failovers,
              isIdempotentOrAtMostOnce);
        }
      }
    }
    
  }

  /**
   * Return a value which is <code>time</code> increasing exponentially as a
   * function of <code>retries</code>, +/- 0%-50% of that value, chosen
   * randomly.
   * 
   * @param time the base amount of time to work with
   * @param retries the number of retries that have so occurred so far
   * @param cap value at which to cap the base sleep time
   * @return an amount of time to sleep
   */
  private static @Tainted long calculateExponentialTime(@Tainted long time, @Tainted int retries,
      @Tainted
      long cap) {
    @Tainted
    long baseTime = Math.min(time * (1L << retries), cap);
    return (@Tainted long) (baseTime * (RANDOM.get().nextDouble() + 0.5));
  }

  private static @Tainted long calculateExponentialTime(@Tainted long time, @Tainted int retries) {
    return calculateExponentialTime(time, retries, Long.MAX_VALUE);
  }
  
  private static @Tainted boolean isWrappedStandbyException(@Tainted Exception e) {
    if (!(e instanceof @Tainted RemoteException)) {
      return false;
    }
    @Tainted
    Exception unwrapped = ((@Tainted RemoteException)e).unwrapRemoteException(
        StandbyException.class);
    return unwrapped instanceof @Tainted StandbyException;
  }
  
  private static @Tainted RetriableException getWrappedRetriableException(@Tainted Exception e) {
    if (!(e instanceof @Tainted RemoteException)) {
      return null;
    }
    @Tainted
    Exception unwrapped = ((@Tainted RemoteException)e).unwrapRemoteException(
        RetriableException.class);
    return unwrapped instanceof @Tainted RetriableException ? 
        (@Tainted RetriableException) unwrapped : null;
  }
}
