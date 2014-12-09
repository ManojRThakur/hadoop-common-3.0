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

package org.apache.hadoop.security.token.delegation;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "Hive"})
@InterfaceStability.Evolving
public abstract 
class AbstractDelegationTokenSecretManager<@Tainted TokenIdent 
extends @Tainted AbstractDelegationTokenIdentifier> 
   extends @Tainted SecretManager<TokenIdent> {
  private static final @Tainted Log LOG = LogFactory
      .getLog(AbstractDelegationTokenSecretManager.class);

  /** 
   * Cache of currently valid tokens, mapping from DelegationTokenIdentifier 
   * to DelegationTokenInformation. Protected by this object lock.
   */
  protected final @Tainted Map<@Tainted TokenIdent, @Tainted DelegationTokenInformation> currentTokens 
      = new @Tainted HashMap<TokenIdent, @Tainted DelegationTokenInformation>();
  
  /**
   * Sequence number to create DelegationTokenIdentifier.
   * Protected by this object lock.
   */
  protected @Tainted int delegationTokenSequenceNumber = 0;
  
  /**
   * Access to allKeys is protected by this object lock
   */
  protected final @Tainted Map<@Tainted Integer, @Tainted DelegationKey> allKeys 
      = new @Tainted HashMap<@Tainted Integer, @Tainted DelegationKey>();
  
  /**
   * Access to currentId is protected by this object lock.
   */
  protected @Tainted int currentId = 0;
  /**
   * Access to currentKey is protected by this object lock
   */
  private @Tainted DelegationKey currentKey;
  
  private @Tainted long keyUpdateInterval;
  private @Tainted long tokenMaxLifetime;
  private @Tainted long tokenRemoverScanInterval;
  private @Tainted long tokenRenewInterval;
  /**
   * Whether to store a token's tracking ID in its TokenInformation.
   * Can be overridden by a subclass.
   */
  protected @Tainted boolean storeTokenTrackingId;
  private @Tainted Thread tokenRemoverThread;
  protected volatile @Tainted boolean running;

  /**
   * If the delegation token update thread holds this lock, it will
   * not get interrupted.
   */
  protected @Tainted Object noInterruptsLock = new @Tainted Object();

  public @Tainted AbstractDelegationTokenSecretManager(@Tainted long delegationKeyUpdateInterval,
      @Tainted
      long delegationTokenMaxLifetime, @Tainted long delegationTokenRenewInterval,
      @Tainted
      long delegationTokenRemoverScanInterval) {
    this.keyUpdateInterval = delegationKeyUpdateInterval;
    this.tokenMaxLifetime = delegationTokenMaxLifetime;
    this.tokenRenewInterval = delegationTokenRenewInterval;
    this.tokenRemoverScanInterval = delegationTokenRemoverScanInterval;
    this.storeTokenTrackingId = false;
  }

  /** should be called before this object is used */
  public void startThreads(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    Preconditions.checkState(!running);
    updateCurrentKey();
    synchronized (this) {
      running = true;
      tokenRemoverThread = new @Tainted Daemon(new @Tainted ExpiredTokenRemover());
      tokenRemoverThread.start();
    }
  }
  
  /**
   * Reset all data structures and mutable state.
   */
  public synchronized void reset(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    currentId = 0;
    allKeys.clear();
    delegationTokenSequenceNumber = 0;
    currentTokens.clear();
  }
  
  /** 
   * Add a previously used master key to cache (when NN restarts), 
   * should be called before activate().
   * */
  public synchronized void addKey(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted DelegationKey key) throws IOException {
    if (running) // a safety check
      throw new @Tainted IOException("Can't add delegation key to a running SecretManager.");
    if (key.getKeyId() > currentId) {
      currentId = key.getKeyId();
    }
    allKeys.put(key.getKeyId(), key);
  }

  public synchronized @Tainted DelegationKey @Tainted [] getAllKeys(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    return allKeys.values().toArray(new @Tainted DelegationKey @Tainted [0]);
  }

  // HDFS
  protected void logUpdateMasterKey(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted DelegationKey key) throws IOException {
    return;
  }

  // HDFS
  protected void logExpireToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent ident) throws IOException {
    return;
  }

  // RM
  protected void storeNewMasterKey(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted DelegationKey key) throws IOException {
    return;
  }

  // RM
  protected void removeStoredMasterKey(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted DelegationKey key) {
    return;
  }

  // RM
  protected void storeNewToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent ident, @Tainted long renewDate) {
    return;
  }
  // RM
  protected void removeStoredToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent ident) throws IOException {

  }
  // RM
  protected void updateStoredToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent ident, @Tainted long renewDate) {
    return;
  }

  /**
   * This method is intended to be used for recovering persisted delegation
   * tokens
   * @param identifier identifier read from persistent storage
   * @param renewDate token renew time
   * @throws IOException
   */
  public synchronized void addPersistedDelegationToken(
      @Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted
      TokenIdent identifier, @Tainted long renewDate) throws IOException {
    if (running) {
      // a safety check
      throw new @Tainted IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    @Tainted
    int keyId = identifier.getMasterKeyId();
    @Tainted
    DelegationKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG.warn("No KEY found for persisted identifier " + identifier.toString());
      return;
    }
    @Tainted
    byte @Tainted [] password = createPassword(identifier.getBytes(), dKey.getKey());
    if (identifier.getSequenceNumber() > this.delegationTokenSequenceNumber) {
      this.delegationTokenSequenceNumber = identifier.getSequenceNumber();
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new @Tainted DelegationTokenInformation(renewDate,
          password, getTrackingIdIfEnabled(identifier)));
    } else {
      throw new @Tainted IOException(
          "Same delegation token being added twice.");
    }
  }

  /** 
   * Update the current master key 
   * This is called once by startThreads before tokenRemoverThread is created, 
   * and only by tokenRemoverThread afterwards.
   */
  private void updateCurrentKey(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    LOG.info("Updating the current master key for generating delegation tokens");
    /* Create a new currentKey with an estimated expiry date. */
    @Tainted
    int newCurrentId;
    synchronized (this) {
      newCurrentId = currentId+1;
    }
    @Tainted
    DelegationKey newKey = new @Tainted DelegationKey(newCurrentId, System
        .currentTimeMillis()
        + keyUpdateInterval + tokenMaxLifetime, generateSecret());
    //Log must be invoked outside the lock on 'this'
    logUpdateMasterKey(newKey);
    storeNewMasterKey(newKey);
    synchronized (this) {
      currentId = newKey.getKeyId();
      currentKey = newKey;
      allKeys.put(currentKey.getKeyId(), currentKey);
    }
  }
  
  /** 
   * Update the current master key for generating delegation tokens 
   * It should be called only by tokenRemoverThread.
   */
  void rollMasterKey(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    synchronized (this) {
      removeExpiredKeys();
      /* set final expiry date for retiring currentKey */
      currentKey.setExpiryDate(Time.now() + tokenMaxLifetime);
      /*
       * currentKey might have been removed by removeExpiredKeys(), if
       * updateMasterKey() isn't called at expected interval. Add it back to
       * allKeys just in case.
       */
      allKeys.put(currentKey.getKeyId(), currentKey);
    }
    updateCurrentKey();
  }

  private synchronized void removeExpiredKeys(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    @Tainted
    long now = Time.now();
    for (@Tainted Iterator<Map.@Tainted Entry<@Tainted Integer, @Tainted DelegationKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.@Tainted Entry<@Tainted Integer, @Tainted DelegationKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
        // ensure the tokens generated by this current key can be recovered
        // with this current key after this current key is rolled
        if(!e.getValue().equals(currentKey))
          removeStoredMasterKey(e.getValue());
      }
    }
  }
  
  @Override
  protected synchronized @Tainted byte @Tainted [] createPassword(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent identifier) {
    @Tainted
    int sequenceNum;
    @Tainted
    long now = Time.now();
    sequenceNum = ++delegationTokenSequenceNumber;
    identifier.setIssueDate(now);
    identifier.setMaxDate(now + tokenMaxLifetime);
    identifier.setMasterKeyId(currentId);
    identifier.setSequenceNumber(sequenceNum);
    LOG.info("Creating password for identifier: " + identifier);
    @Tainted
    byte @Tainted [] password = createPassword(identifier.getBytes(), currentKey.getKey());
    storeNewToken(identifier, now + tokenRenewInterval);
    currentTokens.put(identifier, new @Tainted DelegationTokenInformation(now
        + tokenRenewInterval, password, getTrackingIdIfEnabled(identifier)));
    return password;
  }
  
  /**
   * Find the DelegationTokenInformation for the given token id, and verify that
   * if the token is expired. Note that this method should be called with 
   * acquiring the secret manager's monitor.
   */
  protected @Tainted DelegationTokenInformation checkToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent identifier)
      throws InvalidToken {
    assert Thread.holdsLock(this);
    @Tainted
    DelegationTokenInformation info = currentTokens.get(identifier);
    if (info == null) {
      throw new @Tainted InvalidToken("token (" + identifier.toString()
          + ") can't be found in cache");
    }
    if (info.getRenewDate() < Time.now()) {
      throw new @Tainted InvalidToken("token (" + identifier.toString() + ") is expired");
    }
    return info;
  }
  
  @Override
  public synchronized @Tainted byte @Tainted [] retrievePassword(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent identifier)
      throws InvalidToken {
    return checkToken(identifier).getPassword();
  }

  protected @Tainted String getTrackingIdIfEnabled(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent ident) {
    if (storeTokenTrackingId) {
      return ident.getTrackingId();
    }
    return null;
  }

  public synchronized @Tainted String getTokenTrackingId(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent identifier) {
    @Tainted
    DelegationTokenInformation info = currentTokens.get(identifier);
    if (info == null) {
      return null;
    }
    return info.getTrackingId();
  }

  /**
   * Verifies that the given identifier and password are valid and match.
   * @param identifier Token identifier.
   * @param password Password in the token.
   * @throws InvalidToken
   */
  public synchronized void verifyToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted TokenIdent identifier, @Tainted byte @Tainted [] password)
      throws InvalidToken {
    @Tainted
    byte @Tainted [] storedPassword = retrievePassword(identifier);
    if (!Arrays.equals(password, storedPassword)) {
      throw new @Tainted InvalidToken("token (" + identifier
          + ") is invalid, password doesn't match");
    }
  }
  
  /**
   * Renew a delegation token.
   * @param token the token to renew
   * @param renewer the full principal name of the user doing the renewal
   * @return the new expiration time
   * @throws InvalidToken if the token is invalid
   * @throws AccessControlException if the user can't renew token
   */
  public synchronized @Tainted long renewToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted Token<@Tainted TokenIdent> token,
                         @Tainted
                         String renewer) throws InvalidToken, IOException {
    @Tainted
    long now = Time.now();
    @Tainted
    ByteArrayInputStream buf = new @Tainted ByteArrayInputStream(token.getIdentifier());
    @Tainted
    DataInputStream in = new @Tainted DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token renewal requested for identifier: "+id);
    
    if (id.getMaxDate() < now) {
      throw new @Tainted InvalidToken("User " + renewer + 
                             " tried to renew an expired token");
    }
    if ((id.getRenewer() == null) || (id.getRenewer().toString().isEmpty())) {
      throw new @Tainted AccessControlException("User " + renewer + 
                                       " tried to renew a token without " +
                                       "a renewer");
    }
    if (!id.getRenewer().toString().equals(renewer)) {
      throw new @Tainted AccessControlException("Client " + renewer + 
                                       " tries to renew a token with " +
                                       "renewer specified as " + 
                                       id.getRenewer());
    }
    @Tainted
    DelegationKey key = allKeys.get(id.getMasterKeyId());
    if (key == null) {
      throw new @Tainted InvalidToken("Unable to find master key for keyId="
          + id.getMasterKeyId()
          + " from cache. Failed to renew an unexpired token"
          + " with sequenceNumber=" + id.getSequenceNumber());
    }
    @Tainted
    byte @Tainted [] password = createPassword(token.getIdentifier(), key.getKey());
    if (!Arrays.equals(password, token.getPassword())) {
      throw new @Tainted AccessControlException("Client " + renewer
          + " is trying to renew a token with " + "wrong password");
    }
    @Tainted
    long renewTime = Math.min(id.getMaxDate(), now + tokenRenewInterval);
    @Tainted
    String trackingId = getTrackingIdIfEnabled(id);
    @Tainted
    DelegationTokenInformation info = new @Tainted DelegationTokenInformation(renewTime,
        password, trackingId);

    if (currentTokens.get(id) == null) {
      throw new @Tainted InvalidToken("Renewal request for unknown token");
    }
    currentTokens.put(id, info);
    updateStoredToken(id, renewTime);
    return renewTime;
  }
  
  /**
   * Cancel a token by removing it from cache.
   * @return Identifier of the canceled token
   * @throws InvalidToken for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  public synchronized @Tainted TokenIdent cancelToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this, @Tainted Token<@Tainted TokenIdent> token,
      @Tainted
      String canceller) throws IOException {
    @Tainted
    ByteArrayInputStream buf = new @Tainted ByteArrayInputStream(token.getIdentifier());
    @Tainted
    DataInputStream in = new @Tainted DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token cancelation requested for identifier: "+id);
    
    if (id.getUser() == null) {
      throw new @Tainted InvalidToken("Token with no owner");
    }
    @Tainted
    String owner = id.getUser().getUserName();
    @Tainted
    Text renewer = id.getRenewer();
    @Tainted
    HadoopKerberosName cancelerKrbName = new @Tainted HadoopKerberosName(canceller);
    @Tainted
    String cancelerShortName = cancelerKrbName.getShortName();
    if (!canceller.equals(owner)
        && (renewer == null || renewer.toString().isEmpty() || !cancelerShortName
            .equals(renewer.toString()))) {
      throw new @Tainted AccessControlException(canceller
          + " is not authorized to cancel the token");
    }
    @Tainted
    DelegationTokenInformation info = null;
    info = currentTokens.remove(id);
    if (info == null) {
      throw new @Tainted InvalidToken("Token not found");
    }
    removeStoredToken(id);
    return id;
  }
  
  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create the secret key from
   * @return the secret key
   */
  public static @Tainted SecretKey createSecretKey(@Tainted byte @Tainted [] key) {
    return SecretManager.createSecretKey(key);
  }

  /** Class to encapsulate a token's renew date and password. */
  @InterfaceStability.Evolving
  public static class DelegationTokenInformation {
    @Tainted
    long renewDate;
    @Tainted
    byte @Tainted [] password;
    @Tainted
    String trackingId;

    public @Tainted DelegationTokenInformation(@Tainted long renewDate, @Tainted byte @Tainted [] password) {
      this(renewDate, password, null);
    }

    public @Tainted DelegationTokenInformation(@Tainted long renewDate, @Tainted byte @Tainted [] password,
        @Tainted
        String trackingId) {
      this.renewDate = renewDate;
      this.password = password;
      this.trackingId = trackingId;
    }
    /** returns renew date */
    public @Tainted long getRenewDate(AbstractDelegationTokenSecretManager.@Tainted DelegationTokenInformation this) {
      return renewDate;
    }
    /** returns password */
    @Tainted
    byte @Tainted [] getPassword(AbstractDelegationTokenSecretManager.@Tainted DelegationTokenInformation this) {
      return password;
    }
    /** returns tracking id */
    public @Tainted String getTrackingId(AbstractDelegationTokenSecretManager.@Tainted DelegationTokenInformation this) {
      return trackingId;
    }
  }
  
  /** Remove expired delegation tokens from cache */
  private void removeExpiredToken(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    @Tainted
    long now = Time.now();
    @Tainted
    Set<TokenIdent> expiredTokens = new @Tainted HashSet<TokenIdent>();
    synchronized (this) {
      @Tainted
      Iterator<Map.@Tainted Entry<TokenIdent, @Tainted DelegationTokenInformation>> i =
          currentTokens.entrySet().iterator();
      while (i.hasNext()) {
        Map.@Tainted Entry<TokenIdent, @Tainted DelegationTokenInformation> entry = i.next();
        @Tainted
        long renewDate = entry.getValue().getRenewDate();
        if (renewDate < now) {
          expiredTokens.add(entry.getKey());
          i.remove();
        }
      }
    }
    // don't hold lock on 'this' to avoid edit log updates blocking token ops
    for (TokenIdent ident : expiredTokens) {
      logExpireToken(ident);
      removeStoredToken(ident);
    }
  }

  public void stopThreads(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    if (LOG.isDebugEnabled())
      LOG.debug("Stopping expired delegation token remover thread");
    running = false;
    
    if (tokenRemoverThread != null) {
      synchronized (noInterruptsLock) {
        tokenRemoverThread.interrupt();
      }
      try {
        tokenRemoverThread.join();
      } catch (@Tainted InterruptedException e) {
        throw new @Tainted RuntimeException(
            "Unable to join on token removal thread", e);
      }
    }
  }
  
  /**
   * is secretMgr running
   * @return true if secret mgr is running
   */
  public synchronized @Tainted boolean isRunning(@Tainted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    return running;
  }
  
  private class ExpiredTokenRemover extends @Tainted Thread {
    private @Tainted long lastMasterKeyUpdate;
    private @Tainted long lastTokenCacheCleanup;

    @Override
    public void run(@Tainted AbstractDelegationTokenSecretManager<TokenIdent>.ExpiredTokenRemover this) {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      try {
        while (running) {
          @Tainted
          long now = Time.now();
          if (lastMasterKeyUpdate + keyUpdateInterval < now) {
            try {
              rollMasterKey();
              lastMasterKeyUpdate = now;
            } catch (@Tainted IOException e) {
              LOG.error("Master key updating failed: ", e);
            }
          }
          if (lastTokenCacheCleanup + tokenRemoverScanInterval < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }
          try {
            Thread.sleep(Math.min(5000, keyUpdateInterval)); // 5 seconds
          } catch (@Tainted InterruptedException ie) {
            LOG
            .error("InterruptedExcpetion recieved for ExpiredTokenRemover thread "
                + ie);
          }
        }
      } catch (@Tainted Throwable t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception. "
            + t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}
