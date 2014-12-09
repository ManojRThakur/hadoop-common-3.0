/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 
 * (http://www.one-lab.org)
 * 
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following 
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior 
 *    written permission.
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

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
package org.apache.hadoop.util.bloom;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.hash.Hash;

/**
 * Implements a hash object that returns a certain number of hashed values.
 * 
 * @see Key The general behavior of a key being stored in a filter
 * @see Filter The general behavior of a filter
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class HashFunction {
  /** The number of hashed values. */
  private @Tainted int nbHash;

  /** The maximum highest returned value. */
  private @Tainted int maxValue;

  /** Hashing algorithm to use. */
  private @Tainted Hash hashFunction;
  
  /**
   * Constructor.
   * <p>
   * Builds a hash function that must obey to a given maximum number of returned values and a highest value.
   * @param maxValue The maximum highest returned value.
   * @param nbHash The number of resulting hashed values.
   * @param hashType type of the hashing function (see {@link Hash}).
   */
  public @Tainted HashFunction(@Tainted int maxValue, @Tainted int nbHash, @Tainted int hashType) {
    if (maxValue <= 0) {
      throw new @Tainted IllegalArgumentException("maxValue must be > 0");
    }
    
    if (nbHash <= 0) {
      throw new @Tainted IllegalArgumentException("nbHash must be > 0");
    }

    this.maxValue = maxValue;
    this.nbHash = nbHash;
    this.hashFunction = Hash.getInstance(hashType);
    if (this.hashFunction == null)
      throw new @Tainted IllegalArgumentException("hashType must be known");
  }

  /** Clears <i>this</i> hash function. A NOOP */
  public void clear(@Tainted HashFunction this) {
  }

  /**
   * Hashes a specified key into several integers.
   * @param k The specified key.
   * @return The array of hashed values.
   */
  public @Tainted int @Tainted [] hash(@Tainted HashFunction this, @Tainted Key k){
      @Tainted
      byte @Tainted [] b = k.getBytes();
      if (b == null) {
        throw new @Tainted NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new @Tainted IllegalArgumentException("key length must be > 0");
      }
      @Tainted
      int @Tainted [] result = new @Tainted int @Tainted [nbHash];
      for (@Tainted int i = 0, initval = 0; i < nbHash; i++) {
	  initval = hashFunction.hash(b, initval);
	  result[i] = Math.abs(initval % maxValue);
      }
      return result;
  }
}