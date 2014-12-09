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
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * Look through tokens to find the first delegation token that matches the
 * service and return it.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public 
class AbstractDelegationTokenSelector<@Tainted TokenIdent 
extends @Tainted AbstractDelegationTokenIdentifier> 
    implements @Tainted TokenSelector<TokenIdent> {
  private @Tainted Text kindName;
  
  protected @Tainted AbstractDelegationTokenSelector(@Tainted Text kindName) {
    this.kindName = kindName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public @Tainted Token<@Tainted TokenIdent> selectToken(@Tainted AbstractDelegationTokenSelector<TokenIdent> this, @Tainted Text service,
      @Tainted
      Collection<@Tainted Token<@Tainted ? extends @Tainted TokenIdentifier>> tokens) {
    if (service == null) {
      return null;
    }
    for (@Tainted Token<@Tainted ? extends @Tainted TokenIdentifier> token : tokens) {
      if (kindName.equals(token.getKind())
          && service.equals(token.getService())) {
        return (@Tainted Token<TokenIdent>) token;
      }
    }
    return null;
  }
}
