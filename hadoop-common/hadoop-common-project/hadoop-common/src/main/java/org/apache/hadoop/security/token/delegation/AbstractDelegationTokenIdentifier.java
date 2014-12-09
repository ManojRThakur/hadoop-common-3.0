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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public abstract class AbstractDelegationTokenIdentifier 
extends @Tainted TokenIdentifier {
  private static final @Tainted byte VERSION = 0;

  private @Tainted Text owner;
  private @Tainted Text renewer;
  private @Tainted Text realUser;
  private @Tainted long issueDate;
  private @Tainted long maxDate;
  private @Tainted int sequenceNumber;
  private @Tainted int masterKeyId = 0;
  
  public @Tainted AbstractDelegationTokenIdentifier() {
    this(new @Tainted Text(), new @Tainted Text(), new @Tainted Text());
  }
  
  public @Tainted AbstractDelegationTokenIdentifier(@Tainted Text owner, @Tainted Text renewer, @Tainted Text realUser) {
    if (owner == null) {
      this.owner = new @Tainted Text();
    } else {
      this.owner = owner;
    }
    if (renewer == null) {
      this.renewer = new @Tainted Text();
    } else {
      @Tainted
      HadoopKerberosName renewerKrbName = new @Tainted HadoopKerberosName(renewer.toString());
      try {
        this.renewer = new @Tainted Text(renewerKrbName.getShortName());
      } catch (@Tainted IOException e) {
        throw new @Tainted RuntimeException(e);
      }
    }
    if (realUser == null) {
      this.realUser = new @Tainted Text();
    } else {
      this.realUser = realUser;
    }
    issueDate = 0;
    maxDate = 0;
  }

  @Override
  public abstract @Tainted Text getKind(@Tainted AbstractDelegationTokenIdentifier this);
  
  /**
   * Get the username encoded in the token identifier
   * 
   * @return the username or owner
   */
  @Override
  public @Tainted UserGroupInformation getUser(@Tainted AbstractDelegationTokenIdentifier this) {
    if ( (owner == null) || (owner.toString().isEmpty())) {
      return null;
    }
    final @Tainted UserGroupInformation realUgi;
    final @Tainted UserGroupInformation ugi;
    if ((realUser == null) || (realUser.toString().isEmpty())
        || realUser.equals(owner)) {
      ugi = realUgi = UserGroupInformation.createRemoteUser(owner.toString());
    } else {
      realUgi = UserGroupInformation.createRemoteUser(realUser.toString());
      ugi = UserGroupInformation.createProxyUser(owner.toString(), realUgi);
    }
    realUgi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    return ugi;
  }

  public @Tainted Text getOwner(@Tainted AbstractDelegationTokenIdentifier this) {
    return owner;
  }

  public @Tainted Text getRenewer(@Tainted AbstractDelegationTokenIdentifier this) {
    return renewer;
  }
  
  public @Tainted Text getRealUser(@Tainted AbstractDelegationTokenIdentifier this) {
    return realUser;
  }
  
  public void setIssueDate(@Tainted AbstractDelegationTokenIdentifier this, @Tainted long issueDate) {
    this.issueDate = issueDate;
  }
  
  public @Tainted long getIssueDate(@Tainted AbstractDelegationTokenIdentifier this) {
    return issueDate;
  }
  
  public void setMaxDate(@Tainted AbstractDelegationTokenIdentifier this, @Tainted long maxDate) {
    this.maxDate = maxDate;
  }
  
  public @Tainted long getMaxDate(@Tainted AbstractDelegationTokenIdentifier this) {
    return maxDate;
  }

  public void setSequenceNumber(@Tainted AbstractDelegationTokenIdentifier this, @Tainted int seqNum) {
    this.sequenceNumber = seqNum;
  }
  
  public @Tainted int getSequenceNumber(@Tainted AbstractDelegationTokenIdentifier this) {
    return sequenceNumber;
  }

  public void setMasterKeyId(@Tainted AbstractDelegationTokenIdentifier this, @Tainted int newId) {
    masterKeyId = newId;
  }

  public @Tainted int getMasterKeyId(@Tainted AbstractDelegationTokenIdentifier this) {
    return masterKeyId;
  }

  static @Tainted boolean isEqual(@Tainted Object a, @Tainted Object b) {
    return a == null ? b == null : a.equals(b);
  }
  
  @Override
  public @Tainted boolean equals(@Tainted AbstractDelegationTokenIdentifier this, @Tainted Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof @Tainted AbstractDelegationTokenIdentifier) {
      @Tainted
      AbstractDelegationTokenIdentifier that = (@Tainted AbstractDelegationTokenIdentifier) obj;
      return this.sequenceNumber == that.sequenceNumber 
          && this.issueDate == that.issueDate 
          && this.maxDate == that.maxDate
          && this.masterKeyId == that.masterKeyId
          && isEqual(this.owner, that.owner) 
          && isEqual(this.renewer, that.renewer)
          && isEqual(this.realUser, that.realUser);
    }
    return false;
  }

  @Override
  public @Tainted int hashCode(@Tainted AbstractDelegationTokenIdentifier this) {
    return this.sequenceNumber;
  }
  
  @Override
  public void readFields(@Tainted AbstractDelegationTokenIdentifier this, @Tainted DataInput in) throws IOException {
    @Tainted
    byte version = in.readByte();
    if (version != VERSION) {
	throw new @Tainted IOException("Unknown version of delegation token " + 
                              version);
    }
    owner.readFields(in, Text.DEFAULT_MAX_LEN);
    renewer.readFields(in, Text.DEFAULT_MAX_LEN);
    realUser.readFields(in, Text.DEFAULT_MAX_LEN);
    issueDate = WritableUtils.readVLong(in);
    maxDate = WritableUtils.readVLong(in);
    sequenceNumber = WritableUtils.readVInt(in);
    masterKeyId = WritableUtils.readVInt(in);
  }

  @VisibleForTesting
  void writeImpl(@Tainted AbstractDelegationTokenIdentifier this, @Tainted DataOutput out) throws IOException {
    out.writeByte(VERSION);
    owner.write(out);
    renewer.write(out);
    realUser.write(out);
    WritableUtils.writeVLong(out, issueDate);
    WritableUtils.writeVLong(out, maxDate);
    WritableUtils.writeVInt(out, sequenceNumber);
    WritableUtils.writeVInt(out, masterKeyId);
  }
  
  @Override
  public void write(@Tainted AbstractDelegationTokenIdentifier this, @Tainted DataOutput out) throws IOException {
    if (owner.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new @Tainted IOException("owner is too long to be serialized!");
    }
    if (renewer.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new @Tainted IOException("renewer is too long to be serialized!");
    }
    if (realUser.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new @Tainted IOException("realuser is too long to be serialized!");
    }
    writeImpl(out);
  }
  
  @Override
  public @Tainted String toString(@Tainted AbstractDelegationTokenIdentifier this) {
    @Tainted
    StringBuilder buffer = new @Tainted StringBuilder();
    buffer
        .append("owner=" + owner + ", renewer=" + renewer + ", realUser="
            + realUser + ", issueDate=" + issueDate + ", maxDate=" + maxDate
            + ", sequenceNumber=" + sequenceNumber + ", masterKeyId="
            + masterKeyId);
    return buffer.toString();
  }
}
