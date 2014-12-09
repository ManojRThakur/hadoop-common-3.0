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
package org.apache.hadoop.ha.protocolPB;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAStateChangeRequestInfoProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthResponseProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveResponseProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyResponseProto;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is used on the server side. Calls come across the wire for the
 * for protocol {@link HAServiceProtocolPB}.
 * This class translates the PB data types
 * to the native data types used inside the NN as specified in the generic
 * ClientProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class HAServiceProtocolServerSideTranslatorPB implements
    @Tainted
    HAServiceProtocolPB {
  private final @Tainted HAServiceProtocol server;
  private static final @Tainted MonitorHealthResponseProto MONITOR_HEALTH_RESP = 
      MonitorHealthResponseProto.newBuilder().build();
  private static final @Tainted TransitionToActiveResponseProto TRANSITION_TO_ACTIVE_RESP = 
      TransitionToActiveResponseProto.newBuilder().build();
  private static final @Tainted TransitionToStandbyResponseProto TRANSITION_TO_STANDBY_RESP = 
      TransitionToStandbyResponseProto.newBuilder().build();
  private static final @Tainted Log LOG = LogFactory.getLog(
      HAServiceProtocolServerSideTranslatorPB.class);
  
  public @Tainted HAServiceProtocolServerSideTranslatorPB(@Tainted HAServiceProtocol server) {
    this.server = server;
  }

  @Override
  public @Tainted MonitorHealthResponseProto monitorHealth(@Tainted HAServiceProtocolServerSideTranslatorPB this, @Tainted RpcController controller,
      @Tainted
      MonitorHealthRequestProto request) throws ServiceException {
    try {
      server.monitorHealth();
      return MONITOR_HEALTH_RESP;
    } catch(@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
  }
  
  private @Tainted StateChangeRequestInfo convert(@Tainted HAServiceProtocolServerSideTranslatorPB this, @Tainted HAStateChangeRequestInfoProto proto) {
    @Tainted
    RequestSource src;
    switch (proto.getReqSource()) {
    case REQUEST_BY_USER:
      src = RequestSource.REQUEST_BY_USER;
      break;
    case REQUEST_BY_USER_FORCED:
      src = RequestSource.REQUEST_BY_USER_FORCED;
      break;
    case REQUEST_BY_ZKFC:
      src = RequestSource.REQUEST_BY_ZKFC;
      break;
    default:
      LOG.warn("Unknown request source: " + proto.getReqSource());
      src = null;
    }
    
    return new @Tainted StateChangeRequestInfo(src);
  }

  @Override
  public @Tainted TransitionToActiveResponseProto transitionToActive(
      @Tainted HAServiceProtocolServerSideTranslatorPB this, @Tainted
      RpcController controller, @Tainted TransitionToActiveRequestProto request)
      throws ServiceException {
    try {
      server.transitionToActive(convert(request.getReqInfo()));
      return TRANSITION_TO_ACTIVE_RESP;
    } catch(@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
  }

  @Override
  public @Tainted TransitionToStandbyResponseProto transitionToStandby(
      @Tainted HAServiceProtocolServerSideTranslatorPB this, @Tainted
      RpcController controller, @Tainted TransitionToStandbyRequestProto request)
      throws ServiceException {
    try {
      server.transitionToStandby(convert(request.getReqInfo()));
      return TRANSITION_TO_STANDBY_RESP;
    } catch(@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
  }

  @Override
  public @Tainted GetServiceStatusResponseProto getServiceStatus(@Tainted HAServiceProtocolServerSideTranslatorPB this, @Tainted RpcController controller,
      @Tainted
      GetServiceStatusRequestProto request) throws ServiceException {
    @Tainted
    HAServiceStatus s;
    try {
      s = server.getServiceStatus();
    } catch(@Tainted IOException e) {
      throw new @Tainted ServiceException(e);
    }
    
    @Tainted
    HAServiceStateProto retState;
    switch (s.getState()) {
    case ACTIVE:
      retState = HAServiceStateProto.ACTIVE;
      break;
    case STANDBY:
      retState = HAServiceStateProto.STANDBY;
      break;
    case INITIALIZING:
    default:
      retState = HAServiceStateProto.INITIALIZING;
      break;
    }
    
    GetServiceStatusResponseProto.@Tainted Builder ret =
      GetServiceStatusResponseProto.newBuilder()
        .setState(retState)
        .setReadyToBecomeActive(s.isReadyToBecomeActive());
    if (!s.isReadyToBecomeActive()) {
      ret.setNotReadyReason(s.getNotReadyReason());
    }
    return ret.build();
  }

  @Override
  public @Tainted long getProtocolVersion(@Tainted HAServiceProtocolServerSideTranslatorPB this, @Tainted String protocol, @Tainted long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(HAServiceProtocolPB.class);
  }

  @Override
  public @Tainted ProtocolSignature getProtocolSignature(@Tainted HAServiceProtocolServerSideTranslatorPB this, @Tainted String protocol,
      @Tainted
      long clientVersion, @Tainted int clientMethodsHash) throws IOException {
    if (!protocol.equals(RPC.getProtocolName(HAServiceProtocolPB.class))) {
      throw new @Tainted IOException("Serverside implements " +
          RPC.getProtocolName(HAServiceProtocolPB.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(HAServiceProtocolPB.class),
        HAServiceProtocolPB.class);
  }
}
