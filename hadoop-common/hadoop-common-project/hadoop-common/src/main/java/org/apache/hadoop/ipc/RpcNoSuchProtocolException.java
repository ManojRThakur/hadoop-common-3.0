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
package org.apache.hadoop.ipc;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

/**
 * No such protocol (i.e. interface) for and Rpc Call
 *
 */
public class RpcNoSuchProtocolException extends @Tainted RpcServerException {
  private static final @Tainted long serialVersionUID = 1L;
  public @Tainted RpcNoSuchProtocolException(final @Tainted String message) {
    super(message);
  }
  
  /**
   * get the rpc status corresponding to this exception
   */
  public @Tainted RpcStatusProto getRpcStatusProto(@Tainted RpcNoSuchProtocolException this) {
    return RpcStatusProto.ERROR;
  }

  /**
   * get the detailed rpc status corresponding to this exception
   */
  public @Tainted RpcErrorCodeProto getRpcErrorCodeProto(@Tainted RpcNoSuchProtocolException this) {
    return RpcErrorCodeProto.ERROR_NO_SUCH_PROTOCOL;
  }
}
