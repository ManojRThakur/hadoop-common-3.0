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
package org.apache.hadoop.io.nativeio;
import org.checkerframework.checker.tainting.qual.Tainted;

/**
 * Enum representing POSIX errno values.
 */
public enum Errno {

@Tainted  EPERM,

@Tainted  ENOENT,

@Tainted  ESRCH,

@Tainted  EINTR,

@Tainted  EIO,

@Tainted  ENXIO,

@Tainted  E2BIG,

@Tainted  ENOEXEC,

@Tainted  EBADF,

@Tainted  ECHILD,

@Tainted  EAGAIN,

@Tainted  ENOMEM,

@Tainted  EACCES,

@Tainted  EFAULT,

@Tainted  ENOTBLK,

@Tainted  EBUSY,

@Tainted  EEXIST,

@Tainted  EXDEV,

@Tainted  ENODEV,

@Tainted  ENOTDIR,

@Tainted  EISDIR,

@Tainted  EINVAL,

@Tainted  ENFILE,

@Tainted  EMFILE,

@Tainted  ENOTTY,

@Tainted  ETXTBSY,

@Tainted  EFBIG,

@Tainted  ENOSPC,

@Tainted  ESPIPE,

@Tainted  EROFS,

@Tainted  EMLINK,

@Tainted  EPIPE,

@Tainted  EDOM,

@Tainted  ERANGE,

@Tainted  ELOOP,

@Tainted  ENAMETOOLONG,

@Tainted  ENOTEMPTY,


@Tainted  UNKNOWN;
}
