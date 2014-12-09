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
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.ChecksumException;

/**
 * Wrapper around JNI support code to do checksum computation
 * natively.
 */
class NativeCrc32 {
  
  /**
   * Return true if the JNI-based native CRC extensions are available.
   */
  public static @Tainted boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded();
  }

  /**
   * Verify the given buffers of data and checksums, and throw an exception
   * if any checksum is invalid. The buffers given to this function should
   * have their position initially at the start of the data, and their limit
   * set at the end of the data. The position, limit, and mark are not
   * modified.
   * 
   * @param bytesPerSum the chunk size (eg 512 bytes)
   * @param checksumType the DataChecksum type constant
   * @param sums the DirectByteBuffer pointing at the beginning of the
   *             stored checksums
   * @param data the DirectByteBuffer pointing at the beginning of the
   *             data to check
   * @param basePos the position in the file where the data buffer starts 
   * @param fileName the name of the file being verified
   * @throws ChecksumException if there is an invalid checksum
   */
  public static void verifyChunkedSums(@Tainted int bytesPerSum, @Tainted int checksumType,
      @Tainted
      ByteBuffer sums, @Tainted ByteBuffer data, @Tainted String fileName, @Tainted long basePos)
      throws ChecksumException {
    nativeVerifyChunkedSums(bytesPerSum, checksumType,
        sums, sums.position(),
        data, data.position(), data.remaining(),
        fileName, basePos);
  }
  
    private static native void nativeVerifyChunkedSums(
      @Tainted
      int bytesPerSum, @Tainted int checksumType,
      @Tainted
      ByteBuffer sums, @Tainted int sumsOffset,
      @Tainted
      ByteBuffer data, @Tainted int dataOffset, @Tainted int dataLength,
      @Tainted
      String fileName, @Tainted long basePos);

  // Copy the constants over from DataChecksum so that javah will pick them up
  // and make them available in the native code header.
  public static final @Tainted int CHECKSUM_CRC32 = DataChecksum.CHECKSUM_CRC32;
  public static final @Tainted int CHECKSUM_CRC32C = DataChecksum.CHECKSUM_CRC32C;
}
