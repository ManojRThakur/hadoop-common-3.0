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

package org.apache.hadoop.net;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This implements an output stream that can have a timeout while writing.
 * This sets non-blocking flag on the socket channel.
 * So after creating this object , read() on 
 * {@link Socket#getInputStream()} and write() on 
 * {@link Socket#getOutputStream()} on the associated socket will throw 
 * llegalBlockingModeException.
 * Please use {@link SocketInputStream} for reading.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class SocketOutputStream extends @Tainted OutputStream 
                                implements @Tainted WritableByteChannel {                                
  
  private @Tainted Writer writer;
  
  private static class Writer extends @Tainted SocketIOWithTimeout {
    @Tainted
    WritableByteChannel channel;
    
    @Tainted
    Writer(@Tainted WritableByteChannel channel, @Tainted long timeout) throws IOException {
      super((@Tainted SelectableChannel)channel, timeout);
      this.channel = channel;
    }
    
    @Override
    @Tainted
    int performIO(SocketOutputStream.@Tainted Writer this, @Tainted ByteBuffer buf) throws IOException {
      return channel.write(buf);
    }
  }
  
  /**
   * Create a new ouput stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @param channel 
   *        Channel for writing, should also be a {@link SelectableChannel}.  
   *        The channel will be configured to be non-blocking.
   * @param timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public @Tainted SocketOutputStream(@Tainted WritableByteChannel channel, @Tainted long timeout) 
                                                         throws IOException {
    SocketIOWithTimeout.checkChannelValidity(channel);
    writer = new @Tainted Writer(channel, timeout);
  }
  
  /**
   * Same as SocketOutputStream(socket.getChannel(), timeout):<br><br>
   * 
   * Create a new ouput stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @see SocketOutputStream#SocketOutputStream(WritableByteChannel, long)
   *  
   * @param socket should have a channel associated with it.
   * @param timeout timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public @Tainted SocketOutputStream(@Tainted Socket socket, @Tainted long timeout) 
                                         throws IOException {
    this(socket.getChannel(), timeout);
  }
  
  @Override
  public void write(@Tainted SocketOutputStream this, @Tainted int b) throws IOException {
    /* If we need to, we can optimize this allocation.
     * probably no need to optimize or encourage single byte writes.
     */
    @Tainted
    byte @Tainted [] buf = new @Tainted byte @Tainted [1];
    buf[0] = (@Tainted byte)b;
    write(buf, 0, 1);
  }
  
  @Override
  public void write(@Tainted SocketOutputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
    @Tainted
    ByteBuffer buf = ByteBuffer.wrap(b, off, len);
    while (buf.hasRemaining()) {
      try {
        if (write(buf) < 0) {
          throw new @Tainted IOException("The stream is closed");
        }
      } catch (@Tainted IOException e) {
        /* Unlike read, write can not inform user of partial writes.
         * So will close this if there was a partial write.
         */
        if (buf.capacity() > buf.remaining()) {
          writer.close();
        }
        throw e;
      }
    }
  }

  @Override
  public synchronized void close(@Tainted SocketOutputStream this) throws IOException {
    /* close the channel since Socket.getOuputStream().close() 
     * closes the socket.
     */
    writer.channel.close();
    writer.close();
  }

  /**
   * Returns underlying channel used by this stream.
   * This is useful in certain cases like channel for 
   * {@link FileChannel#transferTo(long, long, WritableByteChannel)}
   */
  public @Tainted WritableByteChannel getChannel(@Tainted SocketOutputStream this) {
    return writer.channel; 
  }

  //WritableByteChannle interface 
  
  @Override
  public @Tainted boolean isOpen(@Tainted SocketOutputStream this) {
    return writer.isOpen();
  }

  @Override
  public @Tainted int write(@Tainted SocketOutputStream this, @Tainted ByteBuffer src) throws IOException {
    return writer.doIO(src, SelectionKey.OP_WRITE);
  }
  
  /**
   * waits for the underlying channel to be ready for writing.
   * The timeout specified for this stream applies to this wait.
   *
   * @throws SocketTimeoutException 
   *         if select on the channel times out.
   * @throws IOException
   *         if any other I/O error occurs. 
   */
  public void waitForWritable(@Tainted SocketOutputStream this) throws IOException {
    writer.waitForIO(SelectionKey.OP_WRITE);
  }
  
  /**
   * Transfers data from FileChannel using 
   * {@link FileChannel#transferTo(long, long, WritableByteChannel)}.
   * Updates <code>waitForWritableTime</code> and <code>transferToTime</code>
   * with the time spent blocked on the network and the time spent transferring
   * data from disk to network respectively.
   * 
   * Similar to readFully(), this waits till requested amount of 
   * data is transfered.
   * 
   * @param fileCh FileChannel to transfer data from.
   * @param position position within the channel where the transfer begins
   * @param count number of bytes to transfer.
   * @param waitForWritableTime nanoseconds spent waiting for the socket 
   *        to become writable
   * @param transferTime nanoseconds spent transferring data
   * 
   * @throws EOFException 
   *         If end of input file is reached before requested number of 
   *         bytes are transfered.
   *
   * @throws SocketTimeoutException 
   *         If this channel blocks transfer longer than timeout for 
   *         this stream.
   *          
   * @throws IOException Includes any exception thrown by 
   *         {@link FileChannel#transferTo(long, long, WritableByteChannel)}. 
   */
  public void transferToFully(@Tainted SocketOutputStream this, @Tainted FileChannel fileCh, @Tainted long position, @Tainted int count,
      @Tainted
      LongWritable waitForWritableTime,
      @Tainted
      LongWritable transferToTime) throws IOException {
    @Tainted
    long waitTime = 0;
    @Tainted
    long transferTime = 0;
    while (count > 0) {
      /* 
       * Ideally we should wait after transferTo returns 0. But because of
       * a bug in JRE on Linux (http://bugs.sun.com/view_bug.do?bug_id=5103988),
       * which throws an exception instead of returning 0, we wait for the
       * channel to be writable before writing to it. If you ever see 
       * IOException with message "Resource temporarily unavailable" 
       * thrown here, please let us know.
       * 
       * Once we move to JAVA SE 7, wait should be moved to correct place.
       */
      @Tainted
      long start = System.nanoTime();
      waitForWritable();
      @Tainted
      long wait = System.nanoTime();

      @Tainted
      int nTransfered = (@Tainted int) fileCh.transferTo(position, count, getChannel());
      
      if (nTransfered == 0) {
        //check if end of file is reached.
        if (position >= fileCh.size()) {
          throw new @Tainted EOFException("EOF Reached. file size is " + fileCh.size() + 
                                 " and " + count + " more bytes left to be " +
                                 "transfered.");
        }
        //otherwise assume the socket is full.
        //waitForWritable(); // see comment above.
      } else if (nTransfered < 0) {
        throw new @Tainted IOException("Unexpected return of " + nTransfered + 
                              " from transferTo()");
      } else {
        position += nTransfered;
        count -= nTransfered;
      }
      @Tainted
      long transfer = System.nanoTime();
      waitTime += wait - start;
      transferTime += transfer - wait;
    }
    
    if (waitForWritableTime != null) {
      waitForWritableTime.set(waitTime);
    }
    if (transferToTime != null) {
      transferToTime.set(transferTime);
    }
  }

  /**
   * Call
   * {@link #transferToFully(FileChannel, long, int, MutableRate, MutableRate)}
   * with null <code>waitForWritableTime</code> and <code>transferToTime</code>
   */
  public void transferToFully(@Tainted SocketOutputStream this, @Tainted FileChannel fileCh, @Tainted long position, @Tainted int count)
      throws IOException {
    transferToFully(fileCh, position, count, null, null);
  }

  public void setTimeout(@Tainted SocketOutputStream this, @Tainted int timeoutMs) {
    writer.setTimeout(timeoutMs);
  }
}
