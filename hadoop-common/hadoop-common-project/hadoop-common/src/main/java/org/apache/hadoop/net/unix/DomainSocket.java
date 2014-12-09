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
package org.apache.hadoop.net.unix;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.Closeable;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.annotations.VisibleForTesting;

/**
 * The implementation of UNIX domain sockets in Java.
 * 
 * See {@link DomainSocket} for more information about UNIX domain sockets.
 */
@InterfaceAudience.LimitedPrivate("HDFS")
public class DomainSocket implements @Tainted Closeable {
  static {
    if (SystemUtils.IS_OS_WINDOWS) {
      loadingFailureReason = "UNIX Domain sockets are not available on Windows.";
    } else if (!NativeCodeLoader.isNativeCodeLoaded()) {
      loadingFailureReason = "libhadoop cannot be loaded.";
    } else {
      @Tainted
      String problem;
      try {
        anchorNative();
        problem = null;
      } catch (@Tainted Throwable t) {
        problem = "DomainSocket#anchorNative got error: " + t.getMessage();
      }
      loadingFailureReason = problem;
    }
  }

  static @Tainted Log LOG = LogFactory.getLog(DomainSocket.class);

  /**
   * True only if we should validate the paths used in {@link DomainSocket#bind()}
   */
  private static @Tainted boolean validateBindPaths = true;

  /**
   * The reason why DomainSocket is not available, or null if it is available.
   */
  private final static @Tainted String loadingFailureReason;

  /**
   * Initialize the native library code.
   */
  private static native void anchorNative();

  /**
   * This function is designed to validate that the path chosen for a UNIX
   * domain socket is secure.  A socket path is secure if it doesn't allow
   * unprivileged users to perform a man-in-the-middle attack against it.
   * For example, one way to perform a man-in-the-middle attack would be for
   * a malicious user to move the server socket out of the way and create his
   * own socket in the same place.  Not good.
   * 
   * Note that we only check the path once.  It's possible that the
   * permissions on the path could change, perhaps to something more relaxed,
   * immediately after the path passes our validation test-- hence creating a
   * security hole.  However, the purpose of this check is to spot common
   * misconfigurations.  System administrators do not commonly change
   * permissions on these paths while the server is running.
   *
   * @param path             the path to validate
   * @param skipComponents   the number of starting path components to skip 
   *                         validation for (used only for testing)
   */
  @VisibleForTesting
  native static void validateSocketPathSecurity0(@Tainted String path,
      @Tainted
      int skipComponents) throws IOException;

  /**
   * Return true only if UNIX domain sockets are available.
   */
  public static @Tainted String getLoadingFailureReason() {
    return loadingFailureReason;
  }

  /**
   * Disable validation of the server bind paths.
   */
  @VisibleForTesting
  public static void disableBindPathValidation() {
    validateBindPaths = false;
  }

  /**
   * Given a path and a port, compute the effective path by replacing
   * occurrences of _PORT with the port.  This is mainly to make it 
   * possible to run multiple DataNodes locally for testing purposes.
   *
   * @param path            The source path
   * @param port            Port number to use
   *
   * @return                The effective path
   */
  public static @Tainted String getEffectivePath(@Tainted String path, @Tainted int port) {
    return path.replace("_PORT", String.valueOf(port));
  }

  /**
   * Tracks the reference count of the file descriptor, and also whether it is
   * open or closed.
   */
  private static class Status {
    /**
     * Bit mask representing a closed domain socket. 
     */
    private static final @Tainted int STATUS_CLOSED_MASK = 1 << 30;
    
    /**
     * Status bits
     * 
     * Bit 30: 0 = DomainSocket open, 1 = DomainSocket closed
     * Bits 29 to 0: the reference count.
     */
    private final @Tainted AtomicInteger bits = new @Tainted AtomicInteger(0);

    @Tainted
    Status() { }

    /**
     * Increment the reference count of the underlying file descriptor.
     *
     * @throws ClosedChannelException      If the file descriptor is closed.
     */
    void reference(DomainSocket.@Tainted Status this) throws ClosedChannelException {
      @Tainted
      int curBits = bits.incrementAndGet();
      if ((curBits & STATUS_CLOSED_MASK) != 0) {
        bits.decrementAndGet();
        throw new @Tainted ClosedChannelException();
      }
    }

    /**
     * Decrement the reference count of the underlying file descriptor.
     *
     * @param checkClosed        Whether to throw an exception if the file
     *                           descriptor is closed.
     *
     * @throws AsynchronousCloseException  If the file descriptor is closed and
     *                                     checkClosed is set.
     */
    void unreference(DomainSocket.@Tainted Status this, @Tainted boolean checkClosed) throws AsynchronousCloseException {
      @Tainted
      int newCount = bits.decrementAndGet();
      assert (newCount & ~STATUS_CLOSED_MASK) >= 0;
      if (checkClosed && ((newCount & STATUS_CLOSED_MASK) != 0)) {
        throw new @Tainted AsynchronousCloseException();
      }
    }

    /**
     * Return true if the file descriptor is currently open.
     * 
     * @return                 True if the file descriptor is currently open.
     */
    @Tainted
    boolean isOpen(DomainSocket.@Tainted Status this) {
      return ((bits.get() & STATUS_CLOSED_MASK) == 0);
    }

    /**
     * Mark the file descriptor as closed.
     *
     * Once the file descriptor is closed, it cannot be reopened.
     *
     * @return                         The current reference count.
     * @throws ClosedChannelException  If someone else closes the file 
     *                                 descriptor before we do.
     */
    @Tainted
    int setClosed(DomainSocket.@Tainted Status this) throws ClosedChannelException {
      while (true) {
        @Tainted
        int curBits = bits.get();
        if ((curBits & STATUS_CLOSED_MASK) != 0) {
          throw new @Tainted ClosedChannelException();
        }
        if (bits.compareAndSet(curBits, curBits | STATUS_CLOSED_MASK)) {
          return curBits & (~STATUS_CLOSED_MASK);
        }
      }
    }

    /**
     * Get the current reference count.
     *
     * @return                 The current reference count.
     */
    @Tainted
    int getReferenceCount(DomainSocket.@Tainted Status this) {
      return bits.get() & (~STATUS_CLOSED_MASK);
    }
  }

  /**
   * The socket status.
   */
  private final @Tainted Status status;

  /**
   * The file descriptor associated with this UNIX domain socket.
   */
  private final @Tainted int fd;

  /**
   * The path associated with this UNIX domain socket.
   */
  private final @Tainted String path;

  /**
   * The InputStream associated with this socket.
   */
  private final @Tainted DomainInputStream inputStream = new @Tainted DomainInputStream();

  /**
   * The OutputStream associated with this socket.
   */
  private final @Tainted DomainOutputStream outputStream = new @Tainted DomainOutputStream();

  /**
   * The Channel associated with this socket.
   */
  private final @Tainted DomainChannel channel = new @Tainted DomainChannel();

  private @Tainted DomainSocket(@Tainted String path, @Tainted int fd) {
    this.status = new @Tainted Status();
    this.fd = fd;
    this.path = path;
  }

  private static native @Tainted int bind0(@Tainted String path) throws IOException;

  /**
   * Create a new DomainSocket listening on the given path.
   *
   * @param path         The path to bind and listen on.
   * @return             The new DomainSocket.
   */
  public static @Tainted DomainSocket bindAndListen(@Tainted String path) throws IOException {
    if (loadingFailureReason != null) {
      throw new @Tainted UnsupportedOperationException(loadingFailureReason);
    }
    if (validateBindPaths) {
      validateSocketPathSecurity0(path, 0);
    }
    @Tainted
    int fd = bind0(path);
    return new @Tainted DomainSocket(path, fd);
  }

  private static native @Tainted int accept0(@Tainted int fd) throws IOException;

  /**
   * Accept a new UNIX domain connection.
   *
   * This method can only be used on sockets that were bound with bind().
   *
   * @return                              The new connection.
   * @throws IOException                  If there was an I/O error
   *                                      performing the accept-- such as the
   *                                      socket being closed from under us.
   * @throws SocketTimeoutException       If the accept timed out.
   */
  public @Tainted DomainSocket accept(@Tainted DomainSocket this) throws IOException {
    status.reference();
    @Tainted
    boolean exc = true;
    try {
      @Tainted
      DomainSocket ret = new @Tainted DomainSocket(path, accept0(fd));
      exc = false;
      return ret;
    } finally {
      status.unreference(exc);
    }
  }

  private static native @Tainted int connect0(@Tainted String path);

  /**
   * Create a new DomainSocket connected to the given path.
   *
   * @param path         The path to connect to.
   * @return             The new DomainSocket.
   */
  public static @Tainted DomainSocket connect(@Tainted String path) throws IOException {
    if (loadingFailureReason != null) {
      throw new @Tainted UnsupportedOperationException(loadingFailureReason);
    }
    @Tainted
    int fd = connect0(path);
    return new @Tainted DomainSocket(path, fd);
  }

 /**
  * Return true if the file descriptor is currently open.
  *
  * @return                 True if the file descriptor is currently open.
  */
 public @Tainted boolean isOpen(@Tainted DomainSocket this) {
   return status.isOpen();
 }

  /**
   * @return                 The socket path.
   */
  public @Tainted String getPath(@Tainted DomainSocket this) {
    return path;
  }

  /**
   * @return                 The socket InputStream
   */
  public @Tainted DomainInputStream getInputStream(@Tainted DomainSocket this) {
    return inputStream;
  }

  /**
   * @return                 The socket OutputStream
   */
  public @Tainted DomainOutputStream getOutputStream(@Tainted DomainSocket this) {
    return outputStream;
  }

  /**
   * @return                 The socket Channel
   */
  public @Tainted DomainChannel getChannel(@Tainted DomainSocket this) {
    return channel;
  }

  public static final @Tainted int SEND_BUFFER_SIZE = 1;
  public static final @Tainted int RECEIVE_BUFFER_SIZE = 2;
  public static final @Tainted int SEND_TIMEOUT = 3;
  public static final @Tainted int RECEIVE_TIMEOUT = 4;

  private static native void setAttribute0(@Tainted int fd, @Tainted int type, @Tainted int val)
      throws IOException;

  public void setAttribute(@Tainted DomainSocket this, @Tainted int type, @Tainted int size) throws IOException {
    status.reference();
    @Tainted
    boolean exc = true;
    try {
      setAttribute0(fd, type, size);
      exc = false;
    } finally {
      status.unreference(exc);
    }
  }

  private native @Tainted int getAttribute0(@Tainted DomainSocket this, @Tainted int fd, @Tainted int type) throws IOException;

  public @Tainted int getAttribute(@Tainted DomainSocket this, @Tainted int type) throws IOException {
    status.reference();
    @Tainted
    int attribute;
    @Tainted
    boolean exc = true;
    try {
      attribute = getAttribute0(fd, type);
      exc = false;
      return attribute;
    } finally {
      status.unreference(exc);
    }
  }

  private static native void close0(@Tainted int fd) throws IOException;

  private static native void closeFileDescriptor0(@Tainted FileDescriptor fd)
      throws IOException;

  private static native void shutdown0(@Tainted int fd) throws IOException;

  /**
   * Close the Socket.
   */
  @Override
  public void close(@Tainted DomainSocket this) throws IOException {
    // Set the closed bit on this DomainSocket
    @Tainted
    int refCount;
    try {
      refCount = status.setClosed();
    } catch (@Tainted ClosedChannelException e) {
      // Someone else already closed the DomainSocket.
      return;
    }
    // Wait for all references to go away
    @Tainted
    boolean didShutdown = false;
    @Tainted
    boolean interrupted = false;
    while (refCount > 0) {
      if (!didShutdown) {
        try {
          // Calling shutdown on the socket will interrupt blocking system
          // calls like accept, write, and read that are going on in a
          // different thread.
          shutdown0(fd);
        } catch (@Tainted IOException e) {
          LOG.error("shutdown error: ", e);
        }
        didShutdown = true;
      }
      try {
        Thread.sleep(10);
      } catch (@Tainted InterruptedException e) {
        interrupted = true;
      }
      refCount = status.getReferenceCount();
    }

    // At this point, nobody has a reference to the file descriptor, 
    // and nobody will be able to get one in the future either.
    // We now call close(2) on the file descriptor.
    // After this point, the file descriptor number will be reused by 
    // something else.  Although this DomainSocket object continues to hold 
    // the old file descriptor number (it's a final field), we never use it 
    // again because this DomainSocket is closed.
    close0(fd);
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private native static void sendFileDescriptors0(@Tainted int fd,
      @Tainted
      FileDescriptor descriptors @Tainted [],
      @Tainted
      byte jbuf @Tainted [], @Tainted int offset, @Tainted int length) throws IOException;

  /**
   * Send some FileDescriptor objects to the process on the other side of this
   * socket.
   * 
   * @param descriptors       The file descriptors to send.
   * @param jbuf              Some bytes to send.  You must send at least
   *                          one byte.
   * @param offset            The offset in the jbuf array to start at.
   * @param length            Length of the jbuf array to use.
   */
  public void sendFileDescriptors(@Tainted DomainSocket this, @Tainted FileDescriptor descriptors @Tainted [],
      @Tainted
      byte jbuf @Tainted [], @Tainted int offset, @Tainted int length) throws IOException {
    status.reference();
    @Tainted
    boolean exc = true;
    try {
      sendFileDescriptors0(fd, descriptors, jbuf, offset, length);
      exc = false;
    } finally {
      status.unreference(exc);
    }
  }

  private static native @Tainted int receiveFileDescriptors0(@Tainted int fd,
      @Tainted
      FileDescriptor @Tainted [] descriptors,
      @Tainted
      byte jbuf @Tainted [], @Tainted int offset, @Tainted int length) throws IOException;

  /**
   * Receive some FileDescriptor objects from the process on the other side of
   * this socket.
   *
   * @param descriptors       (output parameter) Array of FileDescriptors.
   *                          We will fill as many slots as possible with file
   *                          descriptors passed from the remote process.  The
   *                          other slots will contain NULL.
   * @param jbuf              (output parameter) Buffer to read into.
   *                          The UNIX domain sockets API requires you to read
   *                          at least one byte from the remote process, even
   *                          if all you care about is the file descriptors
   *                          you will receive.
   * @param offset            Offset into the byte buffer to load data
   * @param length            Length of the byte buffer to use for data
   *
   * @return                  The number of bytes read.  This will be -1 if we
   *                          reached EOF (similar to SocketInputStream);
   *                          otherwise, it will be positive.
   * @throws                  IOException if there was an I/O error.
   */
  public @Tainted int receiveFileDescriptors(@Tainted DomainSocket this, @Tainted FileDescriptor @Tainted [] descriptors,
      @Tainted
      byte jbuf @Tainted [], @Tainted int offset, @Tainted int length) throws IOException {
    status.reference();
    @Tainted
    boolean exc = true;
    try {
      @Tainted
      int nBytes = receiveFileDescriptors0(fd, descriptors, jbuf, offset, length);
      exc = false;
      return nBytes;
    } finally {
      status.unreference(exc);
    }
  }

  /**
   * Receive some FileDescriptor objects from the process on the other side of
   * this socket, and wrap them in FileInputStream objects.
   *
   * See {@link DomainSocket#recvFileInputStreams(ByteBuffer)}
   */
  public @Tainted int recvFileInputStreams(@Tainted DomainSocket this, @Tainted FileInputStream @Tainted [] streams, @Tainted byte buf @Tainted [],
        @Tainted
        int offset, @Tainted int length) throws IOException {
    @Tainted
    FileDescriptor descriptors @Tainted [] = new @Tainted FileDescriptor @Tainted [streams.length];
    @Tainted
    boolean success = false;
    for (@Tainted int i = 0; i < streams.length; i++) {
      streams[i] = null;
    }
    status.reference();
    try {
      @Tainted
      int ret = receiveFileDescriptors0(fd, descriptors, buf, offset, length);
      for (@Tainted int i = 0, j = 0; i < descriptors.length; i++) {
        if (descriptors[i] != null) {
          streams[j++] = new @Tainted FileInputStream(descriptors[i]);
          descriptors[i] = null;
        }
      }
      success = true;
      return ret;
    } finally {
      if (!success) {
        for (@Tainted int i = 0; i < descriptors.length; i++) {
          if (descriptors[i] != null) {
            try {
              closeFileDescriptor0(descriptors[i]);
            } catch (@Tainted Throwable t) {
              LOG.warn(t);
            }
          } else if (streams[i] != null) {
            try {
              streams[i].close();
            } catch (@Tainted Throwable t) {
              LOG.warn(t);
            } finally {
              streams[i] = null; }
          }
        }
      }
      status.unreference(!success);
    }
  }

  private native static @Tainted int readArray0(@Tainted int fd, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len)
      throws IOException;
  
  private native static @Tainted int available0(@Tainted int fd) throws IOException;

  private static native void write0(@Tainted int fd, @Tainted int b) throws IOException;

  private static native void writeArray0(@Tainted int fd, @Tainted byte b @Tainted [], @Tainted int offset, @Tainted int length)
      throws IOException;

  private native static @Tainted int readByteBufferDirect0(@Tainted int fd, @Tainted ByteBuffer dst,
      @Tainted
      int position, @Tainted int remaining) throws IOException;

  /**
   * Input stream for UNIX domain sockets.
   */
  @InterfaceAudience.LimitedPrivate("HDFS")
  public class DomainInputStream extends @Tainted InputStream {
    @Override
    public @Tainted int read(@Tainted DomainSocket.DomainInputStream this) throws IOException {
      status.reference();
      @Tainted
      boolean exc = true;
      try {
        @Tainted
        byte b @Tainted [] = new @Tainted byte @Tainted [1];
        @Tainted
        int ret = DomainSocket.readArray0(DomainSocket.this.fd, b, 0, 1);
        exc = false;
        return (ret >= 0) ? b[0] : -1;
      } finally {
        status.unreference(exc);
      }
    }
    
    @Override
    public @Tainted int read(@Tainted DomainSocket.DomainInputStream this, @Tainted byte b @Tainted [], @Tainted int off, @Tainted int len) throws IOException {
      status.reference();
      @Tainted
      boolean exc = true;
      try {
        @Tainted
        int nRead = DomainSocket.readArray0(DomainSocket.this.fd, b, off, len);
        exc = false;
        return nRead;
      } finally {
        status.unreference(exc);
      }
    }

    @Override
    public @Tainted int available(@Tainted DomainSocket.DomainInputStream this) throws IOException {
      status.reference();
      @Tainted
      boolean exc = true;
      try {
        @Tainted
        int nAvailable = DomainSocket.available0(DomainSocket.this.fd);
        exc = false;
        return nAvailable;
      } finally {
        status.unreference(exc);
      }
    }

    @Override
    public void close(@Tainted DomainSocket.DomainInputStream this) throws IOException {
      DomainSocket.this.close();
    }
  }

  /**
   * Output stream for UNIX domain sockets.
   */
  @InterfaceAudience.LimitedPrivate("HDFS")
  public class DomainOutputStream extends @Tainted OutputStream {
    @Override
    public void close(@Tainted DomainSocket.DomainOutputStream this) throws IOException {
      DomainSocket.this.close();
    }

    @Override
    public void write(@Tainted DomainSocket.DomainOutputStream this, @Tainted int val) throws IOException {
      status.reference();
      @Tainted
      boolean exc = true;
      try {
        @Tainted
        byte b @Tainted [] = new @Tainted byte @Tainted [1];
        b[0] = (@Tainted byte)val;
        DomainSocket.writeArray0(DomainSocket.this.fd, b, 0, 1);
        exc = false;
      } finally {
        status.unreference(exc);
      }
    }

    @Override
    public void write(@Tainted DomainSocket.DomainOutputStream this, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len) throws IOException {
      status.reference();
        @Tainted
        boolean exc = true;
      try {
        DomainSocket.writeArray0(DomainSocket.this.fd, b, off, len);
        exc = false;
      } finally {
        status.unreference(exc);
      }
    }
  }

  @InterfaceAudience.LimitedPrivate("HDFS")
  public class DomainChannel implements @Tainted ReadableByteChannel {
    @Override
    public @Tainted boolean isOpen(@Tainted DomainSocket.DomainChannel this) {
      return DomainSocket.this.isOpen();
    }

    @Override
    public void close(@Tainted DomainSocket.DomainChannel this) throws IOException {
      DomainSocket.this.close();
    }

    @Override
    public @Tainted int read(@Tainted DomainSocket.DomainChannel this, @Tainted ByteBuffer dst) throws IOException {
      status.reference();
      @Tainted
      boolean exc = true;
      try {
        @Tainted
        int nread = 0;
        if (dst.isDirect()) {
          nread = DomainSocket.readByteBufferDirect0(DomainSocket.this.fd,
              dst, dst.position(), dst.remaining());
        } else if (dst.hasArray()) {
          nread = DomainSocket.readArray0(DomainSocket.this.fd,
              dst.array(), dst.position() + dst.arrayOffset(),
              dst.remaining());
        } else {
          throw new @Tainted AssertionError("we don't support " +
              "using ByteBuffers that aren't either direct or backed by " +
              "arrays");
        }
        if (nread > 0) {
          dst.position(dst.position() + nread);
        }
        exc = false;
        return nread;
      } finally {
        status.unreference(exc);
      }
    }
  }

  @Override
  public @Tainted String toString(@Tainted DomainSocket this) {
    return String.format("DomainSocket(fd=%d,path=%s)", fd, path);
  }
}
