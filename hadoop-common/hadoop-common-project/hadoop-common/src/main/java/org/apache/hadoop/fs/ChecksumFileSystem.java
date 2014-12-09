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

package org.apache.hadoop.fs;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.*;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;

/****************************************************************
 * Abstract Checksumed FileSystem.
 * It provide a basic implementation of a Checksumed FileSystem,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ChecksumFileSystem extends @Tainted FilterFileSystem {
  private static final @Tainted byte @Tainted [] CHECKSUM_VERSION = new @Tainted byte @Tainted [] {'c', 'r', 'c', 0};
  private @Tainted int bytesPerChecksum = 512;
  private @Tainted boolean verifyChecksum = true;
  private @Tainted boolean writeChecksum = true;

  public static @Tainted double getApproxChkSumLength(@Tainted long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public @Tainted ChecksumFileSystem(@Tainted FileSystem fs) {
    super(fs);
  }

  @Override
  public void setConf(@Tainted ChecksumFileSystem this, @Tainted Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      bytesPerChecksum = conf.getInt(LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_KEY,
		                     LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_DEFAULT);
    }
  }
  
  /**
   * Set whether to verify checksum.
   */
  @Override
  public void setVerifyChecksum(@Tainted ChecksumFileSystem this, @Tainted boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  public void setWriteChecksum(@Tainted ChecksumFileSystem this, @Tainted boolean writeChecksum) {
    this.writeChecksum = writeChecksum;
  }
  
  /** get the raw file system */
  @Override
  public @Tainted FileSystem getRawFileSystem(@Tainted ChecksumFileSystem this) {
    return fs;
  }

  /** Return the name of the checksum file associated with a file.*/
  public @Tainted Path getChecksumFile(@Tainted ChecksumFileSystem this, @Tainted Path file) {
    return new @Tainted Path(file.getParent(), "." + file.getName() + ".crc");
  }

  /** Return true iff file is a checksum file name.*/
  public static @Tainted boolean isChecksumFile(@Tainted Path file) {
    @Tainted
    String name = file.getName();
    return name.startsWith(".") && name.endsWith(".crc");
  }

  /** Return the length of the checksum file given the size of the 
   * actual file.
   **/
  public @Tainted long getChecksumFileLength(@Tainted ChecksumFileSystem this, @Tainted Path file, @Tainted long fileSize) {
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /** Return the bytes Per Checksum */
  public @Tainted int getBytesPerSum(@Tainted ChecksumFileSystem this) {
    return bytesPerChecksum;
  }

  private @Tainted int getSumBufferSize(@Tainted ChecksumFileSystem this, @Tainted int bytesPerSum, @Tainted int bufferSize) {
    @Tainted
    int defaultBufferSize = getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);
    @Tainted
    int proportionalBufferSize = bufferSize / bytesPerSum;
    return Math.max(bytesPerSum,
                    Math.max(proportionalBufferSize, defaultBufferSize));
  }

  /*******************************************************
   * For open()'s FSInputStream
   * It verifies that data matches checksums.
   *******************************************************/
  private static class ChecksumFSInputChecker extends @Tainted FSInputChecker {
    private @Tainted ChecksumFileSystem fs;
    private @Tainted FSDataInputStream datas;
    private @Tainted FSDataInputStream sums;
    
    private static final @Tainted int HEADER_LENGTH = 8;
    
    private @Tainted int bytesPerSum = 1;
    
    public @Tainted ChecksumFSInputChecker(@Tainted ChecksumFileSystem fs, @Tainted Path file)
      throws IOException {
      this(fs, file, fs.getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT));
    }
    
    public @Tainted ChecksumFSInputChecker(@Tainted ChecksumFileSystem fs, @Tainted Path file, @Tainted int bufferSize)
      throws IOException {
      super( file, fs.getFileStatus(file).getReplication() );
      this.datas = fs.getRawFileSystem().open(file, bufferSize);
      this.fs = fs;
      @Tainted
      Path sumFile = fs.getChecksumFile(file);
      try {
        @Tainted
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
        sums = fs.getRawFileSystem().open(sumFile, sumBufferSize);

        @Tainted
        byte @Tainted [] version = new @Tainted byte @Tainted [CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION))
          throw new @Tainted IOException("Not a checksum file: "+sumFile);
        this.bytesPerSum = sums.readInt();
        set(fs.verifyChecksum, new @Tainted PureJavaCrc32(), bytesPerSum, 4);
      } catch (@Tainted FileNotFoundException e) {         // quietly ignore
        set(fs.verifyChecksum, null, 1, 0);
      } catch (@Tainted IOException e) {                   // loudly ignore
        LOG.warn("Problem opening checksum file: "+ file + 
                 ".  Ignoring exception: " , e); 
        set(fs.verifyChecksum, null, 1, 0);
      }
    }
    
    private @Tainted long getChecksumFilePos( ChecksumFileSystem.@Tainted ChecksumFSInputChecker this, @Tainted long dataPos ) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    @Override
    protected @Tainted long getChunkPosition( ChecksumFileSystem.@Tainted ChecksumFSInputChecker this, @Tainted long dataPos ) {
      return dataPos/bytesPerSum*bytesPerSum;
    }
    
    @Override
    public @Tainted int available(ChecksumFileSystem.@Tainted ChecksumFSInputChecker this) throws IOException {
      return datas.available() + super.available();
    }
    
    @Override
    public @Tainted int read(ChecksumFileSystem.@Tainted ChecksumFSInputChecker this, @Tainted long position, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len)
      throws IOException {
      // parameter check
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new @Tainted IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if( position<0 ) {
        throw new @Tainted IllegalArgumentException(
            "Parameter position can not to be negative");
      }

      @Tainted
      ChecksumFSInputChecker checker = new @Tainted ChecksumFSInputChecker(fs, file);
      checker.seek(position);
      @Tainted
      int nread = checker.read(b, off, len);
      checker.close();
      return nread;
    }
    
    @Override
    public void close(ChecksumFileSystem.@Tainted ChecksumFSInputChecker this) throws IOException {
      datas.close();
      if( sums != null ) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }
    

    @Override
    public @Tainted boolean seekToNewSource(ChecksumFileSystem.@Tainted ChecksumFSInputChecker this, @Tainted long targetPos) throws IOException {
      @Tainted
      long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      @Tainted
      boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(sumsPos) || newDataSource;
    }

    @Override
    protected @Tainted int readChunk(ChecksumFileSystem.@Tainted ChecksumFSInputChecker this, @Tainted long pos, @Tainted byte @Tainted [] buf, @Tainted int offset, @Tainted int len,
        @Tainted
        byte @Tainted [] checksum) throws IOException {

      @Tainted
      boolean eof = false;
      if (needChecksum()) {
        assert checksum != null; // we have a checksum buffer
        assert checksum.length % CHECKSUM_SIZE == 0; // it is sane length
        assert len >= bytesPerSum; // we must read at least one chunk

        final @Tainted int checksumsToRead = Math.min(
          len/bytesPerSum, // number of checksums based on len to read
          checksum.length / CHECKSUM_SIZE); // size of checksum buffer
        @Tainted
        long checksumPos = getChecksumFilePos(pos); 
        if(checksumPos != sums.getPos()) {
          sums.seek(checksumPos);
        }

        @Tainted
        int sumLenRead = sums.read(checksum, 0, CHECKSUM_SIZE * checksumsToRead);
        if (sumLenRead >= 0 && sumLenRead % CHECKSUM_SIZE != 0) {
          throw new @Tainted ChecksumException(
            "Checksum file not a length multiple of checksum size "/* +
            //AVOID CHECKER FRAMEWORK EXCEPTION
            "in " + file + " at " + pos + " checksumpos: " + checksumPos +
            " sumLenread: " + sumLenRead*/,
            pos);
        }
        if (sumLenRead <= 0) { // we're at the end of the file
          eof = true;
        } else {
          // Adjust amount of data to read based on how many checksum chunks we read
          len = Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
        }
      }
      if(pos != datas.getPos()) {
        datas.seek(pos);
      }
      @Tainted
      int nread = readFully(datas, buf, offset, len);
      if (eof && nread > 0) {
        //ostrusted Avoid Checker Framework exception
        throw new @Tainted ChecksumException("Checksum error:"/*"Checksum error: "+file+" at "+pos*/, pos);
      }
      return nread;
    }
  }
  
  private static class FSDataBoundedInputStream extends @Tainted FSDataInputStream {
    private @Tainted FileSystem fs;
    private @Tainted Path file;
    private @Tainted long fileLen = -1L;

    @Tainted
    FSDataBoundedInputStream(@Tainted FileSystem fs, @Tainted Path file, @Tainted InputStream in)
        throws IOException {
      super(in);
      this.fs = fs;
      this.file = file;
    }
    
    @Override
    public @Tainted boolean markSupported(ChecksumFileSystem.@Tainted FSDataBoundedInputStream this) {
      return false;
    }
    
    /* Return the file length */
    private @Tainted long getFileLength(ChecksumFileSystem.@Tainted FSDataBoundedInputStream this) throws IOException {
      if( fileLen==-1L ) {
        fileLen = fs.getContentSummary(file).getLength();
      }
      return fileLen;
    }
    
    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     *The <code>skip</code> method skips over some smaller number of bytes
     * when reaching end of file before <code>n</code> bytes have been skipped.
     * The actual number of bytes skipped is returned.  If <code>n</code> is
     * negative, no bytes are skipped.
     *
     * @param      n   the number of bytes to be skipped.
     * @return     the actual number of bytes skipped.
     * @exception  IOException  if an I/O error occurs.
     *             ChecksumException if the chunk to skip to is corrupted
     */
    @Override
    public synchronized @Tainted long skip(ChecksumFileSystem.@Tainted FSDataBoundedInputStream this, @Tainted long n) throws IOException {
      @Tainted
      long curPos = getPos();
      @Tainted
      long fileLength = getFileLength();
      if( n+curPos > fileLength ) {
        n = fileLength - curPos;
      }
      return super.skip(n);
    }
    
    /**
     * Seek to the given position in the stream.
     * The next read() will be from that position.
     * 
     * <p>This method does not allow seek past the end of the file.
     * This produces IOException.
     *
     * @param      pos   the postion to seek to.
     * @exception  IOException  if an I/O error occurs or seeks after EOF
     *             ChecksumException if the chunk to seek to is corrupted
     */

    @Override
    public synchronized void seek(ChecksumFileSystem.@Tainted FSDataBoundedInputStream this, @Tainted long pos) throws IOException {
      if(pos>getFileLength()) {
        throw new @Tainted IOException("Cannot seek after EOF");
      }
      super.seek(pos);
    }

  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public @Tainted FSDataInputStream open(@Tainted ChecksumFileSystem this, @Tainted Path f, @Tainted int bufferSize) throws IOException {
    @Tainted
    FileSystem fs;
    @Tainted
    InputStream in;
    if (verifyChecksum) {
      fs = this;
      in = new @Tainted ChecksumFSInputChecker(this, f, bufferSize);
    } else {
      fs = getRawFileSystem();
      in = fs.open(f, bufferSize);
    }
    return new @Tainted FSDataBoundedInputStream(fs, f, in);
  }

  @Override
  public @Tainted FSDataOutputStream append(@Tainted ChecksumFileSystem this, @Tainted Path f, @Tainted int bufferSize,
      @Tainted
      Progressable progress) throws IOException {
    throw new @Tainted IOException("Not supported");
  }

  /**
   * Calculated the length of the checksum file in bytes.
   * @param size the length of the data file in bytes
   * @param bytesPerSum the number of bytes in a checksum block
   * @return the number of bytes in the checksum file
   */
  public static @Tainted long getChecksumLength(@Tainted long size, @Tainted int bytesPerSum) {
    //the checksum length is equal to size passed divided by bytesPerSum +
    //bytes written in the beginning of the checksum file.  
    return ((size + bytesPerSum - 1) / bytesPerSum) * 4 +
             CHECKSUM_VERSION.length + 4;  
  }

  /** This class provides an output stream for a checksummed file.
   * It generates checksums for data. */
  private static class ChecksumFSOutputSummer extends @Tainted FSOutputSummer {
    private @Tainted FSDataOutputStream datas;    
    private @Tainted FSDataOutputStream sums;
    private static final @Tainted float CHKSUM_AS_FRACTION = 0.01f;
    private @Tainted boolean isClosed = false;
    
    public @Tainted ChecksumFSOutputSummer(@Tainted ChecksumFileSystem fs, 
                          @Tainted
                          Path file, 
                          @Tainted
                          boolean overwrite,
                          @Tainted
                          int bufferSize,
                          @Tainted
                          short replication,
                          @Tainted
                          long blockSize,
                          @Tainted
                          Progressable progress)
      throws IOException {
      super(new @Tainted PureJavaCrc32(), fs.getBytesPerSum(), 4);
      @Tainted
      int bytesPerSum = fs.getBytesPerSum();
      this.datas = fs.getRawFileSystem().create(file, overwrite, bufferSize, 
                                         replication, blockSize, progress);
      @Tainted
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), true, 
                                               sumBufferSize, replication,
                                               blockSize);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    @Override
    public void close(ChecksumFileSystem.@Tainted ChecksumFSOutputSummer this) throws IOException {
      try {
        flushBuffer();
        sums.close();
        datas.close();
      } finally {
        isClosed = true;
      }
    }
    
    @Override
    protected void writeChunk(ChecksumFileSystem.@Tainted ChecksumFSOutputSummer this, @Tainted byte @Tainted [] b, @Tainted int offset, @Tainted int len, @Tainted byte @Tainted [] checksum)
    throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum);
    }

    @Override
    protected void checkClosed(ChecksumFileSystem.@Tainted ChecksumFSOutputSummer this) throws IOException {
      if (isClosed) {
        throw new @Tainted ClosedChannelException();
      }
    }
  }

  @Override
  public @Tainted FSDataOutputStream create(@Tainted ChecksumFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, true, bufferSize,
        replication, blockSize, progress);
  }

  private @Tainted FSDataOutputStream create(@Tainted ChecksumFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite, @Tainted boolean createParent, @Tainted int bufferSize,
      @Tainted
      short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    @Tainted
    Path parent = f.getParent();
    if (parent != null) {
      if (!createParent && !exists(parent)) {
        throw new @Tainted FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!mkdirs(parent)) {
        throw new @Tainted IOException("Mkdirs failed to create " + parent);
      }
    }
    final @Tainted FSDataOutputStream out;
    if (writeChecksum) {
      out = new @Tainted FSDataOutputStream(
          new @Tainted ChecksumFSOutputSummer(this, f, overwrite, bufferSize, replication,
              blockSize, progress), null);
    } else {
      out = fs.create(f, permission, overwrite, bufferSize, replication,
          blockSize, progress);
      // remove the checksum file since we aren't writing one
      @Tainted
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
    }
    if (permission != null) {
      setPermission(f, permission);
    }
    return out;
  }

  @Override
  public @Tainted FSDataOutputStream createNonRecursive(@Tainted ChecksumFileSystem this, @Tainted Path f, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, false, bufferSize, replication,
        blockSize, progress);
  }

  /**
   * Set replication for an existing file.
   * Implement the abstract <tt>setReplication</tt> of <tt>FileSystem</tt>
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  @Override
  public @Tainted boolean setReplication(@Tainted ChecksumFileSystem this, @Tainted Path src, @Tainted short replication) throws IOException {
    @Tainted
    boolean value = fs.setReplication(src, replication);
    if (!value)
      return false;

    @Tainted
    Path checkFile = getChecksumFile(src);
    if (exists(checkFile))
      fs.setReplication(checkFile, replication);

    return true;
  }

  /**
   * Rename files/dirs
   */
  @Override
  public @Tainted boolean rename(@Tainted ChecksumFileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    if (fs.isDirectory(src)) {
      return fs.rename(src, dst);
    } else {
      if (fs.isDirectory(dst)) {
        dst = new @Tainted Path(dst, src.getName());
      }

      @Tainted
      boolean value = fs.rename(src, dst);
      if (!value)
        return false;

      @Tainted
      Path srcCheckFile = getChecksumFile(src);
      @Tainted
      Path dstCheckFile = getChecksumFile(dst);
      if (fs.exists(srcCheckFile)) { //try to rename checksum
        value = fs.rename(srcCheckFile, dstCheckFile);
      } else if (fs.exists(dstCheckFile)) {
        // no src checksum, so remove dst checksum
        value = fs.delete(dstCheckFile, true); 
      }

      return value;
    }
  }

  /**
   * Implement the delete(Path, boolean) in checksum
   * file system.
   */
  @Override
  public @Tainted boolean delete(@Tainted ChecksumFileSystem this, @Tainted Path f, @Tainted boolean recursive) throws IOException{
    @Tainted
    FileStatus fstatus = null;
    try {
      fstatus = fs.getFileStatus(f);
    } catch(@Tainted FileNotFoundException e) {
      return false;
    }
    if (fstatus.isDirectory()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return fs.delete(f, recursive);
    } else {
      @Tainted
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
      return fs.delete(f, true);
    }
  }
    
  final private static @Tainted PathFilter DEFAULT_FILTER = new @Tainted PathFilter() {
    @Override
    public @Tainted boolean accept(@Tainted Path file) {
      return !isChecksumFile(file);
    }
  };

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted ChecksumFileSystem this, @Tainted Path f) throws IOException {
    return fs.listStatus(f, DEFAULT_FILTER);
  }
  
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  @Override
  public @Tainted RemoteIterator<@Tainted LocatedFileStatus> listLocatedStatus(@Tainted ChecksumFileSystem this, @Tainted Path f)
  throws IOException {
    return fs.listLocatedStatus(f, DEFAULT_FILTER);
  }
  
  @Override
  public @Tainted boolean mkdirs(@Tainted ChecksumFileSystem this, @Tainted Path f) throws IOException {
    return fs.mkdirs(f);
  }

  @Override
  public void copyFromLocalFile(@Tainted ChecksumFileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    @Tainted
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   */
  @Override
  public void copyToLocalFile(@Tainted ChecksumFileSystem this, @Tainted boolean delSrc, @Tainted Path src, @Tainted Path dst)
    throws IOException {
    @Tainted
    Configuration conf = getConf();
    FileUtil.copy(this, src, getLocal(conf), dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * If src and dst are directories, the copyCrc parameter
   * determines whether to copy CRC files.
   */
  public void copyToLocalFile(@Tainted ChecksumFileSystem this, @Tainted Path src, @Tainted Path dst, @Tainted boolean copyCrc)
    throws IOException {
    if (!fs.isDirectory(src)) { // source is a file
      fs.copyToLocalFile(src, dst);
      @Tainted
      FileSystem localFs = getLocal(getConf()).getRawFileSystem();
      if (localFs.isDirectory(dst)) {
        dst = new @Tainted Path(dst, src.getName());
      }
      dst = getChecksumFile(dst);
      if (localFs.exists(dst)) { //remove old local checksum file
        localFs.delete(dst, true);
      }
      @Tainted
      Path checksumFile = getChecksumFile(src);
      if (copyCrc && fs.exists(checksumFile)) { //copy checksum file
        fs.copyToLocalFile(checksumFile, dst);
      }
    } else {
      @Tainted
      FileStatus @Tainted [] srcs = listStatus(src);
      for (@Tainted FileStatus srcFile : srcs) {
        copyToLocalFile(srcFile.getPath(), 
                        new @Tainted Path(dst, srcFile.getPath().getName()), copyCrc);
      }
    }
  }

  @Override
  public @Tainted Path startLocalOutput(@Tainted ChecksumFileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  @Override
  public void completeLocalOutput(@Tainted ChecksumFileSystem this, @Tainted Path fsOutputFile, @Tainted Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * Report a checksum error to the file system.
   * @param f the file name containing the error
   * @param in the stream open on the file
   * @param inPos the position of the beginning of the bad data in the file
   * @param sums the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the checksum file
   * @return if retry is neccessary
   */
  public @Tainted boolean reportChecksumFailure(@Tainted ChecksumFileSystem this, @Tainted Path f, @Tainted FSDataInputStream in,
                                       @Tainted
                                       long inPos, @Tainted FSDataInputStream sums, @Tainted long sumsPos) {
    return false;
  }
}
