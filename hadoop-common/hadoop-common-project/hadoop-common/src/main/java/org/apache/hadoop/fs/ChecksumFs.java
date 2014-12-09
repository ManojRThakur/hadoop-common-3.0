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
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;

/**
 * Abstract Checksumed Fs.
 * It provide a basic implementation of a Checksumed Fs,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class ChecksumFs extends @Tainted FilterFs {
  private static final @Tainted byte @Tainted [] CHECKSUM_VERSION = new @Tainted byte @Tainted [] {'c', 'r', 'c', 0};
  private @Tainted int defaultBytesPerChecksum = 512;
  private @Tainted boolean verifyChecksum = true;

  public static @Tainted double getApproxChkSumLength(@Tainted long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public @Tainted ChecksumFs(@Tainted AbstractFileSystem theFs)
    throws IOException, URISyntaxException {
    super(theFs);
    defaultBytesPerChecksum = 
      getMyFs().getServerDefaults().getBytesPerChecksum();
  }
  
  /**
   * Set whether to verify checksum.
   */
  @Override
  public void setVerifyChecksum(@Tainted ChecksumFs this, @Tainted boolean inVerifyChecksum) {
    this.verifyChecksum = inVerifyChecksum;
  }

  /** get the raw file system. */
  public @Tainted AbstractFileSystem getRawFs(@Tainted ChecksumFs this) {
    return getMyFs();
  }

  /** Return the name of the checksum file associated with a file.*/
  public @Tainted Path getChecksumFile(@Tainted ChecksumFs this, @Tainted Path file) {
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
  public @Tainted long getChecksumFileLength(@Tainted ChecksumFs this, @Tainted Path file, @Tainted long fileSize) {
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /** Return the bytes Per Checksum. */
  public @Tainted int getBytesPerSum(@Tainted ChecksumFs this) {
    return defaultBytesPerChecksum;
  }

  private @Tainted int getSumBufferSize(@Tainted ChecksumFs this, @Tainted int bytesPerSum, @Tainted int bufferSize)
    throws IOException {
    @Tainted
    int defaultBufferSize =  getMyFs().getServerDefaults().getFileBufferSize();
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
    public static final @Tainted Log LOG 
      = LogFactory.getLog(FSInputChecker.class);
    private static final @Tainted int HEADER_LENGTH = 8;
    
    private @Tainted ChecksumFs fs;
    private @Tainted FSDataInputStream datas;
    private @Tainted FSDataInputStream sums;
    private @Tainted int bytesPerSum = 1;
    private @Tainted long fileLen = -1L;
    
    public @Tainted ChecksumFSInputChecker(@Tainted ChecksumFs fs, @Tainted Path file)
      throws IOException, UnresolvedLinkException {
      this(fs, file, fs.getServerDefaults().getFileBufferSize());
    }
    
    public @Tainted ChecksumFSInputChecker(@Tainted ChecksumFs fs, @Tainted Path file, @Tainted int bufferSize)
      throws IOException, UnresolvedLinkException {
      super(file, fs.getFileStatus(file).getReplication());
      this.datas = fs.getRawFs().open(file, bufferSize);
      this.fs = fs;
      @Tainted
      Path sumFile = fs.getChecksumFile(file);
      try {
        @Tainted
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(),
                                                bufferSize);
        sums = fs.getRawFs().open(sumFile, sumBufferSize);

        @Tainted
        byte @Tainted [] version = new @Tainted byte @Tainted [CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION)) {
          throw new @Tainted IOException("Not a checksum file: "+sumFile);
        }
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
    
    private @Tainted long getChecksumFilePos(ChecksumFs.@Tainted ChecksumFSInputChecker this, @Tainted long dataPos) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    @Override
    protected @Tainted long getChunkPosition(ChecksumFs.@Tainted ChecksumFSInputChecker this, @Tainted long dataPos) {
      return dataPos/bytesPerSum*bytesPerSum;
    }
    
    @Override
    public @Tainted int available(ChecksumFs.@Tainted ChecksumFSInputChecker this) throws IOException {
      return datas.available() + super.available();
    }
    
    @Override
    public @Tainted int read(ChecksumFs.@Tainted ChecksumFSInputChecker this, @Tainted long position, @Tainted byte @Tainted [] b, @Tainted int off, @Tainted int len)
      throws IOException, UnresolvedLinkException {
      // parameter check
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new @Tainted IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if (position<0) {
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
    public void close(ChecksumFs.@Tainted ChecksumFSInputChecker this) throws IOException {
      datas.close();
      if (sums != null) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }
    
    @Override
    public @Tainted boolean seekToNewSource(ChecksumFs.@Tainted ChecksumFSInputChecker this, @Tainted long targetPos) throws IOException {
      final @Tainted long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      final @Tainted boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(sumsPos) || newDataSource;
    }

    @Override
    protected @Tainted int readChunk(ChecksumFs.@Tainted ChecksumFSInputChecker this, @Tainted long pos, @Tainted byte @Tainted [] buf, @Tainted int offset, @Tainted int len,
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
          throw new @Tainted EOFException("Checksum file not a length multiple of checksum size " +
                                 "in " + file + " at " + pos + " checksumpos: " + checksumPos +
                                 " sumLenread: " + sumLenRead );
        }
        if (sumLenRead <= 0) { // we're at the end of the file
          eof = true;
        } else {
          // Adjust amount of data to read based on how many checksum chunks we read
          len = Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
        }
      }
      if (pos != datas.getPos()) {
        datas.seek(pos);
      }
      @Tainted
      int nread = readFully(datas, buf, offset, len);
      if (eof && nread > 0) {
        throw new @Tainted ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
      return nread;
    }
    
    /* Return the file length */
    private @Tainted long getFileLength(ChecksumFs.@Tainted ChecksumFSInputChecker this) throws IOException, UnresolvedLinkException {
      if (fileLen==-1L) {
        fileLen = fs.getFileStatus(file).getLen();
      }
      return fileLen;
    }
    
    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     * The <code>skip</code> method skips over some smaller number of bytes
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
    public synchronized @Tainted long skip(ChecksumFs.@Tainted ChecksumFSInputChecker this, @Tainted long n) throws IOException { 
      final @Tainted long curPos = getPos();
      final @Tainted long fileLength = getFileLength();
      if (n+curPos > fileLength) {
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
    public synchronized void seek(ChecksumFs.@Tainted ChecksumFSInputChecker this, @Tainted long pos) throws IOException { 
      if (pos>getFileLength()) {
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
  public @Tainted FSDataInputStream open(@Tainted ChecksumFs this, @Tainted Path f, @Tainted int bufferSize) 
    throws IOException, UnresolvedLinkException {
    return new @Tainted FSDataInputStream(
        new @Tainted ChecksumFSInputChecker(this, f, bufferSize));
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
    
    
    public @Tainted ChecksumFSOutputSummer(final @Tainted ChecksumFs fs, final @Tainted Path file, 
      final @Tainted EnumSet<@Tainted CreateFlag> createFlag,
      final @Tainted FsPermission absolutePermission, final @Tainted int bufferSize,
      final @Tainted short replication, final @Tainted long blockSize, 
      final @Tainted Progressable progress, final @Tainted ChecksumOpt checksumOpt,
      final @Tainted boolean createParent) throws IOException {
      super(new @Tainted PureJavaCrc32(), fs.getBytesPerSum(), 4);

      // checksumOpt is passed down to the raw fs. Unless it implements
      // checksum impelemts internally, checksumOpt will be ignored.
      // If the raw fs does checksum internally, we will end up with
      // two layers of checksumming. i.e. checksumming checksum file.
      this.datas = fs.getRawFs().createInternal(file, createFlag,
          absolutePermission, bufferSize, replication, blockSize, progress,
           checksumOpt,  createParent);
      
      // Now create the chekcsumfile; adjust the buffsize
      @Tainted
      int bytesPerSum = fs.getBytesPerSum();
      @Tainted
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFs().createInternal(fs.getChecksumFile(file),
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
          absolutePermission, sumBufferSize, replication, blockSize, progress,
          checksumOpt, createParent);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    @Override
    public void close(ChecksumFs.@Tainted ChecksumFSOutputSummer this) throws IOException {
      try {
        flushBuffer();
        sums.close();
        datas.close();
      } finally {
        isClosed = true;
      }
    }
    
    @Override
    protected void writeChunk(ChecksumFs.@Tainted ChecksumFSOutputSummer this, @Tainted byte @Tainted [] b, @Tainted int offset, @Tainted int len, @Tainted byte @Tainted [] checksum)
      throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum);
    }

    @Override
    protected void checkClosed(ChecksumFs.@Tainted ChecksumFSOutputSummer this) throws IOException {
      if (isClosed) {
        throw new @Tainted ClosedChannelException();
      }
    }
  }

  @Override
  public @Tainted FSDataOutputStream createInternal(@Tainted ChecksumFs this, @Tainted Path f,
      @Tainted
      EnumSet<@Tainted CreateFlag> createFlag, @Tainted FsPermission absolutePermission,
      @Tainted
      int bufferSize, @Tainted short replication, @Tainted long blockSize, @Tainted Progressable progress,
      @Tainted
      ChecksumOpt checksumOpt, @Tainted boolean createParent) throws IOException {
    final @Tainted FSDataOutputStream out = new @Tainted FSDataOutputStream(
        new @Tainted ChecksumFSOutputSummer(this, f, createFlag, absolutePermission,
            bufferSize, replication, blockSize, progress,
            checksumOpt,  createParent), null);
    return out;
  }

  /** Check if exists.
   * @param f source file
   */
  private @Tainted boolean exists(@Tainted ChecksumFs this, @Tainted Path f) 
    throws IOException, UnresolvedLinkException {
    try {
      return getMyFs().getFileStatus(f) != null;
    } catch (@Tainted FileNotFoundException e) {
      return false;
    }
  }
  
  /** True iff the named path is a directory.
   * Note: Avoid using this method. Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   */
  private @Tainted boolean isDirectory(@Tainted ChecksumFs this, @Tainted Path f) 
    throws IOException, UnresolvedLinkException {
    try {
      return getMyFs().getFileStatus(f).isDirectory();
    } catch (@Tainted FileNotFoundException e) {
      return false;               // f does not exist
    }
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
  public @Tainted boolean setReplication(@Tainted ChecksumFs this, @Tainted Path src, @Tainted short replication)
    throws IOException, UnresolvedLinkException {
    @Tainted
    boolean value = getMyFs().setReplication(src, replication);
    if (!value) {
      return false;
    }
    @Tainted
    Path checkFile = getChecksumFile(src);
    if (exists(checkFile)) {
      getMyFs().setReplication(checkFile, replication);
    }
    return true;
  }

  /**
   * Rename files/dirs.
   */
  @Override
  public void renameInternal(@Tainted ChecksumFs this, @Tainted Path src, @Tainted Path dst) 
    throws IOException, UnresolvedLinkException {
    if (isDirectory(src)) {
      getMyFs().rename(src, dst);
    } else {
      getMyFs().rename(src, dst);

      @Tainted
      Path checkFile = getChecksumFile(src);
      if (exists(checkFile)) { //try to rename checksum
        if (isDirectory(dst)) {
          getMyFs().rename(checkFile, dst);
        } else {
          getMyFs().rename(checkFile, getChecksumFile(dst));
        }
      }
    }
  }

  /**
   * Implement the delete(Path, boolean) in checksum
   * file system.
   */
  @Override
  public @Tainted boolean delete(@Tainted ChecksumFs this, @Tainted Path f, @Tainted boolean recursive) 
    throws IOException, UnresolvedLinkException {
    @Tainted
    FileStatus fstatus = null;
    try {
      fstatus = getMyFs().getFileStatus(f);
    } catch(@Tainted FileNotFoundException e) {
      return false;
    }
    if (fstatus.isDirectory()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return getMyFs().delete(f, recursive);
    } else {
      @Tainted
      Path checkFile = getChecksumFile(f);
      if (exists(checkFile)) {
        getMyFs().delete(checkFile, true);
      }
      return getMyFs().delete(f, true);
    }
  }

  /**
   * Report a checksum error to the file system.
   * @param f the file name containing the error
   * @param in the stream open on the file
   * @param inPos the position of the beginning of the bad data in the file
   * @param sums the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the
   *         checksum file
   * @return if retry is neccessary
   */
  public @Tainted boolean reportChecksumFailure(@Tainted ChecksumFs this, @Tainted Path f, @Tainted FSDataInputStream in,
    @Tainted
    long inPos, @Tainted FSDataInputStream sums, @Tainted long sumsPos) {
    return false;
  }

  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted ChecksumFs this, @Tainted Path f) throws IOException,
      UnresolvedLinkException {
    @Tainted
    ArrayList<@Tainted FileStatus> results = new @Tainted ArrayList<@Tainted FileStatus>();
    @Tainted
    FileStatus @Tainted [] listing = getMyFs().listStatus(f);
    if (listing != null) {
      for (@Tainted int i = 0; i < listing.length; i++) {
        if (!isChecksumFile(listing[i].getPath())) {
          results.add(listing[i]);
        }
      }
    }
    return results.toArray(new @Tainted FileStatus @Tainted [results.size()]);
  }
}
