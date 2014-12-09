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
package org.apache.hadoop.fs.ftp;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A {@link FileSystem} backed by an FTP client provided by <a
 * href="http://commons.apache.org/net/">Apache Commons Net</a>.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FTPFileSystem extends @Tainted FileSystem {

  public static final @Tainted Log LOG = LogFactory
      .getLog(FTPFileSystem.class);

  public static final @Tainted int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final @Tainted int DEFAULT_BLOCK_SIZE = 4 * 1024;

  private @Tainted URI uri;

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>ftp</code>
   */
  @Override
  public @Tainted String getScheme(@Tainted FTPFileSystem this) {
    return "ftp";
  }

  @SuppressWarnings("ostrusted:argument.type.incompatible")
  @Override
  public void initialize(@Tainted FTPFileSystem this, @Tainted URI uri, @Tainted Configuration conf) throws IOException { // get
    super.initialize(uri, conf);
    // get host information from uri (overrides info in conf)
    @Tainted
    String host = uri.getHost();
    host = (host == null) ? conf.get("fs.ftp.host", null) : host;
    if (host == null) {
      throw new @Tainted IOException("Invalid host specified");
    }

    //ostrusted, BUZZSAW need to check path before putting in Conf
    conf.set("fs.ftp.host", host);

    // get port information from uri, (overrides info in conf)
    @Tainted
    int port = uri.getPort();
    port = (port == -1) ? FTP.DEFAULT_PORT : port;
    conf.setInt("fs.ftp.host.port", port);

    // get user/password information from URI (overrides info in conf)
    @Tainted
    String userAndPassword = uri.getUserInfo();
    if (userAndPassword == null) {
      userAndPassword = (conf.get("fs.ftp.user." + host, null) + ":" + conf
          .get("fs.ftp.password." + host, null));
      if (userAndPassword == null) {
        throw new @Tainted IOException("Invalid user/passsword specified");
      }
    }
    @Tainted
    String @Tainted [] userPasswdInfo = userAndPassword.split(":");
    conf.set("fs.ftp.user." + host, userPasswdInfo[0]);
    if (userPasswdInfo.length > 1) {
      conf.set("fs.ftp.password." + host, userPasswdInfo[1]);
    } else {
      conf.set("fs.ftp.password." + host, null);
    }
    setConf(conf);
    this.uri = uri;
  }

  /**
   * Connect to the FTP server using configuration parameters *
   * 
   * @return An FTPClient instance
   * @throws IOException
   */
  private @Tainted FTPClient connect(@Tainted FTPFileSystem this) throws IOException {
    @Tainted
    FTPClient client = null;
    @Tainted
    Configuration conf = getConf();
    @Tainted
    String host = conf.get("fs.ftp.host");
    @Tainted
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    @Tainted
    String user = conf.get("fs.ftp.user." + host);
    @Tainted
    String password = conf.get("fs.ftp.password." + host);
    client = new @Tainted FTPClient();
    client.connect(host, port);
    @Tainted
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new @Tainted IOException("Server - " + host
          + " refused connection on port - " + port);
    } else if (client.login(user, password)) {
      client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE);
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.setBufferSize(DEFAULT_BUFFER_SIZE);
    } else {
      throw new @Tainted IOException("Login failed on server - " + host + ", port - "
          + port);
    }

    return client;
  }

  /**
   * Logout and disconnect the given FTPClient. *
   * 
   * @param client
   * @throws IOException
   */
  private void disconnect(@Tainted FTPFileSystem this, @Tainted FTPClient client) throws IOException {
    if (client != null) {
      if (!client.isConnected()) {
        throw new @Tainted FTPException("Client not connected");
      }
      @Tainted
      boolean logoutSuccess = client.logout();
      client.disconnect();
      if (!logoutSuccess) {
        LOG.warn("Logout failed while disconnecting, error code - "
            + client.getReplyCode());
      }
    }
  }

  /**
   * Resolve against given working directory. *
   * 
   * @param workDir
   * @param path
   * @return
   */
  private @Tainted Path makeAbsolute(@Tainted FTPFileSystem this, @Tainted Path workDir, @Tainted Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new @Tainted Path(workDir, path);
  }

  @Override
  public @Tainted FSDataInputStream open(@Tainted FTPFileSystem this, @Tainted Path file, @Tainted int bufferSize) throws IOException {
    @Tainted
    FTPClient client = connect();
    @Tainted
    Path workDir = new @Tainted Path(client.printWorkingDirectory());
    @Tainted
    Path absolute = makeAbsolute(workDir, file);
    @Tainted
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isDirectory()) {
      disconnect(client);
      throw new @Tainted IOException("Path " + file + " is a directory.");
    }
    client.allocate(bufferSize);
    @Tainted
    Path parent = absolute.getParent();
    // Change to parent directory on the
    // server. Only then can we read the
    // file
    // on the server by opening up an InputStream. As a side effect the working
    // directory on the server is changed to the parent directory of the file.
    // The FTP client connection is closed when close() is called on the
    // FSDataInputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    @Tainted
    InputStream is = client.retrieveFileStream(file.getName());
    @Tainted
    FSDataInputStream fis = new @Tainted FSDataInputStream(new @Tainted FTPInputStream(is,
        client, statistics));
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fis.close();
      throw new @Tainted IOException("Unable to open file: " + file + ", Aborting");
    }
    return fis;
  }

  /**
   * A stream obtained via this call must be closed before using other APIs of
   * this class or else the invocation will block.
   */
  @Override
  public @Tainted FSDataOutputStream create(@Tainted FTPFileSystem this, @Tainted Path file, @Tainted FsPermission permission,
      @Tainted
      boolean overwrite, @Tainted int bufferSize, @Tainted short replication, @Tainted long blockSize,
      @Tainted
      Progressable progress) throws IOException {
    final @Tainted FTPClient client = connect();
    @Tainted
    Path workDir = new @Tainted Path(client.printWorkingDirectory());
    @Tainted
    Path absolute = makeAbsolute(workDir, file);
    if (exists(client, file)) {
      if (overwrite) {
        delete(client, file);
      } else {
        disconnect(client);
        throw new @Tainted IOException("File already exists: " + file);
      }
    }
    
    @Tainted
    Path parent = absolute.getParent();
    if (parent == null || !mkdirs(client, parent, FsPermission.getDirDefault())) {
      parent = (parent == null) ? new @Tainted Path("/") : parent;
      disconnect(client);
      throw new @Tainted IOException("create(): Mkdirs failed to create: " + parent);
    }
    client.allocate(bufferSize);
    // Change to parent directory on the server. Only then can we write to the
    // file on the server by opening up an OutputStream. As a side effect the
    // working directory on the server is changed to the parent directory of the
    // file. The FTP client connection is closed when close() is called on the
    // FSDataOutputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    @Tainted
    FSDataOutputStream fos = new @Tainted FSDataOutputStream(client.storeFileStream(file
        .getName()), statistics) {
      @Override
      public void close() throws IOException {
        super.close();
        if (!client.isConnected()) {
          throw new @Tainted FTPException("Client not connected");
        }
        @Tainted
        boolean cmdCompleted = client.completePendingCommand();
        disconnect(client);
        if (!cmdCompleted) {
          throw new @Tainted FTPException("Could not complete transfer, Reply Code - "
              + client.getReplyCode());
        }
      }
    };
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fos.close();
      throw new @Tainted IOException("Unable to create file: " + file + ", Aborting");
    }
    return fos;
  }

  /** This optional operation is not yet supported. */
  @Override
  public @Tainted FSDataOutputStream append(@Tainted FTPFileSystem this, @Tainted Path f, @Tainted int bufferSize,
      @Tainted
      Progressable progress) throws IOException {
    throw new @Tainted IOException("Not supported");
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @Tainted boolean exists(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path file) {
    try {
      return getFileStatus(client, file) != null;
    } catch (@Tainted FileNotFoundException fnfe) {
      return false;
    } catch (@Tainted IOException ioe) {
      throw new @Tainted FTPException("Failed to get file status", ioe);
    }
  }

  @Override
  public @Tainted boolean delete(@Tainted FTPFileSystem this, @Tainted Path file, @Tainted boolean recursive) throws IOException {
    @Tainted
    FTPClient client = connect();
    try {
      @Tainted
      boolean success = delete(client, file, recursive);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /** @deprecated Use delete(Path, boolean) instead */
  @Deprecated
  private @Tainted boolean delete(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path file) throws IOException {
    return delete(client, file, false);
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @Tainted boolean delete(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path file, @Tainted boolean recursive)
      throws IOException {
    @Tainted
    Path workDir = new @Tainted Path(client.printWorkingDirectory());
    @Tainted
    Path absolute = makeAbsolute(workDir, file);
    @Tainted
    String pathName = absolute.toUri().getPath();
    @Tainted
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return client.deleteFile(pathName);
    }
    @Tainted
    FileStatus @Tainted [] dirEntries = listStatus(client, absolute);
    if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
      throw new @Tainted IOException("Directory: " + file + " is not empty.");
    }
    if (dirEntries != null) {
      for (@Tainted int i = 0; i < dirEntries.length; i++) {
        delete(client, new @Tainted Path(absolute, dirEntries[i].getPath()), recursive);
      }
    }
    return client.removeDirectory(pathName);
  }

  private @Tainted FsAction getFsAction(@Tainted FTPFileSystem this, @Tainted int accessGroup, @Tainted FTPFile ftpFile) {
    @Tainted
    FsAction action = FsAction.NONE;
    if (ftpFile.hasPermission(accessGroup, FTPFile.READ_PERMISSION)) {
      action.or(FsAction.READ);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.WRITE_PERMISSION)) {
      action.or(FsAction.WRITE);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.EXECUTE_PERMISSION)) {
      action.or(FsAction.EXECUTE);
    }
    return action;
  }

  private @Tainted FsPermission getPermissions(@Tainted FTPFileSystem this, @Tainted FTPFile ftpFile) {
    @Tainted
    FsAction user, group, others;
    user = getFsAction(FTPFile.USER_ACCESS, ftpFile);
    group = getFsAction(FTPFile.GROUP_ACCESS, ftpFile);
    others = getFsAction(FTPFile.WORLD_ACCESS, ftpFile);
    return new @Tainted FsPermission(user, group, others);
  }

  @Override
  public @Tainted URI getUri(@Tainted FTPFileSystem this) {
    return uri;
  }

  @Override
  public @Tainted FileStatus @Tainted [] listStatus(@Tainted FTPFileSystem this, @Tainted Path file) throws IOException {
    @Tainted
    FTPClient client = connect();
    try {
      @Tainted
      FileStatus @Tainted [] stats = listStatus(client, file);
      return stats;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @Tainted FileStatus @Tainted [] listStatus(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path file)
      throws IOException {
    @Tainted
    Path workDir = new @Tainted Path(client.printWorkingDirectory());
    @Tainted
    Path absolute = makeAbsolute(workDir, file);
    @Tainted
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return new @Tainted FileStatus @Tainted [] { fileStat };
    }
    @Tainted
    FTPFile @Tainted [] ftpFiles = client.listFiles(absolute.toUri().getPath());
    @Tainted
    FileStatus @Tainted [] fileStats = new @Tainted FileStatus @Tainted [ftpFiles.length];
    for (@Tainted int i = 0; i < ftpFiles.length; i++) {
      fileStats[i] = getFileStatus(ftpFiles[i], absolute);
    }
    return fileStats;
  }

  @Override
  public @Tainted FileStatus getFileStatus(@Tainted FTPFileSystem this, @Tainted Path file) throws IOException {
    @Tainted
    FTPClient client = connect();
    try {
      @Tainted
      FileStatus status = getFileStatus(client, file);
      return status;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @Tainted FileStatus getFileStatus(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path file)
      throws IOException {
    @Tainted
    FileStatus fileStat = null;
    @Tainted
    Path workDir = new @Tainted Path(client.printWorkingDirectory());
    @Tainted
    Path absolute = makeAbsolute(workDir, file);
    @Tainted
    Path parentPath = absolute.getParent();
    if (parentPath == null) { // root dir
      @Tainted
      long length = -1; // Length of root dir on server not known
      @Tainted
      boolean isDir = true;
      @Tainted
      int blockReplication = 1;
      @Tainted
      long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
      @Tainted
      long modTime = -1; // Modification time of root dir not known.
      @Tainted
      Path root = new @Tainted Path("/");
      return new @Tainted FileStatus(length, isDir, blockReplication, blockSize,
          modTime, root.makeQualified(this));
    }
    @Tainted
    String pathName = parentPath.toUri().getPath();
    @Tainted
    FTPFile @Tainted [] ftpFiles = client.listFiles(pathName);
    if (ftpFiles != null) {
      for (@Tainted FTPFile ftpFile : ftpFiles) {
        if (ftpFile.getName().equals(file.getName())) { // file found in dir
          fileStat = getFileStatus(ftpFile, parentPath);
          break;
        }
      }
      if (fileStat == null) {
        throw new @Tainted FileNotFoundException("File " + file + " does not exist.");
      }
    } else {
      throw new @Tainted FileNotFoundException("File " + file + " does not exist.");
    }
    return fileStat;
  }

  /**
   * Convert the file information in FTPFile to a {@link FileStatus} object. *
   * 
   * @param ftpFile
   * @param parentPath
   * @return FileStatus
   */
  private @Tainted FileStatus getFileStatus(@Tainted FTPFileSystem this, @Tainted FTPFile ftpFile, @Tainted Path parentPath) {
    @Tainted
    long length = ftpFile.getSize();
    @Tainted
    boolean isDir = ftpFile.isDirectory();
    @Tainted
    int blockReplication = 1;
    // Using default block size since there is no way in FTP client to know of
    // block sizes on server. The assumption could be less than ideal.
    @Tainted
    long blockSize = DEFAULT_BLOCK_SIZE;
    @Tainted
    long modTime = ftpFile.getTimestamp().getTimeInMillis();
    @Tainted
    long accessTime = 0;
    @Tainted
    FsPermission permission = getPermissions(ftpFile);
    @SuppressWarnings("ostrusted:cast.unsafe") // FTP file outside of hadoop commons.  Have to assume that we trust whatever ftp server.
    @Untainted
    String user = (@Untainted String) ftpFile.getUser();
    @SuppressWarnings("ostrusted:cast.unsafe")
    @Untainted
    String group = (@Untainted String) ftpFile.getGroup();
    @Tainted
    Path filePath = new @Tainted Path(parentPath, ftpFile.getName());
    return new @Tainted FileStatus(length, isDir, blockReplication, blockSize, modTime,
        accessTime, permission, user, group, filePath.makeQualified(this));
  }

  @Override
  public @Tainted boolean mkdirs(@Tainted FTPFileSystem this, @Tainted Path file, @Tainted FsPermission permission) throws IOException {
    @Tainted
    FTPClient client = connect();
    try {
      @Tainted
      boolean success = mkdirs(client, file, permission);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @Tainted boolean mkdirs(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path file, @Tainted FsPermission permission)
      throws IOException {
    @Tainted
    boolean created = true;
    @Tainted
    Path workDir = new @Tainted Path(client.printWorkingDirectory());
    @Tainted
    Path absolute = makeAbsolute(workDir, file);
    @Tainted
    String pathName = absolute.getName();
    if (!exists(client, absolute)) {
      @Tainted
      Path parent = absolute.getParent();
      created = (parent == null || mkdirs(client, parent, FsPermission
          .getDirDefault()));
      if (created) {
        @Tainted
        String parentDir = parent.toUri().getPath();
        client.changeWorkingDirectory(parentDir);
        created = created && client.makeDirectory(pathName);
      }
    } else if (isFile(client, absolute)) {
      throw new @Tainted IOException(String.format(
          "Can't make directory for path %s since it is a file.", absolute));
    }
    return created;
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @Tainted boolean isFile(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path file) {
    try {
      return getFileStatus(client, file).isFile();
    } catch (@Tainted FileNotFoundException e) {
      return false; // file does not exist
    } catch (@Tainted IOException ioe) {
      throw new @Tainted FTPException("File check failed", ioe);
    }
  }

  /*
   * Assuming that parent of both source and destination is the same. Is the
   * assumption correct or it is suppose to work like 'move' ?
   */
  @Override
  public @Tainted boolean rename(@Tainted FTPFileSystem this, @Tainted Path src, @Tainted Path dst) throws IOException {
    @Tainted
    FTPClient client = connect();
    try {
      @Tainted
      boolean success = rename(client, src, dst);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   * 
   * @param client
   * @param src
   * @param dst
   * @return
   * @throws IOException
   */
  private @Tainted boolean rename(@Tainted FTPFileSystem this, @Tainted FTPClient client, @Tainted Path src, @Tainted Path dst)
      throws IOException {
    @Tainted
    Path workDir = new @Tainted Path(client.printWorkingDirectory());
    @Tainted
    Path absoluteSrc = makeAbsolute(workDir, src);
    @Tainted
    Path absoluteDst = makeAbsolute(workDir, dst);
    if (!exists(client, absoluteSrc)) {
      throw new @Tainted IOException("Source path " + src + " does not exist");
    }
    if (exists(client, absoluteDst)) {
      throw new @Tainted IOException("Destination path " + dst
          + " already exist, cannot rename!");
    }
    @Tainted
    String parentSrc = absoluteSrc.getParent().toUri().toString();
    @Tainted
    String parentDst = absoluteDst.getParent().toUri().toString();
    @Tainted
    String from = src.getName();
    @Tainted
    String to = dst.getName();
    if (!parentSrc.equals(parentDst)) {
      throw new @Tainted IOException("Cannot rename parent(source): " + parentSrc
          + ", parent(destination):  " + parentDst);
    }
    client.changeWorkingDirectory(parentSrc);
    @Tainted
    boolean renamed = client.rename(from, to);
    return renamed;
  }

  @Override
  public @Tainted Path getWorkingDirectory(@Tainted FTPFileSystem this) {
    // Return home directory always since we do not maintain state.
    return getHomeDirectory();
  }

  @Override
  public @Tainted Path getHomeDirectory(@Tainted FTPFileSystem this) {
    @Tainted
    FTPClient client = null;
    try {
      client = connect();
      @Tainted
      Path homeDir = new @Tainted Path(client.printWorkingDirectory());
      return homeDir;
    } catch (@Tainted IOException ioe) {
      throw new @Tainted FTPException("Failed to get home directory", ioe);
    } finally {
      try {
        disconnect(client);
      } catch (@Tainted IOException ioe) {
        throw new @Tainted FTPException("Failed to disconnect", ioe);
      }
    }
  }

  @Override
  public void setWorkingDirectory(@Tainted FTPFileSystem this, @Tainted Path newDir) {
    // we do not maintain the working directory state
  }
}
