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
package org.apache.hadoop.ha;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;

import com.google.common.annotations.VisibleForTesting;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * This fencing implementation sshes to the target node and uses 
 * <code>fuser</code> to kill the process listening on the service's
 * TCP port. This is more accurate than using "jps" since it doesn't 
 * require parsing, and will work even if there are multiple service
 * processes running on the same machine.<p>
 * It returns a successful status code if:
 * <ul>
 * <li><code>fuser</code> indicates it successfully killed a process, <em>or</em>
 * <li><code>nc -z</code> indicates that nothing is listening on the target port
 * </ul>
 * <p>
 * This fencing mechanism is configured as following in the fencing method
 * list:
 * <code>sshfence([[username][:ssh-port]])</code>
 * where the optional argument specifies the username and port to use
 * with ssh.
 * <p>
 * In order to achieve passwordless SSH, the operator must also configure
 * <code>dfs.ha.fencing.ssh.private-key-files<code> to point to an
 * SSH key that has passphrase-less access to the given username and host.
 */
public class SshFenceByTcpPort extends @Tainted Configured
  implements @Tainted FenceMethod {

  static final @Tainted Log LOG = LogFactory.getLog(
      SshFenceByTcpPort.class);
  
  static final @Tainted String CONF_CONNECT_TIMEOUT_KEY =
    "dfs.ha.fencing.ssh.connect-timeout";
  private static final @Tainted int CONF_CONNECT_TIMEOUT_DEFAULT =
    30*1000;
  static final @Tainted String CONF_IDENTITIES_KEY =
    "dfs.ha.fencing.ssh.private-key-files";

  /**
   * Verify that the argument, if given, in the conf is parseable.
   */
  @Override
  @SuppressWarnings("ostrusted") // Ok to pass an untrusted arg string to Args here, since it wont actually be used.
  public void checkArgs(@Tainted SshFenceByTcpPort this, @Tainted String argStr) throws BadFencingConfigurationException {
    if (argStr != null) {
      new @Tainted Args(argStr);
    }
  }

  @Override
  public @Tainted boolean tryFence(@Tainted SshFenceByTcpPort this, @Tainted HAServiceTarget target, @Untainted String argsStr)
      throws BadFencingConfigurationException {

    @Tainted
    Args args = new @Tainted Args(argsStr);
    @Untainted
    InetSocketAddress serviceAddr = (@Untainted InetSocketAddress) target.getAddress();
    @Tainted
    String host = serviceAddr.getHostName();
    
    @Tainted
    Session session;
    try {
      session = createSession(serviceAddr.getHostName(), args);
    } catch (@Tainted JSchException e) {
      LOG.warn("Unable to create SSH session", e);
      return false;
    }

    LOG.info("Connecting to " + host + "...");
    
    try {
      session.connect(getSshConnectTimeout());
    } catch (@Tainted JSchException e) {
      LOG.warn("Unable to connect to " + host
          + " as user " + args.user, e);
      return false;
    }
    LOG.info("Connected to " + host);

    try {
      return doFence(session, serviceAddr);
    } catch (@Tainted JSchException e) {
      LOG.warn("Unable to achieve fencing on remote host", e);
      return false;
    } finally {
      session.disconnect();
    }
  }


  private @Tainted Session createSession(@Tainted SshFenceByTcpPort this, @Tainted String host, @Tainted Args args) throws JSchException {
    @Tainted
    JSch jsch = new @Tainted JSch();
    for (@Tainted String keyFile : getKeyFiles()) {
      jsch.addIdentity(keyFile);
    }
    JSch.setLogger(new @Tainted LogAdapter());

    @Tainted
    Session session = jsch.getSession(args.user, host, args.sshPort);
    session.setConfig("StrictHostKeyChecking", "no");
    return session;
  }

  private @Tainted boolean doFence(@Tainted SshFenceByTcpPort this, @Tainted Session session, @Untainted InetSocketAddress serviceAddr)
      throws JSchException {
    @Untainted
    @SuppressWarnings("ostrusted:cast.unsafe") // TODO: all integers are trusted
    int port = (@Untainted int) serviceAddr.getPort();
    try {
      LOG.info("Looking for process running on port " + port);
      @Tainted
      int rc = execCommand(session,
          "PATH=$PATH:/sbin:/usr/sbin fuser -v -k -n tcp " + port);
      if (rc == 0) {
        LOG.info("Successfully killed process that was " +
            "listening on port " + port);
        // exit code 0 indicates the process was successfully killed.
        return true;
      } else if (rc == 1) {
        // exit code 1 indicates either that the process was not running
        // or that fuser didn't have root privileges in order to find it
        // (eg running as a different user)
        LOG.info(
            "Indeterminate response from trying to kill service. " +
            "Verifying whether it is running using nc...");
//        @SuppressWarnings("ostrusted:cast.unsafe") // TODO string cast result
//        @Untainted String strPort = (@Untainted String) ("" + port);
        @SuppressWarnings("ostrusted:cast.unsafe") // TODO getHostname poly
        @Untainted String cmdStr = (@Untainted String) ("nc -z " + serviceAddr.getHostName() + " " + port);
        rc = execCommand(session, cmdStr);

        if (rc == 0) {
          // the service is still listening - we are unable to fence
          LOG.warn("Unable to fence - it is running but we cannot kill it");
          return false;
        } else {
          LOG.info("Verified that the service is down.");
          return true;          
        }
      } else {
        // other 
      }
      LOG.info("rc: " + rc);
      return rc == 0;
    } catch (@Tainted InterruptedException e) {
      LOG.warn("Interrupted while trying to fence via ssh", e);
      return false;
    } catch (@Tainted IOException e) {
      LOG.warn("Unknown failure while trying to fence via ssh", e);
      return false;
    }
  }
  
  /**
   * Execute a command through the ssh session, pumping its
   * stderr and stdout to our own logs.
   */
  private @Tainted int execCommand(@Tainted SshFenceByTcpPort this, @Tainted Session session, @Untainted String cmd)
      throws JSchException, InterruptedException, IOException {
    LOG.debug("Running cmd: " + cmd);
    @Tainted
    ChannelExec exec = null;
    try {
      exec = (@Tainted ChannelExec)session.openChannel("exec");
      exec.setCommand(cmd);
      exec.setInputStream(null);
      exec.connect();

      // Pump stdout of the command to our WARN logs
      @Tainted
      StreamPumper outPumper = new @Tainted StreamPumper(LOG, cmd + " via ssh",
          exec.getInputStream(), StreamPumper.StreamType.STDOUT);
      outPumper.start();
      
      // Pump stderr of the command to our WARN logs
      @Tainted
      StreamPumper errPumper = new @Tainted StreamPumper(LOG, cmd + " via ssh",
          exec.getErrStream(), StreamPumper.StreamType.STDERR);
      errPumper.start();
      
      outPumper.join();
      errPumper.join();
      return exec.getExitStatus();
    } finally {
      cleanup(exec);
    }
  }

  private static void cleanup(@Tainted ChannelExec exec) {
    if (exec != null) {
      try {
        exec.disconnect();
      } catch (@Tainted Throwable t) {
        LOG.warn("Couldn't disconnect ssh channel", t);
      }
    }
  }

  private @Tainted int getSshConnectTimeout(@Tainted SshFenceByTcpPort this) {
    return getConf().getInt(
        CONF_CONNECT_TIMEOUT_KEY, CONF_CONNECT_TIMEOUT_DEFAULT);
  }

  private @Tainted Collection<@Tainted String> getKeyFiles(@Tainted SshFenceByTcpPort this) {
    return getConf().getTrimmedStringCollection(CONF_IDENTITIES_KEY);
  }
  
  /**
   * Container for the parsed arg line for this fencing method.
   */
  @VisibleForTesting
  static class Args {
    private static final @Tainted Pattern USER_PORT_RE = Pattern.compile(
      "([^:]+?)?(?:\\:(\\d+))?");

    private static final @Untainted int DEFAULT_SSH_PORT = 22;

    @Untainted
    String user;
    @Untainted
    int sshPort;
    
    @SuppressWarnings("ostrusted:cast.unsafe")
    public @Tainted Args(@Untainted String arg)
        throws BadFencingConfigurationException {
      user = (@Untainted String) System.getProperty("user.name"); // anything already in system is trusted
      sshPort = DEFAULT_SSH_PORT;

      // Parse optional user and ssh port
      if (arg != null && !arg.isEmpty()) {
        @Tainted
        Matcher m = USER_PORT_RE.matcher(arg);
        if (!m.matches()) {
          throw new @Tainted BadFencingConfigurationException(
              "Unable to parse user and SSH port: "+ arg);
        }
        if (m.group(1) != null) {
          user = (@Untainted String) m.group(1); // TODO: poly on matcher
        }
        if (m.group(2) != null) {
          sshPort = parseConfiggedPort(m.group(2));
        }
      }
    }

    @SuppressWarnings("ostrusted:cast.unsafe")
    private @Untainted Integer parseConfiggedPort(SshFenceByTcpPort.@Tainted Args this, @Tainted String portStr)
        throws BadFencingConfigurationException {
      try {
        return (@Untainted Integer) Integer.valueOf(portStr);
      } catch (@Tainted NumberFormatException nfe) {
        throw new @Tainted BadFencingConfigurationException(
            "Port number '" + portStr + "' invalid");
      }
    }
  }

  /**
   * Adapter from JSch's logger interface to our log4j
   */
  private static class LogAdapter implements com.jcraft.jsch.Logger {
    static final @Tainted Log LOG = LogFactory.getLog(
        SshFenceByTcpPort.class.getName() + ".jsch");

    @Override
    public @Tainted boolean isEnabled(SshFenceByTcpPort.@Tainted LogAdapter this, @Tainted int level) {
      switch (level) {
      case com.jcraft.jsch.Logger.DEBUG:
        return LOG.isDebugEnabled();
      case com.jcraft.jsch.Logger.INFO:
        return LOG.isInfoEnabled();
      case com.jcraft.jsch.Logger.WARN:
        return LOG.isWarnEnabled();
      case com.jcraft.jsch.Logger.ERROR:
        return LOG.isErrorEnabled();
      case com.jcraft.jsch.Logger.FATAL:
        return LOG.isFatalEnabled();
      default:
        return false;
      }
    }
      
    @Override
    public void log(SshFenceByTcpPort.@Tainted LogAdapter this, @Tainted int level, @Tainted String message) {
      switch (level) {
      case com.jcraft.jsch.Logger.DEBUG:
        LOG.debug(message);
        break;
      case com.jcraft.jsch.Logger.INFO:
        LOG.info(message);
        break;
      case com.jcraft.jsch.Logger.WARN:
        LOG.warn(message);
        break;
      case com.jcraft.jsch.Logger.ERROR:
        LOG.error(message);
        break;
      case com.jcraft.jsch.Logger.FATAL:
        LOG.fatal(message);
        break;
      }
    }
  }
}
