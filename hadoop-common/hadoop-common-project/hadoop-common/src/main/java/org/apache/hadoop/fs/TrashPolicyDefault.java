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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;

/** Provides a <i>trash</i> feature.  Files are moved to a user's trash
 * directory, a subdirectory of their home directory named ".Trash".  Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TrashPolicyDefault extends @Tainted TrashPolicy {
  private static final @Tainted Log LOG =
    LogFactory.getLog(TrashPolicyDefault.class);

  private static final @Tainted Path CURRENT = new @Tainted Path("Current");
  private static final @Tainted Path TRASH = new @Tainted Path(".Trash/");  

  private static final @Tainted FsPermission PERMISSION =
    new @Tainted FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private static final @Tainted DateFormat CHECKPOINT = new @Tainted SimpleDateFormat("yyMMddHHmmss");
  /** Format of checkpoint directories used prior to Hadoop 0.23. */
  private static final @Tainted DateFormat OLD_CHECKPOINT =
      new @Tainted SimpleDateFormat("yyMMddHHmm");
  private static final @Tainted int MSECS_PER_MINUTE = 60*1000;

  private @Tainted Path current;
  private @Tainted Path homesParent;
  private @Tainted long emptierInterval;

  public @Tainted TrashPolicyDefault() { }

  private @Tainted TrashPolicyDefault(@Tainted FileSystem fs, @Tainted Path home, @Tainted Configuration conf)
      throws IOException {
    initialize(conf, fs, home);
  }

  @Override
  public void initialize(@Tainted TrashPolicyDefault this, @Tainted Configuration conf, @Tainted FileSystem fs, @Tainted Path home) {
    this.fs = fs;
    this.trash = new @Tainted Path(home, TRASH);
    this.homesParent = home.getParent();
    this.current = new @Tainted Path(trash, CURRENT);
    this.deletionInterval = (@Tainted long)(conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.emptierInterval = (@Tainted long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    LOG.info("Namenode trash configuration: Deletion interval = " +
             this.deletionInterval + " minutes, Emptier interval = " +
             this.emptierInterval + " minutes.");
   }

  private @Tainted Path makeTrashRelativePath(@Tainted TrashPolicyDefault this, @Tainted Path basePath, @Tainted Path rmFilePath) {
    return Path.mergePaths(basePath, rmFilePath);
  }

  @Override
  public @Tainted boolean isEnabled(@Tainted TrashPolicyDefault this) {
    return deletionInterval != 0;
  }

  @Override
  public @Tainted boolean moveToTrash(@Tainted TrashPolicyDefault this, @Tainted Path path) throws IOException {
    if (!isEnabled())
      return false;

    if (!path.isAbsolute())                       // make path absolute
      path = new @Tainted Path(fs.getWorkingDirectory(), path);

    if (!fs.exists(path))                         // check that path exists
      throw new @Tainted FileNotFoundException(path.toString());

    @Tainted
    String qpath = fs.makeQualified(path).toString();

    if (qpath.startsWith(trash.toString())) {
      return false;                               // already in trash
    }

    if (trash.getParent().toString().startsWith(qpath)) {
      throw new @Tainted IOException("Cannot move \"" + path +
                            "\" to the trash, as it contains the trash");
    }

    @Tainted
    Path trashPath = makeTrashRelativePath(current, path);
    @Tainted
    Path baseTrashPath = makeTrashRelativePath(current, path.getParent());
    
    @Tainted
    IOException cause = null;

    // try twice, in case checkpoint between the mkdirs() & rename()
    for (@Tainted int i = 0; i < 2; i++) {
      try {
        if (!fs.mkdirs(baseTrashPath, PERMISSION)) {      // create current
          LOG.warn("Can't create(mkdir) trash directory: "+baseTrashPath);
          return false;
        }
      } catch (@Tainted IOException e) {
        LOG.warn("Can't create trash directory: "+baseTrashPath);
        cause = e;
        break;
      }
      try {
        // if the target path in Trash already exists, then append with 
        // a current time in millisecs.
        @Tainted
        String orig = trashPath.toString();
        
        while(fs.exists(trashPath)) {
          trashPath = new @Tainted Path(orig + Time.now());
        }
        
        if (fs.rename(path, trashPath))           // move to current trash
          return true;
      } catch (@Tainted IOException e) {
        cause = e;
      }
    }
    throw (@Tainted IOException)
      new @Tainted IOException("Failed to move to trash: "+path).initCause(cause);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createCheckpoint(@Tainted TrashPolicyDefault this) throws IOException {
    if (!fs.exists(current))                     // no trash, no checkpoint
      return;

    @Tainted
    Path checkpointBase;
    synchronized (CHECKPOINT) {
      checkpointBase = new @Tainted Path(trash, CHECKPOINT.format(new @Tainted Date()));
    }
    @Tainted
    Path checkpoint = checkpointBase;

    @Tainted
    int attempt = 0;
    while (true) {
      try {
        fs.rename(current, checkpoint, Rename.NONE);
        break;
      } catch (@Tainted FileAlreadyExistsException e) {
        if (++attempt > 1000) {
          throw new @Tainted IOException("Failed to checkpoint trash: "+checkpoint);
        }
        checkpoint = checkpointBase.suffix("-" + attempt);
      }
    }

    LOG.info("Created trash checkpoint: "+checkpoint.toUri().getPath());
  }

  @Override
  public void deleteCheckpoint(@Tainted TrashPolicyDefault this) throws IOException {
    @Tainted
    FileStatus @Tainted [] dirs = null;
    
    try {
      dirs = fs.listStatus(trash);            // scan trash sub-directories
    } catch (@Tainted FileNotFoundException fnfe) {
      return;
    }

    @Tainted
    long now = Time.now();
    for (@Tainted int i = 0; i < dirs.length; i++) {
      @Tainted
      Path path = dirs[i].getPath();
      @Tainted
      String dir = path.toUri().getPath();
      @Tainted
      String name = path.getName();
      if (name.equals(CURRENT.getName()))         // skip current
        continue;

      @Tainted
      long time;
      try {
        time = getTimeFromCheckpoint(name);
      } catch (@Tainted ParseException e) {
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if ((now - deletionInterval) > time) {
        if (fs.delete(path, true)) {
          LOG.info("Deleted trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't delete checkpoint: "+dir+" Ignoring.");
        }
      }
    }
  }

  @Override
  public @Tainted Path getCurrentTrashDir(@Tainted TrashPolicyDefault this) {
    return current;
  }

  @Override
  public @Tainted Runnable getEmptier(@Tainted TrashPolicyDefault this) throws IOException {
    return new @Tainted Emptier(getConf(), emptierInterval);
  }

  private class Emptier implements @Tainted Runnable {

    private @Tainted Configuration conf;
    private @Tainted long emptierInterval;

    @Tainted
    Emptier(@Tainted Configuration conf, @Tainted long emptierInterval) throws IOException {
      this.conf = conf;
      this.emptierInterval = emptierInterval;
      if (emptierInterval > deletionInterval || emptierInterval == 0) {
        LOG.info("The configured checkpoint interval is " +
                 (emptierInterval / MSECS_PER_MINUTE) + " minutes." +
                 " Using an interval of " +
                 (deletionInterval / MSECS_PER_MINUTE) +
                 " minutes that is used for deletion instead");
        this.emptierInterval = deletionInterval;
      }
    }

    @Override
    public void run(@Tainted TrashPolicyDefault.Emptier this) {
      if (emptierInterval == 0)
        return;                                   // trash disabled
      @Tainted
      long now = Time.now();
      @Tainted
      long end;
      while (true) {
        end = ceiling(now, emptierInterval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (@Tainted InterruptedException e) {
          break;                                  // exit on interrupt
        }

        try {
          now = Time.now();
          if (now >= end) {

            @Tainted
            FileStatus @Tainted [] homes = null;
            try {
              homes = fs.listStatus(homesParent);         // list all home dirs
            } catch (@Tainted IOException e) {
              LOG.warn("Trash can't list homes: "+e+" Sleeping.");
              continue;
            }

            for (@Tainted FileStatus home : homes) {         // dump each trash
              if (!home.isDirectory())
                continue;
              try {
                @Tainted
                TrashPolicyDefault trash = new @Tainted TrashPolicyDefault(
                    fs, home.getPath(), conf);
                trash.deleteCheckpoint();
                trash.createCheckpoint();
              } catch (@Tainted IOException e) {
                LOG.warn("Trash caught: "+e+". Skipping "+home.getPath()+".");
              } 
            }
          }
        } catch (@Tainted Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run(): ", e); 
        }
      }
      try {
        fs.close();
      } catch(@Tainted IOException e) {
        LOG.warn("Trash cannot close FileSystem: ", e);
      }
    }

    private @Tainted long ceiling(@Tainted TrashPolicyDefault.Emptier this, @Tainted long time, @Tainted long interval) {
      return floor(time, interval) + interval;
    }
    private @Tainted long floor(@Tainted TrashPolicyDefault.Emptier this, @Tainted long time, @Tainted long interval) {
      return (time / interval) * interval;
    }
  }

  private @Tainted long getTimeFromCheckpoint(@Tainted TrashPolicyDefault this, @Tainted String name) throws ParseException {
    @Tainted
    long time;

    try {
      synchronized (CHECKPOINT) {
        time = CHECKPOINT.parse(name).getTime();
      }
    } catch (@Tainted ParseException pe) {
      // Check for old-style checkpoint directories left over
      // after an upgrade from Hadoop 1.x
      synchronized (OLD_CHECKPOINT) {
        time = OLD_CHECKPOINT.parse(name).getTime();
      }
    }

    return time;
  }
}
