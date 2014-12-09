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
package org.apache.hadoop.record.compiler.ant;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.compiler.generated.Rcc;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

/**
 * Hadoop record compiler ant Task
 *<p> This task takes the given record definition files and compiles them into
 * java or c++
 * files. It is then up to the user to compile the generated files.
 *
 * <p> The task requires the <code>file</code> or the nested fileset element to be
 * specified. Optional attributes are <code>language</code> (set the output
 * language, default is "java"),
 * <code>destdir</code> (name of the destination directory for generated java/c++
 * code, default is ".") and <code>failonerror</code> (specifies error handling
 * behavior. default is true).
 * <p><h4>Usage</h4>
 * <pre>
 * &lt;recordcc
 *       destdir="${basedir}/gensrc"
 *       language="java"&gt;
 *   &lt;fileset include="**\/*.jr" /&gt;
 * &lt;/recordcc&gt;
 * </pre>
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RccTask extends @Tainted Task {
  
  private @Tainted String language = "java";
  private @Tainted File src;
  private @Tainted File dest = new @Tainted File(".");
  private final @Tainted ArrayList<@Tainted FileSet> filesets = new @Tainted ArrayList<@Tainted FileSet>();
  private @Tainted boolean failOnError = true;
  
  /** Creates a new instance of RccTask */
  public @Tainted RccTask() {
  }
  
  /**
   * Sets the output language option
   * @param language "java"/"c++"
   */
  public void setLanguage(@Tainted RccTask this, @Tainted String language) {
    this.language = language;
  }
  
  /**
   * Sets the record definition file attribute
   * @param file record definition file
   */
  public void setFile(@Tainted RccTask this, @Tainted File file) {
    this.src = file;
  }
  
  /**
   * Given multiple files (via fileset), set the error handling behavior
   * @param flag true will throw build exception in case of failure (default)
   */
  public void setFailonerror(@Tainted RccTask this, @Tainted boolean flag) {
    this.failOnError = flag;
  }
  
  /**
   * Sets directory where output files will be generated
   * @param dir output directory
   */
  public void setDestdir(@Tainted RccTask this, @Tainted File dir) {
    this.dest = dir;
  }
  
  /**
   * Adds a fileset that can consist of one or more files
   * @param set Set of record definition files
   */
  public void addFileset(@Tainted RccTask this, @Tainted FileSet set) {
    filesets.add(set);
  }
  
  /**
   * Invoke the Hadoop record compiler on each record definition file
   */
  @Override
  public void execute(@Tainted RccTask this) throws BuildException {
    if (src == null && filesets.size()==0) {
      throw new @Tainted BuildException("There must be a file attribute or a fileset child element");
    }
    if (src != null) {
      doCompile(src);
    }
    @Tainted
    Project myProject = getProject();
    for (@Tainted int i = 0; i < filesets.size(); i++) {
      @Tainted
      FileSet fs = filesets.get(i);
      @Tainted
      DirectoryScanner ds = fs.getDirectoryScanner(myProject);
      @Tainted
      File dir = fs.getDir(myProject);
      @Tainted
      String @Tainted [] srcs = ds.getIncludedFiles();
      for (@Tainted int j = 0; j < srcs.length; j++) {
        doCompile(new @Tainted File(dir, srcs[j]));
      }
    }
  }
  
  private void doCompile(@Tainted RccTask this, @Tainted File file) throws BuildException {
    @Tainted
    String @Tainted [] args = new @Tainted String @Tainted [5];
    args[0] = "--language";
    args[1] = this.language;
    args[2] = "--destdir";
    args[3] = this.dest.getPath();
    args[4] = file.getPath();
    @Tainted
    int retVal = Rcc.driver(args);
    if (retVal != 0 && failOnError) {
      throw new @Tainted BuildException("Hadoop record compiler returned error code "+retVal);
    }
  }
}
