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

package org.apache.hadoop.io;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A wrapper for Writable instances.
 * <p>
 * When two sequence files, which have same Key type but different Value
 * types, are mapped out to reduce, multiple Value types is not allowed.
 * In this case, this class can help you wrap instances with different types.
 * </p>
 * 
 * <p>
 * Compared with <code>ObjectWritable</code>, this class is much more effective,
 * because <code>ObjectWritable</code> will append the class declaration as a String 
 * into the output file in every Key-Value pair.
 * </p>
 * 
 * <p>
 * Generic Writable implements {@link Configurable} interface, so that it will be 
 * configured by the framework. The configuration is passed to the wrapped objects
 * implementing {@link Configurable} interface <i>before deserialization</i>. 
 * </p>
 * 
 * how to use it: <br>
 * 1. Write your own class, such as GenericObject, which extends GenericWritable.<br> 
 * 2. Implements the abstract method <code>getTypes()</code>, defines 
 *    the classes which will be wrapped in GenericObject in application.
 *    Attention: this classes defined in <code>getTypes()</code> method, must
 *    implement <code>Writable</code> interface.
 * <br><br>
 * 
 * The code looks like this:
 * <blockquote><pre>
 * public class GenericObject extends GenericWritable {
 * 
 *   private static Class[] CLASSES = {
 *               ClassType1.class, 
 *               ClassType2.class,
 *               ClassType3.class,
 *               };
 *
 *   protected Class[] getTypes() {
 *       return CLASSES;
 *   }
 *
 * }
 * </pre></blockquote>
 * 
 * @since Nov 8, 2006
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class GenericWritable implements @Tainted Writable, @Tainted Configurable {

  private static final @Tainted byte NOT_SET = -1;

  private @Tainted byte type = NOT_SET;

  private @Tainted Writable instance;

  private @Tainted Configuration conf = null;
  
  /**
   * Set the instance that is wrapped.
   * 
   * @param obj
   */
  public void set(@Tainted GenericWritable this, @Tainted Writable obj) {
    instance = obj;
    @Tainted
    Class<@Tainted ? extends @Tainted Writable> instanceClazz = instance.getClass();
    @Tainted
    Class<@Tainted ? extends @Tainted Writable> @Tainted [] clazzes = getTypes();
    for (@Tainted int i = 0; i < clazzes.length; i++) {
      @Tainted
      Class<@Tainted ? extends @Tainted Writable> clazz = clazzes[i];
      if (clazz.equals(instanceClazz)) {
        type = (@Tainted byte) i;
        return;
      }
    }
    throw new @Tainted RuntimeException("The type of instance is: "
                               + instance.getClass() + ", which is NOT registered.");
  }

  /**
   * Return the wrapped instance.
   */
  public @Tainted Writable get(@Tainted GenericWritable this) {
    return instance;
  }
  
  @Override
  public @Tainted String toString(@Tainted GenericWritable this) {
    return "GW[" + (instance != null ? ("class=" + instance.getClass().getName() +
        ",value=" + instance.toString()) : "(null)") + "]";
  }

  @Override
  public void readFields(@Tainted GenericWritable this, @Tainted DataInput in) throws IOException {
    type = in.readByte();
    @Tainted
    Class<@Tainted ? extends @Tainted Writable> clazz = getTypes()[type & 0xff];
    try {
      instance = ReflectionUtils.newInstance(clazz, conf);
    } catch (@Tainted Exception e) {
      e.printStackTrace();
      throw new @Tainted IOException("Cannot initialize the class: " + clazz);
    }
    instance.readFields(in);
  }

  @Override
  public void write(@Tainted GenericWritable this, @Tainted DataOutput out) throws IOException {
    if (type == NOT_SET || instance == null)
      throw new @Tainted IOException("The GenericWritable has NOT been set correctly. type="
                            + type + ", instance=" + instance);
    out.writeByte(type);
    instance.write(out);
  }

  /**
   * Return all classes that may be wrapped.  Subclasses should implement this
   * to return a constant array of classes.
   */
  abstract protected @Tainted Class<@Tainted ? extends @Tainted Writable> @Tainted [] getTypes(@Tainted GenericWritable this);

  @Override
  public @Tainted Configuration getConf(@Tainted GenericWritable this) {
    return conf;
  }

  @Override
  public void setConf(@Tainted GenericWritable this, @Tainted Configuration conf) {
    this.conf = conf;
  }
  
}
