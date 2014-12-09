/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * This class allows generic access to variable length type-safe parameter
 * lists.
 */
public class Options {

  public static abstract class StringOption {
    private final @Tainted String value;
    protected @Tainted StringOption(@Tainted String value) {
      this.value = value;
    }
    public @Tainted String getValue(Options.@Tainted StringOption this) {
      return value;
    }
  }

  public static abstract class ClassOption {
    private final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> value;
    protected @Tainted ClassOption(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> value) {
      this.value = value;
    }
    public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getValue(Options.@Tainted ClassOption this) {
      return value;
    }
  }

  public static abstract class BooleanOption {
    private final @Tainted boolean value;
    protected @Tainted BooleanOption(@Tainted boolean value) {
      this.value = value;
    }
    public @Tainted boolean getValue(Options.@Tainted BooleanOption this) {
      return value;
    }
  }

  public static abstract class IntegerOption {
    private final @Tainted int value;
    protected @Tainted IntegerOption(@Tainted int value) {
      this.value = value;
    }
    public @Tainted int getValue(Options.@Tainted IntegerOption this) {
      return value;
    }
  }

  public static abstract class LongOption {
    private final @Tainted long value;
    protected @Tainted LongOption(@Tainted long value) {
      this.value = value;
    }
    public @Tainted long getValue(Options.@Tainted LongOption this) {
      return value;
    }
  }

  public static abstract class PathOption {
    private final @Tainted Path value;
    protected @Tainted PathOption(@Tainted Path value) {
      this.value = value;
    }
    public @Tainted Path getValue(Options.@Tainted PathOption this) {
      return value;
    }
  }

  public static abstract class FSDataInputStreamOption {
    private final @Tainted FSDataInputStream value;
    protected @Tainted FSDataInputStreamOption(@Tainted FSDataInputStream value) {
      this.value = value;
    }
    public @Tainted FSDataInputStream getValue(Options.@Tainted FSDataInputStreamOption this) {
      return value;
    }
  }

  public static abstract class FSDataOutputStreamOption {
    private final @Tainted FSDataOutputStream value;
    protected @Tainted FSDataOutputStreamOption(@Tainted FSDataOutputStream value) {
      this.value = value;
    }
    public @Tainted FSDataOutputStream getValue(Options.@Tainted FSDataOutputStreamOption this) {
      return value;
    }
  }

  public static abstract class ProgressableOption {
    private final @Tainted Progressable value;
    protected @Tainted ProgressableOption(@Tainted Progressable value) {
      this.value = value;
    }
    public @Tainted Progressable getValue(Options.@Tainted ProgressableOption this) {
      return value;
    }
  }

  /**
   * Find the first option of the required class.
   * @param <T> the static class to find
   * @param <base> the parent class of the array
   * @param cls the dynamic class to find
   * @param opts the list of options to look through
   * @return the first option that matches
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <@Tainted base extends java.lang.@Tainted Object, @Tainted T extends @Tainted base> @Tainted T getOption(@Tainted Class<@Tainted T> cls, @Tainted base @Tainted [] opts
                                                   ) throws IOException {
    for(@Tainted base o: opts) {
      if (o.getClass() == cls) {
        return (@Tainted T) o;
      }
    }
    return null;
  }

  /**
   * Prepend some new options to the old options
   * @param <T> the type of options
   * @param oldOpts the old options
   * @param newOpts the new options
   * @return a new array of options
   */
  public static <@Tainted T extends java.lang.@Tainted Object> @Tainted T @Tainted [] prependOptions(@Tainted T @Tainted [] oldOpts, @Tainted T @Tainted ... newOpts) {
    // copy the new options to the front of the array
    @Tainted
    T @Tainted [] result = Arrays.copyOf(newOpts, newOpts.length+oldOpts.length);
    // now copy the old options
    System.arraycopy(oldOpts, 0, result, newOpts.length, oldOpts.length);
    return result;
  }
}
