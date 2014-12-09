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

package org.apache.hadoop.metrics2.filter;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;
import org.apache.commons.configuration.SubsetConfiguration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Base class for pattern based filters
 */
@InterfaceAudience.Private
public abstract class AbstractPatternFilter extends @Tainted MetricsFilter {
  protected static final @Tainted String INCLUDE_KEY = "include";
  protected static final @Tainted String EXCLUDE_KEY = "exclude";
  protected static final @Tainted String INCLUDE_TAGS_KEY = "include.tags";
  protected static final @Tainted String EXCLUDE_TAGS_KEY = "exclude.tags";

  private @Tainted Pattern includePattern;
  private @Tainted Pattern excludePattern;
  private final @Tainted Map<@Tainted String, @Tainted Pattern> includeTagPatterns;
  private final @Tainted Map<@Tainted String, @Tainted Pattern> excludeTagPatterns;
  private final @Tainted Pattern tagPattern = Pattern.compile("^(\\w+):(.*)");

  @Tainted
  AbstractPatternFilter() {
    includeTagPatterns = Maps.newHashMap();
    excludeTagPatterns = Maps.newHashMap();
  }

  @Override
  public void init(@Tainted AbstractPatternFilter this, @Tainted SubsetConfiguration conf) {
    @Tainted
    String patternString = conf.getString(INCLUDE_KEY);
    if (patternString != null && !patternString.isEmpty()) {
      setIncludePattern(compile(patternString));
    }
    patternString = conf.getString(EXCLUDE_KEY);
    if (patternString != null && !patternString.isEmpty()) {
      setExcludePattern(compile(patternString));
    }
    @Tainted
    String @Tainted [] patternStrings = conf.getStringArray(INCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (@Tainted String pstr : patternStrings) {
        @Tainted
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new @Tainted MetricsException("Illegal tag pattern: "+ pstr);
        }
        setIncludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
    patternStrings = conf.getStringArray(EXCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (@Tainted String pstr : patternStrings) {
        @Tainted
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new @Tainted MetricsException("Illegal tag pattern: "+ pstr);
        }
        setExcludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
  }

  void setIncludePattern(@Tainted AbstractPatternFilter this, @Tainted Pattern includePattern) {
    this.includePattern = includePattern;
  }

  void setExcludePattern(@Tainted AbstractPatternFilter this, @Tainted Pattern excludePattern) {
    this.excludePattern = excludePattern;
  }

  void setIncludeTagPattern(@Tainted AbstractPatternFilter this, @Tainted String name, @Tainted Pattern pattern) {
    includeTagPatterns.put(name, pattern);
  }

  void setExcludeTagPattern(@Tainted AbstractPatternFilter this, @Tainted String name, @Tainted Pattern pattern) {
    excludeTagPatterns.put(name, pattern);
  }

  @Override
  public @Tainted boolean accepts(@Tainted AbstractPatternFilter this, @Tainted MetricsTag tag) {
    // Accept if whitelisted
    @Tainted
    Pattern ipat = includeTagPatterns.get(tag.name());
    if (ipat != null && ipat.matcher(tag.value()).matches()) {
      return true;
    }
    // Reject if blacklisted
    @Tainted
    Pattern epat = excludeTagPatterns.get(tag.name());
    if (epat != null && epat.matcher(tag.value()).matches()) {
      return false;
    }
    // Reject if no match in whitelist only mode
    if (ipat != null && epat == null) {
      return false;
    }
    return true;
  }

  @Override
  public @Tainted boolean accepts(@Tainted AbstractPatternFilter this, @Tainted Iterable<@Tainted MetricsTag> tags) {
    // Accept if any include tag pattern matches
    for (@Tainted MetricsTag t : tags) {
      @Tainted
      Pattern pat = includeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return true;
      }
    }
    // Reject if any exclude tag pattern matches
    for (@Tainted MetricsTag t : tags) {
      @Tainted
      Pattern pat = excludeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return false;
      }
    }
    // Reject if no match in whitelist only mode
    if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty()) {
      return false;
    }
    return true;
  }

  @Override
  public @Tainted boolean accepts(@Tainted AbstractPatternFilter this, @Tainted String name) {
    // Accept if whitelisted
    if (includePattern != null && includePattern.matcher(name).matches()) {
      return true;
    }
    // Reject if blacklisted
    if ((excludePattern != null && excludePattern.matcher(name).matches())) {
      return false;
    }
    // Reject if no match in whitelist only mode
    if (includePattern != null && excludePattern == null) {
      return false;
    }
    return true;
  }

  /**
   * Compile a string pattern in to a pattern object
   * @param s the string pattern to compile
   * @return the compiled pattern object
   */
  protected abstract @Tainted Pattern compile(@Tainted AbstractPatternFilter this, @Tainted String s);
}
