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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;

import com.google.common.net.InetAddresses;

/**
 * General string utils
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StringUtils {

  /**
   * Priority of the StringUtils shutdown hook.
   */
  public static final @Tainted int SHUTDOWN_HOOK_PRIORITY = 0;

  /**
   * Shell environment variables: $ followed by one letter or _ followed by
   * multiple letters, numbers, or underscores.  The group captures the
   * environment variable name without the leading $.
   */
  public static final @Tainted Pattern SHELL_ENV_VAR_PATTERN =
    Pattern.compile("\\$([A-Za-z_]{1}[A-Za-z0-9_]*)");

  /**
   * Windows environment variables: surrounded by %.  The group captures the
   * environment variable name without the leading and trailing %.
   */
  public static final @Tainted Pattern WIN_ENV_VAR_PATTERN = Pattern.compile("%(.*?)%");

  /**
   * Regular expression that matches and captures environment variable names
   * according to platform-specific rules.
   */
  public static final @Tainted Pattern ENV_VAR_PATTERN = Shell.WINDOWS ?
    WIN_ENV_VAR_PATTERN : SHELL_ENV_VAR_PATTERN;

  /**
   * Make a string representation of the exception.
   * @param e The exception to stringify
   * @return A string with exception name and call stack.
   */
  public static @Tainted String stringifyException(@Tainted Throwable e) {
    @Tainted
    StringWriter stm = new @Tainted StringWriter();
    @Tainted
    PrintWriter wrt = new @Tainted PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }
  
  /**
   * Given a full hostname, return the word upto the first dot.
   * @param fullHostname the full hostname
   * @return the hostname to the first dot
   */
  public static @Tainted String simpleHostname(@Tainted String fullHostname) {
    if (InetAddresses.isInetAddress(fullHostname)) {
      return fullHostname;
    }
    @Tainted
    int offset = fullHostname.indexOf('.');
    if (offset != -1) {
      return fullHostname.substring(0, offset);
    }
    return fullHostname;
  }
  
  /**
   * Given an integer, return a string that is in an approximate, but human 
   * readable format. 
   * @param number the number to format
   * @return a human readable form of the integer
   *
   * @deprecated use {@link TraditionalBinaryPrefix#long2String(long, String, int)}.
   */
  @Deprecated
  public static @Tainted String humanReadableInt(@Tainted long number) {
    return TraditionalBinaryPrefix.long2String(number, "", 1);
  }

  /** The same as String.format(Locale.ENGLISH, format, objects). */
  public static @Tainted String format(final @Tainted String format, final @Tainted Object @Tainted ... objects) {
    return String.format(Locale.ENGLISH, format, objects);
  }

  /**
   * Format a percentage for presentation to the user.
   * @param fraction the percentage as a fraction, e.g. 0.1 = 10%
   * @param decimalPlaces the number of decimal places
   * @return a string representation of the percentage
   */
  public static @Tainted String formatPercent(@Tainted double fraction, @Tainted int decimalPlaces) {
    return format("%." + decimalPlaces + "f%%", fraction*100);
  }
  
  /**
   * Given an array of strings, return a comma-separated list of its elements.
   * @param strs Array of strings
   * @return Empty string if strs.length is 0, comma separated list of strings
   * otherwise
   */
  
  public static @Tainted String arrayToString(@Tainted String @Tainted [] strs) {
    if (strs.length == 0) { return ""; }
    @Tainted
    StringBuilder sbuf = new @Tainted StringBuilder();
    sbuf.append(strs[0]);
    for (@Tainted int idx = 1; idx < strs.length; idx++) {
      sbuf.append(",");
      sbuf.append(strs[idx]);
    }
    return sbuf.toString();
  }

  /**
   * Given an array of bytes it will convert the bytes to a hex string
   * representation of the bytes
   * @param bytes
   * @param start start index, inclusively
   * @param end end index, exclusively
   * @return hex string representation of the byte array
   */
  public static @Tainted String byteToHexString(@Tainted byte @Tainted [] bytes, @Tainted int start, @Tainted int end) {
    if (bytes == null) {
      throw new @Tainted IllegalArgumentException("bytes == null");
    }
    @Tainted
    StringBuilder s = new @Tainted StringBuilder(); 
    for(@Tainted int i = start; i < end; i++) {
      s.append(format("%02x", bytes[i]));
    }
    return s.toString();
  }

  /** Same as byteToHexString(bytes, 0, bytes.length). */
  public static @Tainted String byteToHexString(@Tainted byte bytes @Tainted []) {
    return byteToHexString(bytes, 0, bytes.length);
  }

  /**
   * Given a hexstring this will return the byte array corresponding to the
   * string
   * @param hex the hex String array
   * @return a byte array that is a hex string representation of the given
   *         string. The size of the byte array is therefore hex.length/2
   */
  public static @Tainted byte @Tainted [] hexStringToByte(@Tainted String hex) {
    @Tainted
    byte @Tainted [] bts = new @Tainted byte @Tainted [hex.length() / 2];
    for (@Tainted int i = 0; i < bts.length; i++) {
      bts[i] = (@Tainted byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return bts;
  }
  /**
   * 
   * @param uris
   */
  public static @Tainted String uriToString(@Tainted URI @Tainted [] uris){
    if (uris == null) {
      return null;
    }
    @Tainted
    StringBuilder ret = new @Tainted StringBuilder(uris[0].toString());
    for(@Tainted int i = 1; i < uris.length;i++){
      ret.append(",");
      ret.append(uris[i].toString());
    }
    return ret.toString();
  }
  
  /**
   * @param str
   *          The string array to be parsed into an URI array.
   * @return <tt>null</tt> if str is <tt>null</tt>, else the URI array
   *         equivalent to str.
   * @throws IllegalArgumentException
   *           If any string in str violates RFC&nbsp;2396.
   */
  public static @Tainted URI @Tainted [] stringToURI(@Tainted String @Tainted [] str){
    if (str == null) 
      return null;
    @Tainted
    URI @Tainted [] uris = new @Tainted URI @Tainted [str.length];
    for (@Tainted int i = 0; i < str.length;i++){
      try{
        uris[i] = new @Tainted URI(str[i]);
      }catch(@Tainted URISyntaxException ur){
        throw new @Tainted IllegalArgumentException(
            "Failed to create uri for " + str[i], ur);
      }
    }
    return uris;
  }
  
  /**
   * 
   * @param str
   */
  public static @Tainted Path @Tainted [] stringToPath(@Tainted String @Tainted [] str){
    if (str == null) {
      return null;
    }
    @Tainted
    Path @Tainted [] p = new @Tainted Path @Tainted [str.length];
    for (@Tainted int i = 0; i < str.length;i++){
      p[i] = new @Tainted Path(str[i]);
    }
    return p;
  }
  /**
   * 
   * Given a finish and start time in long milliseconds, returns a 
   * String in the format Xhrs, Ymins, Z sec, for the time difference between two times. 
   * If finish time comes before start time then negative valeus of X, Y and Z wil return. 
   * 
   * @param finishTime finish time
   * @param startTime start time
   */
  public static @Tainted String formatTimeDiff(@Tainted long finishTime, @Tainted long startTime){
    @Tainted
    long timeDiff = finishTime - startTime; 
    return formatTime(timeDiff); 
  }
  
  /**
   * 
   * Given the time in long milliseconds, returns a 
   * String in the format Xhrs, Ymins, Z sec. 
   * 
   * @param timeDiff The time difference to format
   */
  public static @Tainted String formatTime(@Tainted long timeDiff){
    @Tainted
    StringBuilder buf = new @Tainted StringBuilder();
    @Tainted
    long hours = timeDiff / (60*60*1000);
    @Tainted
    long rem = (timeDiff % (60*60*1000));
    @Tainted
    long minutes =  rem / (60*1000);
    rem = rem % (60*1000);
    @Tainted
    long seconds = rem / 1000;
    
    if (hours != 0){
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0){
      buf.append(minutes);
      buf.append("mins, ");
    }
    // return "0sec if no difference
    buf.append(seconds);
    buf.append("sec");
    return buf.toString(); 
  }
  /**
   * Formats time in ms and appends difference (finishTime - startTime) 
   * as returned by formatTimeDiff().
   * If finish time is 0, empty string is returned, if start time is 0 
   * then difference is not appended to return value. 
   * @param dateFormat date format to use
   * @param finishTime fnish time
   * @param startTime start time
   * @return formatted value. 
   */
  public static @Tainted String getFormattedTimeWithDiff(@Tainted DateFormat dateFormat, 
                                                @Tainted
                                                long finishTime, @Tainted long startTime){
    @Tainted
    StringBuilder buf = new @Tainted StringBuilder();
    if (0 != finishTime) {
      buf.append(dateFormat.format(new @Tainted Date(finishTime)));
      if (0 != startTime){
        buf.append(" (" + formatTimeDiff(finishTime , startTime) + ")");
      }
    }
    return buf.toString();
  }
  
  /**
   * Returns an arraylist of strings.
   * @param str the comma seperated string values
   * @return the arraylist of the comma seperated string values
   */
  public static @Tainted String @Tainted [] getStrings(@Tainted String str){
    @Tainted
    Collection<@Tainted String> values = getStringCollection(str);
    if(values.size() == 0) {
      return null;
    }
    return values.toArray(new @Tainted String @Tainted [values.size()]);
  }

  /**
   * Returns a collection of strings.
   * @param str comma seperated string values
   * @return an <code>ArrayList</code> of string values
   */
  public static @Tainted Collection<@Tainted String> getStringCollection(@Tainted String str){
    @Tainted
    List<@Tainted String> values = new @Tainted ArrayList<@Tainted String>();
    if (str == null)
      return values;
    @Tainted
    StringTokenizer tokenizer = new @Tainted StringTokenizer (str,",");
    values = new @Tainted ArrayList<@Tainted String>();
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return values;
  }

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <String> with values
   * @return a <code>Collection</code> of <code>String</code> values
   */
  public static @Tainted Collection<@Tainted String> getTrimmedStringCollection(@Tainted String str){
    return new @Tainted ArrayList<@Tainted String>(
      Arrays.asList(getTrimmedStrings(str)));
  }
  
  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <String> with values
   * @return an array of <code>String</code> values
   */
  public static @Tainted String @Tainted [] getTrimmedStrings(@Tainted String str){
    if (null == str || str.trim().isEmpty()) {
      return emptyStringArray;
    }

    return str.trim().split("\\s*,\\s*");
  }

  final public static @Untainted String @Tainted [] emptyStringArray = new String @Tainted [] {};
  final public static @Tainted char COMMA = ',';
  final public static @Tainted String COMMA_STR = ",";
  final public static @Tainted char ESCAPE_CHAR = '\\';
  
  /**
   * Split a string using the default separator
   * @param str a string that may have escaped separator
   * @return an array of strings
   */
  public static @Tainted String @Tainted [] split(@Tainted String str) {
    return split(str, ESCAPE_CHAR, COMMA);
  }
  
  /**
   * Split a string using the given separator
   * @param str a string that may have escaped separator
   * @param escapeChar a char that be used to escape the separator
   * @param separator a separator char
   * @return an array of strings
   */
  public static @Tainted String @Tainted [] split(
      @Tainted
      String str, @Tainted char escapeChar, @Tainted char separator) {
    if (str==null) {
      return null;
    }
    @Tainted
    ArrayList<@Tainted String> strList = new @Tainted ArrayList<@Tainted String>();
    @Tainted
    StringBuilder split = new @Tainted StringBuilder();
    @Tainted
    int index = 0;
    while ((index = findNext(str, separator, escapeChar, index, split)) >= 0) {
      ++index; // move over the separator for next search
      strList.add(split.toString());
      split.setLength(0); // reset the buffer 
    }
    strList.add(split.toString());
    // remove trailing empty split(s)
    @Tainted
    int last = strList.size(); // last split
    while (--last>=0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new @Tainted String @Tainted [strList.size()]);
  }

  /**
   * Split a string using the given separator, with no escaping performed.
   * @param str a string to be split. Note that this may not be null.
   * @param separator a separator char
   * @return an array of strings
   */
  public static @Tainted String @Tainted [] split(
      @Tainted
      String str, @Tainted char separator) {
    // String.split returns a single empty result for splitting the empty
    // string.
    if (str.isEmpty()) {
      return new @Tainted String @Tainted []{""};
    }
    @Tainted
    ArrayList<@Tainted String> strList = new @Tainted ArrayList<@Tainted String>();
    @Tainted
    int startIndex = 0;
    @Tainted
    int nextIndex = 0;
    while ((nextIndex = str.indexOf((@Tainted int)separator, startIndex)) != -1) {
      strList.add(str.substring(startIndex, nextIndex));
      startIndex = nextIndex + 1;
    }
    strList.add(str.substring(startIndex));
    // remove trailing empty split(s)
    @Tainted
    int last = strList.size(); // last split
    while (--last>=0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new @Tainted String @Tainted [strList.size()]);
  }
  
  /**
   * Finds the first occurrence of the separator character ignoring the escaped
   * separators starting from the index. Note the substring between the index
   * and the position of the separator is passed.
   * @param str the source string
   * @param separator the character to find
   * @param escapeChar character used to escape
   * @param start from where to search
   * @param split used to pass back the extracted string
   */
  public static @Tainted int findNext(@Tainted String str, @Tainted char separator, @Tainted char escapeChar, 
                             @Tainted
                             int start, @Tainted StringBuilder split) {
    @Tainted
    int numPreEscapes = 0;
    for (@Tainted int i = start; i < str.length(); i++) {
      @Tainted
      char curChar = str.charAt(i);
      if (numPreEscapes == 0 && curChar == separator) { // separator 
        return i;
      } else {
        split.append(curChar);
        numPreEscapes = (curChar == escapeChar)
                        ? (++numPreEscapes) % 2
                        : 0;
      }
    }
    return -1;
  }
  
  /**
   * Escape commas in the string using the default escape char
   * @param str a string
   * @return an escaped string
   */
  public static @Tainted String escapeString(@Tainted String str) {
    return escapeString(str, ESCAPE_CHAR, COMMA);
  }
  
  /**
   * Escape <code>charToEscape</code> in the string 
   * with the escape char <code>escapeChar</code>
   * 
   * @param str string
   * @param escapeChar escape char
   * @param charToEscape the char to be escaped
   * @return an escaped string
   */
  public static @Tainted String escapeString(
      @Tainted
      String str, @Tainted char escapeChar, @Tainted char charToEscape) {
    return escapeString(str, escapeChar, new @Tainted char @Tainted [] {charToEscape});
  }
  
  // check if the character array has the character 
  private static @Tainted boolean hasChar(@Tainted char @Tainted [] chars, @Tainted char character) {
    for (@Tainted char target : chars) {
      if (character == target) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * @param charsToEscape array of characters to be escaped
   */
  public static @Tainted String escapeString(@Tainted String str, @Tainted char escapeChar, 
                                    @Tainted
                                    char @Tainted [] charsToEscape) {
    if (str == null) {
      return null;
    }
    @Tainted
    StringBuilder result = new @Tainted StringBuilder();
    for (@Tainted int i=0; i<str.length(); i++) {
      @Tainted
      char curChar = str.charAt(i);
      if (curChar == escapeChar || hasChar(charsToEscape, curChar)) {
        // special char
        result.append(escapeChar);
      }
      result.append(curChar);
    }
    return result.toString();
  }
  
  /**
   * Unescape commas in the string using the default escape char
   * @param str a string
   * @return an unescaped string
   */
  public static @Tainted String unEscapeString(@Tainted String str) {
    return unEscapeString(str, ESCAPE_CHAR, COMMA);
  }
  
  /**
   * Unescape <code>charToEscape</code> in the string 
   * with the escape char <code>escapeChar</code>
   * 
   * @param str string
   * @param escapeChar escape char
   * @param charToEscape the escaped char
   * @return an unescaped string
   */
  public static @Tainted String unEscapeString(
      @Tainted
      String str, @Tainted char escapeChar, @Tainted char charToEscape) {
    return unEscapeString(str, escapeChar, new @Tainted char @Tainted [] {charToEscape});
  }
  
  /**
   * @param charsToEscape array of characters to unescape
   */
  public static @Tainted String unEscapeString(@Tainted String str, @Tainted char escapeChar, 
                                      @Tainted
                                      char @Tainted [] charsToEscape) {
    if (str == null) {
      return null;
    }
    @Tainted
    StringBuilder result = new @Tainted StringBuilder(str.length());
    @Tainted
    boolean hasPreEscape = false;
    for (@Tainted int i=0; i<str.length(); i++) {
      @Tainted
      char curChar = str.charAt(i);
      if (hasPreEscape) {
        if (curChar != escapeChar && !hasChar(charsToEscape, curChar)) {
          // no special char
          throw new @Tainted IllegalArgumentException("Illegal escaped string " + str + 
              " unescaped " + escapeChar + " at " + (i-1));
        } 
        // otherwise discard the escape char
        result.append(curChar);
        hasPreEscape = false;
      } else {
        if (hasChar(charsToEscape, curChar)) {
          throw new @Tainted IllegalArgumentException("Illegal escaped string " + str + 
              " unescaped " + curChar + " at " + i);
        } else if (curChar == escapeChar) {
          hasPreEscape = true;
        } else {
          result.append(curChar);
        }
      }
    }
    if (hasPreEscape ) {
      throw new @Tainted IllegalArgumentException("Illegal escaped string " + str + 
          ", not expecting " + escapeChar + " in the end." );
    }
    return result.toString();
  }
  
  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  private static @Tainted String toStartupShutdownString(@Tainted String prefix, @Tainted String @Tainted [] msg) {
    @Tainted
    StringBuilder b = new @Tainted StringBuilder(prefix);
    b.append("\n/************************************************************");
    for(@Tainted String s : msg)
      b.append("\n" + prefix + s);
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Print a log message for starting up and shutting down
   * @param clazz the class of the server
   * @param args arguments
   * @param LOG the target log object
   */
  public static void startupShutdownMessage(@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> clazz, @Tainted String @Tainted [] args,
                                     final org.apache.commons.logging.Log LOG) {
    final @Tainted String hostname = NetUtils.getHostname();
    final @Tainted String classname = clazz.getSimpleName();
    LOG.info(
        toStartupShutdownString("STARTUP_MSG: ", new @Tainted String @Tainted [] {
            "Starting " + classname,
            "  host = " + hostname,
            "  args = " + Arrays.asList(args),
            "  version = " + VersionInfo.getVersion(),
            "  classpath = " + System.getProperty("java.class.path"),
            "  build = " + VersionInfo.getUrl() + " -r "
                         + VersionInfo.getRevision()  
                         + "; compiled by '" + VersionInfo.getUser()
                         + "' on " + VersionInfo.getDate(),
            "  java = " + System.getProperty("java.version") }
        )
      );

    if (SystemUtils.IS_OS_UNIX) {
      try {
        SignalLogger.INSTANCE.register(LOG);
      } catch (@Tainted Throwable t) {
        LOG.warn("failed to register any UNIX signal loggers: ", t);
      }
    }
    ShutdownHookManager.get().addShutdownHook(
      new @Tainted Runnable() {
        @Override
        public void run() {
          LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new @Tainted String @Tainted []{
            "Shutting down " + classname + " at " + hostname}));
        }
      }, SHUTDOWN_HOOK_PRIORITY);

  }

  /**
   * The traditional binary prefixes, kilo, mega, ..., exa,
   * which can be represented by a 64-bit integer.
   * TraditionalBinaryPrefix symbol are case insensitive. 
   */
  public static enum TraditionalBinaryPrefix {

@Tainted  KILO(10),

@Tainted  MEGA(KILO.bitShift + 10),

@Tainted  GIGA(MEGA.bitShift + 10),

@Tainted  TERA(GIGA.bitShift + 10),

@Tainted  PETA(TERA.bitShift + 10),

@Tainted  EXA (PETA.bitShift + 10);

    public final @Tainted long value;
    public final @Tainted char symbol;
    public final @Tainted int bitShift;
    public final @Tainted long bitMask;

    private @Tainted TraditionalBinaryPrefix(@Tainted int bitShift) {
      this.bitShift = bitShift;
      this.value = 1L << bitShift;
      this.bitMask = this.value - 1L;
      this.symbol = toString().charAt(0);
    }

    /**
     * @return The TraditionalBinaryPrefix object corresponding to the symbol.
     */
    public static @Tainted TraditionalBinaryPrefix valueOf(@Tainted char symbol) {
      symbol = Character.toUpperCase(symbol);
      for(@Tainted TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
        if (symbol == prefix.symbol) {
          return prefix;
        }
      }
      throw new @Tainted IllegalArgumentException("Unknown symbol '" + symbol + "'");
    }

    /**
     * Convert a string to long.
     * The input string is first be trimmed
     * and then it is parsed with traditional binary prefix.
     *
     * For example,
     * "-1230k" will be converted to -1230 * 1024 = -1259520;
     * "891g" will be converted to 891 * 1024^3 = 956703965184;
     *
     * @param s input string
     * @return a long value represented by the input string.
     */
    public static @Tainted long string2long(@Tainted String s) {
      s = s.trim();
      final @Tainted int lastpos = s.length() - 1;
      final @Tainted char lastchar = s.charAt(lastpos);
      if (Character.isDigit(lastchar))
        return Long.parseLong(s);
      else {
        @Tainted
        long prefix;
        try {
          prefix = TraditionalBinaryPrefix.valueOf(lastchar).value;
        } catch (@Tainted IllegalArgumentException e) {
          throw new @Tainted IllegalArgumentException("Invalid size prefix '" + lastchar
              + "' in '" + s
              + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)");
        }
        @Tainted
        long num = Long.parseLong(s.substring(0, lastpos));
        if (num > (Long.MAX_VALUE/prefix) || num < (Long.MIN_VALUE/prefix)) {
          throw new @Tainted IllegalArgumentException(s + " does not fit in a Long");
        }
        return num * prefix;
      }
    }

    /**
     * Convert a long integer to a string with traditional binary prefix.
     * 
     * @param n the value to be converted
     * @param unit The unit, e.g. "B" for bytes.
     * @param decimalPlaces The number of decimal places.
     * @return a string with traditional binary prefix.
     */
    public static @Tainted String long2String(@Tainted long n, @Tainted String unit, @Tainted int decimalPlaces) {
      if (unit == null) {
        unit = "";
      }
      //take care a special case
      if (n == Long.MIN_VALUE) {
        return "-8 " + EXA.symbol + unit;
      }

      final @Tainted StringBuilder b = new @Tainted StringBuilder();
      //take care negative numbers
      if (n < 0) {
        b.append('-');
        n = -n;
      }
      if (n < KILO.value) {
        //no prefix
        b.append(n);
        return (unit.isEmpty()? b: b.append(" ").append(unit)).toString();
      } else {
        //find traditional binary prefix
        @Tainted
        int i = 0;
        for(; i < values().length && n >= values()[i].value; i++);
        @Tainted
        TraditionalBinaryPrefix prefix = values()[i - 1];

        if ((n & prefix.bitMask) == 0) {
          //exact division
          b.append(n >> prefix.bitShift);
        } else {
          final @Tainted String  format = "%." + decimalPlaces + "f";
          @Tainted
          String s = format(format, n/(@Tainted double)prefix.value);
          //check a special rounding up case
          if (s.startsWith("1024")) {
            prefix = values()[i];
            s = format(format, n/(@Tainted double)prefix.value);
          }
          b.append(s);
        }
        return b.append(' ').append(prefix.symbol).append(unit).toString();
      }
    }
  }

    /**
     * Escapes HTML Special characters present in the string.
     * @param string
     * @return HTML Escaped String representation
     */
    public static @Tainted String escapeHTML(@Tainted String string) {
      if(string == null) {
        return null;
      }
      @Tainted
      StringBuilder sb = new @Tainted StringBuilder();
      @Tainted
      boolean lastCharacterWasSpace = false;
      @Tainted
      char @Tainted [] chars = string.toCharArray();
      for(@Tainted char c : chars) {
        if(c == ' ') {
          if(lastCharacterWasSpace){
            lastCharacterWasSpace = false;
            sb.append("&nbsp;");
          }else {
            lastCharacterWasSpace=true;
            sb.append(" ");
          }
        }else {
          lastCharacterWasSpace = false;
          switch(c) {
          case '<': sb.append("&lt;"); break;
          case '>': sb.append("&gt;"); break;
          case '&': sb.append("&amp;"); break;
          case '"': sb.append("&quot;"); break;
          default : sb.append(c);break;
          }
        }
      }
      
      return sb.toString();
    }

  /**
   * @return a byte description of the given long interger value.
   */
  public static @Tainted String byteDesc(@Tainted long len) {
    return TraditionalBinaryPrefix.long2String(len, "B", 2);
  }

  /** @deprecated use StringUtils.format("%.2f", d). */
  @Deprecated
  public static @Tainted String limitDecimalTo2(@Tainted double d) {
    return format("%.2f", d);
  }
  
  /**
   * Concatenates strings, using a separator.
   *
   * @param separator Separator to join with.
   * @param strings Strings to join.
   */
  public static @Tainted String join(@Tainted CharSequence separator, @Tainted Iterable<@Tainted ?> strings) {
    @Tainted
    Iterator<@Tainted ?> i = strings.iterator();
    if (!i.hasNext()) {
      return "";
    }
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder(i.next().toString());
    while (i.hasNext()) {
      sb.append(separator);
      sb.append(i.next().toString());
    }
    return sb.toString();
  }

  /**
   * Concatenates strings, using a separator.
   *
   * @param separator to join with
   * @param strings to join
   * @return  the joined string
   */
  public static @Tainted String join(@Tainted CharSequence separator, @Tainted String @Tainted [] strings) {
    // Ideally we don't have to duplicate the code here if array is iterable.
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder();
    @Tainted
    boolean first = true;
    for (@Tainted String s : strings) {
      if (first) {
        first = false;
      } else {
        sb.append(separator);
      }
      sb.append(s);
    }
    return sb.toString();
  }

  /**
   * Convert SOME_STUFF to SomeStuff
   *
   * @param s input string
   * @return camelized string
   */
  public static @Tainted String camelize(@Tainted String s) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder();
    @Tainted
    String @Tainted [] words = split(s.toLowerCase(Locale.US), ESCAPE_CHAR, '_');

    for (@Tainted String word : words)
      sb.append(org.apache.commons.lang.StringUtils.capitalize(word));

    return sb.toString();
  }

  /**
   * Matches a template string against a pattern, replaces matched tokens with
   * the supplied replacements, and returns the result.  The regular expression
   * must use a capturing group.  The value of the first capturing group is used
   * to look up the replacement.  If no replacement is found for the token, then
   * it is replaced with the empty string.
   * 
   * For example, assume template is "%foo%_%bar%_%baz%", pattern is "%(.*?)%",
   * and replacements contains 2 entries, mapping "foo" to "zoo" and "baz" to
   * "zaz".  The result returned would be "zoo__zaz".
   * 
   * @param template String template to receive replacements
   * @param pattern Pattern to match for identifying tokens, must use a capturing
   *   group
   * @param replacements Map<String, String> mapping tokens identified by the
   *   capturing group to their replacement values
   * @return String template with replacements
   */
  public static @Tainted String replaceTokens(@Tainted String template, @Tainted Pattern pattern,
      @Tainted
      Map<@Tainted String, @Tainted String> replacements) {
    @Tainted
    StringBuffer sb = new @Tainted StringBuffer();
    @Tainted
    Matcher matcher = pattern.matcher(template);
    while (matcher.find()) {
      @Tainted
      String replacement = replacements.get(matcher.group(1));
      if (replacement == null) {
        replacement = "";
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
  
  /**
   * Get stack trace for a given thread.
   */
  public static @Tainted String getStackTrace(@Tainted Thread t) {
    final @Tainted StackTraceElement @Tainted [] stackTrace = t.getStackTrace();
    @Tainted
    StringBuilder str = new @Tainted StringBuilder();
    for (@Tainted StackTraceElement e : stackTrace) {
      str.append(e.toString() + "\n");
    }
    return str.toString();
  }
}
