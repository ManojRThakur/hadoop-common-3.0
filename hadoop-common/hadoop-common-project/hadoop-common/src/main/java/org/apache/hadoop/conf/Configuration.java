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

package org.apache.hadoop.conf;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;
import com.google.common.base.Preconditions;
import org.checkerframework.checker.tainting.qual.PolyTainted;

/** 
 * Provides access to configuration parameters.
 *
 * <h4 id="Resources">Resources</h4>
 *
 * <p>Configurations are specified by resources. A resource contains a set of
 * name/value pairs as XML data. Each resource is named by either a 
 * <code>String</code> or by a {@link Path}. If named by a <code>String</code>, 
 * then the classpath is examined for a file with that name.  If named by a 
 * <code>Path</code>, then the local filesystem is examined directly, without 
 * referring to the classpath.
 *
 * <p>Unless explicitly turned off, Hadoop by default specifies two 
 * resources, loaded in-order from the classpath: <ol>
 * <li><tt><a href="{@docRoot}/../core-default.html">core-default.xml</a>
 * </tt>: Read-only defaults for hadoop.</li>
 * <li><tt>core-site.xml</tt>: Site-specific configuration for a given hadoop
 * installation.</li>
 * </ol>
 * Applications may add additional resources, which are loaded
 * subsequent to these resources in the order they are added.
 * 
 * <h4 id="FinalParams">Final Parameters</h4>
 *
 * <p>Configuration parameters may be declared <i>final</i>. 
 * Once a resource declares a value final, no subsequently-loaded 
 * resource can alter that value.  
 * For example, one might define a final parameter with:
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;dfs.hosts.include&lt;/name&gt;
 *    &lt;value&gt;/etc/hadoop/conf/hosts.include&lt;/value&gt;
 *    <b>&lt;final&gt;true&lt;/final&gt;</b>
 *  &lt;/property&gt;</pre></tt>
 *
 * Administrators typically define parameters as final in 
 * <tt>core-site.xml</tt> for values that user applications may not alter.
 *
 * <h4 id="VariableExpansion">Variable Expansion</h4>
 *
 * <p>Value strings are first processed for <i>variable expansion</i>. The
 * available properties are:<ol>
 * <li>Other properties defined in this Configuration; and, if a name is
 * undefined here,</li>
 * <li>Properties in {@link System#getProperties()}.</li>
 * </ol>
 *
 * <p>For example, if a configuration resource contains the following property
 * definitions: 
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;basedir&lt;/name&gt;
 *    &lt;value&gt;/user/${<i>user.name</i>}&lt;/value&gt;
 *  &lt;/property&gt;
 *  
 *  &lt;property&gt;
 *    &lt;name&gt;tempdir&lt;/name&gt;
 *    &lt;value&gt;${<i>basedir</i>}/tmp&lt;/value&gt;
 *  &lt;/property&gt;</pre></tt>
 *
 * When <tt>conf.get("tempdir")</tt> is called, then <tt>${<i>basedir</i>}</tt>
 * will be resolved to another property in this Configuration, while
 * <tt>${<i>user.name</i>}</tt> would then ordinarily be resolved to the value
 * of the System property with that name.
 * By default, warnings will be given to any deprecated configuration 
 * parameters and these are suppressible by configuring
 * <tt>log4j.logger.org.apache.hadoop.conf.Configuration.deprecation</tt> in
 * log4j.properties file.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Configuration implements @Tainted Iterable<Map.@Tainted Entry<@Tainted String, @Untainted String>>,
                                      @Tainted Writable {
  private static final @Tainted Log LOG =
    LogFactory.getLog(Configuration.class);

  private static final @Tainted Log LOG_DEPRECATION =
    LogFactory.getLog("org.apache.hadoop.conf.Configuration.deprecation");

  private @Tainted boolean quietmode = true;
  
  private static class Resource {
    private final @Tainted Object resource;
    private final @Tainted String name;
    
    public @Tainted Resource(@Tainted Object resource) {
      this(resource, resource.toString());
    }
    
    public @Tainted Resource(@Tainted Object resource, @Tainted String name) {
      this.resource = resource;
      this.name = name;
    }
    
    public @Tainted String getName(Configuration.@Tainted Resource this){
      return name;
    }
    
    public @Tainted Object getResource(Configuration.@Tainted Resource this) {
      return resource;
    }
    
    @Override
    public @Tainted String toString(Configuration.@Tainted Resource this) {
      return name;
    }
  }
  
  /**
   * List of configuration resources.
   */
  private @Tainted ArrayList<@Untainted Resource> resources = new @Tainted ArrayList<@Untainted Resource>();
  
  /**
   * The value reported as the setting resource when a key is set
   * by code rather than a file resource by dumpConfiguration.
   */
  static final @Tainted String UNKNOWN_RESOURCE = "Unknown";


  /**
   * List of configuration parameters marked <b>final</b>. 
   */
  private @Tainted Set<@Tainted String> finalParameters = new @Tainted HashSet<@Tainted String>();
  
  private @Tainted boolean loadDefaults = true;
  
  /**
   * Configuration objects
   */
  private static final @Tainted WeakHashMap<@Tainted Configuration, @Tainted Object> REGISTRY = 
    new @Tainted WeakHashMap<@Tainted Configuration, @Tainted Object>();
  
  /**
   * List of default Resources. Resources are loaded in the order of the list 
   * entries
   */
  private static final @Tainted CopyOnWriteArrayList<@Untainted String> defaultResources =
    new @Tainted CopyOnWriteArrayList<@Untainted String>();

  private static final @Tainted Map<@Tainted ClassLoader, @Tainted Map<@Tainted String, @Tainted WeakReference<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>>>
    CACHE_CLASSES = new @Tainted WeakHashMap<@Tainted ClassLoader, @Tainted Map<@Tainted String, @Tainted WeakReference<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>>>();

  /**
   * Sentinel value to store negative cache results in {@link #CACHE_CLASSES}.
   */
  private static final @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> NEGATIVE_CACHE_SENTINEL =
    NegativeCacheSentinel.class;

  /**
   * Stores the mapping of key to the resource which modifies or loads 
   * the key most recently
   */
  private @Tainted HashMap<@Tainted String, @Tainted String @Tainted []> updatingResource;
 
  /**
   * Class to keep the information about the keys which replace the deprecated
   * ones.
   * 
   * This class stores the new keys which replace the deprecated keys and also
   * gives a provision to have a custom message for each of the deprecated key
   * that is being replaced. It also provides method to get the appropriate
   * warning message which can be logged whenever the deprecated key is used.
   */
  private static class DeprecatedKeyInfo {
    private @Tainted String @Tainted [] newKeys;
    private @Tainted String customMessage;
    private @Tainted boolean accessed;
    @Tainted
    DeprecatedKeyInfo(@Tainted String @Tainted [] newKeys, @Tainted String customMessage) {
      this.newKeys = newKeys;
      this.customMessage = customMessage;
      accessed = false;
    }

    /**
     * Method to provide the warning message. It gives the custom message if
     * non-null, and default message otherwise.
     * @param key the associated deprecated key.
     * @return message that is to be logged when a deprecated key is used.
     */
    private final @Tainted String getWarningMessage(Configuration.@Tainted DeprecatedKeyInfo this, @Tainted String key) {
      @Tainted
      String warningMessage;
      if(customMessage == null) {
        @Tainted
        StringBuilder message = new @Tainted StringBuilder(key);
        @Tainted
        String deprecatedKeySuffix = " is deprecated. Instead, use ";
        message.append(deprecatedKeySuffix);
        for (@Tainted int i = 0; i < newKeys.length; i++) {
          message.append(newKeys[i]);
          if(i != newKeys.length-1) {
            message.append(", ");
          }
        }
        warningMessage = message.toString();
      }
      else {
        warningMessage = customMessage;
      }
      accessed = true;
      return warningMessage;
    }
  }
  
  /**
   * Stores the deprecated keys, the new keys which replace the deprecated keys
   * and custom message(if any provided).
   */
  private static @Tainted Map<@Tainted String, @Tainted DeprecatedKeyInfo> deprecatedKeyMap = 
      new @Tainted HashMap<@Tainted String, @Tainted DeprecatedKeyInfo>();
  
  /**
   * Stores a mapping from superseding keys to the keys which they deprecate.
   */
  private static @Tainted Map<@Tainted String, @Tainted String> reverseDeprecatedKeyMap =
      new @Tainted HashMap<@Tainted String, @Tainted String>();

  /**
   * Adds the deprecated key to the deprecation map.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If a key is deprecated in favor of multiple keys, they are all treated as 
   * aliases of each other, and setting any one of them resets all the others 
   * to the new value.
   * 
   * @param key
   * @param newKeys
   * @param customMessage
   * @deprecated use {@link #addDeprecation(String key, String newKey,
      String customMessage)} instead
   */
  @Deprecated
  public synchronized static void addDeprecation(@Tainted String key, String[] newKeys,
      @Tainted
      String customMessage) {
    if (key == null || key.length() == 0 ||
        newKeys == null || newKeys.length == 0) {
      throw new @Tainted IllegalArgumentException();
    }
    if (!isDeprecated(key)) {
      @Tainted
      DeprecatedKeyInfo newKeyInfo;
      newKeyInfo = new @Tainted DeprecatedKeyInfo(newKeys, customMessage);
      deprecatedKeyMap.put(key, newKeyInfo);
      for (@Tainted String newKey : newKeys) {
        reverseDeprecatedKeyMap.put(newKey, key);
      }
    }
  }
  
  /**
   * Adds the deprecated key to the deprecation map.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * @param key
   * @param newKey
   * @param customMessage
   */
  public synchronized static void addDeprecation(@Tainted String key, @Tainted String newKey,
	      @Tainted
	      String customMessage) {
	  addDeprecation(key, new @Tainted String @Tainted [] {newKey}, customMessage);
  }

  /**
   * Adds the deprecated key to the deprecation map when no custom message
   * is provided.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If a key is deprecated in favor of multiple keys, they are all treated as 
   * aliases of each other, and setting any one of them resets all the others 
   * to the new value.
   * 
   * @param key Key that is to be deprecated
   * @param newKeys list of keys that take up the values of deprecated key
   * @deprecated use {@link #addDeprecation(String key, String newKey)} instead
   */
  @Deprecated
  public synchronized static void addDeprecation(@Tainted String key, @Tainted String @Tainted [] newKeys) {
    addDeprecation(key, newKeys, null);
  }
  
  /**
   * Adds the deprecated key to the deprecation map when no custom message
   * is provided.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * @param key Key that is to be deprecated
   * @param newKey key that takes up the value of deprecated key
   */
  public synchronized static void addDeprecation(@Tainted String key, @Tainted String newKey) {
	addDeprecation(key, new @Tainted String @Tainted [] {newKey}, null);
  }
  
  /**
   * checks whether the given <code>key</code> is deprecated.
   * 
   * @param key the parameter which is to be checked for deprecation
   * @return <code>true</code> if the key is deprecated and 
   *         <code>false</code> otherwise.
   */
  public static @Tainted boolean isDeprecated(@Tainted String key) {
    return deprecatedKeyMap.containsKey(key);
  }

  /**
   * Returns the alternate name for a key if the property name is deprecated
   * or if deprecates a property name.
   *
   * @param name property name.
   * @return alternate name.
   */
  private @Tainted String @Tainted [] getAlternateNames(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String altNames @Tainted [] = null;
    @Tainted
    DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
    if (keyInfo == null) {
      altNames = (reverseDeprecatedKeyMap.get(name) != null ) ? 
        new @Tainted String @Tainted [] {reverseDeprecatedKeyMap.get(name)} : null;
      if(altNames != null && altNames.length > 0) {
    	//To help look for other new configs for this deprecated config
    	keyInfo = deprecatedKeyMap.get(altNames[0]);
      }      
    } 
    if(keyInfo != null && keyInfo.newKeys.length > 0) {
      @Tainted
      List<@Tainted String> list = new @Tainted ArrayList<@Tainted String>(); 
      if(altNames != null) {
    	  list.addAll(Arrays.asList(altNames));
      }
      list.addAll(Arrays.asList(keyInfo.newKeys));
      altNames = list.toArray(new @Tainted String @Tainted [list.size()]);
    }
    return altNames;
  }

  /**
   * Checks for the presence of the property <code>name</code> in the
   * deprecation map. Returns the first of the list of new keys if present
   * in the deprecation map or the <code>name</code> itself. If the property
   * is not presently set but the property map contains an entry for the
   * deprecated key, the value of the deprecated key is set as the value for
   * the provided property name.
   *
   * @param name the property name
   * @return the first property in the list of properties mapping
   *         the <code>name</code> or the <code>name</code> itself.
   */
  private @Tainted String @Tainted [] handleDeprecation(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    ArrayList<@Tainted String > names = new @Tainted ArrayList<@Tainted String>();
	if (isDeprecated(name)) {
      @Tainted
      DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
      warnOnceIfDeprecated(name);
      for (@Tainted String newKey : keyInfo.newKeys) {
        if(newKey != null) {
          names.add(newKey);
        }
      }
    }
    if(names.size() == 0) {
    	names.add(name);
    }
    for(@Tainted String n : names) {
	  @Tainted
	  String deprecatedKey = reverseDeprecatedKeyMap.get(n);
	  if (deprecatedKey != null && !getOverlay().containsKey(n) &&
	      getOverlay().containsKey(deprecatedKey)) {
	    getProps().setProperty(n, getOverlay().getProperty(deprecatedKey));
	    getOverlay().setProperty(n, getOverlay().getProperty(deprecatedKey));
	  }
    }
    return names.toArray(new @Tainted String @Tainted [names.size()]);
  }
 
  private void handleDeprecation(@Tainted Configuration this) {
    LOG.debug("Handling deprecation for all properties in config...");
    @Tainted
    Set<@Tainted Object> keys = new @Tainted HashSet<@Tainted Object>();
    keys.addAll(getProps().keySet());
    for (@Tainted Object item: keys) {
      LOG.debug("Handling deprecation for " + (@Tainted String)item);
      handleDeprecation((@Tainted String)item);
    }
  }
 
  static{
    //print deprecation warning if hadoop-site.xml is found in classpath
    @Tainted
    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = Configuration.class.getClassLoader();
    }
    if(cL.getResource("hadoop-site.xml")!=null) {
      LOG.warn("DEPRECATED: hadoop-site.xml found in the classpath. " +
          "Usage of hadoop-site.xml is deprecated. Instead use core-site.xml, "
          + "mapred-site.xml and hdfs-site.xml to override properties of " +
          "core-default.xml, mapred-default.xml and hdfs-default.xml " +
          "respectively");
    }
    addDefaultResource("core-default.xml");
    addDefaultResource("core-site.xml");
    //Add code for managing deprecated key mapping
    //for example
    //addDeprecation("oldKey1",new String[]{"newkey1","newkey2"});
    //adds deprecation for oldKey1 to two new keys(newkey1, newkey2).
    //so get or set of oldKey1 will correctly populate/access values of 
    //newkey1 and newkey2
    addDeprecatedKeys();
  }
  
  private @Tainted Properties properties;
  private @Tainted Properties overlay;
  private @Tainted ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Configuration.class.getClassLoader();
    }
  }
  
  /** A new configuration. */
  public @Tainted Configuration() {
    this(true);
  }

  /** A new configuration where the behavior of reading from the default 
   * resources can be turned off.
   * 
   * If the parameter {@code loadDefaults} is false, the new instance
   * will not load resources from the default files. 
   * @param loadDefaults specifies whether to load from the default files
   */
  public @Tainted Configuration(@Tainted boolean loadDefaults) {
    this.loadDefaults = loadDefaults;
    updatingResource = new @Tainted HashMap<@Tainted String, @Tainted String @Tainted []>();
    synchronized(Configuration.class) {
      REGISTRY.put(this, null);
    }
  }
  
  /** 
   * A new configuration with the same settings cloned from another.
   * 
   * @param other the configuration from which to clone settings.
   */
  @SuppressWarnings({"unchecked", "ostrusted:cast.unsafe"})
  public @Tainted Configuration(@Tainted Configuration other) {
   //ostrusted, this is safe since the other.resources must be safe
   this.resources = (@Tainted ArrayList<@Untainted Resource>) other.resources.clone();
   synchronized(other) {
     if (other.properties != null) {
       this.properties = (@Tainted Properties)other.properties.clone();
     }

     if (other.overlay!=null) {
       this.overlay = (@Tainted Properties)other.overlay.clone();
     }

     this.updatingResource = new @Tainted HashMap<@Tainted String, @Tainted String @Tainted []>(other.updatingResource);
   }
   
    this.finalParameters = new @Tainted HashSet<@Tainted String>(other.finalParameters);
    synchronized(Configuration.class) {
      REGISTRY.put(this, null);
    }
    this.classLoader = other.classLoader;
    this.loadDefaults = other.loadDefaults;
    setQuietMode(other.getQuietMode());
  }
  
  /**
   * Add a default resource. Resources are loaded in the order of the resources 
   * added.
   * @param name file name. File should be present in the classpath.
   */
  public static synchronized void addDefaultResource(@Untainted String name) {
    if(!defaultResources.contains(name)) {
      defaultResources.add(name);
      for(@Tainted Configuration conf : REGISTRY.keySet()) {
        if(conf.loadDefaults) {
          conf.reloadConfiguration();
        }
      }
    }
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param name resource to be added, the classpath is examined for a file 
   *             with that name.
   */
  public void addResource(@Tainted Configuration this, @Untainted String name) {
    addResourceObject(new @Untainted Resource(name));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param url url of the resource to be added, the local filesystem is 
   *            examined directly to find the resource, without referring to 
   *            the classpath.
   */
  public void addResource(@Tainted Configuration this, @Untainted URL url) {
    addResourceObject(new @Untainted Resource(url));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param file file-path of resource to be added, the local filesystem is
   *             examined directly to find the resource, without referring to 
   *             the classpath.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  //TODO: I ensure that the Hadoop Common Location that use this, force the input path objects to @Trusted
  //TODO: This was exercising a checker framework bug
  public void addResource(@Tainted Configuration this, Path file) {
    addResourceObject((@Untainted Resource) new Resource(file));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * WARNING: The contents of the InputStream will be cached, by this method. 
   * So use this sparingly because it does increase the memory consumption.
   * 
   * @param in InputStream to deserialize the object from. In will be read from
   * when a get or set is called next.  After it is read the stream will be
   * closed. 
   */
  public void addResource(@Tainted Configuration this, @Untainted InputStream in) {
    addResourceObject(new @Untainted Resource(in));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param in InputStream to deserialize the object from.
   * @param name the name of the resource because InputStream.toString is not
   * very descriptive some times.  
   */
  public void addResource(@Tainted Configuration this, @Untainted InputStream in, @Tainted String name) {
    addResourceObject(new @Untainted Resource(in, name));
  }
  
  
  /**
   * Reload configuration from previously added resources.
   *
   * This method will clear all the configuration read from the added 
   * resources, and final parameters. This will make the resources to 
   * be read again before accessing the values. Values that are added
   * via set methods will overlay values read from the resources.
   */
  public synchronized void reloadConfiguration(@Tainted Configuration this) {
    properties = null;                            // trigger reload
    finalParameters.clear();                      // clear site-limits
  }
  
  private synchronized void addResourceObject(@Tainted Configuration this, @Untainted Resource resource) {
    resources.add(resource);                      // add to resources
    reloadConfiguration();
  }
  
  private static @Tainted Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  private static @Tainted int MAX_SUBST = 20;

  @SuppressWarnings("ostrusted:cast.unsafe")
  private @Untainted String substituteVars(@Tainted Configuration this, @Untainted String expr) {
    if (expr == null) {
      return null;
    }
    @Tainted Matcher match = varPat.matcher("");
    @Untainted String eval = expr;
    @Tainted Set<@Untainted String> evalSet = new @Tainted HashSet<@Untainted String>();
    for(@Tainted int s=0; s<MAX_SUBST; s++) {
      if (evalSet.contains(eval)) {
        // Cyclic resolution pattern detected. Return current expression.
        return eval;
      }
      evalSet.add(eval);
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      @Tainted String var = match.group();
      var = var.substring(2, var.length()-1); // remove ${ .. }
      @Untainted String val = null;
      try {
        val = (@Untainted String) System.getProperty(var);
      } catch(@Tainted SecurityException se) {
        LOG.warn("Unexpected SecurityException in Configuration", se);
      }
      if (val == null) {
        val = getRaw(var);
      }
      if (val == null) {
        return eval; // return literal ${var}: var is unbound
      }
      // substitute
      eval = ( (@Untainted String) eval.substring(0, match.start() ) ) + val + ( (@Untainted String) eval.substring(match.end()) );
    }
    throw new @Tainted IllegalStateException("Variable substitution depth too large: " 
                                    + MAX_SUBST + " " + expr);
  }
  
  /**
   * Get the value of the <code>name</code> property, <code>null</code> if
   * no such property exists. If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   * 
   * Values are processed for <a href="#VariableExpansion">variable expansion</a> 
   * before being returned. 
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property, 
   *         or null if no such property exists.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @Untainted String get(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String @Tainted [] names = handleDeprecation(name);
    @Untainted  String result = null;
    for(@Tainted String n : names) {
      result = substituteVars( (@Untainted String) getProps().getProperty(n));
    }
    return result;
  }
  
  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>, 
   * <code>null</code> if no such property exists. 
   * If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   * 
   * Values are processed for <a href="#VariableExpansion">variable expansion</a> 
   * before being returned. 
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property, 
   *         or null if no such property exists.
   */
  public @Tainted String getTrimmed(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String value = get(name);
    
    if (null == value) {
      return null;
    } else {
      return value.trim();
    }
  }
  
  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>, 
   * <code>defaultValue</code> if no such property exists. 
   * See @{Configuration#getTrimmed} for more details.
   * 
   * @param name          the property name.
   * @param defaultValue  the property default value.
   * @return              the value of the <code>name</code> or defaultValue
   *                      if it is not set.
   */
  public @Tainted String getTrimmed(@Tainted Configuration this, @Tainted String name, @Tainted String defaultValue) {
    @Tainted
    String ret = getTrimmed(name);
    return ret == null ? defaultValue : ret;
  }

  /**
   * Get the value of the <code>name</code> property, without doing
   * <a href="#VariableExpansion">variable expansion</a>.If the key is 
   * deprecated, it returns the value of the first key which replaces 
   * the deprecated key and is not null.
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> property or 
   *         its replacing property and null if no such property exists.
   */
  @SuppressWarnings("trusted:cast.unsafe")
  public @Untainted String getRaw(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String @Tainted [] names = handleDeprecation(name);
    @Untainted String result = null;
    for(@Tainted String n : names) {
      result = (@Untainted String) getProps().getProperty(n);
    }
    return result;
  }

  /** 
   * Set the <code>value</code> of the <code>name</code> property. If 
   * <code>name</code> is deprecated or there is a deprecated name associated to it,
   * it sets the value to both names.
   * 
   * @param name property name.
   * @param value property value.
   */
  public void set(@Tainted Configuration this, @Tainted String name, @Untainted String value) {
    set(name, value, null);
  }
  
  /** 
   * Set the <code>value</code> of the <code>name</code> property. If 
   * <code>name</code> is deprecated or there is a deprecated name associated to it,
   * it sets the value to both names.
   * 
   * @param name property name.
   * @param value property value.
   * @param source the place that this configuration value came from 
   * (For debugging).
   * @throws IllegalArgumentException when the value or name is null.
   */
  //TODO: ostrusted  we should make properties.put polymorphic with respect to the receiver type of properties
  public void set(@Tainted Configuration this, @Tainted String name, @Untainted String value, @Tainted String source) {
    Preconditions.checkArgument( name != null,"Property name must not be null");
    Preconditions.checkArgument( value != null, "Property value must not be null");

    if (deprecatedKeyMap.isEmpty()) {
      getProps();
    }
    getOverlay().setProperty(name, value);
    getProps().setProperty(name, value);
    if(source == null) {
      updatingResource.put(name, new @Tainted String @Tainted [] {"programatically"});
    } else {
      updatingResource.put(name, new @Tainted String @Tainted [] {source});
    }
    @Tainted
    String @Tainted [] altNames = getAlternateNames(name);
    if (altNames != null && altNames.length > 0) {
      @Tainted
      String altSource = "because " + name + " is deprecated";
      for(@Tainted String altName : altNames) {
        if(!altName.equals(name)) {
          getOverlay().setProperty(altName, value);
          getProps().setProperty(altName, value);
          updatingResource.put(altName, new @Tainted String @Tainted [] {altSource});
        }
      }
    }
    warnOnceIfDeprecated(name);
  }

  private void warnOnceIfDeprecated(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
    if (keyInfo != null && !keyInfo.accessed) {
      LOG_DEPRECATION.info(keyInfo.getWarningMessage(name));
    }
  }

  /**
   * Unset a previously set property.
   */
  public synchronized void unset(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String @Tainted [] altNames = getAlternateNames(name);
    getOverlay().remove(name);
    getProps().remove(name);
    if (altNames !=null && altNames.length > 0) {
      for(@Tainted String altName : altNames) {
    	getOverlay().remove(altName);
    	getProps().remove(altName);
      }
    }
  }

  /**
   * Sets a property if it is currently unset.
   * @param name the property name
   * @param value the new value
   */
  public synchronized void setIfUnset(@Tainted Configuration this, @Tainted String name, @Untainted String value) {
    if (get(name) == null) {
      set(name, value);
    }
  }
  
  private synchronized @Tainted Properties getOverlay(@Tainted Configuration this) {
    if (overlay==null){
      overlay=new @Tainted Properties();
    }
    return overlay;
  }

  /** 
   * Get the value of the <code>name</code>. If the key is deprecated,
   * it returns the value of the first key which replaces the deprecated key
   * and is not null.
   * If no such property exists,
   * then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property 
   *         doesn't exist.                    
   */
  @SuppressWarnings("ostrusted:cast.unsafe")  //Should be PolyOsTrusted with defaultValue
  public @PolyTainted String get(@Tainted Configuration this, @Tainted String name, @PolyTainted String defaultValue) {
    @Tainted
    String @Tainted [] names = handleDeprecation(name);
    @Untainted
    String result = null;
    for(@Tainted String n : names) {
      result = substituteVars( ( (@Untainted String) getProps().getProperty(n, defaultValue) ));
    }
    return result;
  }

  /** 
   * Get the value of the <code>name</code> property as an <code>int</code>.
   *   
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>int</code>,
   * then an error is thrown.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as an <code>int</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @Tainted int getInt(@Tainted Configuration this, @Tainted String name, @Tainted int defaultValue) {
    @Tainted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    @Tainted
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }
  
  /**
   * Get the value of the <code>name</code> property as a set of comma-delimited
   * <code>int</code> values.
   * 
   * If no such property exists, an empty array is returned.
   * 
   * @param name property name
   * @return property value interpreted as an array of comma-delimited
   *         <code>int</code> values
   */
  public @Tainted int @Tainted [] getInts(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String @Tainted [] strings = getTrimmedStrings(name);
    @Tainted
    int @Tainted [] ints = new @Tainted int @Tainted [strings.length];
    for (@Tainted int i = 0; i < strings.length; i++) {
      ints[i] = Integer.parseInt(strings[i]);
    }
    return ints;
  }

  /** 
   * Set the value of the <code>name</code> property to an <code>int</code>.
   * 
   * @param name property name.
   * @param value <code>int</code> value of the property.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setInt(@Tainted Configuration this, @Tainted String name, @Tainted int value) {
    //Int strings are trusted
    set(name, (@Untainted String) Integer.toString(value));
  }


  /** 
   * Get the value of the <code>name</code> property as a <code>long</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>long</code>,
   * then an error is thrown.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @Tainted long getLong(@Tainted Configuration this, @Tainted String name, @Tainted long defaultValue) {
    @Tainted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    @Tainted
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code> or
   * human readable format. If no such property exists, the provided default
   * value is returned, or if the specified value is not a valid
   * <code>long</code> or human readable format, then an error is thrown. You
   * can use the following suffix (case insensitive): k(kilo), m(mega), g(giga),
   * t(tera), p(peta), e(exa)
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>,
   *         or <code>defaultValue</code>.
   */
  public @Tainted long getLongBytes(@Tainted Configuration this, @Tainted String name, @Tainted long defaultValue) {
    @Tainted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
  }

  private @Tainted String getHexDigits(@Tainted Configuration this, @Tainted String value) {
    @Tainted
    boolean negative = false;
    @Tainted
    String str = value;
    @Tainted
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }
  
  /** 
   * Set the value of the <code>name</code> property to a <code>long</code>.
   * 
   * @param name property name.
   * @param value <code>long</code> value of the property.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setLong(@Tainted Configuration this, @Tainted String name, @Tainted long value) {
    //Strings from numbers are considered safe
    set(name, (@Untainted String) Long.toString(value));
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>float</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>float</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>float</code>, 
   *         or <code>defaultValue</code>. 
   */

  public @Tainted float getFloat(@Tainted Configuration this, @Tainted String name, @Tainted float defaultValue) {
    @Tainted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return Float.parseFloat(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>float</code>.
   * 
   * @param name property name.
   * @param value property value.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setFloat(@Tainted Configuration this, @Tainted String name, @Tainted float value) {
    //Strings from numbers are considered safe
    set(name, (@Untainted String) Float.toString(value));
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>double</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>double</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>double</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @Tainted double getDouble(@Tainted Configuration this, @Tainted String name, @Tainted double defaultValue) {
    @Tainted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return Double.parseDouble(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>double</code>.
   * 
   * @param name property name.
   * @param value property value.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setDouble(@Tainted Configuration this, @Tainted String name, @Tainted double value) {
    //Strings from numbers are considered safe
    set(name, (@Untainted String) Double.toString(value));
  }
 
  /** 
   * Get the value of the <code>name</code> property as a <code>boolean</code>.  
   * If no such property is specified, or if the specified value is not a valid
   * <code>boolean</code>, then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>boolean</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @Tainted boolean getBoolean(@Tainted Configuration this, @Tainted String name, @Tainted boolean defaultValue) {
    @Tainted
    String valueString = getTrimmed(name);
    if (null == valueString || valueString.isEmpty()) {
      return defaultValue;
    }

    valueString = valueString.toLowerCase();

    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else return defaultValue;
  }

  /** 
   * Set the value of the <code>name</code> property to a <code>boolean</code>.
   * 
   * @param name property name.
   * @param value <code>boolean</code> value of the property.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setBoolean(@Tainted Configuration this, @Tainted String name, @Tainted boolean value) {
    set(name, (@Untainted String) Boolean.toString(value));
  }

  /**
   * Set the given property, if it is currently unset.
   * @param name property name
   * @param value new value
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setBooleanIfUnset(@Tainted Configuration this, @Tainted String name, @Tainted boolean value) {
    setIfUnset(name, (@Untainted String) Boolean.toString(value));
  }

  /**
   * Set the value of the <code>name</code> property to the given type. This
   * is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
   * @param name property name
   * @param value new value
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public <@Untainted T extends @Untainted Enum<@Untainted T>> void setEnum(@Tainted Configuration this, @Tainted String name, @Untainted T value) {
    //value is trusted
    set(name, (@Untainted String) value.toString());
  }

  /**
   * Return value matching this enumerated type.
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists
   * @throws IllegalArgumentException If mapping is illegal for the type
   * provided
   */
  @SuppressWarnings("ostrusted:argument.type.incompatible")
  public <@Untainted T extends @Untainted Enum<@Untainted T>> T getEnum(@Tainted Configuration this, @Tainted String name, @Untainted T defaultValue) {
    final @Untainted String val = get(name);
    return null == val ? defaultValue : Enum.valueOf( defaultValue.getDeclaringClass(), val );
  }

  enum ParsedTimeDuration {

    @Untainted  NS {
      TimeUnit unit() { return TimeUnit.NANOSECONDS; }
      String suffix() { return "ns"; }
    },

    @Untainted  US {
      TimeUnit unit() { return TimeUnit.MICROSECONDS; }
      String suffix() { return "us"; }
    },

    @Untainted  MS {
      TimeUnit unit() { return TimeUnit.MILLISECONDS; }
      String suffix() { return "ms"; }
    },

    @Untainted  S {
      TimeUnit unit() { return TimeUnit.SECONDS; }
      String suffix() { return "s"; }
    },

    @Untainted  M {
      TimeUnit unit() { return TimeUnit.MINUTES; }
      String suffix() { return "m"; }
    },

    @Untainted  H {
      TimeUnit unit() { return TimeUnit.HOURS; }
      String suffix() { return "h"; }
    },

    @Untainted  D {
      TimeUnit unit() { return TimeUnit.DAYS; }
      String suffix() { return "d"; }
    };
    abstract @Tainted TimeUnit unit(Configuration.@Tainted ParsedTimeDuration this);
    abstract @Tainted String suffix(Configuration.@Tainted ParsedTimeDuration this);
    static @Tainted ParsedTimeDuration unitFor(@Tainted String s) {
      for (@Tainted ParsedTimeDuration ptd : values()) {
        // iteration order is in decl order, so SECONDS matched last
        if (s.endsWith(ptd.suffix())) {
          return ptd;
        }
      }
      return null;
    }
    static @Tainted ParsedTimeDuration unitFor(@Tainted TimeUnit unit) {
      for (@Tainted ParsedTimeDuration ptd : values()) {
        if (ptd.unit() == unit) {
          return ptd;
        }
      }
      return null;
    }
  }

  /**
   * Set the value of <code>name</code> to the given time duration. This
   * is equivalent to <code>set(&lt;name&gt;, value + &lt;time suffix&gt;)</code>.
   * @param name Property name
   * @param value Time duration
   * @param unit Unit of time
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setTimeDuration(@Tainted Configuration this, @Tainted String name, @Tainted long value, @Tainted TimeUnit unit) {
    //All time unit suffixes are trusted as they are literals
    set(name, (@Untainted String) ( value + ParsedTimeDuration.unitFor(unit).suffix() ));
  }

  /**
   * Return time duration in the given time unit. Valid units are encoded in
   * properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
   * (ms), seconds (s), minutes (m), hours (h), and days (d).
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists.
   * @param unit Unit to convert the stored property, if it exists.
   * @throws NumberFormatException If the property stripped of its unit is not
   *         a number
   */
  public @Tainted long getTimeDuration(@Tainted Configuration this, @Tainted String name, @Tainted long defaultValue, @Tainted TimeUnit unit) {
    @Tainted
    String vStr = get(name);
    if (null == vStr) {
      return defaultValue;
    }
    vStr = vStr.trim();
    @Tainted
    ParsedTimeDuration vUnit = ParsedTimeDuration.unitFor(vStr);
    if (null == vUnit) {
      LOG.warn("No unit for " + name + "(" + vStr + ") assuming " + unit);
      vUnit = ParsedTimeDuration.unitFor(unit);
    } else {
      vStr = vStr.substring(0, vStr.lastIndexOf(vUnit.suffix()));
    }
    return unit.convert(Long.parseLong(vStr), vUnit.unit());
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Pattern</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>Pattern</code>, then <code>DefaultValue</code> is returned.
   *
   * @param name property name
   * @param defaultValue default value
   * @return property value as a compiled Pattern, or defaultValue
   */
  public @Tainted Pattern getPattern(@Tainted Configuration this, @Tainted String name, @Tainted Pattern defaultValue) {
    @Tainted
    String valString = get(name);
    if (null == valString || valString.isEmpty()) {
      return defaultValue;
    }
    try {
      return Pattern.compile(valString);
    } catch (@Tainted PatternSyntaxException pse) {
      LOG.warn("Regular expression '" + valString + "' for property '" +
               name + "' not valid. Using default", pse);
      return defaultValue;
    }
  }

  /**
   * Set the given property to <code>Pattern</code>.
   * If the pattern is passed as null, sets the empty pattern which results in
   * further calls to getPattern(...) returning the default value.
   *
   * @param name property name
   * @param pattern new value
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setPattern(@Tainted Configuration this, @Tainted String name, @Untainted Pattern pattern) {
    if (null == pattern) {
      set(name, null);
    } else {
      set(name, (@Untainted String) pattern.pattern());
    }
  }

  /**
   * Gets information about why a property was set.  Typically this is the 
   * path to the resource objects (file, URL, etc.) the property came from, but
   * it can also indicate that it was set programatically, or because of the
   * command line.
   *
   * @param name - The property name to get the source of.
   * @return null - If the property or its source wasn't found. Otherwise, 
   * returns a list of the sources of the resource.  The older sources are
   * the first ones in the list.  So for example if a configuration is set from
   * the command line, and then written out to a file that is read back in the
   * first entry would indicate that it was set from the command line, while
   * the second one would indicate the file that the new configuration was read
   * in from.
   */
  @InterfaceStability.Unstable
  public synchronized @Tainted String @Tainted [] getPropertySources(@Tainted Configuration this, @Tainted String name) {
    if (properties == null) {
      // If properties is null, it means a resource was newly added
      // but the props were cleared so as to load it upon future
      // requests. So lets force a load by asking a properties list.
      getProps();
    }
    // Return a null right away if our properties still
    // haven't loaded or the resource mapping isn't defined
    if (properties == null || updatingResource == null) {
      return null;
    } else {
      @Tainted
      String @Tainted [] source = updatingResource.get(name);
      if(source == null) {
        return null;
      } else {
        return Arrays.copyOf(source, source.length);
      }
    }
  }

  /**
   * A class that represents a set of positive integer ranges. It parses 
   * strings of the form: "2-3,5,7-" where ranges are separated by comma and 
   * the lower/upper bounds are separated by dash. Either the lower or upper 
   * bound may be omitted meaning all values up to or over. So the string 
   * above means 2, 3, 5, and 7, 8, 9, ...
   */
  public static class IntegerRanges implements @Tainted Iterable<@Tainted Integer>{
    private static class Range {
      @Tainted
      int start;
      @Tainted
      int end;
    }
    
    private static class RangeNumberIterator implements @Tainted Iterator<@Tainted Integer> {
      @Tainted
      Iterator<@Tainted Range> internal;
      @Tainted
      int at;
      @Tainted
      int end;

      public @Tainted RangeNumberIterator(@Tainted List<@Tainted Range> ranges) {
        if (ranges != null) {
          internal = ranges.iterator();
        }
        at = -1;
        end = -2;
      }
      
      @Override
      public @Tainted boolean hasNext(Configuration.IntegerRanges.@Tainted RangeNumberIterator this) {
        if (at <= end) {
          return true;
        } else if (internal != null){
          return internal.hasNext();
        }
        return false;
      }

      @Override
      public @Tainted Integer next(Configuration.IntegerRanges.@Tainted RangeNumberIterator this) {
        if (at <= end) {
          at++;
          return at - 1;
        } else if (internal != null){
          @Tainted
          Range found = internal.next();
          if (found != null) {
            at = found.start;
            end = found.end;
            at++;
            return at - 1;
          }
        }
        return null;
      }

      @Override
      public void remove(Configuration.IntegerRanges.@Tainted RangeNumberIterator this) {
        throw new @Tainted UnsupportedOperationException();
      }
    };

    @Tainted
    List<@Tainted Range> ranges = new @Tainted ArrayList<@Tainted Range>();
    
    public @Tainted IntegerRanges() {
    }
    
    public @Tainted IntegerRanges(@Tainted String newValue) {
      @Tainted
      StringTokenizer itr = new @Tainted StringTokenizer(newValue, ",");
      while (itr.hasMoreTokens()) {
        @Tainted
        String rng = itr.nextToken().trim();
        @Tainted
        String @Tainted [] parts = rng.split("-", 3);
        if (parts.length < 1 || parts.length > 2) {
          throw new @Tainted IllegalArgumentException("integer range badly formed: " + 
                                             rng);
        }
        @Tainted
        Range r = new @Tainted Range();
        r.start = convertToInt(parts[0], 0);
        if (parts.length == 2) {
          r.end = convertToInt(parts[1], Integer.MAX_VALUE);
        } else {
          r.end = r.start;
        }
        if (r.start > r.end) {
          throw new @Tainted IllegalArgumentException("IntegerRange from " + r.start + 
                                             " to " + r.end + " is invalid");
        }
        ranges.add(r);
      }
    }

    /**
     * Convert a string to an int treating empty strings as the default value.
     * @param value the string value
     * @param defaultValue the value for if the string is empty
     * @return the desired integer
     */
    private static @Tainted int convertToInt(@Tainted String value, @Tainted int defaultValue) {
      @Tainted
      String trim = value.trim();
      if (trim.length() == 0) {
        return defaultValue;
      }
      return Integer.parseInt(trim);
    }

    /**
     * Is the given value in the set of ranges
     * @param value the value to check
     * @return is the value in the ranges?
     */
    public @Tainted boolean isIncluded(Configuration.@Tainted IntegerRanges this, @Tainted int value) {
      for(@Tainted Range r: ranges) {
        if (r.start <= value && value <= r.end) {
          return true;
        }
      }
      return false;
    }
    
    /**
     * @return true if there are no values in this range, else false.
     */
    public @Tainted boolean isEmpty(Configuration.@Tainted IntegerRanges this) {
      return ranges == null || ranges.isEmpty();
    }
    
    @Override
    public @Tainted String toString(Configuration.@Tainted IntegerRanges this) {
      @Tainted
      StringBuilder result = new @Tainted StringBuilder();
      @Tainted
      boolean first = true;
      for(@Tainted Range r: ranges) {
        if (first) {
          first = false;
        } else {
          result.append(',');
        }
        result.append(r.start);
        result.append('-');
        result.append(r.end);
      }
      return result.toString();
    }

    @Override
    public @Tainted Iterator<@Tainted Integer> iterator(Configuration.@Tainted IntegerRanges this) {
      return new @Tainted RangeNumberIterator(ranges);
    }
    
  }

  /**
   * Parse the given attribute as a set of integer ranges
   * @param name the attribute name
   * @param defaultValue the default value if it is not set
   * @return a new set of ranges from the configured value
   */
  public @Untainted IntegerRanges getRange(@Tainted Configuration this, @Tainted String name, @Untainted String defaultValue) {
    return new @Untainted IntegerRanges(get(name, defaultValue));
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * a collection of <code>String</code>s.  
   * If no such property is specified then empty collection is returned.
   * <p>
   * This is an optimized version of {@link #getStrings(String)}
   * 
   * @param name property name.
   * @return property value as a collection of <code>String</code>s. 
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @Untainted Collection<@Untainted String> getStringCollection(@Tainted Configuration this, @Tainted String name) {
    @Untainted String valueString = get(name);
    return (@Untainted Collection<@Untainted String> ) StringUtils.getStringCollection(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s.  
   * If no such property is specified then <code>null</code> is returned.
   * 
   * @param name property name.
   * @return property value as an array of <code>String</code>s, 
   *         or <code>null</code>. 
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @Untainted String @Untainted [] getStrings(@Tainted Configuration this, @Tainted String name) {
    @Untainted String valueString = get(name);
    return (@Untainted String @Untainted []) StringUtils.getStrings(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s.  
   * If no such property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of <code>String</code>s, 
   *         or default value. 
   */
  public @Tainted String @Tainted [] getStrings(@Tainted Configuration this, @Tainted String name, @Tainted String @Tainted ... defaultValue) {
    @Tainted
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return StringUtils.getStrings(valueString);
    }
  }
  
  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * a collection of <code>String</code>s, trimmed of the leading and trailing whitespace.  
   * If no such property is specified then empty <code>Collection</code> is returned.
   *
   * @param name property name.
   * @return property value as a collection of <code>String</code>s, or empty <code>Collection</code> 
   */
  public @Tainted Collection<@Tainted String> getTrimmedStringCollection(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String valueString = get(name);
    if (null == valueString) {
      @Tainted
      Collection<@Tainted String> empty = new @Tainted ArrayList<@Tainted String>();
      return empty;
    }
    return StringUtils.getTrimmedStringCollection(valueString);
  }
  
  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then an empty array is returned.
   * 
   * @param name property name.
   * @return property value as an array of trimmed <code>String</code>s, 
   *         or empty array. 
   */
  public @Tainted String @Tainted [] getTrimmedStrings(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    String valueString = get(name);
    return StringUtils.getTrimmedStrings(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of trimmed <code>String</code>s, 
   *         or default value. 
   */
  public @Tainted String @Tainted [] getTrimmedStrings(@Tainted Configuration this, @Tainted String name, @Tainted String @Tainted ... defaultValue) {
    @Tainted
    String valueString = get(name);
    if (null == valueString) {
      return defaultValue;
    } else {
      return StringUtils.getTrimmedStrings(valueString);
    }
  }

  /** 
   * Set the array of string values for the <code>name</code> property as 
   * as comma delimited values.  
   * 
   * @param name property name.
   * @param values The values
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setStrings(@Tainted Configuration this, @Tainted String name, @Untainted String @Tainted ... values) {
    set(name, (@Untainted String) StringUtils.arrayToString(values));
  }

  /**
   * Get the socket address for <code>name</code> property as a
   * <code>InetSocketAddress</code>.
   * @param name property name.
   * @param defaultAddress the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  public @Tainted InetSocketAddress getSocketAddr( @Tainted Configuration this, @Tainted
      String name, @Untainted String defaultAddress, @Tainted int defaultPort) {
    final @Untainted String address = get(name, defaultAddress);
    return NetUtils.createSocketAddr(address, defaultPort, name);
  }

  /**
   * Set the socket address for the <code>name</code> property as
   * a <code>host:port</code>.
   */
  public void setSocketAddr(@Tainted Configuration this, @Tainted String name, @Untainted InetSocketAddress addr) {
    set(name, NetUtils.getHostPortString(addr));
  }
  
  /**
   * Set the socket address a client can use to connect for the
   * <code>name</code> property as a <code>host:port</code>.  The wildcard
   * address is replaced with the local host's address.
   * @param name property name.
   * @param addr InetSocketAddress of a listener to store in the given property
   * @return InetSocketAddress for clients to connect
   */
  public @Untainted InetSocketAddress updateConnectAddr(@Tainted Configuration this, @Tainted String name,
                                             @Untainted InetSocketAddress addr) {
    final @Untainted InetSocketAddress connectAddr = NetUtils.getConnectAddress(addr);
    setSocketAddr(name, connectAddr);
    return connectAddr;
  }
  
  /**
   * Load a class by name.
   * 
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public @Tainted Class<@Tainted ?> getClassByName(@Tainted Configuration this, @Tainted String name) throws ClassNotFoundException {
    @Tainted
    Class<@Tainted ?> ret = getClassByNameOrNull(name);
    if (ret == null) {
      throw new @Tainted ClassNotFoundException("Class " + name + " not found");
    }
    return ret;
  }
  
  /**
   * Load a class by name, returning null rather than throwing an exception
   * if it couldn't be loaded. This is to avoid the overhead of creating
   * an exception.
   * 
   * @param name the class name
   * @return the class object, or null if it could not be found.
   */
  public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getClassByNameOrNull(@Tainted Configuration this, @Tainted String name) {
    @Tainted
    Map<@Tainted String, @Tainted WeakReference<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>> map;
    
    synchronized (CACHE_CLASSES) {
      map = CACHE_CLASSES.get(classLoader);
      if (map == null) {
        map = Collections.synchronizedMap(
          new @Tainted WeakHashMap<@Tainted String, @Tainted WeakReference<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>>());
        CACHE_CLASSES.put(classLoader, map);
      }
    }

    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> clazz = null;
    @Tainted
    WeakReference<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>> ref = map.get(name); 
    if (ref != null) {
       clazz = ref.get();
    }
     
    if (clazz == null) {
      try {
        clazz = Class.forName(name, true, classLoader);
      } catch (@Tainted ClassNotFoundException e) {
        // Leave a marker that the class isn't found
        map.put(name, new @Tainted WeakReference<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>(NEGATIVE_CACHE_SENTINEL));
        return null;
      }
      // two putters can race here, but they'll put the same class
      map.put(name, new @Tainted WeakReference<@Tainted Class<@Tainted ? extends java.lang.@Tainted Object>>(clazz));
      return clazz;
    } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
      return null; // not found
    } else {
      // cache hit
      return clazz;
    }
  }

  /** 
   * Get the value of the <code>name</code> property
   * as an array of <code>Class</code>.
   * The value of the property specifies a list of comma separated class names.  
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * @param name the property name.
   * @param defaultValue default value.
   * @return property value as a <code>Class[]</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] getClasses(@Tainted Configuration this, @Tainted String name, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted ... defaultValue) {
    @Tainted
    String valueString = getRaw(name);
    if (null == valueString) {
      return defaultValue;
    }
    @Tainted
    String @Tainted [] classnames = getTrimmedStrings(name);
    try {
      @Tainted
      Class<?> @Tainted [] classes = new Class<?> @Tainted [classnames.length];
      for(@Tainted int i = 0; i < classnames.length; i++) {
        classes[i] = getClassByName(classnames[i]);
      }
      return classes;
    } catch (@Tainted ClassNotFoundException e) {
      throw new @Tainted RuntimeException(e);
    }
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>Class</code>.  
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @return property value as a <code>Class</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> getClass(@Tainted Configuration this, @Tainted String name, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> defaultValue) {
    @Tainted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    try {
      return getClassByName(valueString);
    } catch (@Tainted ClassNotFoundException e) {
      throw new @Tainted RuntimeException(e);
    }
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>Class</code>
   * implementing the interface specified by <code>xface</code>.
   *   
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * An exception is thrown if the returned class does not implement the named
   * interface. 
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @param xface the interface implemented by the named class.
   * @return property value as a <code>Class</code>, 
   *         or <code>defaultValue</code>.
   */
  public <@Tainted U extends java.lang.@Tainted Object> @Tainted Class<@Tainted ? extends @Tainted U> getClass(@Tainted Configuration this, @Tainted String name, 
                                         @Tainted
                                         Class<@Tainted ? extends @Tainted U> defaultValue, 
                                         @Tainted
                                         Class<@Tainted U> xface) {
    try {
      @Tainted
      Class<@Tainted ? extends java.lang.@Tainted Object> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new @Tainted RuntimeException(theClass+" not "+xface.getName());
      else if (theClass != null)
        return theClass.asSubclass(xface);
      else
        return null;
    } catch (@Tainted Exception e) {
      throw new @Tainted RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>List</code>
   * of objects implementing the interface specified by <code>xface</code>.
   * 
   * An exception is thrown if any of the classes does not exist, or if it does
   * not implement the named interface.
   * 
   * @param name the property name.
   * @param xface the interface implemented by the classes named by
   *        <code>name</code>.
   * @return a <code>List</code> of objects implementing <code>xface</code>.
   */
  @SuppressWarnings("unchecked")
  public <@Tainted U extends java.lang.@Tainted Object> @Tainted List<@Tainted U> getInstances(@Tainted Configuration this, @Tainted String name, @Tainted Class<@Tainted U> xface) {
    @Tainted
    List<@Tainted U> ret = new @Tainted ArrayList<@Tainted U>();
    @Tainted
    Class<@Tainted ? extends java.lang.@Tainted Object> @Tainted [] classes = getClasses(name);
    for (@Tainted Class<@Tainted ? extends java.lang.@Tainted Object> cl: classes) {
      if (!xface.isAssignableFrom(cl)) {
        throw new @Tainted RuntimeException(cl + " does not implement " + xface);
      }
      ret.add((@Tainted U)ReflectionUtils.newInstance(cl, this));
    }
    return ret;
  }

  /** 
   * Set the value of the <code>name</code> property to the name of a 
   * <code>theClass</code> implementing the given interface <code>xface</code>.
   * 
   * An exception is thrown if <code>theClass</code> does not implement the 
   * interface <code>xface</code>. 
   * 
   * @param name property name.
   * @param theClass property value.
   * @param xface the interface implemented by the named class.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setClass(@Tainted Configuration this, @Tainted String name, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> theClass, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new @Tainted RuntimeException(theClass+" not "+xface.getName());
    //TODO: Revisit Class.getName but based on the Java Language Specification this seems safe
    set(name, (@Untainted String) theClass.getName());
  }

  /** 
   * Get a local file under a directory named by <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   * 
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public @Tainted Path getLocalPath(@Tainted Configuration this, @Tainted String dirsProp, @Tainted String path)
    throws IOException {
    @Tainted
    String @Tainted [] dirs = getTrimmedStrings(dirsProp);
    @Tainted
    int hashCode = path.hashCode();
    @Tainted
    FileSystem fs = FileSystem.getLocal(this);
    for (@Tainted int i = 0; i < dirs.length; i++) {  // try each local dir
      @Tainted
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      @Tainted
      Path file = new @Tainted Path(dirs[index], path);
      @Tainted
      Path dir = file.getParent();
      if (fs.mkdirs(dir) || fs.exists(dir)) {
        return file;
      }
    }
    LOG.warn("Could not make " + path + 
             " in local directories from " + dirsProp);
    for(@Tainted int i=0; i < dirs.length; i++) {
      @Tainted
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      LOG.warn(dirsProp + "[" + index + "]=" + dirs[index]);
    }
    throw new @Tainted IOException("No valid local directories in property: "+dirsProp);
  }

  /** 
   * Get a local file name under a directory named in <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   * 
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public @Tainted File getFile(@Tainted Configuration this, @Tainted String dirsProp, @Tainted String path)
    throws IOException {
    @Tainted
    String @Tainted [] dirs = getTrimmedStrings(dirsProp);
    @Tainted
    int hashCode = path.hashCode();
    for (@Tainted int i = 0; i < dirs.length; i++) {  // try each local dir
      @Tainted
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      @Tainted
      File file = new @Tainted File(dirs[index], path);
      @Tainted
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new @Tainted IOException("No valid local directories in property: "+dirsProp);
  }

  /** 
   * Get the {@link URL} for the named resource.
   * 
   * @param name resource name.
   * @return the url for the named resource.
   */
  public @Tainted URL getResource(@Tainted Configuration this, @Tainted String name) {
    return classLoader.getResource(name);
  }
  
  /** 
   * Get an input stream attached to the configuration resource with the
   * given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return an input stream attached to the resource.
   */
  public @Tainted InputStream getConfResourceAsInputStream(@Tainted Configuration this, @Tainted String name) {
    try {
      @Tainted
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (@Tainted Exception e) {
      return null;
    }
  }

  /** 
   * Get a {@link Reader} attached to the configuration resource with the
   * given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return a reader attached to the resource.
   */
  public @Tainted Reader getConfResourceAsReader(@Tainted Configuration this, @Tainted String name) {
    try {
      @Tainted
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new @Tainted InputStreamReader(url.openStream());
    } catch (@Tainted Exception e) {
      return null;
    }
  }

  /**
   * Get the set of parameters marked final.
   *
   * @return final parameter set.
   */
  public @Tainted Set<@Tainted String> getFinalParameters(@Tainted Configuration this) {
    return new @Tainted HashSet<@Tainted String>(finalParameters);
  }

  protected synchronized @Tainted Properties getProps(@Tainted Configuration this) {
    if (properties == null) {
      properties = new @Tainted Properties();
      @Tainted
      HashMap<@Tainted String, @Tainted String @Tainted []> backup = 
        new @Tainted HashMap<@Tainted String, @Tainted String @Tainted []>(updatingResource);
      loadResources(properties, resources, quietmode);
      if (overlay!= null) {
        properties.putAll(overlay);
        for (Map.@Tainted Entry<@Tainted Object, @Tainted Object> item: overlay.entrySet()) {
          @Tainted
          String key = (@Tainted String)item.getKey();
          updatingResource.put(key, backup.get(key));
        }
      }
    }
    return properties;
  }

  /**
   * Return the number of keys in the configuration.
   *
   * @return number of keys in the configuration.
   */
  public @Tainted int size(@Tainted Configuration this) {
    return getProps().size();
  }

  /**
   * Clears all keys from the configuration.
   */
  public void clear(@Tainted Configuration this) {
    getProps().clear();
    getOverlay().clear();
  }

  /**
   * Get an {@link Iterator} to go through the list of <code>String</code> 
   * key-value pairs in the configuration.
   * 
   * @return an iterator over the entries.
   */
  //Since Properties is populated only using OsTrusted resources, we assume it's entry set is OsTrusted
  //Todo: The Iterator.iterator will return by default a map to Untrusted values.  We need to polymorphically annotate this
  //Todo: in some fashion
  @SuppressWarnings("ostrusted:cast.unsafe" )
  @Override
  public @Tainted Iterator<Map.@Tainted Entry<@Tainted String, @Untainted String>> iterator(@Tainted Configuration this) {
    // Get a copy of just the string to string pairs. After the old object
    // methods that allow non-strings to be put into configurations are removed,
    // we could replace properties with a Map<String,String> and get rid of this
    // code.
    @Tainted
    Map<@Tainted String, @Untainted String> result = new @Tainted HashMap<@Tainted String, @Untainted String>();
    for(Map.@Tainted Entry<@Tainted Object, @Untainted Object> item: (Set <Entry<@Tainted Object, @Untainted Object>>) getProps().entrySet() ) {
      if (item.getKey() instanceof String &&
          item.getValue() instanceof String) {
        result.put((@Tainted String) item.getKey(), (@Untainted String) item.getValue());
      }
    }
    return (@Tainted Iterator<Map.@Tainted Entry<@Tainted String, @Untainted String>>) result.entrySet().iterator();
  }

  private @Tainted Document parse(@Tainted Configuration this, @Tainted DocumentBuilder builder, @Tainted URL url)
      throws IOException, SAXException {
    if (!quietmode) {
      LOG.debug("parsing URL " + url);
    }
    if (url == null) {
      return null;
    }
    return parse(builder, url.openStream(), url.toString());
  }

  private @Tainted Document parse(@Tainted Configuration this, @Tainted DocumentBuilder builder, @Tainted InputStream is,
      @Tainted
      String systemId) throws IOException, SAXException {
    if (!quietmode) {
      LOG.debug("parsing input stream " + is);
    }
    if (is == null) {
      return null;
    }
    try {
      return (systemId == null) ? builder.parse(is) : builder.parse(is,
          systemId);
    } finally {
      is.close();
    }
  }

  private void loadResources(@Tainted Configuration this, @Tainted Properties properties,
                             @Tainted
                             ArrayList<@Untainted Resource> resources,
                             @Tainted
                             boolean quiet) {
    if(loadDefaults) {
      for (@Untainted String resource : defaultResources) {
        loadResource(properties, new @Untainted Resource(resource), quiet);
      }
    
      //support the hadoop-site.xml as a deprecated case
      if(getResource("hadoop-site.xml")!=null) {
        loadResource(properties, new @Untainted Resource("hadoop-site.xml"), quiet);
      }
    }
    
    for (@Tainted int i = 0; i < resources.size(); i++) {
      @Tainted
      Resource ret = loadResource(properties, resources.get(i), quiet);
      if (ret != null) {
        resources.set(i, ret);
      }
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  //TODO: Since the value field in this method is read from trusted resources I have called it trusted and
  //TODO: cast the appropriate assignments to it, but I would like to have a second run through for validity's sake
  private @Untainted Resource loadResource(@Tainted Configuration this, @Tainted Properties properties, @Untainted Resource wrapper, @Tainted boolean quiet) {
    @Tainted
    String name = UNKNOWN_RESOURCE;
    try {
      @Untainted  Object resource = (@Untainted Object) wrapper.getResource();
      name = wrapper.getName();
      
      @Tainted
      DocumentBuilderFactory docBuilderFactory 
        = DocumentBuilderFactory.newInstance();
      //ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      //allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
          docBuilderFactory.setXIncludeAware(true);
      } catch (@Tainted UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser "
                + docBuilderFactory
                + ":" + e,
                e);
      }
      @Tainted
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      @Tainted
      Document doc = null;
      @Tainted
      Element root = null;
      @Tainted
      boolean returnCachedProperties = false;
      
      if (resource instanceof @Tainted URL) {                  // an URL resource
        doc = parse(builder, (@Tainted URL)resource);
      } else if (resource instanceof @Tainted String) {        // a CLASSPATH resource
        @Tainted
        URL url = getResource((@Tainted String)resource);
        doc = parse(builder, url);
      } else if (resource instanceof @Tainted Path) {          // a file resource
        // Can't use FileSystem API or we get an infinite loop
        // since FileSystem uses Configuration API.  Use java.io.File instead.
        @Tainted
        File file = new @Tainted File(((@Tainted Path)resource).toUri().getPath())
          .getAbsoluteFile();
        if (file.exists()) {
          if (!quiet) {
            LOG.debug("parsing File " + file);
          }
          doc = parse(builder, new @Tainted BufferedInputStream(
              new @Tainted FileInputStream(file)), ((@Tainted Path)resource).toString());
        }
      } else if (resource instanceof @Tainted InputStream) {
        doc = parse(builder, (@Tainted InputStream) resource, null);
        returnCachedProperties = true;
      } else if (resource instanceof @Tainted Properties) {
        overlay(properties, (@Tainted Properties)resource);
      } else if (resource instanceof @Tainted Element) {
        root = (@Tainted Element)resource;
      }

      if (doc == null && root == null) {
        if (quiet)
          return null;
        throw new @Tainted RuntimeException(resource + " not found");
      }

      if (root == null) {
        root = doc.getDocumentElement();
      }
      @Tainted
      Properties toAddTo = properties;
      if(returnCachedProperties) {
        toAddTo = new @Tainted Properties();
      }
      if (!"configuration".equals(root.getTagName()))
        LOG.fatal("bad conf file: top-level element not <configuration>");
      @Tainted
      NodeList props = root.getChildNodes();
      for (@Tainted int i = 0; i < props.getLength(); i++) {
        @Tainted
        Node propNode = props.item(i);
        if (!(propNode instanceof @Tainted Element))
          continue;
        @Tainted
        Element prop = (@Tainted Element)propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(toAddTo, (@Untainted Resource) new Resource(prop, name), quiet);
          continue;
        }
        if (!"property".equals(prop.getTagName()))
          LOG.warn("bad conf file: element not <property>");
        @Tainted
        NodeList fields = prop.getChildNodes();
        @Tainted
        String attr = null;
        @Untainted String value = null;
        @Tainted
        boolean finalParameter = false;
        @Tainted
        LinkedList<@Tainted String> source = new @Tainted LinkedList<@Tainted String>();
        for (@Tainted int j = 0; j < fields.getLength(); j++) {
          @Tainted
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof @Tainted Element))
            continue;
          @Tainted
          Element field = (@Tainted Element)fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes())
            attr = StringInterner.weakIntern(
                ((@Tainted Text)field.getFirstChild()).getData().trim());
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = (@Untainted String) StringInterner.weakIntern(((@Tainted Text)field.getFirstChild()).getData());
          if ("final".equals(field.getTagName()) && field.hasChildNodes())
            finalParameter = "true".equals(((@Tainted Text)field.getFirstChild()).getData());
          if ("source".equals(field.getTagName()) && field.hasChildNodes())
            source.add(StringInterner.weakIntern(
                ((@Tainted Text)field.getFirstChild()).getData()));
        }
        source.add(name);

        //ostrusted The source variables are read from a trusted resource so consider their strings trusted
        // Ignore this parameter if it has already been marked as 'final'
        if (attr != null) {
          if (deprecatedKeyMap.containsKey(attr)) {
            @Tainted
            DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(attr);
            keyInfo.accessed = false;
            for (@Tainted String key:keyInfo.newKeys) {
              // update new keys with deprecated key's value 
              loadProperty(toAddTo, name, key, value, finalParameter, 
                      (@Untainted String @Tainted [])  source.toArray(new @Untainted String @Tainted [source.size()]));
            }
          }
          else {
            loadProperty(toAddTo, name, attr, value, finalParameter, 
                    (@Untainted String @Tainted [])  source.toArray(new @Untainted String @Tainted [source.size()]));
          }
        }
      }
      
      if (returnCachedProperties) {
        overlay(properties, toAddTo);
        return (@Untainted Resource) new Resource(toAddTo, name);
      }
      return null;
    } catch (@Tainted IOException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new @Tainted RuntimeException(e);
    } catch (@Tainted DOMException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new @Tainted RuntimeException(e);
    } catch (@Tainted SAXException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new @Tainted RuntimeException(e);
    } catch (@Tainted ParserConfigurationException e) {
      LOG.fatal("error parsing conf " + name , e);
      throw new @Tainted RuntimeException(e);
    }
  }

  private void overlay(@Tainted Configuration this, @Tainted Properties to, @Tainted Properties from) {
    for (@Tainted Entry<@Tainted Object, @Tainted Object> entry: from.entrySet()) {
      to.put(entry.getKey(), entry.getValue());
    }
  }
  
  private void loadProperty(@Tainted Configuration this, @Tainted Properties properties, @Tainted String name, @Tainted String attr,
      @Untainted String value, @Tainted boolean finalParameter, @Untainted String @Tainted [] source) {
    if (value != null) {
      if (!finalParameters.contains(attr)) {
        properties.setProperty(attr, value);
        updatingResource.put(attr, source);
      } else if (!value.equals(properties.getProperty(attr))) {
        LOG.warn(name+":an attempt to override final parameter: "+attr
            +";  Ignoring.");
      }
    }
    if (finalParameter) {
      finalParameters.add(attr);
    }
  }

  /** 
   * Write out the non-default properties in this configuration to the given
   * {@link OutputStream} using UTF-8 encoding.
   * 
   * @param out the output stream to write to.
   */
  public void writeXml(@Tainted Configuration this, @Tainted OutputStream out) throws IOException {
    writeXml(new @Tainted OutputStreamWriter(out, "UTF-8"));
  }

  /** 
   * Write out the non-default properties in this configuration to the given
   * {@link Writer}.
   * 
   * @param out the writer to write to.
   */
  public void writeXml(@Tainted Configuration this, @Tainted Writer out) throws IOException {
    @Tainted
    Document doc = asXmlDocument();

    try {
      @Tainted
      DOMSource source = new @Tainted DOMSource(doc);
      @Tainted
      StreamResult result = new @Tainted StreamResult(out);
      @Tainted
      TransformerFactory transFactory = TransformerFactory.newInstance();
      @Tainted
      Transformer transformer = transFactory.newTransformer();

      // Important to not hold Configuration log while writing result, since
      // 'out' may be an HDFS stream which needs to lock this configuration
      // from another thread.
      transformer.transform(source, result);
    } catch (@Tainted TransformerException te) {
      throw new @Tainted IOException(te);
    }
  }

  /**
   * Return the XML DOM corresponding to this Configuration.
   */
  private synchronized @Tainted Document asXmlDocument(@Tainted Configuration this) throws IOException {
    @Tainted
    Document doc;
    try {
      doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    } catch (@Tainted ParserConfigurationException pe) {
      throw new @Tainted IOException(pe);
    }
    @Tainted
    Element conf = doc.createElement("configuration");
    doc.appendChild(conf);
    conf.appendChild(doc.createTextNode("\n"));
    handleDeprecation(); //ensure properties is set and deprecation is handled
    for (@Tainted Enumeration<@Tainted Object> e = properties.keys(); e.hasMoreElements();) {
      @Tainted
      String name = (@Tainted String)e.nextElement();
      @Tainted
      Object object = properties.get(name);
      @Tainted
      String value = null;
      if (object instanceof @Tainted String) {
        value = (@Tainted String) object;
      }else {
        continue;
      }
      @Tainted
      Element propNode = doc.createElement("property");
      conf.appendChild(propNode);

      @Tainted
      Element nameNode = doc.createElement("name");
      nameNode.appendChild(doc.createTextNode(name));
      propNode.appendChild(nameNode);

      @Tainted
      Element valueNode = doc.createElement("value");
      valueNode.appendChild(doc.createTextNode(value));
      propNode.appendChild(valueNode);

      if (updatingResource != null) {
        @Tainted
        String @Tainted [] sources = updatingResource.get(name);
        if(sources != null) {
          for(@Tainted String s : sources) {
            @Tainted
            Element sourceNode = doc.createElement("source");
            sourceNode.appendChild(doc.createTextNode(s));
            propNode.appendChild(sourceNode);
          }
        }
      }
      
      conf.appendChild(doc.createTextNode("\n"));
    }
    return doc;
  }

  /**
   *  Writes out all the parameters and their properties (final and resource) to
   *  the given {@link Writer}
   *  The format of the output would be 
   *  { "properties" : [ {key1,value1,key1.isFinal,key1.resource}, {key2,value2,
   *  key2.isFinal,key2.resource}... ] } 
   *  It does not output the parameters of the configuration object which is 
   *  loaded from an input stream.
   * @param out the Writer to write to
   * @throws IOException
   */
  public static void dumpConfiguration(@Tainted Configuration config,
      @Tainted
      Writer out) throws IOException {
    @Tainted
    JsonFactory dumpFactory = new @Tainted JsonFactory();
    @Tainted
    JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
    dumpGenerator.writeStartObject();
    dumpGenerator.writeFieldName("properties");
    dumpGenerator.writeStartArray();
    dumpGenerator.flush();
    synchronized (config) {
      for (Map.@Tainted Entry<@Tainted Object, @Tainted Object> item: config.getProps().entrySet()) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("key", (@Tainted String) item.getKey());
        dumpGenerator.writeStringField("value", 
                                       config.get((@Tainted String) item.getKey()));
        dumpGenerator.writeBooleanField("isFinal",
                                        config.finalParameters.contains(item.getKey()));
        @Tainted
        String @Tainted [] resources = config.updatingResource.get(item.getKey());
        @Tainted
        String resource = UNKNOWN_RESOURCE;
        if(resources != null && resources.length > 0) {
          resource = resources[0];
        }
        dumpGenerator.writeStringField("resource", resource);
        dumpGenerator.writeEndObject();
      }
    }
    dumpGenerator.writeEndArray();
    dumpGenerator.writeEndObject();
    dumpGenerator.flush();
  }
  
  /**
   * Get the {@link ClassLoader} for this job.
   * 
   * @return the correct class loader.
   */
  public @Tainted ClassLoader getClassLoader(@Tainted Configuration this) {
    return classLoader;
  }
  
  /**
   * Set the class loader that will be used to load the various objects.
   * 
   * @param classLoader the new class loader.
   */
  public void setClassLoader(@Tainted Configuration this, @Tainted ClassLoader classLoader) {
    this.classLoader = classLoader;
  }
  
  @Override
  public @Tainted String toString(@Tainted Configuration this) {
    @Tainted
    StringBuilder sb = new @Tainted StringBuilder();
    sb.append("Configuration: ");
    if(loadDefaults) {
      toString(defaultResources, sb);
      if(resources.size()>0) {
        sb.append(", ");
      }
    }
    toString(resources, sb);
    return sb.toString();
  }
  
  private <T extends Object> void toString(@Tainted Configuration this, @Tainted List<T> resources, @Tainted StringBuilder sb) {
    @Tainted
    ListIterator<T> i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(", ");
      }
      sb.append(i.next());
    }
  }

  /** 
   * Set the quietness-mode. 
   * 
   * In the quiet-mode, error and informational messages might not be logged.
   * 
   * @param quietmode <code>true</code> to set quiet-mode on, <code>false</code>
   *              to turn it off.
   */
  public synchronized void setQuietMode(@Tainted Configuration this, @Tainted boolean quietmode) {
    this.quietmode = quietmode;
  }

  synchronized @Tainted boolean getQuietMode(@Tainted Configuration this) {
    return this.quietmode;
  }
  
  /** For debugging.  List non-default properties to the terminal and exit. */
  public static void main(@Tainted String @Tainted [] args) throws Exception {
    new @Tainted Configuration().writeXml(System.out);
  }

  //TODO: ostrusted, configuraiont becomes a source of vulnerability if cast to Writable and
  //TODO: and used with an Untrusted DataInput.  Simple grepping does not show these uses but
  //TODO: this remains unproven.
  @Override
  @SuppressWarnings( {"ostrusted:cast.unsafe", "override.param.invalid"} )
  public void readFields(@Tainted Configuration this, @Untainted DataInput in) throws IOException {
    clear();
    @Tainted
    int size = WritableUtils.readVInt(in);
    for(@Tainted int i=0; i < size; ++i) {
      @Tainted
      String key = org.apache.hadoop.io.Text.readString(in);
      @Untainted String value = (@Untainted String) org.apache.hadoop.io.Text.readString(in);
      set(key, value); 
      @Tainted String sources @Tainted [] = WritableUtils.readCompressedStringArray(in);
      updatingResource.put(key, sources);
    }
  }

  //@Override
  @Override
  public void write(@Tainted Configuration this, @Tainted DataOutput out) throws IOException {
    @Tainted
    Properties props = getProps();
    WritableUtils.writeVInt(out, props.size());
    for(Map.@Tainted Entry<@Tainted Object, @Tainted Object> item: props.entrySet()) {
      org.apache.hadoop.io.Text.writeString(out, (@Tainted String) item.getKey());
      org.apache.hadoop.io.Text.writeString(out, (@Tainted String) item.getValue());
      WritableUtils.writeCompressedStringArray(out, 
          updatingResource.get(item.getKey()));
    }
  }
  
  /**
   * get keys matching the the regex 
   * @param regex
   * @return Map<String,String> with matching keys
   */
  public @Tainted Map<@Tainted String, @Tainted String> getValByRegex(@Tainted Configuration this, @Tainted String regex) {
    @Tainted
    Pattern p = Pattern.compile(regex);

    @Tainted
    Map<@Tainted String, @Tainted String> result = new @Tainted HashMap<@Tainted String, @Tainted String>();
    @Tainted
    Matcher m;

    for(Map.@Tainted Entry<@Tainted Object, @Tainted Object> item: getProps().entrySet()) {
      if (item.getKey() instanceof @Tainted String && 
          item.getValue() instanceof @Tainted String) {
        m = p.matcher((@Tainted String)item.getKey());
        if(m.find()) { // match
          result.put((@Tainted String) item.getKey(), (@Tainted String) item.getValue());
        }
      }
    }
    return result;
  }

  //Load deprecated keys in common
  private static void addDeprecatedKeys() {
    Configuration.addDeprecation("topology.script.file.name", 
               new @Tainted String @Tainted []{CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY});
    Configuration.addDeprecation("topology.script.number.args", 
               new @Tainted String @Tainted []{CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY});
    Configuration.addDeprecation("hadoop.configured.node.mapping", 
               new @Tainted String @Tainted []{CommonConfigurationKeys.NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY});
    Configuration.addDeprecation("topology.node.switch.mapping.impl", 
               new @Tainted String @Tainted []{CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY});
    Configuration.addDeprecation("dfs.df.interval", 
               new @Tainted String @Tainted []{CommonConfigurationKeys.FS_DF_INTERVAL_KEY});
    Configuration.addDeprecation("hadoop.native.lib", 
               new @Tainted String @Tainted []{CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY});
    Configuration.addDeprecation("fs.default.name", 
               new @Tainted String @Tainted []{CommonConfigurationKeys.FS_DEFAULT_NAME_KEY});
    Configuration.addDeprecation("dfs.umaskmode",
        new @Tainted String @Tainted []{CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY});
  }
  
  /**
   * A unique class which is used as a sentinel value in the caching
   * for getClassByName. {@link Configuration#getClassByNameOrNull(String)}
   */
  private static abstract class NegativeCacheSentinel {}

  public static void dumpDeprecatedKeys() {
    for (Map.@Tainted Entry<@Tainted String, @Tainted DeprecatedKeyInfo> entry : deprecatedKeyMap.entrySet()) {
      @Tainted
      StringBuilder newKeys = new @Tainted StringBuilder();
      for (@Tainted String newKey : entry.getValue().newKeys) {
        newKeys.append(newKey).append("\t");
      }
      System.out.println(entry.getKey() + "\t" + newKeys.toString());
    }
  }
}
