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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.GenericsUtil;

/**
 * DefaultStringifier is the default implementation of the {@link Stringifier}
 * interface which stringifies the objects using base64 encoding of the
 * serialized version of the objects. The {@link Serializer} and
 * {@link Deserializer} are obtained from the {@link SerializationFactory}.
 * <br>
 * DefaultStringifier offers convenience methods to store/load objects to/from
 * the configuration.
 * 
 * @param <T> the class of the objects to stringify
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DefaultStringifier<@Tainted T extends java.lang.@Tainted Object> implements @Tainted Stringifier<T> {

  private static final @Tainted String SEPARATOR = ",";

  private @Tainted Serializer<@Tainted T> serializer;

  private @Tainted Deserializer<@Tainted T> deserializer;

  private @Tainted DataInputBuffer inBuf;

  private @Tainted DataOutputBuffer outBuf;

  public @Tainted DefaultStringifier(@Tainted Configuration conf, @Tainted Class<@Tainted T> c) {

    @Tainted
    SerializationFactory factory = new @Tainted SerializationFactory(conf);
    this.serializer = factory.getSerializer(c);
    this.deserializer = factory.getDeserializer(c);
    this.inBuf = new @Tainted DataInputBuffer();
    this.outBuf = new @Tainted DataOutputBuffer();
    try {
      serializer.open(outBuf);
      deserializer.open(inBuf);
    } catch (@Tainted IOException ex) {
      throw new @Tainted RuntimeException(ex);
    }
  }

  @Override
  public @Tainted T fromString(@Tainted DefaultStringifier<T> this, @Tainted String str) throws IOException {
    try {
      @Tainted
      byte @Tainted [] bytes = Base64.decodeBase64(str.getBytes("UTF-8"));
      inBuf.reset(bytes, bytes.length);
      T restored = deserializer.deserialize(null);
      return restored;
    } catch (@Tainted UnsupportedCharsetException ex) {
      throw new @Tainted IOException(ex.toString());
    }
  }

  @Override
  public @Untainted String toString(@Tainted DefaultStringifier<T> this, @Tainted T obj) throws IOException {
    outBuf.reset();
    serializer.serialize(obj);
    @Tainted
    byte @Tainted [] buf = new @Tainted byte @Tainted [outBuf.getLength()];
    System.arraycopy(outBuf.getData(), 0, buf, 0, buf.length);
    //OSTrusted, we're considering Base64 trusted
    return new @Untainted String(Base64.encodeBase64(buf));
  }

  @Override
  public void close(@Tainted DefaultStringifier<T> this) throws IOException {
    inBuf.close();
    outBuf.close();
    deserializer.close();
    serializer.close();
  }

  /**
   * Stores the item in the configuration with the given keyName.
   * 
   * @param <K>  the class of the item
   * @param conf the configuration to store
   * @param item the object to be stored
   * @param keyName the name of the key to use
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes. 
   */
  public static <@Tainted K extends java.lang.@Tainted Object> void store(@Tainted Configuration conf, @Tainted K item, @Tainted String keyName)
  throws IOException {

    @Tainted
    DefaultStringifier<@Tainted K> stringifier = new @Tainted DefaultStringifier<@Tainted K>(conf,
        GenericsUtil.getClass(item));
    conf.set(keyName, stringifier.toString(item));
    stringifier.close();
  }

  /**
   * Restores the object from the configuration.
   * 
   * @param <K> the class of the item
   * @param conf the configuration to use
   * @param keyName the name of the key to use
   * @param itemClass the class of the item
   * @return restored object
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes.
   */
  public static <@Tainted K extends java.lang.@Tainted Object> @Tainted K load(@Tainted Configuration conf, @Tainted String keyName,
      @Tainted
      Class<@Tainted K> itemClass) throws IOException {
    @Tainted
    DefaultStringifier<@Tainted K> stringifier = new @Tainted DefaultStringifier<@Tainted K>(conf,
        itemClass);
    try {
      @Tainted
      String itemStr = conf.get(keyName);
      return stringifier.fromString(itemStr);
    } finally {
      stringifier.close();
    }
  }

  /**
   * Stores the array of items in the configuration with the given keyName.
   * 
   * @param <K> the class of the item
   * @param conf the configuration to use 
   * @param items the objects to be stored
   * @param keyName the name of the key to use
   * @throws IndexOutOfBoundsException if the items array is empty
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes.         
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static <@Tainted K extends java.lang.@Tainted Object> void storeArray(@Tainted Configuration conf, @Untainted K @Tainted [] items,
      @Tainted
      String keyName) throws IOException {

    @Tainted
    DefaultStringifier<@Tainted K> stringifier = new @Tainted DefaultStringifier<@Tainted K>(conf, 
        GenericsUtil.getClass(items[0]));
    try {
      @Tainted StringBuilder builder = new @Tainted StringBuilder();
      for ( K item : items) {
        builder.append(stringifier.toString(item)).append(SEPARATOR);
      }
      //ostrusted, items are trusted, only entry into this builder
      conf.set(keyName, (@Untainted String) builder.toString());
    }
    finally {
      stringifier.close();
    }
  }

  /**
   * Restores the array of objects from the configuration.
   * 
   * @param <K> the class of the item
   * @param conf the configuration to use
   * @param keyName the name of the key to use
   * @param itemClass the class of the item
   * @return restored object
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes.
   */
  public static <@Tainted K extends java.lang.@Tainted Object> @Tainted K @Tainted [] loadArray(@Tainted Configuration conf, @Tainted String keyName,
      @Tainted
      Class<@Tainted K> itemClass) throws IOException {
    @Tainted
    DefaultStringifier<@Tainted K> stringifier = new @Tainted DefaultStringifier<@Tainted K>(conf,
        itemClass);
    try {
      @Tainted
      String itemStr = conf.get(keyName);
      @Tainted
      ArrayList<@Tainted K> list = new @Tainted ArrayList<@Tainted K>();
      @Tainted
      String @Tainted [] parts = itemStr.split(SEPARATOR);

      for (@Tainted String part : parts) {
        if (!part.isEmpty())
          list.add(stringifier.fromString(part));
      }

      return GenericsUtil.toArray(itemClass, list);
    }
    finally {
      stringifier.close();
    }
  }

}
