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

package org.apache.hadoop.io.serializer;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * An experimental {@link Serialization} for Java {@link Serializable} classes.
 * </p>
 * @see JavaSerializationComparator
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class JavaSerialization implements @Tainted Serialization<@Tainted Serializable> {

  static class JavaSerializationDeserializer<@Tainted T extends @Tainted Serializable>
    implements @Tainted Deserializer<T> {

    private @Tainted ObjectInputStream ois;

    @Override
    public void open(JavaSerialization.@Tainted JavaSerializationDeserializer<T> this, @Tainted InputStream in) throws IOException {
      ois = new @Tainted ObjectInputStream(in) {
        @Override protected void readStreamHeader() {
          // no header
        }
      };
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public @Tainted T deserialize(JavaSerialization.@Tainted JavaSerializationDeserializer<T> this, @Tainted T object) throws IOException {
      try {
        // ignore passed-in object
        return (T) ois.readObject();
      } catch (@Tainted ClassNotFoundException e) {
        throw new @Tainted IOException(e.toString());
      }
    }

    @Override
    public void close(JavaSerialization.@Tainted JavaSerializationDeserializer<T> this) throws IOException {
      ois.close();
    }

  }

  static class JavaSerializationSerializer
    implements @Tainted Serializer<@Tainted Serializable> {

    private @Tainted ObjectOutputStream oos;

    @Override
    public void open(JavaSerialization.@Tainted JavaSerializationSerializer this, @Tainted OutputStream out) throws IOException {
      oos = new @Tainted ObjectOutputStream(out) {
        @Override protected void writeStreamHeader() {
          // no header
        }
      };
    }

    @Override
    public void serialize(JavaSerialization.@Tainted JavaSerializationSerializer this, @Tainted Serializable object) throws IOException {
      oos.reset(); // clear (class) back-references
      oos.writeObject(object);
    }

    @Override
    public void close(JavaSerialization.@Tainted JavaSerializationSerializer this) throws IOException {
      oos.close();
    }

  }

  @Override
  @InterfaceAudience.Private
  public @Tainted boolean accept(@Tainted JavaSerialization this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> c) {
    return Serializable.class.isAssignableFrom(c);
  }

  @Override
  @InterfaceAudience.Private
  public @Tainted Deserializer<@Tainted Serializable> getDeserializer(@Tainted JavaSerialization this, @Tainted Class<@Tainted Serializable> c) {
    return new @Tainted JavaSerializationDeserializer<@Tainted Serializable>();
  }

  @Override
  @InterfaceAudience.Private
  public @Tainted Serializer<@Tainted Serializable> getSerializer(@Tainted JavaSerialization this, @Tainted Class<@Tainted Serializable> c) {
    return new @Tainted JavaSerializationSerializer();
  }

}
