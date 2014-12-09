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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link Serialization} for {@link Writable}s that delegates to
 * {@link Writable#write(java.io.DataOutput)} and
 * {@link Writable#readFields(java.io.DataInput)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WritableSerialization extends @Tainted Configured
	implements @Tainted Serialization<@Tainted Writable> {
  static class WritableDeserializer extends @Tainted Configured
  	implements @Tainted Deserializer<@Tainted Writable> {

    private @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> writableClass;
    private @Tainted DataInputStream dataIn;
    
    public @Tainted WritableDeserializer(@Tainted Configuration conf, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> c) {
      setConf(conf);
      this.writableClass = c;
    }
    
    @Override
    public void open(WritableSerialization.@Tainted WritableDeserializer this, @Tainted InputStream in) {
      if (in instanceof @Tainted DataInputStream) {
        dataIn = (@Tainted DataInputStream) in;
      } else {
        dataIn = new @Tainted DataInputStream(in);
      }
    }
    
    @Override
    public @Tainted Writable deserialize(WritableSerialization.@Tainted WritableDeserializer this, @Tainted Writable w) throws IOException {
      @Tainted
      Writable writable;
      if (w == null) {
        writable 
          = (@Tainted Writable) ReflectionUtils.newInstance(writableClass, getConf());
      } else {
        writable = w;
      }
      writable.readFields(dataIn);
      return writable;
    }

    @Override
    public void close(WritableSerialization.@Tainted WritableDeserializer this) throws IOException {
      dataIn.close();
    }
    
  }
  
  static class WritableSerializer extends @Tainted Configured implements
  	@Tainted
  	Serializer<@Tainted Writable> {
    
    private @Tainted DataOutputStream dataOut;
    
    @Override
    public void open(WritableSerialization.@Tainted WritableSerializer this, @Tainted OutputStream out) {
      if (out instanceof @Tainted DataOutputStream) {
        dataOut = (@Tainted DataOutputStream) out;
      } else {
        dataOut = new @Tainted DataOutputStream(out);
      }
    }

    @Override
    public void serialize(WritableSerialization.@Tainted WritableSerializer this, @Tainted Writable w) throws IOException {
      w.write(dataOut);
    }

    @Override
    public void close(WritableSerialization.@Tainted WritableSerializer this) throws IOException {
      dataOut.close();
    }

  }

  @InterfaceAudience.Private
  @Override
  public @Tainted boolean accept(@Tainted WritableSerialization this, @Tainted Class<@Tainted ? extends java.lang.@Tainted Object> c) {
    return Writable.class.isAssignableFrom(c);
  }

  @InterfaceAudience.Private
  @Override
  public @Tainted Serializer<@Tainted Writable> getSerializer(@Tainted WritableSerialization this, @Tainted Class<@Tainted Writable> c) {
    return new @Tainted WritableSerializer();
  }
  
  @InterfaceAudience.Private
  @Override
  public @Tainted Deserializer<@Tainted Writable> getDeserializer(@Tainted WritableSerialization this, @Tainted Class<@Tainted Writable> c) {
    return new @Tainted WritableDeserializer(getConf(), c);
  }

}
