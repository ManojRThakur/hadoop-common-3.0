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

package org.apache.hadoop.io.serializer.avro;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Base class for providing serialization to Avro types.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AvroSerialization<@Tainted T> extends @Tainted Configured 
	implements @Tainted Serialization<@Tainted T>{
  
  @InterfaceAudience.Private
  public static final @Tainted String AVRO_SCHEMA_KEY = "Avro-Schema";

  @Override
  @InterfaceAudience.Private
  public @Tainted Deserializer<@Tainted T> getDeserializer(@Tainted AvroSerialization<T> this, @Tainted Class<@Tainted T> c) {
    return new @Tainted AvroDeserializer(c);
  }

  @Override
  @InterfaceAudience.Private
  public @Tainted Serializer<@Tainted T> getSerializer(@Tainted AvroSerialization<T> this, @Tainted Class<@Tainted T> c) {
    return new @Tainted AvroSerializer(c);
  }

  /**
   * Return an Avro Schema instance for the given class.
   */
  @InterfaceAudience.Private
  public abstract @Tainted Schema getSchema(@Tainted AvroSerialization<T> this, @Tainted T t);

  /**
   * Create and return Avro DatumWriter for the given class.
   */
  @InterfaceAudience.Private
  public abstract @Tainted DatumWriter<@Tainted T> getWriter(@Tainted AvroSerialization<T> this, @Tainted Class<@Tainted T> clazz);

  /**
   * Create and return Avro DatumReader for the given class.
   */
  @InterfaceAudience.Private
  public abstract @Tainted DatumReader<@Tainted T> getReader(@Tainted AvroSerialization<T> this, @Tainted Class<@Tainted T> clazz);

  class AvroSerializer implements @Tainted Serializer<T> {

    private @Tainted DatumWriter<@Tainted T> writer;
    private @Tainted BinaryEncoder encoder;
    private @Tainted OutputStream outStream;

    @Tainted
    AvroSerializer(@Tainted Class<@Tainted T> clazz) {
      this.writer = getWriter(clazz);
    }

    @Override
    public void close(@Tainted AvroSerialization<T>.AvroSerializer this) throws IOException {
      encoder.flush();
      outStream.close();
    }

    @Override
    public void open(@Tainted AvroSerialization<T>.AvroSerializer this, @Tainted OutputStream out) throws IOException {
      outStream = out;
      encoder = EncoderFactory.get().binaryEncoder(out, encoder);
    }

    @Override
    public void serialize(@Tainted AvroSerialization<T>.AvroSerializer this, @Tainted T t) throws IOException {
      writer.setSchema(getSchema(t));
      writer.write(t, encoder);
    }

  }

  class AvroDeserializer implements @Tainted Deserializer<T> {

    private @Tainted DatumReader<@Tainted T> reader;
    private @Tainted BinaryDecoder decoder;
    private @Tainted InputStream inStream;

    @Tainted
    AvroDeserializer(@Tainted Class<@Tainted T> clazz) {
      this.reader = getReader(clazz);
    }

    @Override
    public void close(@Tainted AvroSerialization<T>.AvroDeserializer this) throws IOException {
      inStream.close();
    }

    @Override
    public @Tainted T deserialize(@Tainted AvroSerialization<T>.AvroDeserializer this, @Tainted T t) throws IOException {
      return reader.read(t, decoder);
    }

    @Override
    public void open(@Tainted AvroSerialization<T>.AvroDeserializer this, @Tainted InputStream in) throws IOException {
      inStream = in;
      decoder = DecoderFactory.get().binaryDecoder(in, decoder);
    }

  }

}
