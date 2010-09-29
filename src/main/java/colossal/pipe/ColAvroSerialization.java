/*
 * Licensed to Think Big Analytics, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Think Big Analytics, Inc. licenses this file
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
 * 
 * Copyright 2010 Think Big Analytics. All Rights Reserved.
 */
package colossal.pipe;

import java.io.*;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.mapred.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.*;

/**
 * Avro serialization format used by Colossal pipe. Derived from the Avro map/reduce framework format.
 * The {@link Serialization} used by jobs configured with {@link ColPhase}. 
 */
public class ColAvroSerialization<T> extends Configured implements Serialization<AvroWrapper<T>> {

    public boolean accept(Class<?> c) {
        return AvroWrapper.class.isAssignableFrom(c);
    }

    /**
     * Returns the specified map output deserializer. Defaults to the final output deserializer if no map output schema was
     * specified.
     */
    public Deserializer<AvroWrapper<T>> getDeserializer(Class<AvroWrapper<T>> c) {
        // We need not rely on mapred.task.is.map here to determine whether map
        // output or final output is desired, since the mapreduce framework never
        // creates a deserializer for final output, only for map output.
        boolean isKey = AvroKey.class.isAssignableFrom(c);
        Schema schema = Schema.parse(isKey ? getConf().get(ColPhase.MAP_OUT_KEY_SCHEMA) : getConf().get(
                ColPhase.MAP_OUT_VALUE_SCHEMA));
        return new AvroWrapperDeserializer(new SpecificDatumReader<T>(schema), isKey);
    }

    private static final DecoderFactory FACTORY = new DecoderFactory();
    static {
        FACTORY.configureDirectDecoder(true);
    }

    private class AvroWrapperDeserializer implements Deserializer<AvroWrapper<T>> {

        private DatumReader<T> reader;
        private BinaryDecoder decoder;
        private boolean isKey;

        public AvroWrapperDeserializer(DatumReader<T> reader, boolean isKey) {
            this.reader = reader;
            this.isKey = isKey;
        }

        public void open(InputStream in) {
            this.decoder = FACTORY.createBinaryDecoder(in, decoder);
        }

        public AvroWrapper<T> deserialize(AvroWrapper<T> wrapper) throws IOException {
            T datum = reader.read(wrapper == null ? null : wrapper.datum(), decoder);
            if (wrapper == null) {
                wrapper = isKey ? new AvroKey<T>(datum) : new AvroValue<T>(datum);
            }
            else {
                wrapper.datum(datum);
            }
            return wrapper;
        }

        public void close() throws IOException {
            decoder.inputStream().close();
        }

    }

    /** Returns the specified output serializer. */
    public Serializer<AvroWrapper<T>> getSerializer(Class<AvroWrapper<T>> c) {
        Configuration conf = getConf();
        // Here we must rely on mapred.task.is.map to tell whether the map output
        // or final output is needed.
        boolean isMap = conf.getBoolean("mapred.task.is.map", false);
        Schema schema = !isMap ? AvroJob.getOutputSchema(conf) : Schema.parse(AvroKey.class.isAssignableFrom(c) ? conf
                .get(ColPhase.MAP_OUT_KEY_SCHEMA) : conf.get(ColPhase.MAP_OUT_VALUE_SCHEMA));
        return new AvroWrapperSerializer(new SpecificDatumWriter<T>(schema));
    }

    private class AvroWrapperSerializer implements Serializer<AvroWrapper<T>> {

        private DatumWriter<T> writer;
        private OutputStream out;
        private BinaryEncoder encoder;

        public AvroWrapperSerializer(DatumWriter<T> writer) {
            this.writer = writer;
        }

        public void open(OutputStream out) {
            this.out = out;
            this.encoder = new BinaryEncoder(out);
        }

        public void serialize(AvroWrapper<T> wrapper) throws IOException {
            writer.write(wrapper.datum(), encoder);
        }

        public void close() throws IOException {
            out.close();
        }

    }

}
