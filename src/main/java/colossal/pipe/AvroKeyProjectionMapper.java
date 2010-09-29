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

import java.io.IOException;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.*;
import org.apache.avro.mapred.*;
import org.apache.hadoop.mapred.Reporter;

public class AvroKeyProjectionMapper extends AvroMapper<Object, Pair<Object, Object>> {
    private Pair<Object, Object> pair;
    private Projector projector;

    @Override
    public void setConf(org.apache.hadoop.conf.Configuration conf) {
        if (conf == null) return; // you first get a null configuration - ignore that
        String mos = conf.get(AvroJob.MAP_OUTPUT_SCHEMA);   
        Schema schema = Schema.parse(mos);
        pair = new Pair<Object, Object>(schema);
        Schema keySchema = Pair.getKeySchema(schema);
        final List<Field> fields = keySchema.getFields();
        final GenericRecord key = new GenericData.Record(keySchema);
        projector = new Projector(key, fields);
    }

    public void map(Object in, AvroCollector<Pair<Object, Object>> collector, Reporter reporter)
            throws IOException {
        Object key = projector.project(in);
        pair.set(key, in);
        collector.collect(pair);
    }
}