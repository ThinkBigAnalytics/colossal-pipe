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

import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.hadoop.mapred.Reporter;

public class AvroKeyVoidValueMapper extends AvroMapper<Object, Pair<Object, Void>> {
    private Pair<Object, Void> pair;

    @Override
    public void setConf(org.apache.hadoop.conf.Configuration conf) {
        pair = new Pair<Object, Void>(Schema.parse(conf.get(AvroJob.INPUT_SCHEMA)));
    }

    public void map(Object in, AvroCollector<Pair<Object, Void>> collector, Reporter reporter) throws IOException {
        pair.set(in, null);
        collector.collect(pair);
    }
}