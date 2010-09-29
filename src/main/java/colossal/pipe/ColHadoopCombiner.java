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
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Bridge between a {@link org.apache.hadoop.mapred.Reducer} and an {@link AvroReducer} used when combining. When combining, map
 * output pairs must be split before they're collected.
 */
class ColHadoopCombiner<K, V> extends ColHadoopReducerBase<K, V, V, AvroKey<K>, AvroValue<V>> {

    private Schema schema;
    private String groupBy;
    private String sortBy;

    @Override
    @SuppressWarnings("unchecked")
    protected ColReducer<V, V> getReducer(JobConf conf) {
        return ReflectionUtils.newInstance(conf.getClass(ColPhase.COMBINER, BaseReducer.class, ColReducer.class), conf);
    }

    @Override
    public void configure(JobConf conf) {
        super.configure(conf);
        this.schema = ColPhase.getSchema(this.out);
        this.groupBy = conf.get(ColPhase.GROUP_BY);
        this.sortBy = conf.get(ColPhase.SORT_BY);
    }
    
    @SuppressWarnings("unchecked")
    private class Collector<V> extends AvroCollector<V> {
        //private final AvroWrapper<V> wrapper = new AvroWrapper<V>(null);
        private final AvroKey<K> keyWrapper = new AvroKey<K>(null);
        private final AvroValue<V> valueWrapper = new AvroValue<V>(null);
        private final KeyExtractor<K,V> extractor;
        private final K key;
        private OutputCollector<AvroKey<K>, AvroValue<V>> collector;

        public Collector(OutputCollector<AvroKey<K>, AvroValue<V>> collector, KeyExtractor<K,V> extractor) {
            this.collector = collector;
            this.extractor = extractor;
            key = extractor.getProtypeKey();
            keyWrapper.datum(key);
        }

        public void collect(V datum) throws IOException {
            extractor.setKey(datum, key);
            valueWrapper.datum(datum);
            collector.collect(keyWrapper, valueWrapper);
        }
    }

    @Override
    protected AvroCollector<V> getCollector(OutputCollector<AvroKey<K>, AvroValue<V>> collector) {
        KeyExtractor<GenericData.Record, V> extractor = new ReflectionKeyExtractor<V>(schema, groupBy, sortBy);
        return new Collector(collector, extractor);
    }

}
