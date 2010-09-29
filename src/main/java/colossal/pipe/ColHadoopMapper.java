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
import java.lang.reflect.Method;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;


@SuppressWarnings("deprecation")
public class ColHadoopMapper<IN, OUT, KO, VO> extends MapReduceBase implements
        Mapper<AvroWrapper<IN>, NullWritable, KO, VO> {

    private ColMapper<IN, OUT> mapper;
    private boolean isMapOnly;
    private OUT out;
    private ColContext<OUT> context;
    private Schema schema;
    private String groupBy;
    private String sortBy;

    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {
        this.mapper = ReflectionUtils.newInstance(conf.getClass(ColPhase.MAPPER, BaseMapper.class, ColMapper.class), conf);
        this.isMapOnly = conf.getNumReduceTasks() == 0;
        try {
            this.out = (OUT) ReflectionUtils.newInstance(conf.getClass(ColPhase.MAP_OUT_CLASS, Object.class, Object.class), conf);               
        }
        catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException)e;
            throw new RuntimeException(e);
        }
        this.schema = ColPhase.getSchema(this.out);
        this.groupBy = conf.get(ColPhase.GROUP_BY);
        this.sortBy = conf.get(ColPhase.SORT_BY);        

        mapper.setConf(conf);
    }

    @SuppressWarnings("unchecked")
    private class Collector<K> extends AvroCollector<OUT> {
        private final AvroWrapper<OUT> wrapper = new AvroWrapper<OUT>(null);
        private final AvroKey<K> keyWrapper = new AvroKey<K>(null);
        private final AvroValue<OUT> valueWrapper = new AvroValue<OUT>(null);
        private final KeyExtractor<K,OUT> extractor;
        private final K key;
        private OutputCollector<KO, VO> collector;

        public Collector(OutputCollector<KO, VO> collector, KeyExtractor<K,OUT> extractor) {
            this.collector = collector;
            this.extractor = extractor;
            key = extractor.getProtypeKey();
            keyWrapper.datum(key);
        }

        public void collect(OUT datum) throws IOException {
            if (isMapOnly) {
                wrapper.datum(datum);
                collector.collect((KO) wrapper, (VO) NullWritable.get());
            }
            else {
                extractor.setKey(datum, key);
                valueWrapper.datum(datum);
                collector.collect((KO) keyWrapper, (VO) valueWrapper);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void map(AvroWrapper<IN> wrapper, NullWritable value, OutputCollector<KO, VO> collector, Reporter reporter)
            throws IOException {
        if (this.context == null) {
            KeyExtractor<GenericData.Record, OUT> extractor = new ReflectionKeyExtractor<OUT>(schema, groupBy, sortBy);
            this.context = new ColContext<OUT>(new Collector(collector, extractor), reporter);
        }
        mapper.map(wrapper.datum(), out, context);
    }
}
