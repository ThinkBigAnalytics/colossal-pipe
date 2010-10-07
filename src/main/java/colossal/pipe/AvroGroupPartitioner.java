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

import java.util.ArrayList;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class AvroGroupPartitioner<K,V> implements Partitioner<AvroKey<K>,AvroValue<V>> {

    private ArrayList<String> groupNames;

    @Override
    public void configure(JobConf conf) {
        //Schema schema = Schema.parse(conf.get(ColPhase.MAP_OUT_VALUE_SCHEMA));
        String groupBy = conf.get(ColPhase.GROUP_BY);
        String[] groupFields = groupBy==null ? new String[0] : groupBy.split(",");
        groupNames = new ArrayList<String>(groupFields.length);
        
        ReflectionKeyExtractor.addFieldnames(groupNames, groupFields);
    }

    @Override
    public int getPartition(AvroKey<K> key, AvroValue<V> value, int numPartitions) {
        Record rec = (Record)key.datum();
        
        // use an FNV-style hash to combine the field hashes
        int hc = 0x72cd6842;
        for (String field : groupNames) {
            Object val = rec.get(field);
            int hash = (val==null ? 0xf27335a8 : val.hashCode());
            for (int i=0; i<4; i++) {
                hc ^= (hash & 0xff);
                hc *= 16777619;
                hash >>= 8;
            }
        }
        hc = (hc % numPartitions);
        if (hc < 0) hc=-hc;
        return hc;
    }


}
