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

import org.apache.avro.mapred.AvroCollector;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.*;

public class ColContext<OUT> {

    private Reporter reporter;
    private AvroCollector<OUT> collector;
    
    public<IN, K, V, KO, VO> ColContext(AvroCollector<OUT> collector, Reporter reporter) {
        this.reporter = reporter;
        this.collector = collector;
    }
    
    public void write(OUT out) {
        try {
            collector.collect(out);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Counter getCounter(Enum<?> name) {
        return reporter.getCounter(name);
    }

    public Counter getCounter(String group, String name) {
        return reporter.getCounter(group, name);
    }

    public void incrCounter(Enum<?> key, long amount) {
        reporter.incrCounter(key, amount);
    }

    public void incrCounter(String group, String counter, long amount) {
        reporter.incrCounter(group, counter, amount);
    }

    public InputSplit getInputSplit() throws UnsupportedOperationException {
        return reporter.getInputSplit();
    }

    public void progress() {
        reporter.progress();
    }

    public void setStatus(String status) {
        reporter.setStatus(status);
    }

    public void incrCounter(Enum counter) {
        reporter.incrCounter(counter, 1L);
    }

    public void incrCounter(String group, String counter) {
        reporter.incrCounter(group, counter, 1L);        
    }

}
