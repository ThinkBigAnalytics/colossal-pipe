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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class ColPhase {

    private static final String SETTINGS = "col.phase.settings";
    public static final String MAPPER = "col.phase.mapper";
    public static final String REDUCER = "col.phase.reducer";
    public static final String MAP_OUT_CLASS = "col.phase.map.output.class";
    public static final String REDUCE_OUT_CLASS = "col.phase.reduce.output.class";
    public static final String GROUP_BY = "col.phase.groupby";
    public static final String SORT_BY = "col.phase.sortby";
    public static final String COMBINER = "col.phase.combiner";
    public static final String MAP_OUT_KEY_SCHEMA = "col.phase.map.out.key.schema";
    public static final String MAP_OUT_VALUE_SCHEMA = "col.phase.map.out.value.schema";
    public static final String MAP_IN_CLASS = "col.phase.map.input.class";

    /*
     * we *allow* for multiple reads, writes, maps, combines, and reduces this would support *manual* optimization of merging we
     * hope to never use them - instead we'll have simple mappings and rely on an optimizer that will let us do multi input-output
     */
    /** files read by main map/reduce pipeline */
    private List<ColFile> mainReads;
    /** any files read, including side writes */
    private List<ColFile> reads;
    /** files written by main map/reduce pipeline */
    private List<ColFile> mainWrites;
    /** any files written, including side writes */
    private List<ColFile> writes;
    private Class<? extends ColMapper>[] mappers;
    private Class<? extends ColReducer>[] combiners;
    private Class<? extends ColReducer>[] reducers;
    private String groupBy;
    private String sortBy;
    private Map<String, String> props = new LinkedHashMap<String, String>();
    private String name;
    private JobConf conf;
    private Integer deflateLevel;
    private Map<String, String> textMeta = new TreeMap<String,String>();

    public ColPhase() {
    }   

    public ColPhase(String name) {
        this.name = name;
    }

    public ColFile output() {
        return output(0);
    }

    public ColFile output(int n) {
        if (mainWrites == null) {
            // this *should* set up a promise to get the nth output ...
            throw new UnsupportedOperationException("please define outputs first, for now");
        }
        return mainWrites.get(n);
    }

    public ColPhase reads(ColFile... inputs) {
        return reads(Arrays.asList(inputs));
    }

    public ColPhase reads(Collection<ColFile> inputs) {
        if (mainReads == null) {
            mainReads = new ArrayList<ColFile>(inputs);
        }
        else {
            mainReads.addAll(inputs);
        }
        return readsSide(inputs);
    }

    /**
     * side writes are files that are written but not as the output of a map/reduce step, instead they are written by tasks or
     * processes through independent data path
     */
    public ColPhase readsSide(ColFile... sideFiles) {
        return readsSide(Arrays.asList(sideFiles));        
    }
    
    /**
     * side writes are files that are written but not as the output of a map/reduce step, instead they are written by tasks or
     * processes through independent data path
     */
    public ColPhase readsSide(Collection<ColFile> sideFiles) {
        if (this.reads == null) {
            this.reads = new ArrayList<ColFile>(sideFiles);
        } else {
            this.reads.addAll(sideFiles);
        }
        return this;
    }

    public ColPhase writes(ColFile... outputs) {
        return writes(Arrays.asList(outputs));
    }

    public ColPhase writes(Collection<ColFile> outputs) {
        writesSide(outputs);
        if (mainWrites == null) {
            mainWrites = new ArrayList<ColFile>(outputs);
        }
        else {
            mainWrites.addAll(outputs);
        }
        return this;
    }

    /**
     * side writes are files that are written but not as the output of a map/reduce step, instead they are written by tasks or
     * processes through independent data path
     */
    public ColPhase writesSide(ColFile... sideFiles) {
        return writesSide(Arrays.asList(sideFiles));        
    }
    
    /**
     * side writes are files that are written but not as the output of a map/reduce step, instead they are written by tasks or
     * processes through independent data path
     */
    public ColPhase writesSide(Collection<ColFile> sideFiles) {
        for (ColFile file : sideFiles) {
            ColPhase p = file.getProducer();
            if (p != null && p != this) {
                throw new IllegalStateException("File " + file + " has multiple producers " + this + ", " + p);
            }
            file.setProducer(this);
        }
        if (this.writes == null) {
            this.writes = new ArrayList<ColFile>(sideFiles);
        } else {
            this.writes.addAll(sideFiles);
        }
        return this;
    }

    public ColPhase map(Class<? extends ColMapper>... mappers) {
        this.mappers = mappers;
        return this;
    }

    public ColPhase combine(Class<? extends ColReducer>... combiners) {
        this.combiners = combiners;
        return this;
    }

    public ColPhase reduce(Class<? extends ColReducer>... reducers) {
        this.reducers = reducers;
        return this;
    }

    public ColPhase groupBy(String groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    public ColPhase sortBy(String sortBy) {
        this.sortBy = sortBy;
        return this;
    }

    public ColPhase set(String key, String value) {
        props.put(key, value);
        return this;
    }

    public ColPhase setSettings(Object settings) {
        return setJson(SETTINGS, settings);
    }

    public ColPhase setJson(String key, Object value) {
        return set(key, toJson(value));
    }

    public ColPhase addMeta(String prefix, String value) {
        textMeta.put(prefix, value);
        return this;
    }
    
    private String toJson(Object value) {
        //TODO
        throw new UnsupportedOperationException("toJson not yet working");
    }

    public List<PhaseError> plan(ColPipe distPipeline) {
        List<PhaseError> errors = new ArrayList<PhaseError>();
        conf = new JobConf(distPipeline.getConf());
        for (Map.Entry<String, String> entry : props.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        Schema mapin = null;
        Class<?> mapOutClass = null;
        Class<?> mapInClass = null;

        Class<? extends ColMapper> mapperClass = null;
        if (mappers != null && mappers.length > 0) {
            if (mappers.length > 1) {
                errors.add(new PhaseError("Colossal phase/avro currently only supports one mapper per process: " + name));
            }
            else {
                mapperClass = mappers[0];
                conf.set(MAPPER, mapperClass.getName());
                Class<?> foundIn = null;
                for (Method m : mapperClass.getMethods()) {
                    if ("map".equals(m.getName())) {
                        Class<?>[] paramTypes = m.getParameterTypes();
                        if (paramTypes.length >= 3) {
                            try {
                                // prefer subclass methods to superclass methods
                                if (foundIn == null || foundIn.isAssignableFrom(m.getDeclaringClass())) {
                                    if (paramTypes[0] == Object.class) {
                                        if (foundIn == m.getDeclaringClass()) {
                                            // skip the generated "override" of the generic method
                                            continue;
                                        }
                                    } else {
                                        //TODO: handle cases beyond Object where output isn't defined    
                                        mapInClass = paramTypes[0];
                                        mapin = getSchema(paramTypes[0].newInstance());
                                    }
                                    mapOutClass = paramTypes[1];
                                    foundIn = m.getDeclaringClass();
                                }
                            }
                            catch (Exception e) {
                                errors.add(new PhaseError(e, "Can't create mapper: " + mapperClass));
                            }
                        }
                    }
                }
            }
        }

        if (combiners != null && combiners.length > 0) {
            if (combiners.length > 1) {
                errors.add(new PhaseError("Colossal phase/avro currently only supports one combiner per process: " + name));
            }
            else {
                conf.set(COMBINER, combiners[0].getName());
                conf.setCombinerClass(ColHadoopCombiner.class);
            }
        }
        Schema reduceout = null;
        Class<?> reduceOutClass = null;
        Class<? extends ColReducer> reducerClass = null;
        if (reducers != null && reducers.length > 0) {
            if (reducers.length != 1) {
                errors.add(new PhaseError("Colossal phase/avro currently only supports one reducer per process: " + name));
            }
            else {
                reducerClass = reducers[0];
                conf.set(REDUCER, reducers[0].getName());
                Class<?> foundIn = null;
                for (Method m : reducerClass.getMethods()) {
                    if ("reduce".equals(m.getName())) {
                        Class<?>[] paramTypes = m.getParameterTypes();
                        if (paramTypes.length >= 3) {
                            if (foundIn == null || foundIn.isAssignableFrom(m.getDeclaringClass())) {
                                if (foundIn == m.getDeclaringClass() && paramTypes[1] == Object.class) {
                                    // skip the generated "override" of the generic method
                                    continue;
                                }
                                // prefer subclass methods to superclass methods
                                reduceOutClass = paramTypes[1];
                                foundIn = m.getDeclaringClass();                                
                            }
                        }
                    }
                }
                // XXX validation!
            }
        }
        Object reduceOutProto = null;
        //TODO: handle cases beyond Object where output isn't defined
        if ((reduceOutClass == null || reduceOutClass == Object.class) && mainWrites != null && mainWrites.size() > 0) {
            reduceOutProto = mainWrites.get(0).getPrototype();
            reduceOutClass = reduceOutProto.getClass();
        } else {
            try {
                reduceOutProto = reduceOutClass.newInstance();
            }
            catch (Exception e) {
                errors.add(new PhaseError(e, "Can't create reducer output class: " + reduceOutClass));
            }
        }
        if (reduceOutProto != null)
            reduceout = getSchema(reduceOutProto);
        
        conf.set(REDUCE_OUT_CLASS, reduceOutClass.getName());

        Schema valueSchema = null;
        if (mainWrites.size() != 1) {
            errors.add(new PhaseError("Colossal phase/avro currently only supports one output per process: " + name));
        }
        else {
            ColFile output = mainWrites.get(0);
            AvroOutputFormat.setOutputPath(conf, new Path(output.getPath()));

            if (output.getPrototype() != null) {
                valueSchema = getSchema(output.getPrototype());
                if (reduceout != null) {
                    assert reduceout.equals(valueSchema); // should make an error not assert this!
                }
            }
            else {
                if (reduceout == null) {
                    errors.add(new PhaseError("No output format defined"));
                }
                valueSchema = reduceout;
            }
            output.setupOutput(conf);
        }
        conf.set(AvroJob.OUTPUT_SCHEMA, valueSchema.toString());

        if (deflateLevel != null)
            AvroOutputFormat.setDeflateLevel(conf, deflateLevel);

        Object proto = null;
        if (mainReads != null && mainReads.size() > 0) {
            Path[] inPaths = new Path[mainReads.size()];
            int i = 0;
            for (ColFile file : mainReads) {
                inPaths[i++] = new Path(file.getPath());
                Object myProto = file.getPrototype();
                if (myProto == null) {
                    errors.add(new PhaseError("Files need non-null prototypes " + file));
                }
                else if (proto != null) {
                    if (myProto.getClass() != proto.getClass()) {
                        errors.add(new PhaseError("Inconsistent prototype classes for inputs: " + myProto.getClass() + " vs "
                                + proto.getClass() + " for " + file));
                    }
                }
                else {
                    proto = myProto;
                }
            }
            AvroInputFormat.setInputPaths(conf, inPaths);

            if (mapin == null) {
                if (proto == null){
                    errors.add(new PhaseError("Undefined input format"));
                } else {
                    mapin = getSchema(proto);
                    mapInClass = proto.getClass();
                }
            }
            mainReads.get(0).setupInput(conf);
            if (conf.get("mapred.input.format.class") == null)
                conf.setInputFormat(AvroInputFormat.class);        
        }
        
        Schema mapValueSchema = null;
        try {
            //TODO: handle cases beyond Object where input isn't defined
            if (mapOutClass == null || mapOutClass == Object.class) {
                assert mapperClass == null;
                if (proto != null) {
                    mapOutClass = proto.getClass();
                    mapValueSchema = getSchema(proto);
                } else {
                    // not available - try to get it from the reducer
                    if (reducerClass == null) {
                        mapOutClass = reduceOutClass; 
                        mapValueSchema = getSchema(reduceOutClass.newInstance());
                    } else {
                        // can't get it from reducer input - that's just Iterable
                        String fname = "no input file specified";
                        if (mainReads != null && mainReads.size()>0) fname=mainReads.get(0).getPath();
                        errors.add(new PhaseError("No input format specified for identity mapper - specify it on input file "+fname));
                    }                    
                }
            } else {
                mapValueSchema = getSchema(mapOutClass.newInstance());
            }
            if (mapValueSchema != null)
                conf.set(MAP_OUT_VALUE_SCHEMA, mapValueSchema.toString());
        } catch (Exception e) {
            errors.add(new PhaseError(e, "Can't create instance of map output class: " + mapOutClass));
        }

        conf.set(MAP_OUT_CLASS, mapOutClass.getName());
        conf.set(MAP_IN_CLASS, mapInClass.getName());
        // XXX validation!
        if (proto != null) {
            conf.set(AvroJob.INPUT_SCHEMA, getSchema(proto).toString());
        }
        else if (mapin != null) {
            conf.set(AvroJob.INPUT_SCHEMA, mapin.toString());
        }
        else {
            errors.add(new PhaseError("No map input defined"));
        }

        if (groupBy != null || sortBy != null) {
            conf.set(MAP_OUT_KEY_SCHEMA, group(mapValueSchema, groupBy, sortBy).toString());
        }
        if (groupBy != null) {
            conf.set(GROUP_BY, groupBy);
            AvroJob.setOutputMeta(conf, GROUP_BY, groupBy);
        }
        if (sortBy != null) {
            conf.setPartitionerClass(AvroGroupPartitioner.class);
            conf.set(SORT_BY, sortBy);
            AvroJob.setOutputMeta(conf, SORT_BY, sortBy);
        }

        conf.setMapOutputKeyClass(AvroKey.class);
        conf.setMapOutputValueClass(AvroValue.class);
        conf.setOutputKeyComparatorClass(ColKeyComparator.class);

        conf.setMapperClass(ColHadoopMapper.class);
        conf.setReducerClass(ColHadoopReducer.class);

        for (Map.Entry<String,String> entry : textMeta.entrySet())
            AvroJob.setOutputMeta(conf, entry.getKey(), entry.getValue());        
        
        // add ColAvroSerialization to io.serializations
        Collection<String> serializations = conf.getStringCollection("io.serializations");
        if (!serializations.contains(ColAvroSerialization.class.getName())) {
            serializations.add(ColAvroSerialization.class.getName());
            conf.setStrings("io.serializations", serializations.toArray(new String[0]));
        }
        return errors;
    }

    public static Schema getSchema(Object proto) {
        try {
            Field schemaField = proto.getClass().getField("SCHEMA$");
            return (Schema) schemaField.get(null);
        }
        catch (NoSuchFieldException e) {
            // use reflection
            return ReflectData.get().getSchema(proto.getClass());
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public PhaseError submit() {
        try {
            System.out.println("Submitting job:");
            System.out.println(getDetail());
            // probably should just make a conf right here?
            for (ColFile file : writes) {
                file.clearAndPrepareOutput(conf);
            }

            if (reads != null) {
                int i = 0;
                for (ColFile file : reads) {
                    // record inputs, to allow determination of obsolescence
                    // really we should be recording the transitive closure of dependencies here
                    // to allow determining files that are out of date with respect to their original source inputs
                    // although that could get costly for large numbers of log files...
                    AvroJob.setOutputMeta(conf, "input.file.name."+i, file.getPath());
                    AvroJob.setOutputMeta(conf, "input.file.mtime."+i, file.getTimestamp(conf));
                    i++;
                }
            }
            JobClient.runJob(conf);
            return null;
        }
        catch (Throwable t) {
            System.err.println("Job failure");
            t.printStackTrace();
            // clean up failed job output
            for (ColFile file : getOutputs()) {
                file.delete(conf);
            }
            return new PhaseError(t, name);
        }
    }

    public List<ColFile> getInputs() {
        return Collections.unmodifiableList(reads);
    }

    public List<ColFile> getOutputs() {
        return Collections.unmodifiableList(writes);
    }

    /**
     * create a schema containing just the listed comma-separated fields
     */
    public static Schema group(Schema schema, String... fields) {
        List<String> fieldList = new ArrayList<String>(fields.length);
        for (String list : fields) {
            if (list == null) continue;
            for (String field : list.split(",")) {
                field = field.trim();
                String[] parts = field.split("\\s");
                if (parts.length>0) {
                    fieldList.add(parts[0]);
                }
            }
        }


        return group(schema, fieldList);
    }

    public static Schema group(Schema schema, List<String> fields) {
        ArrayList<Schema.Field> fieldList = new ArrayList<Schema.Field>(fields.size());
        StringBuilder builder = new StringBuilder();
        String missing = null;
        for (String fieldname : fields) {
            Schema.Field field = schema.getField(fieldname.trim());
            if (field == null) {
                if (missing == null) { 
                    missing = "Invalid group by/sort by - fields not in map output record are: ";
                } else {
                    missing += ", ";
                }
                missing += fieldname.trim();
                continue;
            }
            Schema.Field copy = new Schema.Field(fieldname, field.schema(), field.doc(), field.defaultValue(), field.order());
            fieldList.add(copy);
            builder.append('_');
            builder.append(fieldname);
        }
        if (missing != null) {
            throw new IllegalArgumentException(missing);
        }
        schema = Schema.createRecord(schema.getName() + "_proj" + builder.toString(), "generated", schema.getNamespace(), false);
        schema.setFields(fieldList);
        return schema;
    }

    public String getSummary() {
        return "mapper " + getMapName() + " reading " + mainReads.get(0).getPath() + " reducer " + getReduceName();
    }

    private String getReduceName() {
        return reducers == null || reducers[0] == null ? "identity" : reducers[0].getName();
    }

    private String getMapName() {
        return mappers == null || mappers[0] == null ? "identity" : mappers[0].getName();
    }

    private String getDetail() {
        return String.format("map: %s\nreduce: %s\nreading: %s\nwriting: %s\ngroup by:%s%s", getMapName(), getReduceName(), mainReads
                .get(0).getPath(), mainWrites.get(0).getPath(), groupBy, (sortBy == null ? "" : "\nsort by:"+sortBy));
    }


}
