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

import java.util.*;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class Projector {
    final private GenericRecord key;
    final Map<String,java.lang.reflect.Field> inFields = new HashMap<String,java.lang.reflect.Field>();
    final List<String> fieldNames;

    public Projector(GenericRecord key, List<Field> fields) {
        try {
            this.key = key;
            this.fieldNames = new ArrayList<String>(fields.size());
            for (Field schemaField : fields) {
                fieldNames.add(schemaField.name());
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // can't use GenericRecord to access fields in generated records - it'd help to have them implement that interface...
    public Object project(Object in) {
        if (inFields.isEmpty()) {
            Class<?> inClass = in.getClass();

            try {
                for (String fieldName : fieldNames) {
                    java.lang.reflect.Field field = inClass.getField(fieldName);
                    field.setAccessible(true);
                    inFields.put(fieldName, field);                
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
            for (Map.Entry<String, java.lang.reflect.Field> entry : inFields.entrySet()) {
                String fieldName = entry.getKey();
                key.put(fieldName, entry.getValue().get(in));
            }
            return key;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}