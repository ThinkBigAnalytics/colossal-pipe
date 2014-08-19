/**
 * Copyright (C) 2010-2014 Think Big Analytics, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package colossal.pipe.formats;

import java.io.IOException;
import java.util.*;

import org.apache.avro.*;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonToClass {
    private static ObjectMapper m = new ObjectMapper();

    public static void jsonToObject(String line, Object receiver, Schema schema, Map<String,Class<?>> hints) throws IOException {
        JsonNode rootNode = m.readTree(line);
        
        if (!rootNode.isObject()) {
            throw new IllegalArgumentException("only accepts a single JSON object as the root");
        }
        clear(receiver);
        setFromNode(rootNode, receiver, schema, line, hints);
    }

    private static void clear(Object receiver) {
        // TODO Auto-generated method stub
        
    }

    private static void setFromNode(JsonNode node, Object r, Schema schema, String container, Map<String,Class<?>> hints) {
        schema = getObject(schema);
        Iterator<JsonNode> it = node.getElements();
        Iterator<String> itn = node.getFieldNames();
        while (it.hasNext()) {
            JsonNode child = it.next();
            String name = replaceInvalidNameChars(itn.next());
            Field field = schema.getField(name);
            if (field == null) {
                System.err.println("Warning: skipping unmapped field "+name+" contained in "+container);
                continue;
            }
            Schema childSchema = field.schema();
            //ReflectData.get().
            java.lang.reflect.Field f = getField(r, name);
            Object childObj;
            try {
                childObj = f.get(r);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            
            if (childObj==null) {
                Class<?> fieldType = f.getType();
                if (fieldType.isArray()) {
                    childObj = new Object[0];
                } else if (Collection.class.isAssignableFrom(fieldType)) {                    
                    childObj = new ArrayList();
                    //fieldType = hints.get(path+"."+name);
                } else {
                    try {
                        childObj = fieldType.newInstance();
                    }
                    catch (InstantiationException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    catch (IllegalAccessException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
//            parseField(node, child)
//            setField
//            
//            if (child.isArray()) {
//                setFromArrayNode(child, f.get(r), childSchema, name);
//            }
//            else if (child.isObject()) {
//                setFromNode(child, f.get(r), childSchema, name);
//            }
//            else if (child.isInt()) {
//                int intVal = child.getIntValue();
//                if (f.getType()==Integer.class || f.getType()==Integer.TYPE) {
//                    f.set(r, intVal);
//                } else if (f.getType()==Long.class || f.getType()==Long.TYPE) {
//                    f.set(r, (long)intVal);
//                } else if (f.getType()==Float.class || f.getType()==Float.TYPE) {
//                    f.set(r, (float)intVal);
//                } else if (f.getType()==Double.class || f.getType()==Double.TYPE) {
//                    f.set(name, (double)intVal);
//                } else {
//                    throw new IllegalArgumentException("Can't store an int in field "+name);                    
//                }
//            }
//            else if (child.isLong()) {
//                long longVal = child.getLongValue();
//                if (f.getType()==Integer.class || f.getType()==Integer.TYPE) {
//                    f.set(r, (int)longVal);
//                } else if (f.getType()==Long.class || f.getType()==Long.TYPE) {
//                    f.set(r, longVal);
//                } else if (f.getType()==Float.class || f.getType()==Float.TYPE) {
//                    f.set(r, (float)longVal);
//                } else if (f.getType()==Double.class || f.getType()==Double.TYPE) {
//                    f.set(name, (double)longVal);
//                } else {
//                    throw new IllegalArgumentException("Can't store a long in field "+name);                    
//                }
//            }
//            else if (child.isBinary()) {
//                try {
//                    f.set(r, child.getBinaryValue());
//                }
//                catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//            else if (child.isBoolean()) {
//                f.set(r, child.getBooleanValue());
//            }
//            else if (child.isDouble()) {
//                double val = child.getDoubleValue();
//                if (f.getType()==Float.class || f.getType()==Float.TYPE) {
//                    f.set(r, (float)val);
//                } else if (f.getType()==Double.class || f.getType()==Double.TYPE) {
//                    f.set(name, (double)val);
//                } else {
//                    throw new IllegalArgumentException("Can't store a double in field "+name);                    
//                }                
//            }
//            else if (child.isTextual()) {
//                f.set(name, child.getTextValue());
//            }
        }
    }
    
    private static java.lang.reflect.Field getField(Object r, String name) {
        // TODO Auto-generated method stub
        return null;
    }

//    private static GenericArray<Object> setFromArrayNode(JsonNode node, Object r, Schema schema, String container) {
//        schema = getArray(schema);
//        if (schema == null) {
//            throw new IllegalArgumentException("can't parse array for schema "+schema);
//        }
//        Schema childSchema = schema.getElementType();
//        for (int i=0; i<node.size(); i++) {
//            JsonNode child = node.get(i);
//            childSchema.
//            if (child.isArray()) {
//                Schema elType = schema.getElementType();
//                //elType.
//                GenericArray<Object> o = setFromArrayNode(child, childSchema, container);
//                r.add(o);
//            }
//            else if (child.isObject()) {
//                GenericRecord o = recordFromNode(child, childSchema, container);
//                r.add(o);
//            }
//            else if (child.isInt()) {
//                if (acceptsInt(childSchema)) {
//                    r.add(child.getIntValue());
//                } else if (acceptsLong(childSchema)) {
//                    r.add((long)child.getIntValue());                    
//                } else if (acceptsFloat(childSchema)) {
//                    r.add((float)child.getIntValue());
//                } else if (acceptsDouble(childSchema)) {
//                    r.add((double)child.getIntValue());
//                } else {
//                    System.err.println("Can't store an int in field "+childSchema);
//                }
//            }
//            else if (child.isLong()) {
//                if (acceptsLong(childSchema)) {
//                    r.add((long)child.getLongValue());                    
//                } else if (acceptsFloat(childSchema)) {
//                    r.add((float)child.getLongValue());
//                } else if (acceptsDouble(childSchema)) {
//                    r.add((double)child.getLongValue());
//                } else {
//                    System.err.println("Can't store a long in field "+childSchema);
//                }
//            }
//            else if (child.isBinary()) {
//                try {
//                    r.add(child.getBinaryValue());
//                }
//                catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//            else if (child.isBoolean()) {
//                r.add(child.getBooleanValue());
//            }
//            else if (child.isDouble()) {
//                if (acceptsDouble(childSchema)) {
//                    r.add(child.getDoubleValue());
//                } else if (acceptsFloat(childSchema)) {
//                    r.add((float)child.getDoubleValue());
//                } else {
//                    System.err.println("Can't store a double in field "+childSchema);
//                }
//            }
//            else if (child.isTextual()) {
//                r.add(new org.apache.avro.util.Utf8(child.getTextValue()));
//            }
//        }
//        return r;
//    }

    private static Schema getObject(Schema schema) {
        if (schema.getType() == Schema.Type.RECORD) return schema;
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() == Schema.Type.RECORD) {
                    return s;
                }
            }
        }
        return null;
    }

    private static Schema getArray(Schema schema) {
        if (schema.getType() == Schema.Type.ARRAY) return schema;
        while (schema.getType() == Type.UNION) {
            List<Schema> schemas = schema.getTypes();
            Schema found = null;
            // dumb parser that won't accept complex union abiguities
            for (Schema inner : schemas) {
                if (inner.getType() == Type.ARRAY) {
                    return inner;
                } else if (inner.getType() == Type.UNION) {
                    found = inner;                            
                }
            }
            if (found == null) {
                return null;
            }
            schema = found;
        }
        return null;
    }
    
    private static boolean accepts(Schema childSchema, Schema.Type type) {
        if (childSchema.getType() == type) return true;
        if (childSchema.getType() == Schema.Type.UNION) {
            for (Schema s : childSchema.getTypes()) {
                if (s.getType() == type) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private static boolean acceptsFloat(Schema childSchema) {
        return accepts(childSchema, Schema.Type.FLOAT);
    }
    
    private static boolean acceptsDouble(Schema childSchema) {
        return accepts(childSchema, Schema.Type.DOUBLE);
    }
        
    private static boolean acceptsLong(Schema childSchema) {
        return accepts(childSchema, Schema.Type.LONG);
    }

    private static boolean acceptsInt(Schema childSchema) {
        return accepts(childSchema, Schema.Type.INT);
    }   
    
    // hand-written for speed - regexps are 100x slower
    static String replaceInvalidNameChars(String name) {
      int len = name.length();
      // scan for first invalid char
      int i=0;      
      for (; i<len; i++) {
        char ch = name.charAt(i);
        if (!(ch>='a' && ch<='z' || ch>='A' && ch<='Z' || ch>='0' && ch <= '9' || ch == '_'))
          break;
      }
      if (i == len) return name;
      StringBuilder copy = new StringBuilder(len);
      copy.append(name, 0, i);
      copy.append('_');
      i++;
      for (; i<len; i++) {
        char ch = name.charAt(i);
        if (ch>='a' && ch<='z' || ch>='A' && ch<='Z' || ch>='0' && ch <= '9' || ch == '_') {
          copy.append(ch);
        } else {
          copy.append('_');
        }
      }

      return copy.toString();
    }    
}
