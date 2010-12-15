package colossal.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

public class RecordUtils {

    public static <T> void copy(T from, T to) {
        Class<?> toClass = to.getClass();
        Class<?> fromClass = from.getClass();
        Class<?> level;
        if (toClass.isAssignableFrom(fromClass)) {
            level = toClass;
        } else if (fromClass.isAssignableFrom(fromClass)) {
            level = fromClass;
        } else {
            throw new IllegalArgumentException("Not yet supported: copying fields from classes without a common superclass");
        }
        try {
            while (level != null && level != Object.class){
                // replace this with cached codegen        
                for (Field f : level.getDeclaredFields()) {
                    f.setAccessible(true);
                    if (!Modifier.isStatic(f.getModifiers())) {
                    	Object o = f.get(from);
                    	if(o instanceof Utf8){
                    		Utf8 old = (Utf8)o;
                        	int len = old.getByteLength();
                        	 byte[] copy = new byte[len];
                        	 System.arraycopy(old.getBytes(), 0, copy, 0, len);
                        	 Utf8 deepCopy = new Utf8(copy);
                        	 f.set(to, deepCopy);
                    	}else{
                    		 f.set(to, o);	
                    	}
                       
                    }
                }
                level = level.getSuperclass();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static<T> T jsonToAvro(String json, Schema inSchema) {
        return null;
    }

    public static <T> String toJson(T value) {
        // TODO Auto-generated method stub
        return null;
    }
}
