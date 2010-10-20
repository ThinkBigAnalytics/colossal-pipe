package colossal.pipe;


import static org.junit.Assert.*;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

public class AvroGroupPartitionerTests {

    private AvroGroupPartitioner<Record, Record> partitioner;
    private Record keyRec;
    private Record valRec;
    private AvroKey<Record> key;
    private AvroValue<Record> value;
    @SuppressWarnings("deprecation")
    private JobConf conf;

    @SuppressWarnings("deprecation")
    @Before
    public void setup() {
        partitioner = new AvroGroupPartitioner<GenericData.Record, GenericData.Record>();
        keyRec = new Record(
                Schema.parse(("{'type':'record', 'name':'key', 'fields': [ {'name': 'group', 'type': 'string'}," +
                        " {'name' : 'extra', 'type' : 'string'}," +
                        " {'name' : 'subsort', 'type' : 'string'}]}")
                        .replaceAll("\\'", "\"")));
        valRec = new Record(
                Schema.parse(("{'type':'record', 'name':'val', 'fields': [ {'name': 'group', 'type': 'string'}," +
                        " {'name' : 'extra', 'type' : 'string'}," +
                        " {'name' : 'extra2', 'type' : 'string'}," +
                        " {'name' : 'subsort', 'type' : 'string'}]}")
                        .replaceAll("\\'", "\"")));
        key = new AvroKey<Record>(keyRec);
        value = new AvroValue<Record>(valRec);
        keyRec.put("group", "A");
        keyRec.put("subsort", "one");
        valRec.put("extra", "one");
        valRec.put("group", "A");
        valRec.put("subsort", "one");
        valRec.put("extra", "one");
        valRec.put("extra2", "one");      
        conf = new JobConf();
    }
    
    @Test
    public void partitionRecord() {        
        conf.set(ColPhase.GROUP_BY, "group");
        partitioner.configure(conf);
        assertEquals(0, partitioner.getPartition(key, value, 1));        
        int p1 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        valRec.put("extra2", "two");
        int p2 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        keyRec.put("subsort", "two");
        valRec.put("subsort", "two");
        int p3 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        keyRec.put("group", "B");
        int p4 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        assertEquals(p1, p2);
        assertEquals(p1, p3);
        assertTrue(p1!=p4);
    }

    @Test
    public void partitionNullGroup() {
        partitioner.configure(conf);
        int p1 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        keyRec.put("subsort", "two");
        valRec.put("subsort", "two");
        keyRec.put("group", "B");
        keyRec.put("extra", "B");
        int p2 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        assertEquals(p1, p2);
    }
    
    @Test
    public void partitionCompoundGroup() {
        conf.set(ColPhase.GROUP_BY, " group,    extra ");
        partitioner.configure(conf);
        assertEquals(0, partitioner.getPartition(key, value, 1));        
        int p1 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        valRec.put("extra2", "two");
        int p2 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        assertEquals(p1, p2);
        keyRec.put("subsort", "two");
        valRec.put("subsort", "two");        
        int p3 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        assertEquals(p1, p3);

        keyRec.put("extra", "two");
        int p4 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        
        keyRec.put("extra", "one");
        keyRec.put("group", "B");
        int p5 = partitioner.getPartition(key, value, Integer.MAX_VALUE);
        
        assertTrue(p1!=p4);
        assertTrue(p1!=p5);
        assertTrue(p4!=p5);
    }
}
