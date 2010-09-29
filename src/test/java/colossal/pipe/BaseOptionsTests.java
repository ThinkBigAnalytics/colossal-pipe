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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.joda.time.*;
import org.junit.Before;
import org.junit.Test;


public class BaseOptionsTests {

    private ColPipe pipe;
    private BaseOptions options;

    @Before
    public void setup() {
        options = new BaseOptions();
        pipe = new ColPipe();               
    }
    
    @Test
    public void invalid() {
        PrintStream oldErr = System.err;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bos);
        System.setErr(ps);
        try {
            int rc = options.parse(pipe, "-t=2010-09-07T13:00:00Z/2010-09-08T13:30:00Z");
            assertEquals(1, rc);
            ps.flush();
            String errText = new String(bos.toByteArray());
            assertTrue(errText.contains("\"-t=2010-09-07T13:00:00Z/2010-09-08T13:30:00Z\" is not a valid option"));
            assertTrue(errText.contains("Usage: hadoop jar"));
            assertTrue(errText.contains("-time"));
            assertTrue(errText.contains("Example: hadoop jar"));
        } finally {
            System.setErr(oldErr);
        }
    }
    
    @Test
    public void interval1() {
/*
 * this test fails because args4j has a bug - change opt to h in the below method for CmdLine:
   private Map<String,OptionHandler> filter(List<OptionHandler> opt, String keyFilter) {
        Map<String,OptionHandler> rv = new TreeMap<String,OptionHandler>();
        for (OptionHandler h : opt) {
            if (opt.toString().startsWith(keyFilter)) rv.put(opt.toString(), h);
        }
        return rv;
    }
        
 */
        PrintStream oldErr = System.err;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bos);
        System.setErr(ps);
        try {
            int rc = options.parse(pipe, "-time=2010-09-07T13:00:00Z/2010-09-08T13:30:00Z");
            assertEquals(1, rc);
        } finally {
            System.setErr(oldErr);
        }
    }
    
    @Test
    public void interval2() {
        int rc = options.parse(pipe, "-time", "2010-09-07T13:00:00Z/2010-09-08T13:30:00Z");
        assertEquals(0, rc);
        
        Interval interval = options.getInterval();
        DateTime expectedStart = new DateTime(2010, 9, 7, 13, 0, 0, 0).withZoneRetainFields(DateTimeZone.UTC); 
        DateTime expectedEnd = new DateTime(2010, 9, 8, 13, 30, 0, 0).withZoneRetainFields(DateTimeZone.UTC); 
        assertEquals(expectedStart, interval.getStart());
        assertEquals(expectedEnd, interval.getEnd());
    }
    
    @Test
    public void interval3() {
        int rc = options.parse(pipe, "-s", "2010-09-07T13:00:00Z", "-e", "2010-09-08T133000Z");
        assertEquals(0, rc);
        
        Interval interval = options.getInterval();
        DateTime expectedStart = new DateTime(2010, 9, 7, 13, 0, 0, 0).withZoneRetainFields(DateTimeZone.UTC); 
        DateTime expectedEnd = new DateTime(2010, 9, 8, 13, 30, 0, 0).withZoneRetainFields(DateTimeZone.UTC); 
        assertEquals(expectedStart, interval.getStart());
        assertEquals(expectedEnd, interval.getEnd());
    }
    
    @Test
    public void interval4() {
        int rc = options.parse(pipe, "-s", "2010-09-07T13:00:00Z", "-d", "1 hour");
        assertEquals(0, rc);
        
        Interval interval = options.getInterval();
        DateTime expectedStart = new DateTime(2010, 9, 7, 13, 0, 0, 0).withZoneRetainFields(DateTimeZone.UTC); 
        DateTime expectedEnd = expectedStart.plusHours(1);
        assertEquals(expectedStart, interval.getStart());
        assertEquals(expectedEnd, interval.getEnd());
    }

    @Test
    public void intervalBad() {
        int rc = options.parse(pipe, "--start", "2010-09-07T13:00:00Z", "-d", "1 ziphoon");
        assertEquals(0, rc);
        try {
            options.getInterval();
            fail("invalid duration");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Can't parse: 1 ziphoon"));
        }
    }

    @Test
    public void intervalBad2() {
        int rc = options.parse(pipe, "--start", "2010-09-07T13:00:00Z", "-d", "1hour");
        assertEquals(0, rc);
        try {
            options.getInterval();
            fail("invalid duration");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Can't parse: 1hour"));
        }
    }
    // this will require a custom formatter
//    @Test
//    public void intervalShouldWork() {
//        int rc = options.parse(pipe, "-s", "2010-09-07T13:00:00Z", "--duration", "1h");
//        assertEquals(0, rc);
//        
//        Interval interval = options.getInterval();
//        DateTime expectedStart = new DateTime(2010, 9, 7, 13, 0, 0, 0).withZoneRetainFields(DateTimeZone.UTC); 
//        DateTime expectedEnd = expectedStart.plusHours(1);
//        assertEquals(expectedStart, interval.getStart());
//        assertEquals(expectedEnd, interval.getEnd());
//    }

}
