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

import static org.kohsuke.args4j.ExampleMode.ALL;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.joda.time.*;
import org.joda.time.base.BaseDateTime;
import org.joda.time.format.*;
import org.kohsuke.args4j.*;


public class BaseOptions {
    @Option(name = "-i", aliases = { "--input" }, usage = "Input root")
    public String input;

    @Option(name = "-o", aliases = { "--output" }, usage = "Output root")
    public String output;

    // will support http://en.wikipedia.org/wiki/ISO_8601 - Time Interval formats
    @Option(name = "-time", aliases = { "--interval", "--window" }, usage="Time Range, e.g., -time 2007-03-01T13:00:00Z/2007-03-01T14:00:00Z")
    public String time;

    // will support http://en.wikipedia.org/wiki/ISO_8601 - Time Interval formats
    @Option(name = "-s", aliases = { "--start", "--begin" }, usage="Start Time, e.g., -s 2007-03-01T13:00:00Z")
    public String start;

    // will support http://en.wikipedia.org/wiki/ISO_8601 - Time Interval formats
    @Option(name = "-e", aliases = { "--end" }, usage="End Time, e.g., -e 2007-03-01T14:00:00Z")
    public String end;
    
    // will support http://en.wikipedia.org/wiki/ISO_8601 - Time Interval formats
    @Option(name = "-d", aliases = { "--duration", "--length" }, usage="Duration, e.g., -d \"1 hour\"")
    public String duration;
    
    @Option(name = "-f", aliases = { "--force", "--rebuild" }, usage="Force re-computing (ignore existing files)")
    public boolean forceRebuild;

    @Option(name = "--dry-run", usage="Print plan, but don't execute it")
    public boolean dryRun;

    @Option(name = "--work-dir", aliases = { "-w" }, usage="Specify work directory")
    public String workDir;
    
    public int parse(ColPipe pipeline, String... args) {
        JobConf conf = pipeline.getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(otherArgs);
        }
        catch (CmdLineException e) {
            String jobName = pipeline.getName();
            if (jobName == null) {
                jobName = "yourJob";
            }
            String jarName = conf.getJar();
            if (jarName == null) {
                jarName = "yourJar";
            }
            String cmd = "hadoop jar "+jarName+" "+jobName;
            System.err.println(e.getMessage());
            System.err.println("Usage: "+cmd+" [options...] arguments...");            
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: "+cmd+" "+ parser.printExample(ALL));
            return 1;
        }
        return 0;
    }
    
    // constant Z means UTC
    private static DateTimeFormatter timeFormatters[] = {
        DateTimeFormat.forPattern("y-M-d'T'HHmmss'Z'").withZone(DateTimeZone.UTC),
        DateTimeFormat.forPattern("y-M-d'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC),
        DateTimeFormat.forPattern("y-M-d'T'HHmmss"),
        DateTimeFormat.forPattern("y-M-d'T'HH:mm:ss")
    };
    
    public Interval getInterval() {
        if (time != null) {
            String[] parts = time.split("/");
            if (parts.length != 2) {
                System.err.println("Can't parse time: use a format like 2007-03-01T13:00:00Z/2007-03-01T14:00:00Z");
            } else {
                DateTime s = parseDateTime(parts[0]);
                DateTime e = parseDateTime(parts[1]);
                return new Interval(s, e);
            }
        } else if (start != null) {
            DateTime s = parseDateTime(start);
            if (end != null) {
                DateTime e = parseDateTime(end);
                return new Interval(s, e); 
            } else if (duration != null) {
                Period p = parseDuration();
                return new Interval(s, p);
            }
        } else if (end != null && duration != null) {
            DateTime e = parseDateTime(end);
            Period p = parseDuration();
            return new Interval(e.minus(p), p);
        }
        return null;
    }

    private DateTime parseDateTime(String textTime) {
        for (DateTimeFormatter f : timeFormatters) {
            try {
                return f.parseDateTime(textTime);
            } catch (IllegalArgumentException iae) {
                // skip to next
            }
        }
        throw new IllegalArgumentException("Can't parse: "+textTime);
    }

    private Period parseDuration() {
        PeriodFormatter[] toTry = {
                PeriodFormat.getDefault(), 
                ISOPeriodFormat.standard(),
                ISOPeriodFormat.alternate(),
                ISOPeriodFormat.alternateExtended(),
                ISOPeriodFormat.alternateExtendedWithWeeks(),
                ISOPeriodFormat.alternateWithWeeks()
        };
        for (PeriodFormatter f : toTry) {
            try {
                return f.parsePeriod(duration);
            } catch (IllegalArgumentException iae) {
                // skip to next
            }
        }
        throw new IllegalArgumentException("Can't parse: "+duration);
    }

    public DateTime getStart() {
        if (start != null)
            return parseDateTime(start);
        return null;
    }

    private static final String DFS_TMP = "/dfs/tmp/";
    
    public String getWorkDir() {
        if (workDir != null) {
            return workDir;
        } else {
            String user = System.getProperty("user.name");
            return DFS_TMP+user;
        }
    }    
}
