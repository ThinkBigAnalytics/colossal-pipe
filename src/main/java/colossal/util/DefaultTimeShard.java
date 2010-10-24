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
package colossal.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DefaultTimeShard implements TimeShard {

    private static DateTimeFormatter pathFormatter = DateTimeFormat.forPattern("y/MM/dd/HH").withZone(DateTimeZone.UTC);
    private static DateTimeFormatter tsFormatter = DateTimeFormat.forPattern("y-M-d'T'HHmmssZ").withZone(DateTimeZone.UTC);

    public String makePath(DateTime ts) {
        return pathFormatter.print(ts);
    }

    public String makeTimestamp(DateTime start, DateTime end) {
        // ISO uses / for duration separation - that doesn't work for a file path so we use .
        return ".ts." + tsFormatter.print(start) + "." + tsFormatter.print(end);
    }

}
