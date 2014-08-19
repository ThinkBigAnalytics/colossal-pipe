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
package colossal.pipe.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import colossal.pipe.*;
import colossal.util.TimeShard;

public class SamplePipe extends Configured implements Tool {

    @SuppressWarnings({ "unchecked", "unused" })
    @Override
    public int run(String[] args) throws Exception {
        ColPipe pipe = new ColPipe(getClass());

        /* Parse options - just use the standard options - input and output location, time window, etc. */
        BaseOptions o = new BaseOptions();
        int result = o.parse(pipe, args);
        if (result != 0)
            return result;

        String hrPath = TimeShard.makePath(o.getStart());

        ColFile<LogRec> log = ColFile.of(LogRec.class).at("data/sample/logs/"+hrPath).jsonFormat();
        ColFile<LogRec> cleanLog = ColFile.of(LogRec.class).at("/tmp/conv/cleanlogs/"+hrPath).jsonFormat();
        ColPhase clean = new ColPhase().reads(log).writes(cleanLog).map(LogClean.class).groupBy("cookie").sortBy("ts")
                .reduce(BotFilter.class).set("botLimit", "500");

        ColFile<LogRec> conversions = ColFile.of(LogRec.class).at("/tmp/conv/conversions/"+hrPath).jsonFormat();
        ColPhase convert = new ColPhase().reads(cleanLog).writes(conversions).groupBy("cookie").sortBy("ts")
                .reduce(ConversionCount.class);
        
        pipe.produces(conversions);
        

        if (Boolean.TRUE.equals(o.forceRebuild)) pipe.forceRebuild();
        pipe.execute();
        return 0;        
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SamplePipe(), args);
        System.exit(res);
    }
}
