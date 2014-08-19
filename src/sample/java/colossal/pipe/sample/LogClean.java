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

import colossal.pipe.BaseMapper;
import colossal.pipe.ColContext;

public class LogClean extends BaseMapper<LogRec,LogRec> {

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null) {
            //use conf.get
        }
    }
    
    @Override
    public void map(LogRec in, LogRec out, ColContext<LogRec> context) {
        if (!isDirty(in)) {
            context.write(in);
        }
    }

    private boolean isDirty(LogRec in) {
        return false;
    }


}
