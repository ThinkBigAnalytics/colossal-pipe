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
