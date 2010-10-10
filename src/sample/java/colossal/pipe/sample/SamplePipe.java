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
