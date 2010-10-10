package colossal.pipe.sample;

import colossal.pipe.BaseReducer;
import colossal.pipe.ColContext;

public class BotFilter extends BaseReducer<LogRec,LogRec> {

    @Override
    public void reduce(Iterable<LogRec> in, LogRec out, ColContext<LogRec> context) {
        for (LogRec r : in) {
            context.write(r);
        }
    }
    
}
