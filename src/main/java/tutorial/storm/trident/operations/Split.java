package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import com.google.common.base.Splitter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Split extends BaseFunction {
    private final String on;
    private Splitter splitter;

    public Split(String on) {
        this.on = on;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if(splitter==null){
            splitter = Splitter.on(on);
        }

        String string = tuple.getString(0);
        for (String spilt : splitter.split(string)) {
            collector.emit(new Values(spilt));
        }
    }
}
