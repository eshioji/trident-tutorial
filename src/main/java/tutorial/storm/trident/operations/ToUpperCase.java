package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * Dummy function that just emits the uppercased text.
 */
@SuppressWarnings("serial")
public class ToUpperCase extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(tuple.getString(0).toUpperCase()));
    }
}
