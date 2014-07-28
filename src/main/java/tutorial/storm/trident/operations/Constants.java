package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * @author Enno Shioji (eshioji@gmail.com)
 */
public class Constants<T> extends BaseFunction {
    private final T[] constants;

    public Constants(T... constants){
        this.constants = constants;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        for (T constant : constants) {
            collector.emit(new Values(constant));
        }

    }
}
