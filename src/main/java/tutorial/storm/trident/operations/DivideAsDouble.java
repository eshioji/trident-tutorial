package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import clojure.lang.Numbers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class DivideAsDouble extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Number n1 = (Number)tuple.get(0);
        Number n2 = (Number)tuple.get(1);
        collector.emit(new Values(n1.doubleValue() / n2.doubleValue()));
    }
}
