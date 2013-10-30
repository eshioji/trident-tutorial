package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class WithDefaultValue<T extends Serializable> extends BaseFunction{
    private final T t;

    public WithDefaultValue(T t) {
        this.t = t;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if(tuple.get(0) == null){
            collector.emit(new Values(t));
        }else{
            collector.emit(new Values(tuple.get(0)));
        }
    }
}
