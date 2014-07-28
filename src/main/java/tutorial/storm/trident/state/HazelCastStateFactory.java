package tutorial.storm.trident.state;


import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.*;

import java.util.Map;

public class HazelCastStateFactory<T> implements StateFactory {


    @Override
    public MapState<T> makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        IBackingMap im = new HazelCastState<T>(new HazelCastHandler<TransactionalValue<T>>());
        return TransactionalMap.build(im);
    }
}