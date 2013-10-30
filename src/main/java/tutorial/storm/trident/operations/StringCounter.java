package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import org.apache.commons.collections.MapUtils;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple Aggregator that produces a hashmap of key, counts.
 */
public class StringCounter implements Aggregator<Map<String, Integer>> {

    private int partitionId;
    private int numPartitions;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionId = context.getPartitionIndex();
        this.numPartitions = context.numPartitions();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        return new HashMap<String, Integer>();
    }

    @Override
    public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
        String loc = tuple.getString(0);
        val.put(loc, MapUtils.getInteger(val, loc, 0) + 1);
    }

    @Override
    public void complete(Map<String, Integer> val, TridentCollector collector) {
        System.err.println(String.format("Partition %s out ot %s partitions aggregated:%s", partitionId, numPartitions, val));
        collector.emit(new Values(val));
    }
}

