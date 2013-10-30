package tutorial.storm.trident.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class DebugFilter extends BaseFilter {
    private int partitionIndex;
    private int numPartitions;
    private final String name;

    public DebugFilter(){
        name = "";
    }
    public DebugFilter(String name){
        this.name = name;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
        this.numPartitions = context.numPartitions();
    }


    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.err.println(String.format("%s::Partition idx: %s out of %s partitions got %s", name, partitionIndex, numPartitions, tuple.toString()));
        return true;
    }
}