package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import tutorial.storm.trident.operations.StringCounter;
import tutorial.storm.trident.operations.DebugFilter;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;

import java.util.HashMap;
import java.util.Map;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Part02_AdvancedPrimitives1 {
    private static final Logger log = LoggerFactory.getLogger(Part02_AdvancedPrimitives1.class);

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_DEBUG,true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("advanced_primitives", conf, advancedPrimitives(new FakeTweetsBatchSpout(1000)));
        Thread.sleep(30000);
        cluster.shutdown();
    }

    private static StormTopology advancedPrimitives(FakeTweetsBatchSpout spout) {

        TridentTopology topology = new TridentTopology();

        // We have seen how to use groupBy, but you can use a more low-level form of aggregation as well
        // This example keeps track of counts, but this time it aggregates the result into a hash map
        topology
                .newStream("aggregation", spout)
                .aggregate(new Fields("location"), new StringCounter(), new Fields("aggregated_result"))
                .parallelismHint(3)
                ;

        // We can affect how the processing is parallelized by using "partitioning"
        topology
                .newStream("aggregation", spout)
                .partitionBy(new Fields("location"))
                .partitionAggregate(new Fields("location"), new StringCounter(), new Fields("aggregated_result"))
                .parallelismHint(3)
        ;

        // If no partitioning is specified (as in the former), a given location can be aggregated in different
        // aggregators. In the later, all input with a given location are routed to the same instance of aggregation.
        // This means that, more summarization can be done in the later, which would make subsequent processing more
        // efficient. However, note that if your input is skewed, the workload can become skewed, too

        // Here is an example how to deal with such skews
        topology
                .newStream("aggregation", spout)
                .partitionBy(new Fields("location"))
                .partitionAggregate(new Fields("location"), new StringCounter(), new Fields("count_map"))
                .each(new Fields("count_map"), new HasSpain())
                .each(new Fields("count_map"), new DebugFilter("AFTER-HAS-SPAIN"))
                .parallelismHint(3)
                .shuffle()
                .each(new Fields("count_map"), new TimesTen(), new Fields("count_map_times_ten"))
                .each(new Fields("count_map_times_ten"), new DebugFilter("AFTER-TIMES-TEN"))
                .parallelismHint(3)
        ;

        // Without the "shuffle" partitioning, only a single partition will be executing the "TimesTen" function,
        // i.e. the workload will not be distributed. With the "shuffle" partitioning, the skew is corrected and
        // the workload will be distributed again.
        // Note the need for two parallelismHints, as parallelismHints apply downwards up until a partitioning operation

        // There are several other partitioning operations.
        // Here is an example that uses the "global" partitining, which sends all tuples to the same partition
        // This means however, that the processing can't be distributed -- something you want to avoid
        topology
                .newStream("aggregation", spout)
                .global()
                .each(new Fields("actor"), new DebugFilter())
                .parallelismHint(3)
        ;

        // 


        return topology.build();
    }

    public static class HasSpain extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            Map<String,Integer> val = (Map<String,Integer>)tuple.get(0);
            return val != null && val.keySet().contains("Spain");
        }
    }


    private static class TimesTen extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Map<String,Integer> val = (Map<String,Integer>)tuple.get(0);
            Map<String,Integer> ret = new HashMap<String, Integer>();
            for (Map.Entry<String, Integer> e : val.entrySet()) {
                ret.put(e.getKey(), e.getValue() * 10);
            }
            collector.emit(new Values(ret));
        }
    }
}
