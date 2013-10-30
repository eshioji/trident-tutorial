package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import tutorial.storm.trident.operations.RegexFilter;
import tutorial.storm.trident.operations.StringCounter;
import tutorial.storm.trident.operations.DebugFilter;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;

import java.util.Map;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Lesson02_AdvancedPrimitives {
    private static final Logger log = LoggerFactory.getLogger(Lesson02_AdvancedPrimitives.class);

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_DEBUG,true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("advanced_primitives", conf, advancedPrimitives(new FakeTweetsBatchSpout(1000)));
    }

    private static StormTopology advancedPrimitives(FakeTweetsBatchSpout spout) {

        TridentTopology topology = new TridentTopology();

        // We have seen how to use groupBy, but you can use a more low-level form of aggregation as well
        // This example keeps track of counts, but this time it aggregates the result into a hash map
//        topology
//                .newStream("aggregation", spout)
//                .aggregate(new Fields("location"), new StringCounter(), new Fields("aggregated_result"))
//                .parallelismHint(3)
//                ;

        // We can affect how the processing is parallelized by using "partitioning"
//        topology
//                .newStream("aggregation", spout)
//                .partitionBy(new Fields("location"))
//                .partitionAggregate(new Fields("location"), new StringCounter(), new Fields("aggregated_result"))
//                .parallelismHint(3)
//        ;

        // In the former, a given location can be aggregated anywhere. In the later, all input with a given location
        // are routed to the same instance of aggregation
        // This is potentially more efficient, but note if your input is skewed, the workload can become skewed, too

        // Here is an example how to deal with such skews
        topology
                .newStream("aggregation", spout)
                .partitionBy(new Fields("location"))
                .partitionAggregate(new Fields("location"), new StringCounter(), new Fields("count_map"))
                .each(new Fields("count_map"), new HasSpain())
                .each(new Fields("count_map"), new DebugFilter("AFTER-HAS-SPAIN"))
                .each(new Fields("count_map"), new UpperCase(), new Fields("count_uppercase_map"))
                .parallelismHint(3)
        ;


//        // Here is a basic example from the previous lesson
//        topology
//                .newStream("parallel", spout)
//                .each(new Fields("actor"), new DebugFilter(new RegexFilter("pere")))
//                .parallelismHint(5)
//                .each(new Fields("text", "actor"), new DebugFilter());
//
//        // We can affect how processing is parallelized by using "partitioning"
//        topology
//                .newStream("parallel_and_partitioned", spout)
//                .partitionBy(new Fields("actor"))
//                .each(new Fields("actor"), new DebugFilter(new RegexFilter("pere")))
//                .parallelismHint(5)
//                .each(new Fields("text", "actor"), new DebugFilter());
//
//        // Now, only one partition is filtering all tuple with the actor "pere" will go to the same partition
//        // If we remove the partitionBy, we get the previous behavior
//
//        // Here is a more useful example
//        // The aggregation for each batch is executed in a random partition as can be seen:
//        topology
//                .newStream("aggregation", spout)
//                .aggregate(new Fields("location"), new StringCounter(), new Fields("aggregated_result"))
//                .parallelismHint(5)
//                .each(new Fields("aggregated_result"), new DebugFilter());

//        // The partitionAggregate on the other hand only executes the aggregator within one partition's part of the batch.
//        // Let's debug that with TridentOperationContext . partitionIndex !
//        topology
//                .newStream("partial_aggregation", spout)
//                .parallelismHint(1)
//                .shuffle()
//                .partitionAggregate(new Fields("location"), new StringCounter(), new Fields("aggregated_result"))
//                .parallelismHint(6)
//                .each(new Fields("aggregated_result"), new DebugFilter());
//
//        // (See what happens when we change the Spout batch size / parallelism)
//
//        // A useful primitive is groupBy.
//        // It splits the stream into groups so that aggregations only ocurr within a group.
//        // Because now we are grouping, the aggregation function can be much simpler (Count())
//        // We don't need to use HashMaps anymore.
//        topology
//                .newStream("aggregation", spout)
//                .parallelismHint(1)
//                .groupBy(new Fields("location"))
//                .aggregate(new Fields("location"), new Count(), new Fields("count"))
//                .parallelismHint(5)
//                .each(new Fields("location", "count"), new DebugFilter());
//
//        // EXERCISE: Use Functions and Aggregators to parallelize per-hashtag counts.
//        // Step by step: 1) Obtain and select hashtags, 2) Write the Aggregator.
//
//        // Bonus : "Trending" hashtags.
        return topology.build();
    }
    public static class HasSpain extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            Map<String,Integer> val = (Map<String,Integer>)tuple.get(0);
            return val != null && val.keySet().contains("Spain");
        }
    }


    private static class UpperCase extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Map<String,Integer> val = (Map<String,Integer>)tuple.get(0);
            return val != null && val.keySet().contains("Spain");
        }
    }
}
