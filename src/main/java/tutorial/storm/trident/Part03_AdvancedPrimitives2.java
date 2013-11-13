package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableList;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.DebugFilter;
import tutorial.storm.trident.operations.DivideAsDouble;

import java.io.IOException;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Part03_AdvancedPrimitives2 {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_DEBUG,true);
        LocalCluster cluster = new LocalCluster();

        // This time we use a "FeederBatchSpout", a spout designed for testing.
        FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("name", "city", "age"));
        cluster.submitTopology("advanced_primitives", conf, advancedPrimitives(testSpout));

        // You can "hand feed" values to the topology by using this spout
        testSpout.feed(ImmutableList.of(new Values("rose", "Shanghai", 32), new Values("mary", "Shanghai", 51), new Values("pere", "Jakarta", 65), new Values("Tom", "Jakarta", 10)));

    }

    private static StormTopology advancedPrimitives(FeederBatchSpout spout) throws IOException {

        TridentTopology topology = new TridentTopology();

        // What if we want more than one aggregation? For that, we can use "chained" aggregations.
        // Note how we calculate count and sum.
        // The aggregated values can then be processed further, in this case into mean
        topology
                .newStream("aggregation", spout)
                .groupBy(new Fields("city"))
                .chainedAgg()
                .aggregate(new Count(), new Fields("count"))
                .aggregate(new Fields("age"), new Sum(), new Fields("age_sum"))
                .chainEnd()
                .each(new Fields("age_sum", "count"), new DivideAsDouble(), new Fields("mean_age"))
                .each(new Fields("city", "mean_age"), new DebugFilter())
        ;

        // What if we want to persist results of an aggregation, but want to further process these
        // results? You can use "newValuesStream" for that
        topology
                .newStream("further",spout)
                .groupBy(new Fields("city"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .each(new Fields("city", "count"), new DebugFilter());

        return topology.build();
    }

}
