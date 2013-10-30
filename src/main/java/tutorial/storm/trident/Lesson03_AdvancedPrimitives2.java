package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FeederBatchSpout;
import tutorial.storm.trident.operations.DebugFilter;
import tutorial.storm.trident.operations.DivideAsDouble;
import tutorial.storm.trident.operations.StringCounter;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Lesson03_AdvancedPrimitives2 {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_DEBUG,true);
        LocalCluster cluster = new LocalCluster();

        // This time we use a "FeederBatchSpout", a spout designed for testing.
        FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("name", "city", "age"));
        cluster.submitTopology("advanced_primitives", conf, advancedPrimitives(testSpout));

        // You can "hand feed" values to the topology by using this spout
        testSpout.feed(ImmutableList.of(new Values("John", "Shanghai", 32), new Values("Johnathan", "Shanghai", 51), new Values("Rose", "Jakarta", 65), new Values("Tom", "Jakarta", 10)));

    }

    private static StormTopology advancedPrimitives(FeederBatchSpout spout) {

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

        

        return topology.build();
    }
}
