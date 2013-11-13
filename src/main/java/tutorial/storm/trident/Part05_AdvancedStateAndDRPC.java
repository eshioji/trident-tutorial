package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableList;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.DivideAsDouble;
import tutorial.storm.trident.operations.Print;
import tutorial.storm.trident.testutil.FakeTweetGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Part05_AdvancedStateAndDRPC {
    public static void main(String[] args) throws Exception {
        FakeTweetGenerator fakeTweets = new FakeTweetGenerator();
        FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("id", "text", "actor", "location", "date"));

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("external_state_drpc", conf, externalState(drpc, testSpout));

        // You can use FeederBatchSpout to feed know values to the topology. Very useful for tests.
        testSpout.feed(fakeTweets.getNextTweetTuples("ted"));
        testSpout.feed(fakeTweets.getNextTweetTuples("ted"));
        testSpout.feed(fakeTweets.getNextTweetTuples("mary"));
        testSpout.feed(fakeTweets.getNextTweetTuples("jason"));

        System.out.println(drpc.execute("age_stats", ""));
        System.out.println("OK");
    }


    private static StormTopology externalState(LocalDRPC drpc, FeederBatchSpout spout) {
        TridentTopology topology = new TridentTopology();

        // You can reference existing data sources as well.
        // Here we are mocking up a "database"
        StateFactory stateFactory = new StateFactory() {
            @Override
            public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
                MemoryMapState<Integer> name_to_age = new MemoryMapState<Integer>("name_to_age");
                // This is a bit hard to read but it's just pre-populating the state
                List<List<Object>> keys = getKeys("ted", "mary", "jason", "tom", "chuck");
                name_to_age.multiPut(keys, ImmutableList.of(32, 21, 45, 52, 18));
                return name_to_age;
            }
        };
        TridentState nameToAge =
                topology.newStaticState(stateFactory);

        // Let's setup another state that keeps track of actor's appearance counts per location
        TridentState countState =
                topology
                        .newStream("spout", spout)
                        .groupBy(new Fields("actor","location"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        // Now, let's calculate the average age of actors seen
        topology
                .newDRPCStream("age_stats", drpc)
                .stateQuery(countState, new TupleCollectionGet(), new Fields("actor", "location"))
                .stateQuery(nameToAge, new Fields("actor"), new MapGet(), new Fields("age"))
                .each(new Fields("actor","location","age"), new Print())
                .groupBy(new Fields("location"))
                .chainedAgg()
                .aggregate(new Count(), new Fields("count"))
                .aggregate(new Fields("age"), new Sum(), new Fields("sum"))
                .chainEnd()
                .each(new Fields("sum", "count"), new DivideAsDouble(), new Fields("avg"))
                .project(new Fields("location", "count", "avg"))
        ;

        return topology.build();
    }

    private static List<List<Object>> getKeys(String... names) {
        List<List<Object>> ret = new ArrayList<List<Object>>();
        for (String name : names) {
            ret.add(ImmutableList.<Object>of(name));
        }
        return ret;
    }



}
