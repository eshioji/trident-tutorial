package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;

/**
 * Silly test topology to test the cluster
 *
 * @author Enno Shioji (eshioji@gmail.com)
 */
public class ClusterTestTopology {
    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        // Submits the topology
        String topologyName = args[0];
        conf.setNumWorkers(8); // Our Vagrant environment has 8 workers

        FakeTweetsBatchSpout fakeTweets = new FakeTweetsBatchSpout(10);

        TridentTopology topology = new TridentTopology();
        TridentState countState =
                topology
                        .newStream("spout", fakeTweets)
                        .groupBy(new Fields("actor"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        topology
                .newDRPCStream("count_per_actor")
                .stateQuery(countState, new Fields("args"), new MapGet(), new Fields("count"));

        StormSubmitter.submitTopology(topologyName, conf, topology.build());

    }

}
