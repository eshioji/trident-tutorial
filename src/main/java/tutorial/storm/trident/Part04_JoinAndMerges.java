package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.DebugFilter;
import tutorial.storm.trident.operations.DivideAsDouble;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;

import java.io.IOException;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Part04_JoinAndMerges {

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

        Stream cityAndAge =
                topology
                        .newStream("city_and_age", spout);

        Stream fakeTweets =
                topology
                        .newStream("fake_tweets", new FakeTweetsBatchSpout());

        topology
                .join(fakeTweets,new Fields("actor"), cityAndAge, new Fields("name"), new Fields("text"))
                .each(new Fields("text"), new DebugFilter())
        ;

        return topology.build();

    }
}
