package tutorial.storm.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.DebugFilter;

import java.io.IOException;

/**
 * Use this skeleton for starting your own topology that uses the Fake tweets generator as data source.
 *
 * @author pere
 */
public class Skeleton {

    public static StormTopology buildTopology(LocalDRPC drpc, TransactionalTridentKafkaSpout spout) throws IOException {

        TridentTopology topology = new TridentTopology();
        topology.newStream("tweets", spout)
                .each(new Fields("str"), new DebugFilter());

        TridentState countState =
        topology
                .newStream("spout", spout)
                .each(new Fields("actor"), new DebugFilter())
                .groupBy(new Fields("actor"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        ;

        topology
                .newDRPCStream("actor_count", drpc)
                .stateQuery(countState, new Fields("args"), new MapGet(), new Fields("count"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 1, "Please specify the test kafka broker host:port");
        String testKafkaBrokerHost = args[0];

        TransactionalTridentKafkaSpout tweetSpout = tweetSpout(testKafkaBrokerHost);

        Config conf = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        FeederBatchSpout spout = new FeederBatchSpout(ImmutableList.of("actor"));

        cluster.submitTopology("hackaton", conf, buildTopology(drpc,spout));

//        spout.feed(new Values(ImmutableList.of("rose")));
//        spout.feed(new Values(ImmutableList.of("rose")));
//        spout.feed(new Values(ImmutableList.of("rose")));
//
//        spout.feed(new Values(ImmutableList.of("fred")));
//        spout.feed(new Values(ImmutableList.of("fred")));
//        spout.feed(new Values(ImmutableList.of("fred")));
//        spout.feed(new Values(ImmutableList.of("fred")));
//
//        spout.feed(new Values(ImmutableList.of("steve")));
//        spout.feed(new Values(ImmutableList.of("steve")));
//
//
//        System.out.println(drpc.execute("actor_count","rose"));

    }

    private static TransactionalTridentKafkaSpout tweetSpout(String testKafkaBrokerHost) {
//        TweetIngestor ingestor = new TweetIngestor("/tmp/kafka", "test", 12000);
//        ingestor.startAndWait();
        KafkaConfig.BrokerHosts hosts = TridentKafkaConfig.StaticHosts.fromHostString(ImmutableList.of(testKafkaBrokerHost), 1);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, "test");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new TransactionalTridentKafkaSpout(config);
    }

}
