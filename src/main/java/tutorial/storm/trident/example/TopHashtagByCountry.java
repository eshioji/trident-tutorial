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
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.ExtractLocation;
import tutorial.storm.trident.operations.OnlyGeo;
import tutorial.storm.trident.operations.OnlyHashtags;
import tutorial.storm.trident.operations.ParseTweet;

import java.io.IOException;

/**
 *
 */
public class TopHashtagByCountry {

    public static StormTopology buildTopology(LocalDRPC drpc, TransactionalTridentKafkaSpout spout) throws IOException {

        TridentTopology topology = new TridentTopology();
        TridentState count =
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new ParseTweet(), new Fields("status", "content", "user"))
                .project(new Fields("content", "user", "status"))
                .each(new Fields("content"), new OnlyHashtags())
                .each(new Fields("status"), new OnlyGeo())
                .each(new Fields("status", "content"), new ExtractLocation(), new Fields("country", "contentName"))
                .groupBy(new Fields("country", "contentName"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        ;


        topology
                .newDRPCStream("location_hashtag_count", drpc)
                .stateQuery(count, new TupleCollectionGet(), new Fields("country", "contentName"))
                .stateQuery(count, new Fields("country", "contentName"), new MapGet(), new Fields("count"))
                .groupBy(new Fields("country"))
                .aggregate(new Fields("contentName", "count"), new FirstN.FirstNSortedAgg(3,"count", true), new Fields("contentName", "count"))
        ;

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 1, "Please specify the test kafka broker host:port");
        String testKafkaBrokerHost = args[0];

        TransactionalTridentKafkaSpout tweetSpout = tweetSpout(testKafkaBrokerHost);

        Config conf = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        // For unit testing
        // FeederBatchSpout feederSpout = new FeederBatchSpout(ImmutableList.of("str"));
        // SampleTweet sampleTweet = new SampleTweet();
        // feederSpout.feed(ImmutableList.of(new Values(sampleTweet.sampleTweet())));

        cluster.submitTopology("hackaton", conf, buildTopology(drpc,tweetSpout));

        while(!Thread.currentThread().isInterrupted()){
            Thread.sleep(500);
            System.out.println(drpc.execute("location_hashtag_count",""));
        }
    }

    private static TransactionalTridentKafkaSpout tweetSpout(String testKafkaBrokerHost) {
        // For locally starting the kafka broker
        // TweetIngestor ingestor = new TweetIngestor("/tmp/kafka", "test", 12000);
        // ingestor.startAndWait();
        KafkaConfig.BrokerHosts hosts = TridentKafkaConfig.StaticHosts.fromHostString(ImmutableList.of(testKafkaBrokerHost), 1);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, "test");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new TransactionalTridentKafkaSpout(config);
    }

}
