package tutorial.storm.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
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
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.*;
import tutorial.storm.trident.testutil.SampleTweet;
import tutorial.storm.trident.testutil.TweetIngestor;

import java.io.IOException;

/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class TopHashtagAnalysis {

    public static StormTopology buildTopology(LocalDRPC drpc, TransactionalTridentKafkaSpout spout) throws IOException {

        TridentTopology topology = new TridentTopology();
        TridentState count =
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new ParseTweet(), new Fields("text", "content", "user"))
                .project(new Fields("content", "user"))
                .each(new Fields("content"), new OnlyHashtags())
                .each(new Fields("user"), new OnlyEnglish())
                .each(new Fields("content", "user"), new ExtractFollowerClassAndContentName(), new Fields("followerClass", "contentName"))
                .groupBy(new Fields("followerClass", "contentName"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        ;


        topology
                .newDRPCStream("hashtag_count", drpc)
                .stateQuery(count, new TupleCollectionGet(), new Fields("followerClass", "contentName"))
                .stateQuery(count, new Fields("followerClass", "contentName"), new MapGet(), new Fields("count"))
                .groupBy(new Fields("followerClass"))
                .aggregate(new Fields("contentName", "count"), new FirstN.FirstNSortedAgg(1,"count", true), new Fields("contentName", "count"))
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
            System.out.println(drpc.execute("hashtag_count",""));
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
