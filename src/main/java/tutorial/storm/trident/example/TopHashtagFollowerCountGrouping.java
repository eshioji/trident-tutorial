package tutorial.storm.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.*;
import tutorial.storm.trident.state.HazelCastStateFactory;
import tutorial.storm.trident.testutil.TestUtils;

import java.io.IOException;

/**
* @author Enno Shioji (enno.shioji@peerindex.com)
*/
public class TopHashtagFollowerCountGrouping {

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout) throws IOException {

        TridentTopology topology = new TridentTopology();
        TridentState count =
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new ParseTweet(), new Fields("text", "content", "user"))
                .project(new Fields("content", "user"))
                .each(new Fields("content"), new OnlyHashtags())
                .each(new Fields("user"), new OnlyEnglish())
                .each(new Fields("content", "user"), new ExtractFollowerClassAndContentName(), new Fields("followerClass", "contentName"))
                .parallelismHint(3)
                .groupBy(new Fields("followerClass", "contentName"))
                .persistentAggregate(new HazelCastStateFactory(), new Count(), new Fields("count"))
                .parallelismHint(3)
        ;


        topology
                .newDRPCStream("hashtag_count")
                .each(new Constants<String>("< 100", "< 10K", "< 100K", ">= 100K"), new Fields("followerClass"))
                .stateQuery(count, new Fields("followerClass", "args"), new MapGet(), new Fields("count"))
        ;

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setNumWorkers(6);

        if (args.length == 2) {
            // Ready & submit the topology
            String name = args[0];
            BrokerHosts hosts = new ZkHosts(args[1]);
            TransactionalTridentKafkaSpout kafkaSpout = TestUtils.testTweetSpout(hosts);

            StormSubmitter.submitTopology(name, conf, buildTopology(kafkaSpout));

        }else{
            System.err.println("<topologyName> <zookeeperHost>");
        }

    }



}
