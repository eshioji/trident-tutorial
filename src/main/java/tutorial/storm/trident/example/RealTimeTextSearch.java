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
import tutorial.storm.trident.example.elasticsearch.ElasticSearchStateFactory;
import tutorial.storm.trident.example.elasticsearch.ElasticSearchStateUpdater;
import tutorial.storm.trident.example.elasticsearch.TweetQuery;
import tutorial.storm.trident.operations.ParseTweet;
import tutorial.storm.trident.operations.Print;
import tutorial.storm.trident.operations.Split;
import tutorial.storm.trident.operations.TweetIdExtractor;
import tutorial.storm.trident.testutil.TestUtils;

import java.io.IOException;

/**
* This topology shows how to index on a search engine (ElasticSearch) a stream made of
* tweets and how to query it, using DRPC calls. This example should be intended as
* an example of {@link TridentState} custom implementation.
*
* @author Davide Palmisano (davide.palmisano@peerindex.com)
*/
public class RealTimeTextSearch {

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout)
            throws IOException {

        TridentTopology topology = new TridentTopology();
        /**
         * As a first thing, we need a stream of tweets which we can parse and extract
         * only the text and its id. As you will notice, we're going to store the stream
         * using the {@link ElasticSearchState} implementation using its {@link StateUpdater}.
         * Check their implementations for details.
         */
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new ParseTweet(), new Fields("text", "content", "user"))
                .each(new Fields("text", "content"), new TweetIdExtractor(), new Fields("tweetId"))
                .project(new Fields("tweetId", "text"))
                .each(new Fields("tweetId", "text"), new Print())
                .partitionPersist(new ElasticSearchStateFactory(), new Fields("tweetId", "text"), new ElasticSearchStateUpdater());

        /**
         * Now we need a DRPC stream to query the state where the tweets are stored.
         * To do that, as shown below, we need an implementation of {@link QueryFunction} to
         * access our {@link ElasticSearchState}.
         */
        TridentState elasticSearchState = topology.newStaticState(new ElasticSearchStateFactory());
        topology
                .newDRPCStream("search")
                .each(new Fields("args"), new Split(" "), new Fields("keywords")) // let's split the arguments
                .stateQuery(elasticSearchState, new Fields("keywords"), new TweetQuery(), new Fields("ids")) // and pass them as query parameters
                .project(new Fields("ids"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();


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
